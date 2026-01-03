// /api/campaign-markdown-pack/index.js
// Deterministic Markdown Pack Builder (Model A)
// 03-01-2026 v2.4
// Deterministic. No AI. No scoring. No business interpretation.

"use strict";

const crypto = require("crypto");
const { BlobServiceClient } = require("@azure/storage-blob");
const { enqueueTo } = require("../lib/campaign-queue");

const STORAGE = process.env.AzureWebJobsStorage;
const INPUT_CONTAINER = "input";
const RESULTS_CONTAINER = "results";

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function nowISO() {
  return new Date().toISOString();
}

function stripMarkdown(s) {
  return String(s)
    .replace(/^\s*[-*•]\s+/, "")
    .replace(/^\s*\d+\.\s+/, "")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/[*_`]/g, "")
    .trim();
}

async function writeBlobText(container, name, text) {
  const body = String(text || "");
  const blob = container.getBlockBlobClient(name);
  await blob.upload(body, Buffer.byteLength(body), {
    blobHTTPHeaders: { blobContentType: "text/markdown; charset=utf-8" }
  });
}

// ------------------------------------------------------------
// Bucket mapping (canonical)
// ------------------------------------------------------------
function headingToIndustryBucket(h) {
  // Normalise: lower, strip leading numbering like "1." / "6.1"
  const k = String(h || "")
    .toLowerCase()
    .trim()
    .replace(/^\s*\d+(\.\d+)*\s*[\.\)]\s*/, ""); // "6.1 " or "1. " etc.

  // Explicit canonical headings (industry specific template)
  if (k.includes("sector overview")) return "industry_drivers";
  if (k.includes("industry drivers") || k.includes("drivers & trends") || k.includes("drivers and trends"))
    return "industry_drivers";
  if (k.includes("sector risks") || k.includes("risks & constraints") || k.includes("risks and constraints"))
    return "industry_risks";
  if (k.includes("buyer landscape")) return "persona_pressures";
  if (k.includes("business problems") || k.includes("connectivity solutions") || k.includes("matrix"))
    return "persona_pressures";
  if (k.includes("role of connectivity") || k.includes("technology in this sector"))
    return "content_pillars";
  if (k.includes("competitor")) return "competitor_profiles";
  if (k.includes("stats") || k.includes("benchmarks")) return "industry_stats";
  if (k.includes("strategic implications")) return "content_pillars";

  // Keyword fallbacks (for general overview)
  if (k.includes("driver")) return "industry_drivers";
  if (k.includes("risk")) return "industry_risks";
  if (k.includes("persona")) return "persona_pressures";
  if (k.includes("pillar")) return "content_pillars";
  if (k.includes("stat")) return "industry_stats";

  return "industry_other";
}

// Supplier buckets (Tier 2)
function headingToSupplierBucket(h) {
  // Normalise numbering as well (supplier files may use numbered headings)
  const k = String(h || "")
    .toLowerCase()
    .trim()
    .replace(/^\s*\d+(\.\d+)*\s*[\.\)]\s*/, "");

  // Strong matches
  if (k.includes("capabil") || k.includes("capability")) return "supplier_capabilities";
  if (k.includes("strength")) return "supplier_strengths";
  if (k.includes("different") || k.includes("differentiator")) return "supplier_differentiators";

  // Common variants (defensive)
  if (k.includes("why us") || k.includes("why choose")) return "supplier_differentiators";
  if (k.includes("what we do") || k.includes("solutions") || k.includes("services") || k.includes("offerings"))
    return "supplier_capabilities";

  return "supplier_other";
}

function headingToCompetitorBucket(/* h */) {
  // Competitor markdown is always a profile pack, regardless of headings.
  return "competitor_profiles";
}

// ------------------------------------------------------------
// Azure helpers
// ------------------------------------------------------------
async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

async function readBlobText(container, name) {
  const blob = container.getBlockBlobClient(name);
  if (!(await blob.exists())) return null;
  const dl = await blob.download();
  return streamToString(dl.readableStreamBody);
}
// ------------------------------------------------------------
// Deterministic JSON + status helpers (for idempotency + locks)
// ------------------------------------------------------------
function sha1Json(obj) {
  const s = JSON.stringify(obj ?? null);
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function canonicalizeForHash(pack) {
  // IMPORTANT: Never include pack.sha1 in the hash
  // and never include any volatile fields you don’t want to break determinism.
  // If you keep generated_at, it will change the hash if you rebuild the pack.
  // So we MUST exclude it (and any other time-based fields).
  const { sha1, generated_at, ...rest } = pack || {};
  return rest;
}

function sha1OfPack(pack) {
  return sha1Json(canonicalizeForHash(pack));
}

function stableSortBy(arr, fn) {
  if (!Array.isArray(arr)) return [];
  return [...arr].sort((a, b) => {
    const ka = String(fn(a) ?? "");
    const kb = String(fn(b) ?? "");
    return ka.localeCompare(kb) || String(a?.id || "").localeCompare(String(b?.id || ""));
  });
}

async function readJsonIfExists(container, blobPath) {
  try {
    const blob = container.getBlobClient(blobPath);
    const ok = await blob.exists();
    if (!ok) return null;
    const dl = await blob.download();
    const text = await streamToString(dl.readableStreamBody);
    if (!text) return null;
    return JSON.parse(text);
  } catch {
    return null;
  }
}

async function writeJson(container, blobPath, obj) {
  const block = container.getBlockBlobClient(blobPath);
  const json = JSON.stringify(obj, null, 2);
  const data = Buffer.from(json, "utf8");
  await block.uploadData(data, {
    blobHTTPHeaders: { blobContentType: "application/json" }
  });
}

function ensureStatusShape(st) {
  const s = (st && typeof st === "object") ? st : {};
  if (!Array.isArray(s.history)) s.history = [];
  if (!s.markers || typeof s.markers !== "object") s.markers = {};
  return s;
}

async function writeStatusMarker(resultsContainer, basePrefix, patch, note = "") {
  const statusPath = `${basePrefix}status.json`;

  // Read once (best-effort)
  const cur1 = ensureStatusShape(await readJsonIfExists(resultsContainer, statusPath));

  // Apply patch
  const next1 = {
    ...cur1,
    markers: { ...(cur1.markers || {}), ...(patch || {}) }
  };
  if (note) {
    next1.history.push({ at: nowISO(), phase: "markdown_pack", note });
  }

  // Re-read just before write (best-effort merge to reduce race loss)
  const cur2 = ensureStatusShape(await readJsonIfExists(resultsContainer, statusPath));
  const merged = {
    ...cur2,
    markers: { ...(cur2.markers || {}), ...(next1.markers || {}) },
    history: Array.isArray(cur2.history) ? cur2.history : []
  };

  if (note) merged.history.push({ at: nowISO(), phase: "markdown_pack", note });

  await writeJson(resultsContainer, statusPath, merged);
  return merged;
}

// ------------------------------------------------------------
// Markdown parser (deterministic; bullets only)
// ------------------------------------------------------------
function parseMarkdown({ text, source_file, scope }) {
  const lines = String(text || "").split(/\r?\n/);

  let currentHeading = "General";

  function scopeBucket(scope, heading) {
    if (scope === "supplier") return headingToSupplierBucket(heading);
    if (scope === "competitor") return headingToCompetitorBucket(heading);
    return headingToIndustryBucket(heading); // default: industry
  }

  let currentBucket = scopeBucket(scope, currentHeading);

  const out = [];
  const seen = new Set();

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const line = raw.trim();
    if (!line) continue;

    // Headings (## / ### only)
    if (/^#{1,3}\s+/.test(line)) {
      currentHeading = line.replace(/^#{1,3}\s+/, "").trim();
      currentBucket = scopeBucket(scope, currentHeading); // ✅ Correct for all scopes
      continue;
    }

    // Bullets
    if (/^([-*•]|\d+\.)\s+/.test(line)) {
      const textClean = stripMarkdown(line);
      if (!textClean) continue;

      const hash = sha1(`${source_file}|${currentHeading}|${textClean}`);
      if (seen.has(hash)) continue;
      seen.add(hash);

      out.push({
        id: `md_${hash.slice(0, 8)}`,
        text: textClean,
        bucket: currentBucket,
        source_type: "markdown",
        source_file,
        source_heading: currentHeading,
        line_no: i + 1,
        sha1: hash
      });
    }
  }

  return out;
}

function parseSupplierTemplateMarkdown({ text, source_file }) {
  const lines = String(text || "").split(/\r?\n/);

  const out = [];
  const seen = new Set();

  let currentHeading = "General";

  // Track higher-level context so we can embed it into evidence text.
  let currentOffer = null;
  let currentSegment = null;
  let currentUseCase = null;
  let currentDifferentiator = null;
  let currentReason = null;

  function addItem(bucket, heading, rawText, lineNo) {
    const textClean = stripMarkdown(rawText);
    if (!textClean) return;

    const hash = sha1(`${source_file}|${heading}|${bucket}|${textClean}`);
    if (seen.has(hash)) return;
    seen.add(hash);

    out.push({
      id: `md_${hash.slice(0, 8)}`,
      text: textClean,
      bucket,
      source_type: "markdown",
      source_file,
      source_heading: heading,
      line_no: lineNo,
      sha1: hash
    });
  }

  function kv(bucket, heading, key, value, lineNo) {
    const k = stripMarkdown(key).replace(/:\s*$/, "").trim();
    const v = stripMarkdown(value).trim();
    if (!k || !v) return;

    addItem(bucket, heading, `${k}: ${v}`, lineNo);
  }

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const line = raw.trim();
    if (!line) continue;

    // Headings (## / ###)
    if (/^#{2,3}\s+/.test(line)) {
      currentHeading = line.replace(/^#{2,3}\s+/, "").trim();
      continue;
    }

    // Only bullets matter
    if (!/^([-*•]|\d+\.)\s+/.test(line)) continue;

    const bullet = stripMarkdown(line);

    // ------------------------------------------------------------
    // ENTITY DETECTION
    // ------------------------------------------------------------

    // Offer group blocks
    if (/^offer group name:/i.test(bullet)) {
      currentOffer = bullet.replace(/^offer group name:\s*/i, "").trim();
      currentSegment = null;
      currentUseCase = null;
      currentDifferentiator = null;
      currentReason = null;
      // record the offer itself as a capability
      addItem("supplier_capabilities", currentHeading, `Offer group: ${currentOffer}`, i + 1);
      continue;
    }

    // Segment blocks
    if (/^segment:/i.test(bullet)) {
      currentSegment = bullet.replace(/^segment:\s*/i, "").trim();
      currentOffer = null;
      currentUseCase = null;
      currentDifferentiator = null;
      currentReason = null;
      addItem("supplier_capabilities", currentHeading, `Priority segment: ${currentSegment}`, i + 1);
      continue;
    }

    // Use case blocks
    if (/^use case name:/i.test(bullet)) {
      currentUseCase = bullet.replace(/^use case name:\s*/i, "").trim();
      currentOffer = null;
      currentSegment = null;
      currentDifferentiator = null;
      currentReason = null;
      addItem("supplier_capabilities", currentHeading, `Use case: ${currentUseCase}`, i + 1);
      continue;
    }

    // Differentiator blocks
    if (/^differentiator:/i.test(bullet)) {
      currentDifferentiator = bullet.replace(/^differentiator:\s*/i, "").trim();
      currentOffer = null;
      currentSegment = null;
      currentUseCase = null;
      currentReason = null;
      addItem("supplier_differentiators", currentHeading, `Differentiator: ${currentDifferentiator}`, i + 1);
      continue;
    }

    // Right-to-play reason blocks
    if (/^reason:/i.test(bullet) && currentHeading.toLowerCase().includes("right-to-play")) {
      currentReason = bullet.replace(/^reason:\s*/i, "").trim();
      addItem("supplier_differentiators", currentHeading, `Right-to-play: ${currentReason}`, i + 1);
      continue;
    }

    // SWOT Strength blocks
    if (/^strength:/i.test(bullet)) {
      const strength = bullet.replace(/^strength:\s*/i, "").trim();
      addItem("supplier_strengths", currentHeading, `Strength: ${strength}`, i + 1);
      continue;
    }

    // Weakness, Opportunity, Threat are not Tier-2 strengths/diffs
    // (but we can keep them as supplier_other for completeness)
    if (/^(weakness|opportunity|threat):/i.test(bullet)) {
      addItem("supplier_other", currentHeading, bullet, i + 1);
      continue;
    }

    // ------------------------------------------------------------
    // KEY/VALUE EXTRACTION
    // ------------------------------------------------------------
    // Pattern: "Key: Value"
    const m = bullet.match(/^([^:]{2,80}):\s*(.+)$/);
    if (m) {
      const key = m[1];
      const value = m[2];

      const keyLower = key.toLowerCase();

      // Route certain keys to differentiators
      if (
        keyLower.includes("why it matters") ||
        keyLower.includes("evidence") ||
        keyLower.includes("positioning sentence") ||
        keyLower.includes("right-to-play")
      ) {
        kv("supplier_differentiators", currentHeading, key, value, i + 1);
        continue;
      }

      // Route “fit level” / “reason for fit” as capability-context
      if (keyLower.includes("fit level") || keyLower.includes("reason for fit")) {
        kv("supplier_capabilities", currentHeading, key, value, i + 1);
        continue;
      }

      // Default: capability (most supplier template bullets are descriptive facts)
      kv("supplier_capabilities", currentHeading, key, value, i + 1);
      continue;
    }

    // ------------------------------------------------------------
    // FALLBACK: bullet text as-is
    // ------------------------------------------------------------
    // If it’s under Differentiators section, treat as differentiator
    if (String(currentHeading).toLowerCase().includes("differentiator")) {
      addItem("supplier_differentiators", currentHeading, bullet, i + 1);
      continue;
    }

    // If it’s under Strengths section, treat as strength
    if (String(currentHeading).toLowerCase().includes("strength")) {
      addItem("supplier_strengths", currentHeading, bullet, i + 1);
      continue;
    }

    // Otherwise treat as capability
    addItem("supplier_capabilities", currentHeading, bullet, i + 1);
  }

  return out;
}

// ------------------------------------------------------------
// Main handler
// ------------------------------------------------------------
module.exports = async function (context, msg) {
  const log = context.log;

  if (!STORAGE) {
    log.error("[markdown_pack] AzureWebJobsStorage not configured");
    return;
  }

  const {
    runId,
    prefix,
    industry_slug,
    supplier_slug,
    competitor_slugs = [],
    page = "campaign"
  } = msg || {};

  if (!runId || !prefix) {
    log.error("[markdown_pack] missing runId or prefix");
    return;
  }

  const blobSvc = BlobServiceClient.fromConnectionString(STORAGE);
  const input = blobSvc.getContainerClient(INPUT_CONTAINER);
  const results = blobSvc.getContainerClient(RESULTS_CONTAINER);

  const base = prefix.endsWith("/") ? prefix : `${prefix}/`;

  // ---------------------------------------------------------------------------
  // Canonical output schema (MUST match EvidenceDigest expectations)
  // ---------------------------------------------------------------------------
  const pack = {
    schema: "markdown-pack-v2",
    generated_at: nowISO(),
    runId,
    prefix: base,

    source: {
      industry_slug: industry_slug || null,
      supplier_slug: supplier_slug || null,
      competitor_slugs: Array.isArray(competitor_slugs) ? competitor_slugs : []
    },

    // audit + debug
    meta: {
      version: "2026-01-03-v2.4",
      builder: "campaign-markdown-pack",
      deterministic: true
    },

    warnings: [],
    stats: {
      dropped_items: 0,
      dropped_buckets: {},
      total_items: 0,
      by_bucket: {}
    },

    // ✅ Canonical “pillar views” for downstream synthesis compatibility
    // These are NOT new facts; they are a deterministic projection of existing buckets.
    // campaign-pillars can safely read these as primary inputs.
    markdown_pillars: [],   // supplier themes (capabilities / strengths / differentiators)
    industry_pillars: [],   // industry themes (drivers / risks / persona pressures / content pillars)
    competitor_pillars: [], // competitor profiles condensed into pillar items

    // Tier 1 (strategic external truth)
    industry_drivers: [],
    industry_risks: [],
    persona_pressures: [],
    competitor_profiles: [],

    // Tier 2 (supplier strategic context)
    supplier_capabilities: [],
    supplier_strengths: [],
    supplier_differentiators: [],

    // Tier 3 (supporting narrative context)
    content_pillars: [],
    industry_stats: [],

    // Other / catch-all
    industry_other: [],
    supplier_other: []
  };

  // Global dedupe across ALL ingestions (industry + supplier + competitor)
  const seenGlobal = new Set();
  function pushItems(items) {
    if (!Array.isArray(items)) return;

    for (const it of items) {
      const bucket = String(it?.bucket || "").trim();
      if (!bucket) continue;

      // Global dedupe by sha1 (preferred) else by stable composite key
      const key =
        String(it?.sha1 || "") ||
        sha1(`${it?.source_file || ""}|${it?.source_heading || ""}|${it?.text || ""}`);

      if (seenGlobal.has(key)) continue;
      seenGlobal.add(key);

      if (!pack[bucket]) {
        const b = bucket || "UNKNOWN";
        pack.stats.dropped_items += 1;
        pack.stats.dropped_buckets[b] = (pack.stats.dropped_buckets[b] || 0) + 1;

        pack.warnings.push({
          at: nowISO(),
          type: "dropped_item_unknown_bucket",
          bucket,
          source_file: it?.source_file || null,
          source_heading: it?.source_heading || null
        });

        log.warn("[markdown_pack] unknown bucket; item dropped", {
          bucket,
          source_file: it?.source_file,
          source_heading: it?.source_heading
        });
        continue;
      }

      pack[bucket].push(it);
    }
  }

  // ---------------------------------------------------------------------------
  // 1) Industry sources — ALWAYS included (general truth)
  // ---------------------------------------------------------------------------
  try {
    const sourcesPath = `packs/industry/sources.md`;
    const sourcesTxt = await readBlobText(input, sourcesPath);

    if (sourcesTxt) {
      const items = parseMarkdown({
        text: sourcesTxt,
        source_file: `input/${sourcesPath}`,
        scope: "industry"
      });

      pushItems(items);
      log("[markdown_pack] ingested industry sources", {
        path: sourcesPath,
        count: items.length
      });
    } else {
      log.warn("[markdown_pack] industry sources not found (non-fatal)", {
        path: sourcesPath
      });
    }
  } catch (e) {
    log.warn(
      "[markdown_pack] industry sources ingestion failed (non-fatal)",
      String(e?.message || e)
    );
  }

  // ---------------------------------------------------------------------------
  // 2) Specific industry markdown (if selected/dominant)
  // ---------------------------------------------------------------------------
  if (industry_slug) {
    try {
      const industryPath = `packs/industry/${industry_slug}.md`;
      const industryTxt = await readBlobText(input, industryPath);

      if (industryTxt) {
        const items = parseMarkdown({
          text: industryTxt,
          source_file: `input/${industryPath}`,
          scope: "industry"
        });

        pushItems(items);
        log("[markdown_pack] ingested industry pack", {
          industry_slug,
          path: industryPath,
          count: items.length
        });
      } else {
        log.warn("[markdown_pack] industry pack missing (non-fatal)", {
          industry_slug,
          path: industryPath
        });
      }
    } catch (e) {
      log.warn(
        "[markdown_pack] industry ingestion failed (non-fatal)",
        String(e?.message || e)
      );
    }
  }

  // ---------------------------------------------------------------------------
  // 3) Supplier markdown
  // Supports canonical flat-file approach:
  //   packs/suppliers/<supplier_slug>.md   (preferred)
  // with legacy fallback:
  //   packs/supplier/<supplier_slug>.md
  //
  // Also writes canonical profile.md into RESULTS for EvidenceDigest compatibility:
  //   <prefix>/packs/<supplier_slug>/profile.md
  // ---------------------------------------------------------------------------
  if (supplier_slug) {
    try {
      const slug = String(supplier_slug || "").trim().toLowerCase();
      const candidatePaths = [
        `packs/suppliers/${slug}.md`, // ✅ canonical
        `packs/supplier/${slug}.md`   // legacy
      ];

      let foundPath = null;
      let txt = null;

      for (const p of candidatePaths) {
        const t = await readBlobText(input, p);
        if (t) {
          foundPath = p;
          txt = t;
          break;
        }
      }

      if (txt && foundPath) {
        // 1) Parse into markdown_pack (Tier-2 arrays)
        const items = parseSupplierTemplateMarkdown({
          text: txt,
          source_file: `input/${foundPath}`
        });
        pushItems(items);

        // 2) Write canonical supplier profile for EvidenceDigest (Tier-2 supplier_profile)
        const canonicalOut = `${base}packs/${slug}/profile.md`;
        await writeBlobText(results, canonicalOut, txt);

        log("[markdown_pack] ingested supplier pack + wrote canonical profile", {
          supplier_slug: slug,
          input_path: foundPath,
          canonical_out: canonicalOut,
          count: items.length
        });
      } else {
        log.warn("[markdown_pack] supplier pack missing (non-fatal)", {
          supplier_slug,
          tried: candidatePaths
        });
      }
    } catch (e) {
      log.warn("[markdown_pack] supplier ingestion failed (non-fatal)", String(e?.message || e));
    }
  }

  // ---------------------------------------------------------------------------
  // 4) Competitor markdown
  // NOTE: competitors live in packs/supplier/ for repo simplicity (as you had)
  // Scope competitor forces everything into competitor_profiles
  // Deterministic: stable slug order + dedupe
  // ---------------------------------------------------------------------------
  if (Array.isArray(competitor_slugs) && competitor_slugs.length) {
    // Normalise + dedupe + stable sort
    const competitorSlugsStable = Array.from(
      new Set(
        competitor_slugs
          .map((s) => String(s || "").trim().toLowerCase())
          .filter(Boolean)
      )
    ).sort((a, b) => a.localeCompare(b));

    const summary = {
      requested: competitor_slugs.length,
      unique: competitorSlugsStable.length,
      ingested: 0,
      missing: 0,
      failed: 0
    };

    for (const slug of competitorSlugsStable) {
      try {
        const competitorPath = `packs/supplier/${slug}.md`;
        const competitorTxt = await readBlobText(input, competitorPath);

        if (!competitorTxt) {
          summary.missing += 1;
          log("[markdown_pack] competitor profile not found (expected possible)", {
            slug,
            path: competitorPath
          });
          continue;
        }

        const competitorItems = parseMarkdown({
          text: competitorTxt,
          source_file: `input/${competitorPath}`,
          scope: "competitor"
        });

        pushItems(competitorItems);
        summary.ingested += 1;

        log("[markdown_pack] ingested competitor profile", {
          slug,
          path: competitorPath,
          count: Array.isArray(competitorItems) ? competitorItems.length : 0
        });
      } catch (e) {
        summary.failed += 1;
        log.warn("[markdown_pack] competitor ingestion failed (non-fatal)", {
          slug,
          err: String(e?.message || e)
        });
      }
    }

    log("[markdown_pack] competitor ingestion summary", summary);
  }
  // ---------------------------------------------------------------------------
  // Canonical pillar views (deterministic projection)
  // These are derived ONLY from the items already in buckets.
  // ---------------------------------------------------------------------------

  function toPillarItem(it, kind) {
    if (!it || typeof it !== "object") return null;
    const title = String(it.text || "").trim();
    if (!title) return null;

    return {
      id: String(it.id || ""),
      kind, // supplier | industry | competitor
      title,
      summary: "", // intentionally blank; no new facts
      tags: [],
      source: {
        source_file: it.source_file || null,
        source_heading: it.source_heading || null,
        bucket: it.bucket || null,
        sha1: it.sha1 || null,
        line_no: it.line_no || null
      }
    };
  }

  // Supplier pillars: prefer differentiators + strengths + capabilities (in that order)
  const supplierCandidates = []
    .concat(pack.supplier_differentiators || [])
    .concat(pack.supplier_strengths || [])
    .concat(pack.supplier_capabilities || []);

  pack.markdown_pillars = supplierCandidates
    .map(it => toPillarItem(it, "supplier"))
    .filter(Boolean)
    .slice(0, 80);

  // Industry pillars: content_pillars + drivers + risks + persona pressures
  const industryCandidates = []
    .concat(pack.content_pillars || [])
    .concat(pack.industry_drivers || [])
    .concat(pack.industry_risks || [])
    .concat(pack.persona_pressures || []);

  pack.industry_pillars = industryCandidates
    .map(it => toPillarItem(it, "industry"))
    .filter(Boolean)
    .slice(0, 120);

  // Competitor pillars: competitor_profiles only
  pack.competitor_pillars = (pack.competitor_profiles || [])
    .map(it => toPillarItem(it, "competitor"))
    .filter(Boolean)
    .slice(0, 120);

  // ---------------------------------------------------------------------------
  // Deterministic ordering + stats
  // ---------------------------------------------------------------------------
  const bucketKeys = Object.keys(pack).filter((k) => Array.isArray(pack[k]));

  for (const k of bucketKeys) {
    // stable order: by source_file, then line_no, then sha1
    pack[k] = stableSortBy(pack[k], (it) => {
      const sf = String(it?.source_file || "");
      const ln = String(it?.line_no || "").padStart(6, "0");
      const sh = String(it?.sha1 || "");
      return `${sf}::${ln}::${sh}`;
    });
    pack.stats.by_bucket[k] = pack[k].length;
  }

  pack.stats.total_items = bucketKeys.reduce((sum, k) => sum + (pack[k].length || 0), 0);

  // ---------------------------------------------------------------------------
  // Write markdown_pack.json (exactly one artefact)
  // ---------------------------------------------------------------------------
  const outPath = `${base}evidence_v2/markdown_pack.json`;

  // Compute SHA1 of the final pack (after deterministic ordering)
  // NOTE: this excludes generated_at + sha1 itself (canonicalizeForHash)
  const markdownPackSha1 = sha1OfPack(pack);

  // Embed the hash INTO the pack for downstream audit + idempotency checks
  pack.sha1 = markdownPackSha1;


  // Idempotency: if existing artefact has same SHA1, skip rewrite and only ensure markers
  const existingPack = await readJsonIfExists(results, outPath);
  const existingSha1 =
    (existingPack && typeof existingPack?.sha1 === "string")
      ? existingPack.sha1
      : (existingPack ? sha1OfPack(existingPack) : null);

  if (existingSha1 && existingSha1 === markdownPackSha1) {
    log("[markdown_pack] existing markdown_pack.json matches sha1; skipping rewrite", {
      runId,
      path: outPath,
      sha1: markdownPackSha1
    });

    await writeStatusMarker(results, base, {
      markdownPackCompleted: true,
      markdownPackSha1,
      markdownPackSha1Locked: true,     // optional but useful
      markdownPackEnqueued: true,        // if router expects it
      markdownPackPath: "evidence_v2/markdown_pack.json",
      markdownPackTotal: pack.stats.total_items
    }, "markdown_pack: skip (sha1 match)");

  } else {
    const outBlob = results.getBlockBlobClient(outPath);
    const payload = JSON.stringify(pack, null, 2);
    await outBlob.upload(payload, Buffer.byteLength(payload), {
      blobHTTPHeaders: { blobContentType: "application/json" }
    });

    log("[markdown_pack] written", {
      runId,
      path: outPath,
      sha1: markdownPackSha1,
      counts: pack.stats.by_bucket
    });

    await writeStatusMarker(results, base, {
      markdownPackCompleted: true,
      markdownPackSha1: markdownPackSha1,
      markdownPackPath: "evidence_v2/markdown_pack.json",
      markdownPackTotal: pack.stats.total_items
    }, "markdown_pack: completed");
  }

  // ---------------------------------------------------------------------------
  // Hand off to router → aftermarkdown
  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------
  // Hand off to router → aftermarkdown
  // ---------------------------------------------------------------------------
  const routerQueue = process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";

  await enqueueTo(routerQueue, {
    op: "aftermarkdown",
    runId,
    page,
    prefix: base
  });

  log("[markdown_pack] aftermarkdown enqueued", {
    runId,
    queue: routerQueue,
    prefix: base
  });
};
