// /api/campaign-markdown-pack/index.js
// Deterministic Markdown Pack Builder (Model A)
// 30-12-2025 v2.2
//
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

// ------------------------------------------------------------
// Bucket mapping (canonical)
// ------------------------------------------------------------

// Industry buckets (Tier 1 + Tier 3)
function headingToIndustryBucket(h) {
  const k = String(h || "").toLowerCase();

  if (k.includes("driver")) return "industry_drivers";
  if (k.includes("risk")) return "industry_risks";
  if (k.includes("persona")) return "persona_pressures";
  if (k.includes("competitor")) return "competitor_profiles";
  if (k.includes("pillar")) return "content_pillars";
  if (k.includes("stat")) return "industry_stats";

  return "industry_other";
}

// Supplier buckets (Tier 2)
function headingToSupplierBucket(h) {
  const k = String(h || "").toLowerCase();

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
    if (/^#{2,3}\s+/.test(line)) {
      currentHeading = line.replace(/^#{2,3}\s+/, "").trim();
      currentBucket = scopeBucket(scope, currentHeading); // ✅ FIX
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
        sha1: hash,
        created_at: nowISO()
      });
    }
  }

  return out;
}

// ------------------------------------------------------------
// Main handler
// ------------------------------------------------------------

module.exports = async function (context, msg) {
  const log = context.log;

  // IMPORTANT: STORAGE check must happen inside the handler (context is in scope)
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
    source: {
      industry_slug: industry_slug || null,
      supplier_slug: supplier_slug || null,
      competitor_slugs: Array.isArray(competitor_slugs) ? competitor_slugs : []
    },

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

  function pushItems(items) {
    if (!Array.isArray(items)) return;

    for (const it of items) {
      const bucket = String(it?.bucket || "").trim();
      if (!bucket) continue;

      if (!pack[bucket]) {
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
  // Industry sources — ALWAYS included (general truth)
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
  // Specific industry markdown (if selected/dominant)
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
  // Supplier markdown
  // ---------------------------------------------------------------------------
  if (supplier_slug) {
    try {
      const path = `packs/supplier/${supplier_slug}.md`;
      const txt = await readBlobText(input, path);
      if (txt) {
        const items = parseMarkdown({
          text: txt,
          source_file: `input/${path}`,
          scope: "supplier"
        });
        pushItems(items);
        log("[markdown_pack] ingested supplier pack", { supplier_slug, path, count: items.length });
      } else {
        log.warn("[markdown_pack] supplier pack missing (non-fatal)", { supplier_slug, path });
      }
    } catch (e) {
      log.warn("[markdown_pack] supplier ingestion failed (non-fatal)", String(e?.message || e));
    }
  }

  // ---------------------------------------------------------------------------
  // Competitor markdown
  // NOTE: competitors live in packs/supplier/ for repo simplicity
  // --------------------------------------------------------------------------
  if (Array.isArray(competitor_slugs) && competitor_slugs.length) {
    for (const slug of competitor_slugs) {
      try {
        const competitorPath = `packs/supplier/${slug}.md`;
        const competitorTxt = await readBlobText(input, competitorPath);

        if (!competitorTxt) {
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

        log("[markdown_pack] ingested competitor profile", {
          slug,
          path: competitorPath,
          count: competitorItems.length
        });
      } catch (e) {
        log.warn("[markdown_pack] competitor ingestion failed (non-fatal)", {
          slug,
          err: String(e?.message || e)
        });
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Write markdown_pack.json (exactly one artefact)
  // ---------------------------------------------------------------------------
  const outPath = `${base}evidence_v2/markdown_pack.json`;
  const outBlob = results.getBlockBlobClient(outPath);
  const payload = JSON.stringify(pack, null, 2);

  await outBlob.upload(payload, Buffer.byteLength(payload));

  log("[markdown_pack] written", {
    runId,
    path: outPath,
    counts: Object.fromEntries(
      Object.entries(pack)
        .filter(([, v]) => Array.isArray(v))
        .map(([k, v]) => [k, v.length])
    )
  });

  // ---------------------------------------------------------------------------
  // Hand off to router → aftermarkdown
  // ---------------------------------------------------------------------------
  await enqueueTo(process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs", {
    op: "aftermarkdown",
    runId,
    page,
    prefix: base
  });

  log("[markdown_pack] aftermarkdown enqueued", { runId, prefix: base });
};
