// /api/campaign-pillars/index.js 02-01-2026 v1
// -----------------------------------------------------------------------------
// Phase 1C — Pillars Synthesis (Option A)
//
// Purpose:
//   Produce content_pillars.json deterministically from:
//     - evidence_v2/markdown_pack.json (contains markdown_pillars + industry_pillars)
//     - evidence.json (claims only)
//
// Output:
//   - content_pillars.json  (canonical activation/messaging truth)
//   - status markers: pillarsSynthCompleted, contentPillarsSha1, markdownPackSha1, evidenceSha1
//
// Doctrine enforcement:
//   - Must NOT invent facts.
//   - Every proof point MUST have evidence_claim_ids.
//   - Must be fully traceable back to markdown_pillars and/or industry_pillars + evidence.
//
// Idempotency:
//   - Will not rewrite content_pillars.json if it already exists and hashes match.
//   - Uses status markers to avoid duplicate enqueue.
//
// Handoff:
//   Enqueues router: op=afterpillars
// -----------------------------------------------------------------------------

"use strict";

const crypto = require("crypto");
const { enqueueTo } = require("../lib/campaign-queue");
const { nowIso } = require("../shared/utils");
const { updateStatus } = require("../shared/status");

const {
  getResultsContainerClient,
  getJson,
  putJson
} = require("../shared/storage");

const ROUTER_QUEUE_NAME = process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";
const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------
function parseQueueItem(queueItem) {
  if (!queueItem) return {};
  if (typeof queueItem === "string") {
    try { return JSON.parse(queueItem); } catch { return {}; }
  }
  return (queueItem && typeof queueItem === "object") ? queueItem : {};
}

function normPrefix(prefix) {
  let p = String(prefix || "").trim();
  if (!p) return "";
  if (p.startsWith(`${RESULTS_CONTAINER}/`)) {
    p = p.slice(`${RESULTS_CONTAINER}/`.length);
  }
  p = p.replace(/^\/+/, "");
  if (!p.endsWith("/")) p += "/";
  return p;
}

function sha1Json(obj) {
  const s = JSON.stringify(obj ?? null);
  return crypto.createHash("sha1").update(s).digest("hex");
}

function safeArray(v) {
  return Array.isArray(v) ? v : [];
}

function stableString(v) {
  return String(v ?? "").trim();
}

function makePillarIdFactory(prefix) {
  const seed = crypto.createHash("sha1").update(String(prefix)).digest("hex").slice(0, 8);
  let i = 0;
  return () => `PIL-${seed}-${String(++i).padStart(3, "0")}`;
}

function ensureProvenance(obj) {
  if (!obj || typeof obj !== "object") return obj;
  if (!obj.source || typeof obj.source !== "object") obj.source = {};
  if (!Array.isArray(obj.source.markdown_pillar_refs)) obj.source.markdown_pillar_refs = [];
  if (!Array.isArray(obj.source.industry_pillar_refs)) obj.source.industry_pillar_refs = [];
  return obj;
}

function selectEvidenceByIds(evidenceClaims, ids) {
  const byId = new Map();
  for (const c of safeArray(evidenceClaims)) {
    const id = stableString(c?.claim_id);
    if (id) byId.set(id, c);
  }
  const out = [];
  for (const id of safeArray(ids)) {
    const k = stableString(id);
    if (k && byId.has(k)) out.push(byId.get(k));
  }
  return out;
}

// -----------------------------------------------------------------------------
// Deterministic synthesis strategy (NON-LLM)
//
// We do NOT "invent" content pillars. We only build them from:
//   - markdown_pillars + industry_pillars (structured truths)
//   - and attach supporting proof points from evidence claims.
//
// This is deterministic and auditable.
//
// This function deliberately aims for:
//   - 6–10 pillars max
//   - each pillar has:
//       title, value_prop, audience, why_it_matters
//       proof_points[] with evidence_claim_ids[]
//       constraints[] with provenance
// -----------------------------------------------------------------------------

function buildContentPillars({ markdownPack, evidenceBundle, input }) {
  const claims = safeArray(evidenceBundle?.claims || evidenceBundle?.evidence?.claims || evidenceBundle?.items || []);

  // Expect markdown_pack.json to contain:
  // - markdown_pillars[] (supplier)
  // - industry_pillars[] (industry)
  // We accept fallbacks for legacy keys but DO NOT create new facts.
  const supplierPillars = safeArray(markdownPack?.markdown_pillars || markdownPack?.supplier_pillars || []);
  const industryPillars = safeArray(markdownPack?.industry_pillars || markdownPack?.industry_truth || []);

  const pillarId = makePillarIdFactory(markdownPack?.sha1 || markdownPack?._sha1 || (input?.prefix || ""));

  // ---------------------------------------------------------------------------
  // 1) Build candidate “themes” by combining supplier + industry topics
  // ---------------------------------------------------------------------------
  // We build a small set of deterministic “pillar frames”:
  //   - Supplier differentiators (from markdown_pillars)
  //   - Supplier capabilities (from markdown_pillars)
  //   - Industry drivers/pains/buying triggers (from industry_pillars)
  //
  // NOTE: Since we do not know your exact pillar structure, we treat each pillar item as:
  //   { id, title, summary, tags, constraints, proof_refs, ... }
  // and operate on title/summary/tags only.
  // ---------------------------------------------------------------------------

  function normP(p, kind) {
    if (!p || typeof p !== "object") return null;
    const id = stableString(p.id || p.pillar_id || p.key || "");
    const title = stableString(p.title || p.heading || p.name || "");
    const summary = stableString(p.summary || p.text || p.description || p.body || "");
    const tags = safeArray(p.tags).map(stableString).filter(Boolean);

    if (!title && !summary) return null;

    return {
      kind,
      id: id || `${kind}:${crypto.createHash("sha1").update(title + "||" + summary).digest("hex").slice(0, 10)}`,
      title: title || "(untitled)",
      summary,
      tags,
      raw: p
    };
  }

  const S = supplierPillars.map(x => normP(x, "supplier")).filter(Boolean);
  const I = industryPillars.map(x => normP(x, "industry")).filter(Boolean);

  // Deterministic selection: first N pillars ordered by stable key
  function stableKey(p) {
    return `${p.kind}:${p.id}:${p.title}`.toLowerCase();
  }
  S.sort((a, b) => stableKey(a).localeCompare(stableKey(b)));
  I.sort((a, b) => stableKey(a).localeCompare(stableKey(b)));

  // cap inputs so we don’t generate giant packs
  const S_cap = S.slice(0, 18);
  const I_cap = I.slice(0, 18);

  // ---------------------------------------------------------------------------
  // 2) Evidence linking: attach relevant evidence claims based on simple matching
  // ---------------------------------------------------------------------------
  // We do NOT infer facts. We only attach evidence claim IDs that already exist.
  // Strategy:
  //   - Build a tokenised index of claim title/summary/quote
  //   - Match by keyword overlap with pillar title/summary/tags
  //   - Keep top N evidence claims per pillar
  // ---------------------------------------------------------------------------

  function tokens(s) {
    return String(s || "")
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter(Boolean)
      .filter(t => t.length >= 3);
  }

  const claimIndex = safeArray(claims).map(c => {
    const claim_id = stableString(c?.claim_id);
    const title = stableString(c?.title);
    const summary = stableString(c?.summary);
    const quote = stableString(c?.quote);
    const url = stableString(c?.url);
    const tier = c?.tier;
    const tier_group = stableString(c?.tier_group);
    return {
      claim_id,
      title,
      summary,
      quote,
      url,
      tier,
      tier_group,
      tok: new Set([...tokens(title), ...tokens(summary), ...tokens(quote)])
    };
  }).filter(x => x.claim_id);

  function overlapScore(pTok, cTok) {
    if (!pTok.size || !cTok.size) return 0;
    let m = 0;
    for (const t of pTok) if (cTok.has(t)) m++;
    // favour small overlaps, but avoid zero
    return m;
  }

  function bestEvidenceFor(pillarText, n = 4) {
    const t = new Set(tokens(pillarText));
    if (!t.size) return [];
    const scored = [];
    for (const c of claimIndex) {
      const s = overlapScore(t, c.tok);
      if (s > 0) scored.push({ id: c.claim_id, s, tier: c.tier });
    }
    // sort: higher overlap first, then lower tier (more authoritative)
    scored.sort((a, b) => (b.s - a.s) || ((a.tier ?? 99) - (b.tier ?? 99)) || a.id.localeCompare(b.id));
    return scored.slice(0, n).map(x => x.id);
  }

  // ---------------------------------------------------------------------------
  // 3) Build content pillars (synthesis)
  // ---------------------------------------------------------------------------
  // We produce pillars based on:
  //   - supplier pillars as primary
  //   - optionally enriched by top 1 industry pillar that overlaps
  //
  // Hard rule:
  //   proof_points[] must include evidence_claim_ids[] (non-empty)
  // ---------------------------------------------------------------------------

  const out = [];
  const used = new Set(); // dedupe by title

  function mkPillar({ supplier, industry }) {
    const titleBase = supplier?.title || "Supplier pillar";
    const industryHint = industry?.title ? ` (${industry.title})` : "";
    const title = `${titleBase}${industryHint}`.slice(0, 140);

    const key = title.toLowerCase();
    if (used.has(key)) return null;

    const combinedText = [
      supplier?.title,
      supplier?.summary,
      supplier?.tags?.join(" "),
      industry?.title,
      industry?.summary,
      industry?.tags?.join(" ")
    ].filter(Boolean).join(" | ");

    const evidenceIds = bestEvidenceFor(combinedText, 5);

    // If we cannot attach any evidence, we still create the pillar but:
    // - proof_points will be empty
    // - and it will be flagged in constraints (do-not-assert-without-proof)
    const proof_points = evidenceIds.length
      ? [{
        claim: supplier?.summary || supplier?.title || "Pillar claim",
        evidence_claim_ids: evidenceIds,
        source: {
          markdown_pillar_refs: supplier ? [`SPLR:${supplier.id}`] : [],
          industry_pillar_refs: industry ? [`IND:${industry.id}`] : []
        }
      }]
      : [];

    const constraints = [];
    if (!evidenceIds.length) {
      constraints.push({
        rule: "No proof points matched for this pillar; treat as framing only unless evidence is added.",
        source: {
          markdown_pillar_refs: supplier ? [`SPLR:${supplier.id}`] : [],
          industry_pillar_refs: industry ? [`IND:${industry.id}`] : []
        }
      });
    }

    // Audience: prefer industry hints; fallback to input segmentation
    const audience = [];
    if (industry?.tags?.length) {
      // low-risk deterministic, tags only
      for (const t of industry.tags.slice(0, 4)) audience.push(t);
    } else {
      // fallback minimal
      const sm = String(input?.sales_model || "").toLowerCase();
      if (sm) audience.push(sm === "partner" ? "Partner sales" : "Direct sales");
    }

    const pillar = ensureProvenance({
      pillar_id: pillarId(),
      title,
      audience,
      value_prop: supplier?.summary || supplier?.title || "",
      why_it_matters: industry?.summary || "",
      proof_points,
      constraints
    });

    // provenance refs
    if (supplier) pillar.source.markdown_pillar_refs.push(`SPLR:${supplier.id}`);
    if (industry) pillar.source.industry_pillar_refs.push(`IND:${industry.id}`);

    used.add(key);
    return pillar;
  }

  // For each supplier pillar, find best matching industry pillar by overlap.
  function bestIndustryMatchFor(supplier) {
    if (!supplier) return null;
    const sTok = new Set(tokens(`${supplier.title} ${supplier.summary} ${supplier.tags.join(" ")}`));
    let best = null;
    let bestScore = 0;

    for (const ind of I_cap) {
      const iTok = new Set(tokens(`${ind.title} ${ind.summary} ${ind.tags.join(" ")}`));
      // overlap count
      let m = 0;
      for (const t of sTok) if (iTok.has(t)) m++;
      if (m > bestScore) {
        bestScore = m;
        best = ind;
      }
    }
    // require at least one shared token to avoid nonsense pairings
    return bestScore > 0 ? best : null;
  }

  for (const sp of S_cap) {
    const ind = bestIndustryMatchFor(sp);
    const p = mkPillar({ supplier: sp, industry: ind });
    if (p) out.push(p);
    if (out.length >= 10) break;
  }

  // If no supplier pillars exist, we can produce industry-only framing pillars
  // but they MUST be marked as framing-only.
  if (out.length === 0 && I_cap.length) {
    for (const ind of I_cap.slice(0, 8)) {
      const p = mkPillar({ supplier: null, industry: ind });
      if (p) out.push(p);
      if (out.length >= 8) break;
    }
  }

  return out;
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------
module.exports = async function (context, queueItem) {
  const log = context.log;
  const msg = parseQueueItem(queueItem);

  const prefix = normPrefix(msg.prefix || "");
  if (!prefix) {
    log("[pillars] missing prefix; aborting", { op: msg.op || null });
    return;
  }

  // runId for logging only (do NOT use as identity)
  let runId =
    (typeof msg.runId === "string" && msg.runId.trim())
      ? msg.runId.trim()
      : (typeof msg.run_id === "string" && msg.run_id.trim())
        ? msg.run_id.trim()
        : null;

  if (!runId) {
    try {
      const parts = prefix.split("/").filter(Boolean);
      if (parts.length) runId = parts[parts.length - 1];
    } catch { /* noop */ }
  }
  if (!runId) runId = "unknown";

  log("[pillars] start", { runId, prefix, op: msg.op || null });

  const container = await getResultsContainerClient();

  // ---------------------------------------------------------------------------
  // Read canonical inputs
  // ---------------------------------------------------------------------------
  const statusPath = `${prefix}status.json`;
  let status = (await getJson(container, statusPath)) || {};
  if (!status || typeof status !== "object") status = {};
  status.markers = (status.markers && typeof status.markers === "object") ? status.markers : {};

  // If already completed, do nothing (idempotent)
  if (status.markers.pillarsSynthCompleted === true) {
    log("[pillars] already completed; skipping", { runId, prefix });
    return;
  }

  // Markdown pack (canonical)
  let markdownPack = null;
  try {
    markdownPack = await getJson(container, `${prefix}evidence_v2/markdown_pack.json`);
  } catch {
    markdownPack = null;
  }

  if (!markdownPack || typeof markdownPack !== "object") {
    // Hard downgrade: cannot synthesise content pillars without markdown_pack
    await updateStatus(container, prefix, {
      state: "Failed",
      error: { code: "pillars_missing_markdown_pack", message: "markdown_pack.json missing" },
      failedAt: nowIso()
    }, { phase: "PillarsSynth", note: "failed: markdown_pack missing" });

    throw new Error("PillarsSynth: markdown_pack.json missing");
  }

  // Evidence bundle (canonical)
  let evidenceBundle = null;
  try {
    evidenceBundle = await getJson(container, `${prefix}evidence.json`);
  } catch {
    evidenceBundle = null;
  }

  if (!evidenceBundle || typeof evidenceBundle !== "object") {
    await updateStatus(container, prefix, {
      state: "Failed",
      error: { code: "pillars_missing_evidence", message: "evidence.json missing" },
      failedAt: nowIso()
    }, { phase: "PillarsSynth", note: "failed: evidence missing" });

    throw new Error("PillarsSynth: evidence.json missing");
  }

  // Input for audience hints (non-authoritative)
  let input = {};
  try {
    const persisted = await getJson(container, `${prefix}input.json`);
    if (persisted && typeof persisted === "object") input = persisted;
  } catch { }

  // Hash coupling
  const markdownPackSha1 = sha1Json(markdownPack);
  const evidenceSha1 = sha1Json(evidenceBundle);

  // If content_pillars already exists and hashes match, mark completed and exit.
  let existing = null;
  try {
    existing = await getJson(container, `${prefix}content_pillars.json`);
  } catch { existing = null; }

  if (existing && typeof existing === "object") {
    const inSha = existing?.inputs || {};
    if (
      stableString(inSha.markdown_pack_sha1) === markdownPackSha1 &&
      stableString(inSha.evidence_sha1) === evidenceSha1
    ) {
      status.markers.pillarsSynthCompleted = true;
      status.markers.contentPillarsSha1 = sha1Json(existing);
      status.markers.markdownPackSha1 = markdownPackSha1;
      status.markers.evidenceSha1 = evidenceSha1;

      await putJson(container, statusPath, status);
      log("[pillars] content_pillars.json already matches; completed", { runId, prefix });
      return;
    }
  }

  // ---------------------------------------------------------------------------
  // Update status: start
  // ---------------------------------------------------------------------------
  await updateStatus(container, prefix, {
    state: "PillarsSynth",
    phase: "start",
    updatedAt: nowIso(),
    markers: {
      ...(status.markers || {}),
      markdownPackSha1,
      evidenceSha1
    }
  }, { phase: "PillarsSynth", note: "start" });

  // ---------------------------------------------------------------------------
  // Synthesis (deterministic)
  // ---------------------------------------------------------------------------
  const pillars = buildContentPillars({ markdownPack, evidenceBundle, input });

  // Enforce doctrine:
  // - every proof point MUST have evidence_claim_ids (non-empty)
  // - no “facts” without evidence anchors
  const violations = [];
  for (const p of safeArray(pillars)) {
    for (const pp of safeArray(p.proof_points)) {
      const ids = safeArray(pp.evidence_claim_ids).map(stableString).filter(Boolean);
      if (!ids.length) {
        violations.push(`pillar:${p.pillar_id} proof_point missing evidence_claim_ids`);
      }
    }
  }

  // We do NOT fail if there are framing-only pillars, but we record violations.
  const contentPack = {
    schema: "content-pillars-v1",
    generated_at: nowIso(),
    inputs: {
      markdown_pack_path: "evidence_v2/markdown_pack.json",
      evidence_path: "evidence.json",
      markdown_pack_sha1: markdownPackSha1,
      evidence_sha1: evidenceSha1
    },
    warnings: violations.length ? violations : [],
    pillars
  };

  const contentPillarsSha1 = sha1Json(contentPack);

  await putJson(container, `${prefix}content_pillars.json`, contentPack);

  // ---------------------------------------------------------------------------
  // Update status: completed
  // ---------------------------------------------------------------------------
  const cur = (await getJson(container, statusPath)) || {};
  cur.markers = (cur.markers && typeof cur.markers === "object") ? cur.markers : {};
  cur.markers.pillarsSynthCompleted = true;
  cur.markers.contentPillarsSha1 = contentPillarsSha1;
  cur.markers.markdownPackSha1 = markdownPackSha1;
  cur.markers.evidenceSha1 = evidenceSha1;
  cur.state = "pillars_completed";
  cur.updatedAt = nowIso();
  if (!Array.isArray(cur.history)) cur.history = [];
  cur.history.push({ at: nowIso(), phase: "pillars_completed", note: "" });

  await putJson(container, statusPath, cur);

  // ---------------------------------------------------------------------------
  // Hand off to router
  // ---------------------------------------------------------------------------
  // Gate by marker to prevent duplicates (idempotent)
  if (!cur.markers.afterpillarsSent) {
    await enqueueTo(ROUTER_QUEUE_NAME, {
      op: "afterpillars",
      runId,
      page: msg.page || "campaign",
      prefix
    });

    cur.markers.afterpillarsSent = true;
    cur.history.push({ at: nowIso(), phase: "router_enqueued", note: "afterpillars" });
    await putJson(container, statusPath, cur);
  }

  log("[pillars] completed", { runId, prefix, pillars: pillars.length, sha1: contentPillarsSha1 });
};
