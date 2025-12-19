// /api/campaign-competitor-score/index.js
// Phase 4 — Competitor scoring (deterministic, non-evidence, no LLM)
// 19-12-2025 v1.2 — diagnostics hardened (empty-but-valid is explainable)

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");
const { nowIso, buildDiagnostics, uniqStrings } = require("../shared/diagnostics");

const ROUTER_QUEUE =
  process.env.Q_CAMPAIGN_ROUTER ||
  "campaign-router-jobs";

// ---------------- helpers ----------------

function parseQueueItem(queueItem) {
  if (!queueItem) return {};
  if (typeof queueItem === "string") {
    try { return JSON.parse(queueItem); } catch { return {}; }
  }
  return (queueItem && typeof queueItem === "object") ? queueItem : {};
}

function normalisePrefix(prefix) {
  let p = String(prefix || "").trim();
  p = p.replace(/^\/+/, "");
  if (!p.endsWith("/")) p += "/";
  return p;
}

function pushHistory(status, phase, note) {
  if (!status || typeof status !== "object") return;
  if (!Array.isArray(status.history)) status.history = [];
  status.history.push({
    at: new Date().toISOString(),
    phase: String(phase || "status"),
    note: note ? String(note) : ""
  });
}

function uniq(arr) {
  return Array.from(new Set((arr || []).map(x => String(x || "").trim()).filter(Boolean)));
}

function toText(v) {
  try {
    if (v == null) return "";
    if (typeof v === "string") return v;
    return JSON.stringify(v);
  } catch {
    return "";
  }
}

function norm(s) {
  return String(s || "").toLowerCase().trim();
}

// Very conservative tokenisation: words length >= 4 (avoid noise)
function keywordsFromStrings(list) {
  const out = new Set();
  for (const raw of list || []) {
    const s = norm(raw);
    if (!s) continue;
    for (const w of s.split(/[^a-z0-9]+/g)) {
      if (w && w.length >= 4) out.add(w);
    }
  }
  return Array.from(out);
}

function isScoringSignal(scores) {
  if (!scores || typeof scores !== "object") return false;
  return (
    scores.coverage_overlap !== "unknown" ||
    scores.differentiation_clarity !== "unknown" ||
    scores.buyer_relevance !== "unknown" ||
    scores.proof_strength !== "none"
  );
}

function claimsForGroup(competitor, group) {
  return (competitor?.claims || []).filter(
    c => c && c.tier_group === group && typeof c.summary === "string"
  );
}

function allClaimText(competitor) {
  return (competitor?.claims || [])
    .map(c => String(c.summary || "").trim())
    .filter(Boolean);
}

function hasClaims(competitor) {
  return Array.isArray(competitor?.claims) && competitor.claims.length > 0;
}

// ---------------- scoring rules (v1) ----------------

function scoreCoverageOverlap(competitor, strategyCore) {
  const offerings = claimsForGroup(competitor, "supplier_capability")
    .map(c => c.summary);

  if (!offerings.length) return "unknown";

  const stratText = norm(toText(strategyCore));
  if (!stratText) return "unknown";

  const hits = offerings.filter(o => {
    const t = norm(o);
    if (t.length < 4) return false;
    return stratText.includes(t);
  }).length;

  if (hits >= 3) return "high";
  if (hits >= 1) return "medium";
  return "low";
}

function scoreDifferentiationClarity(competitor, strategyCore) {
  const offerings = claimsForGroup(competitor, "supplier_capability")
    .map(c => c.summary);

  if (!offerings.length) return "unknown";

  const ourAdv = []
    .concat(strategyCore?.competitive_strategy?.our_advantage || [])
    .concat(strategyCore?.competitive_strategy?.defensible_differentiators || []);

  const advText = uniq(ourAdv.map(x => String(x || "").trim()).filter(Boolean));
  if (!advText.length) return "unknown";

  const advKeys = keywordsFromStrings(advText);
  if (!advKeys.length) return "unknown";

  const compText = norm(offerings.join(" | "));
  const distinct = advKeys.filter(k => !compText.includes(norm(k)));

  if (distinct.length >= 6) return "clear";
  if (distinct.length >= 1) return "partial";
  return "weak";
}

// 3) proof_strength: strictly evidence-gated.
// - "strong" if >=2 items in evidence explicitly reference competitor by name/slug OR competitor-specific tags exist
// - "moderate" if >=1 such item
// - "weak" if we have evidence overall but nothing competitor-specific
// - "none" if no evidence items
function scoreProofStrength(competitor, evidenceCanon) {
  const claims = Array.isArray(evidenceCanon?.claims) ? evidenceCanon.claims : [];
  if (!claims.length) return "none";

  const name = norm(competitor?.name);
  const slug = norm(competitor?.slug);
  if (!name && !slug) return "weak";

  const hits = claims.filter(c => {
    const t = norm(c?.title);
    const s = norm(c?.summary);
    const u = norm(c?.url);
    const tag = norm(c?.tag);
    // competitor-specific tags (optional future-proof)
    if (tag === "competitor" || tag === "competitor_fact" || tag === "competitive_context") return true;
    // explicit mention
    if (name && (t.includes(name) || s.includes(name) || u.includes(name))) return true;
    if (slug && (t.includes(slug) || s.includes(slug) || u.includes(slug))) return true;
    return false;
  }).length;

  if (hits >= 2) return "strong";
  if (hits >= 1) return "moderate";

  // we have evidence but not competitor-specific
  return "weak";
}

function scoreBuyerRelevance(competitor, buyerLogic) {
  if (!hasClaims(competitor)) return "low";

  const blTxt = norm(toText(buyerLogic));
  if (!blTxt) return "unknown";

  const frame = []
    .concat((buyerLogic?.problems || []).map(x => x?.label || x))
    .concat((buyerLogic?.urgency_factors || []).map(x => x?.label || x))
    .concat((buyerLogic?.commercial_impacts || []).map(x => x?.label || x));

  const frameKeys = keywordsFromStrings(frame);
  if (!frameKeys.length) return "unknown";

  const claimText = norm(allClaimText(competitor).join(" | "));
  const overlap = frameKeys.some(k => claimText.includes(norm(k)));

  return overlap ? "high" : "medium";
}

function buildConstraints(competitor, scores) {
  const constraints = [];

  if (!competitor?.inputs?.markdown_present) {
    constraints.push("No competitor markdown available");
  } else if (!hasClaims(competitor)) {
    constraints.push("Supplier markdown present but no extractable claims");
  }

  if (scores?.proof_strength === "none") {
    constraints.push("No evidence corpus available");
  } else if (scores?.proof_strength === "weak") {
    constraints.push("No competitor-specific proof found in evidence");
  }

  if (scores?.coverage_overlap === "unknown") {
    constraints.push("Insufficient supplier capability claims to assess overlap");
  }

  if (scores?.differentiation_clarity === "unknown") {
    constraints.push("Insufficient differentiator claims to assess clarity");
  }

  return constraints;
}

// ---------------- main ----------------

module.exports = async function (context, queueItem) {
  const log = context.log;

  // ============================================================
  // STEP 4.2 — STRICT PARSE
  // ============================================================

  const msg = parseQueueItem(queueItem);

  if (msg.op !== "score_competitors") {
    log("[competitor-score] ignored message with op:", msg.op);
    return;
  }

  if (!msg.prefix || typeof msg.prefix !== "string") {
    throw new Error("competitor-score: missing or invalid prefix");
  }

  const runId =
    (typeof msg.runId === "string" && msg.runId.trim()) ||
    (typeof msg.run_id === "string" && msg.run_id.trim()) ||
    "unknown";

  const page =
    (typeof msg.page === "string" && msg.page.trim()) ||
    "campaign";

  const prefix = normalisePrefix(msg.prefix);

  log("[competitor-score] starting", { runId, prefix });

  // ============================================================
  // STEP 4.3 — LOAD INPUTS (READ-ONLY)
  // ============================================================

  const container = await getResultsContainerClient();

  // Existence checks for REQUIRED diagnostics keys (no inference).
  // These reads are for diagnostics only and do not alter scoring logic.
  const competitorsDocRaw = await getJson(container, `${prefix}competitors.json`);
  const markdownPackRaw = await getJson(container, `${prefix}evidence_v2/markdown_pack.json`);
  const strategyWrapperRaw = await getJson(container, `${prefix}strategy_v2/campaign_strategy.json`);
  const enrichedRaw = await getJson(container, `${prefix}competitors_enriched.json`);
  const evidenceCanonRaw = await getJson(container, `${prefix}evidence.json`);
  const buyerLogicRaw = await getJson(container, `${prefix}buyer_logic.json`);

  const inputs_present = {
    // Required block fields
    competitors_json: competitorsDocRaw != null,
    markdown_pack: markdownPackRaw != null,
    strategy_json: strategyWrapperRaw != null,

    // Additional precise inputs for this stage
    competitors_enriched_json: enrichedRaw != null,
    evidence_json: evidenceCanonRaw != null,
    buyer_logic_json: buyerLogicRaw != null
  };

  // Normalised objects used by scoring (fail-safe, no throws for emptiness)
  const enrichedDoc = (enrichedRaw && typeof enrichedRaw === "object") ? enrichedRaw : {};
  const competitors = Array.isArray(enrichedDoc.competitors) ? enrichedDoc.competitors : [];

  const strategyWrapper = (strategyWrapperRaw && typeof strategyWrapperRaw === "object") ? strategyWrapperRaw : {};
  const strategyCore =
    (strategyWrapper && typeof strategyWrapper === "object" && strategyWrapper.strategy_v2 && typeof strategyWrapper.strategy_v2 === "object")
      ? strategyWrapper.strategy_v2
      : (strategyWrapper && typeof strategyWrapper === "object" ? strategyWrapper : {});

  const evidenceCanon = (evidenceCanonRaw && typeof evidenceCanonRaw === "object") ? evidenceCanonRaw : {};
  const buyerLogic = (buyerLogicRaw && typeof buyerLogicRaw === "object") ? buyerLogicRaw : {};

  const declared_count = competitors.length;
  let attempted_count = 0;

  // ============================================================
  // STEP 4.4 / 4.5 — SCORE COMPETITORS (DETERMINISTIC)
  // ============================================================

  const scored = (competitors || []).map(c => {
    attempted_count++;

    const scores = {
      coverage_overlap: scoreCoverageOverlap(c, strategyCore),
      differentiation_clarity: scoreDifferentiationClarity(c, strategyCore),
      proof_strength: scoreProofStrength(c, evidenceCanon),
      buyer_relevance: scoreBuyerRelevance(c, buyerLogic)
    };

    return {
      name: c?.name || "",
      slug: c?.slug || "",
      scores,
      constraints: buildConstraints(c, scores)
    };
  });

  const produced_count = scored.filter(x => isScoringSignal(x?.scores)).length;
  out.diagnostics.produced_entries_count = produced_entries_count;

  // ============================================================
  // STEP 4.6 — WRITE OUTPUT (ALWAYS, DIAGNOSTIC, BACKWARD COMPAT)
  // ============================================================

  const skip_reasons = [];

  // deterministic, non-inferential reasons
  if (!inputs_present.competitors_enriched_json) skip_reasons.push("competitors_enriched_missing");
  if (inputs_present.competitors_enriched_json && !Array.isArray(enrichedDoc.competitors)) skip_reasons.push("competitors_enriched_invalid_shape");
  if (declared_count === 0) skip_reasons.push("no_declared_competitors");

  if (!inputs_present.strategy_json) skip_reasons.push("strategy_json_missing_optional");
  if (!inputs_present.evidence_json) skip_reasons.push("evidence_json_missing");
  if (!inputs_present.buyer_logic_json) skip_reasons.push("buyer_logic_json_missing");

  if (produced_count === 0 && attempted_count === 0) skip_reasons.push("no_attempts_made");
  if (produced_count === 0 && attempted_count > 0) skip_reasons.push("produced_count_zero");

  const out = {
    schema: "competitor-scores-v2",
    generated_at: nowIso(),
    prefix,
    method: "deterministic-rule-set-v1",
    diagnostics: buildDiagnostics({
      declared_count,
      attempted_count,
      produced_count,
      skip_reasons: uniqStrings(skip_reasons),
      inputs_present
    }),

    // backward compatible payload
    competitors: scored
  };

  const outPath = `${prefix}competitor_scores.json`;
  await putJson(container, outPath, out);

  // ============================================================
  // STEP 4.7 — UPDATE STATUS
  // ============================================================

  const statusPath = `${prefix}status.json`;
  const status = (await getJson(container, statusPath)) || { runId, markers: {}, history: [] };

  if (!status || typeof status !== "object") {
    // Recovery behaviour, unchanged from your original intent
    log("[competitor-score] WARNING: status.json invalid; recreating minimal status");
  }

  const st = (status && typeof status === "object") ? status : { runId, markers: {}, history: [] };
  if (!st.markers || typeof st.markers !== "object") st.markers = {};
  if (!Array.isArray(st.history)) st.history = [];

  st.markers.competitorScoringCompleted = true;
  st.state = "competitor_scored";
  pushHistory(st, "competitor_scored");

  await putJson(container, statusPath, st);

  // ============================================================
  // STEP 4.8 — ROUTER CONTINUATION
  // ============================================================

  await enqueueTo(ROUTER_QUEUE, {
    op: "aftercompetitorscored",
    runId,
    page,
    prefix
  });

  log("[competitor-score] completed", {
    runId,
    declared: declared_count,
    attempted: attempted_count,
    produced: produced_count,
    outPath
  });
};
