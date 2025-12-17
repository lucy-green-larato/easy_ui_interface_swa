// /api/campaign-competitor-score/index.js
// Phase 4 — Competitor scoring (deterministic, non-evidence, no LLM)
// 17-12-2025 v1.0

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");

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

function containsAny(haystack, needles) {
  const h = norm(haystack);
  if (!h) return false;
  for (const n of needles || []) {
    const t = norm(n);
    if (!t) continue;
    if (h.includes(t)) return true;
  }
  return false;
}

// ---------------- scoring rules (v1) ----------------

// 1) coverage_overlap: competitor offerings vs our strategy text (very conservative)
function scoreCoverageOverlap(competitor, strategyCore) {
  const offerings = uniq(competitor?.facts?.offerings || []);
  if (!offerings.length) return "unknown";

  const stratText = norm(toText(strategyCore));
  if (!stratText) return "unknown";

  const hits = offerings.filter(o => {
    const t = norm(o);
    if (!t || t.length < 4) return false;
    // avoid matching "industry" etc; just a cheap contains
    return stratText.includes(t);
  }).length;

  // Conservative buckets
  if (hits >= 3) return "high";
  if (hits >= 1) return "medium";
  return "low";
}

// 2) differentiation_clarity: do we have at least one distinct differentiator signal?
// We only call it "clear" if (a) we have our advantage text and (b) competitor has offerings,
// and (c) there exists at least one term in our advantage that is NOT present in competitor offerings.
function scoreDifferentiationClarity(competitor, strategyCore) {
  const offerings = uniq(competitor?.facts?.offerings || []);
  if (!offerings.length) return "unknown";

  const ourAdv = []
    .concat(strategyCore?.competitive_strategy?.our_advantage || [])
    .concat(strategyCore?.competitive_strategy?.defensible_differentiators || []);

  const advText = uniq(ourAdv.map(x => String(x || "").trim()).filter(Boolean));
  if (!advText.length) return "unknown";

  const advKeys = keywordsFromStrings(advText);
  if (!advKeys.length) return "unknown";

  const compText = norm(toText(offerings));
  if (!compText) return "unknown";

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

// 4) buyer_relevance: based on buyer_logic + industry mentions in competitor facts.
// - unknown if we have no buyer_logic signals
// - high if competitor industries/geography overlap any buyer_logic keywords
// - medium if we have any competitor facts but no overlap
// - low only if competitor is declared but has zero facts (very conservative)
function scoreBuyerRelevance(competitor, buyerLogic) {
  const blTxt = norm(toText(buyerLogic));
  if (!blTxt) return "unknown";

  const factsTxt = []
    .concat(competitor?.facts?.industries || [])
    .concat(competitor?.facts?.geography || [])
    .concat(competitor?.facts?.offerings || []);

  const facts = uniq(factsTxt);
  if (!facts.length) return "low";

  // Use buyer_logic problems/urgency/risk as "buyer frame" keywords
  const frame = []
    .concat((buyerLogic?.problems || []).map(x => x?.label || x))
    .concat((buyerLogic?.urgency_factors || []).map(x => x?.label || x))
    .concat((buyerLogic?.commercial_impacts || []).map(x => x?.label || x));

  const frameKeys = keywordsFromStrings(frame);
  const factsJoined = norm(facts.join(" | "));

  const overlap = frameKeys.some(k => factsJoined.includes(norm(k)));

  if (overlap) return "high";
  return "medium";
}

function buildConstraints(competitor, scores) {
  const constraints = [];

  if (!competitor?.inputs?.markdown_present) {
    constraints.push("No competitor markdown available");
  }

  if (scores?.proof_strength === "none") {
    constraints.push("No evidence corpus available");
  } else if (scores?.proof_strength === "weak") {
    constraints.push("No competitor-specific proof found in evidence");
  }

  if (scores?.coverage_overlap === "unknown") {
    constraints.push("Insufficient competitor offering data to assess overlap");
  }

  if (scores?.differentiation_clarity === "unknown") {
    constraints.push("Insufficient differentiator signals to assess clarity");
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

  const enriched = await getJson(container, `${prefix}competitors_enriched.json`);
  if (!enriched || typeof enriched !== "object" || !Array.isArray(enriched.competitors)) {
    throw new Error("competitor-score: competitors_enriched.json missing or invalid");
  }

  const strategyWrapper = await getJson(container, `${prefix}strategy_v2/campaign_strategy.json`);
  const strategyCore =
    (strategyWrapper && typeof strategyWrapper === "object" && strategyWrapper.strategy_v2 && typeof strategyWrapper.strategy_v2 === "object")
      ? strategyWrapper.strategy_v2
      : (strategyWrapper && typeof strategyWrapper === "object" ? strategyWrapper : {});

  const evidenceCanon = (await getJson(container, `${prefix}evidence.json`)) || {};
  const buyerLogic = (await getJson(container, `${prefix}buyer_logic.json`)) || {};

  // ============================================================
  // STEP 4.4 / 4.5 — SCORE COMPETITORS (DETERMINISTIC)
  // ============================================================

  const scored = (enriched.competitors || []).map(c => {
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

  // ============================================================
  // STEP 4.6 — WRITE OUTPUT
  // ============================================================

  const out = {
    schema: "competitor-scores-v1",
    generated_at: new Date().toISOString(),
    method: "deterministic-rule-set-v1",
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
    // If status is corrupt, replace with minimum safe object
    // (still deterministic, and avoids pipeline deadlock)
    // NOTE: This is a recovery behaviour, not a preferred state.
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
    competitors: scored.length,
    outPath
  });
};
