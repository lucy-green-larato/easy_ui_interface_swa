// /api/campaign-evidence/buyerLogic.js
// 2025-12-12 v6
//
// -----------------------------------------------------------------------------
// PHASE 1 — BUYER LOGIC (DETERMINISTIC CLASSIFICATION)
// -----------------------------------------------------------------------------
//
// PURPOSE
// --------
// Classifies WHAT MATTERS to the buyer, based strictly on verified inputs.
// This module performs NO interpretation, NO narrative synthesis, and
// NO inference beyond deterministic classification.
//
// DOCTRINE
// --------
// Evidence answers what is true.
// Buyer logic classifies what matters.
// Insights explain what it means.
//
// This module:
//   ✔ consumes evidence.json (tiered, authoritative)
//   ✔ consumes csv_normalized.json (raw signals)
//   ✔ consumes needs_map.json (derived but deterministic)
//   ✖ does NOT consume markdown_pack.json directly
//   ✖ does NOT consume insights.json
//
// OUTPUT
// ------
// Writes:
//   <prefix>insights_v1/buyer_logic.json
//
// This output feeds Phase 2+ strategy and insights engines,
// but MUST remain non-interpretive.
//
// -----------------------------------------------------------------------------

const { getJson, putJson } = require("../shared/storage");
const { validateAndWarn } = require("../shared/schemaValidators");

// -----------------------------------------------------------------------------
// Helpers: shape + safe loading
// -----------------------------------------------------------------------------

function ensureBuyerLogicShape(v) {
  const base = {
    problems: [],
    root_causes: [],
    operational_impacts: [],
    commercial_impacts: [],
    emotional_drivers: [],
    urgency_factors: []
  };

  if (!v || typeof v !== "object") return base;

  for (const k of Object.keys(base)) {
    if (!Array.isArray(v[k])) v[k] = [];
  }
  return v;
}

async function readJsonSafe(container, rel, fallback = null) {
  try {
    const v = await getJson(container, rel);
    return v ?? fallback;
  } catch {
    return fallback;
  }
}

// -----------------------------------------------------------------------------
// Deterministic keyword classifier
// (Neutral, text-only, no doctrine assumptions)
// -----------------------------------------------------------------------------

function classifyByKeywords(text) {
  const t = String(text || "").toLowerCase();

  return {
    operational: /downtime|outage|manual|inefficien|process|operational|capacity|workload|backlog|sla|ticket|support desk|staffing/.test(t),
    commercial: /revenue|profit|margin|cost|budget|capex|opex|price|pricing|roi|payback|pipeline|sales|churn|arpu|uplift/.test(t),
    emotional: /stress|pressure|fear|worry|anxiety|confidence|trust|reputation|frustrat|blame|career/.test(t),
    urgency: /deadline|regulat|compliance|fine|penalt|renewal|contract|eol|end[- ]of[- ]life|urgent|time[- ]critical/.test(t)
  };
}

// -----------------------------------------------------------------------------
// Bucket pushers with deterministic dedupe
// -----------------------------------------------------------------------------

function makeBucketPushers(buyerLogic) {
  const seen = {
    problems: new Set(),
    root_causes: new Set(),
    operational_impacts: new Set(),
    commercial_impacts: new Set(),
    emotional_drivers: new Set(),
    urgency_factors: new Set()
  };

  function push(bucket, type, label, origin) {
    const clean = String(label || "").trim();
    if (!clean || !buyerLogic[bucket]) return;

    const key = `${type}:${clean.toLowerCase()}`;
    if (seen[bucket].has(key)) return;
    seen[bucket].add(key);

    buyerLogic[bucket].push({
      type,
      label: clean,
      origin: origin || {}
    });
  }

  return {
    addProblem: (t, l, o) => push("problems", t, l, o),
    addRootCause: (t, l, o) => push("root_causes", t, l, o),
    addOperational: (t, l, o) => push("operational_impacts", t, l, o),
    addCommercial: (t, l, o) => push("commercial_impacts", t, l, o),
    addEmotional: (t, l, o) => push("emotional_drivers", t, l, o),
    addUrgency: (t, l, o) => push("urgency_factors", t, l, o)
  };
}

// -----------------------------------------------------------------------------
// MAIN BUILDER
// -----------------------------------------------------------------------------

async function buildBuyerLogic(container, prefix) {
  const buyerLogic = ensureBuyerLogicShape({});
  const push = makeBucketPushers(buyerLogic);

  // ---------------------------------------------------------------------------
  // 1) Load authoritative inputs (best-effort, no throws)
  // ---------------------------------------------------------------------------

  const [evidenceBundle, csvNorm, needsMap] = await Promise.all([
    readJsonSafe(container, `${prefix}evidence.json`, null),
    readJsonSafe(container, `${prefix}csv_normalized.json`, null),
    readJsonSafe(container, `${prefix}needs_map.json`, null)
  ]);

  const evidenceClaims = Array.isArray(evidenceBundle?.claims)
    ? evidenceBundle.claims
    : [];

  // ---------------------------------------------------------------------------
  // 2) CSV SIGNALS → problems, root causes, impacts, urgency
  // ---------------------------------------------------------------------------

  if (csvNorm?.signals) {
    const sig = csvNorm.signals || {};
    const glob = csvNorm.global_signals || {};

    const csvNeeds = []
      .concat(sig.top_needs_supplier || sig.top_needs || [])
      .concat(glob.top_needs_supplier || glob.top_needs || []);

    const csvBlockers = []
      .concat(sig.top_blockers || [])
      .concat(glob.top_blockers || []);

    csvNeeds.forEach((txt, idx) => {
      const label = String(txt || "").trim();
      if (!label) return;

      const origin = {
        source: "csv_signals",
        kind: "need",
        index: idx
      };

      push.addProblem("csv_need", label, origin);

      const cls = classifyByKeywords(label);
      if (cls.operational) push.addOperational("csv_need", label, origin);
      if (cls.commercial) push.addCommercial("csv_need", label, origin);
      if (cls.emotional) push.addEmotional("csv_need", label, origin);
      if (cls.urgency) push.addUrgency("csv_need", label, origin);
    });

    csvBlockers.forEach((txt, idx) => {
      const label = String(txt || "").trim();
      if (!label) return;

      const origin = {
        source: "csv_signals",
        kind: "blocker",
        index: idx
      };

      push.addProblem("csv_blocker", label, origin);
      push.addRootCause("csv_blocker", label, origin);

      const cls = classifyByKeywords(label);
      if (cls.operational) push.addOperational("csv_blocker", label, origin);
      if (cls.commercial) push.addCommercial("csv_blocker", label, origin);
      if (cls.emotional) push.addEmotional("csv_blocker", label, origin);
      if (cls.urgency) push.addUrgency("csv_blocker", label, origin);
    });
  }

  // ---------------------------------------------------------------------------
  // 3) NEEDS MAP → problems + urgency (gaps only)
  // ---------------------------------------------------------------------------

  if (Array.isArray(needsMap?.items)) {
    needsMap.items.forEach((it, idx) => {
      const label = String(it?.need || "").trim();
      if (!label) return;

      const status = String(it?.status || "").toLowerCase();
      const origin = {
        source: "needs_map",
        index: idx,
        status
      };

      push.addProblem("need_map", label, origin);

      if (status.includes("gap")) {
        push.addUrgency("need_gap", label, origin);
      }

      const cls = classifyByKeywords(label);
      if (cls.operational) push.addOperational("need_map", label, origin);
      if (cls.commercial) push.addCommercial("need_map", label, origin);
      if (cls.emotional) push.addEmotional("need_map", label, origin);
      if (cls.urgency) push.addUrgency("need_map", label, origin);
    });
  }

  // ---------------------------------------------------------------------------
  // 4) EVIDENCE CLAIMS (TIER-GUARDED CLASSIFICATION)
  // ---------------------------------------------------------------------------
  //
  // Doctrine:
  //   - Tier 0–2: allowed (strategic truth + supplier context)
  //   - Tier 3: industry_stats → excluded
  //   - Tier 4: derived structure → excluded
  //   - Tier 5: case studies → allowed (supporting only)
  //   - Tier 6: microclaims → allowed (supporting only)
  //   - Tier 7: LinkedIn → excluded
  //

  evidenceClaims.forEach(c => {
    if (!c || typeof c !== "object") return;

    if (c.tier === 3 || c.tier === 4 || c.tier === 7) return;

    const text = `${c.title || ""} ${c.summary || ""}`.trim();
    if (!text) return;

    const origin = {
      source: "evidence",
      claim_id: c.claim_id || null,
      tier: c.tier,
      tier_group: c.tier_group || null
    };

    const cls = classifyByKeywords(text);

    // Evidence never creates new "problems" directly.
    // It only reinforces impacts and urgency.
    if (cls.operational) push.addOperational("evidence", text, origin);
    if (cls.commercial) push.addCommercial("evidence", text, origin);
    if (cls.emotional) push.addEmotional("evidence", text, origin);
    if (cls.urgency) push.addUrgency("evidence", text, origin);
  });

  // ---------------------------------------------------------------------------
  // 5) Persist output (idempotent, schema-validated)
  // ---------------------------------------------------------------------------

  const out = ensureBuyerLogicShape(buyerLogic);
  validateAndWarn("buyer_logic", out, console.log);
  await putJson(container, `${prefix}insights_v1/buyer_logic.json`, out);

  return out;
}

module.exports = { buildBuyerLogic };
