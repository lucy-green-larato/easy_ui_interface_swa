// /api/campaign-evidence/buyerLogic.js 2025-12-08 v4
// Builds: <prefix>insights_v1/buyer_logic.json
// Inputs:
//   - evidence.json
//   - csv_normalized.json
//   - needs_map.json
//   - evidence_v2/markdown_pack.json
//   - insights_v1/insights.json
// Output: structured buyer_logic with no narrative or hallucination.
// Phase 1: buildBuyerLogic
// Deterministic classifier.
// - No model calls
// - No narrative generation
// - Only classifies existing evidence, CSV signals and markdown pack content
// - Output feeds strategy_v2 but does not contain strategy

const { getJson, putJson } = require("../shared/storage");
const { validateAndWarn } = require("../shared/schemaValidators");


// ---- shape + small helpers ----

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

// Simple keyword classifier: deterministic, no generation
function classifyByKeywords(text) {
  const t = String(text || "").toLowerCase();

  const operational = /downtime|outage|manual|inefficien|process|operational|capacity|workload|backlog|service level|sla|ticket|support desk|staffing/.test(t);
  const commercial = /revenue|profit|margin|cost|budget|capex|opex|price|pricing|roi|payback|pipeline|sales|churn|arpu|uplift/.test(t);
  const emotional = /stress|pressure|fear|worry|anxiety|confidence|trust|reputation|embarrass|frustrat|blame|career/.test(t);
  const urgency = /deadline|regulat|compliance|fine|penalt|renewal|contract|end[- ]of[- ]life|eol|now\b|immediate|urgent|time[- ]critical/.test(t);

  return { operational, commercial, emotional, urgency };
}

// Per-bucket dedupe (label + type)
function makeBucketPushers(buyerLogic) {
  const seen = {
    problems: new Set(),
    root_causes: new Set(),
    operational_impacts: new Set(),
    commercial_impacts: new Set(),
    emotional_drivers: new Set(),
    urgency_factors: new Set()
  };

  function push(bucketName, type, label, origin) {
    const clean = String(label || "").trim();
    if (!clean || !buyerLogic[bucketName]) return;

    const key = `${type}:${clean.toLowerCase()}`;
    if (seen[bucketName].has(key)) return;
    seen[bucketName].add(key);

    buyerLogic[bucketName].push({
      type,
      label: clean,
      origin: origin || {}
    });
  }

  return {
    addProblem: (type, label, origin) =>
      push("problems", type, label, origin),
    addRootCause: (type, label, origin) =>
      push("root_causes", type, label, origin),
    addOperational: (type, label, origin) =>
      push("operational_impacts", type, label, origin),
    addCommercial: (type, label, origin) =>
      push("commercial_impacts", type, label, origin),
    addEmotional: (type, label, origin) =>
      push("emotional_drivers", type, label, origin),
    addUrgency: (type, label, origin) =>
      push("urgency_factors", type, label, origin)
  };
}

// ---- main builder ----

async function buildBuyerLogic(container, prefix) {
  const buyerLogic = ensureBuyerLogicShape({});
  const push = makeBucketPushers(buyerLogic);

  // 1) Load all inputs (best-effort, no throwing)
  const [
    evidenceBundle,
    csvNorm,
    needsMap,
    markdownPack,
    insights
  ] = await Promise.all([
    readJsonSafe(container, `${prefix}evidence.json`, null),
    readJsonSafe(container, `${prefix}csv_normalized.json`, null),
    readJsonSafe(container, `${prefix}needs_map.json`, null),
    readJsonSafe(container, `${prefix}evidence_v2/markdown_pack.json`, null),
    readJsonSafe(container, `${prefix}insights_v1/insights.json`, null)
  ]);

  const evidenceClaims = Array.isArray(evidenceBundle?.claims)
    ? evidenceBundle.claims
    : [];

  // 2) CSV needs & blockers → problems, root_causes + keyword buckets
  if (csvNorm && csvNorm.signals) {
    const sig = csvNorm.signals || {};
    const global = csvNorm.global_signals || {};

    const csvNeeds = []
      .concat(sig.top_needs_supplier || sig.top_needs || [])
      .concat(global.top_needs_supplier || global.top_needs || []);
    const csvBlockers = []
      .concat(sig.top_blockers || [])
      .concat(global.top_blockers || []);

    csvNeeds.forEach((txt, idx) => {
      const label = String(txt || "").trim();
      if (!label) return;
      const origin = {
        source: "csv_signals",
        kind: "need",
        field: "top_needs_supplier|top_needs",
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
        field: "top_blockers",
        index: idx
      };

      // Blockers are both problems and candidate root-causes
      push.addProblem("csv_blocker", label, origin);
      push.addRootCause("csv_blocker", label, origin);

      const cls = classifyByKeywords(label);
      if (cls.operational) push.addOperational("csv_blocker", label, origin);
      if (cls.commercial) push.addCommercial("csv_blocker", label, origin);
      if (cls.emotional) push.addEmotional("csv_blocker", label, origin);
      if (cls.urgency) push.addUrgency("csv_blocker", label, origin);
    });
  }

  // 3) Needs map → problems + urgency based on gaps
  if (needsMap && Array.isArray(needsMap.items)) {
    needsMap.items.forEach((it, idx) => {
      const label = String(it?.need || "").trim();
      if (!label) return;

      const status = String(it?.status || "").toLowerCase();
      const origin = {
        source: "needs_map",
        index: idx,
        status,
        hits: Array.isArray(it?.hits)
          ? it.hits.map(h => h?.name).filter(Boolean)
          : []
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

  // 4) Markdown pack pressures & risks → problems, root_causes, emotional, urgency
  const mp = markdownPack || {};
  const personaPressures = Array.isArray(mp.persona_pressures)
    ? mp.persona_pressures
    : [];
  const industryRisks = Array.isArray(mp.industry_risks)
    ? mp.industry_risks
    : [];

  personaPressures.forEach((item, idx) => {
    const label = String(item?.text || item?.label || item || "").trim();
    if (!label) return;

    const origin = {
      source: "markdown_pack",
      bucket: "persona_pressures",
      id: item?.id || null,
      index: idx
    };

    // Pressures are problems in buyer terms
    push.addProblem("persona_pressure", label, origin);
    push.addEmotional("persona_pressure", label, origin);

    const cls = classifyByKeywords(label);
    if (cls.operational) push.addOperational("persona_pressure", label, origin);
    if (cls.commercial) push.addCommercial("persona_pressure", label, origin);
    if (cls.urgency) push.addUrgency("persona_pressure", label, origin);
  });

  industryRisks.forEach((item, idx) => {
    const label = String(item?.text || item?.label || item || "").trim();
    if (!label) return;

    const origin = {
      source: "markdown_pack",
      bucket: "industry_risks",
      id: item?.id || null,
      index: idx
    };

    // Risks are strong candidates for root causes + urgency
    push.addRootCause("industry_risk", label, origin);

    const cls = classifyByKeywords(label);
    if (cls.operational) push.addOperational("industry_risk", label, origin);
    if (cls.commercial) push.addCommercial("industry_risk", label, origin);
    if (cls.emotional) push.addEmotional("industry_risk", label, origin);
    if (cls.urgency) push.addUrgency("industry_risk", label, origin);
  });

  // 5) Insights clusters → structured problems/root causes/urgency
  if (insights && insights.patterns) {
    const pats = insights.patterns;

    const needClusters = Array.isArray(pats.need_clusters)
      ? pats.need_clusters
      : [];
    const blockerClusters = Array.isArray(pats.blocker_clusters)
      ? pats.blocker_clusters
      : [];
    const signalThemes = Array.isArray(pats.signal_themes)
      ? pats.signal_themes
      : [];

    needClusters.forEach((c, idx) => {
      const label = String(c?.label || c?.name || c?.key || "").trim();
      if (!label) return;
      const origin = {
        source: "insights",
        cluster_type: "need_clusters",
        index: idx,
        members: Array.isArray(c?.members) ? c.members : []
      };
      push.addProblem("need_cluster", label, origin);

      const cls = classifyByKeywords(label);
      if (cls.operational) push.addOperational("need_cluster", label, origin);
      if (cls.commercial) push.addCommercial("need_cluster", label, origin);
      if (cls.emotional) push.addEmotional("need_cluster", label, origin);
      if (cls.urgency) push.addUrgency("need_cluster", label, origin);
    });

    blockerClusters.forEach((c, idx) => {
      const label = String(c?.label || c?.name || c?.key || "").trim();
      if (!label) return;
      const origin = {
        source: "insights",
        cluster_type: "blocker_clusters",
        index: idx,
        members: Array.isArray(c?.members) ? c.members : []
      };
      push.addRootCause("blocker_cluster", label, origin);

      const cls = classifyByKeywords(label);
      if (cls.operational) push.addOperational("blocker_cluster", label, origin);
      if (cls.commercial) push.addCommercial("blocker_cluster", label, origin);
      if (cls.emotional) push.addEmotional("blocker_cluster", label, origin);
      if (cls.urgency) push.addUrgency("blocker_cluster", label, origin);
    });

    signalThemes.forEach((c, idx) => {
      const label = String(c?.label || c?.name || c?.key || "").trim();
      if (!label) return;
      const origin = {
        source: "insights",
        cluster_type: "signal_themes",
        index: idx,
        members: Array.isArray(c?.members) ? c.members : []
      };

      // Themes can contribute to multiple buckets depending on text
      const cls = classifyByKeywords(label);
      if (cls.operational) push.addOperational("signal_theme", label, origin);
      if (cls.commercial) push.addCommercial("signal_theme", label, origin);
      if (cls.emotional) push.addEmotional("signal_theme", label, origin);
      if (cls.urgency) push.addUrgency("signal_theme", label, origin);
      // If no classification hits, we leave it out (no hallucinated category).
    });
  }

  // 6) Evidence claims (optional, very light touch: no generation, just classification of existing text)
  evidenceClaims.forEach((c, idx) => {
    const title = String(c?.title || "").trim();
    const summary = String(c?.summary || "").trim();
    const txt = `${title} ${summary}`.trim();
    if (!txt) return;

    const origin = {
      source_type: "evidence",
      claim_id: c.claim_id || null,
      claim_url: c.url || null,
      claim_source_type: c.source_type || null
    };
    const cls = classifyByKeywords(txt);

    if (cls.operational) push.addOperational("evidence", txt, origin);
    if (cls.commercial) push.addCommercial("evidence", txt, origin);
    if (cls.emotional) push.addEmotional("evidence", txt, origin);
    if (cls.urgency) push.addUrgency("evidence", txt, origin);
  });

  // 7) Persist output (idempotent; always full schema)
  const out = ensureBuyerLogicShape(buyerLogic);
  validateAndWarn("buyer_logic", out, console.log);
  await putJson(container, `${prefix}insights_v1/buyer_logic.json`, out);
  return out;
}

module.exports = { buildBuyerLogic };
