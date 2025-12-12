// /api/campaign-evidence/insights.js 21-11-2025 v5
// Deterministic Insight Engine for campaign runs.
//
// Consumes (all best-effort; missing files are tolerated):
//   <prefix>evidence.json
//   <prefix>csv_normalized.json
//   <prefix>needs_map.json
//   <prefix>evidence_v2/markdown_pack.json
//
// Writes:
//   <prefix>insights_v1/insights.json
//
// Structure:
// {
//   "market_environment": [],
//   "buyer_pressures": [],
//   "demand_signals": [],
//   "adoption_barriers": [],
//   "risk_landscape": [],
//   "timing_drivers": [],
//   "opportunity_map": [],
//   "patterns": {
//     "need_clusters": [],
//     "blocker_clusters": [],
//     "signal_themes": []
//   }
// }
//
// Rules:
//  - No narratives or free-text interpretation.
//  - No hallucinations: only derive from existing evidence / CSV / needs_map / markdown_pack.
//  - Every item is traceable to: claim_id, markdown id, or specific CSV fields.

const { getJson, putJson } = require("../shared/storage");
const seenMap = new WeakMap();
const { validateAndWarn } = require("../shared/schemaValidators");

// ----- small helpers -----

function safeArray(v) {
  return Array.isArray(v) ? v : [];
}

/**
 * normaliseMarkdownPack:
 * Encodes the assumed shape of evidence_v2/markdown_pack.json.
 * If the markdown pack schema changes, update this normaliser first.
 *
 * NOTE:
 *   - We are intentionally only using the "contextual" buckets here:
 *     industry_drivers, industry_risks, persona_pressures, competitor_profiles,
 *     content_pillars, industry_stats.
 *   - Supplier buckets (Tier-2a) remain available in markdown_pack.json but are
 *     consumed later by strategy_v2 rather than this Phase 1 insights engine.
 */
function normaliseMarkdownPack(raw) {
  const keys = [
    "industry_drivers",
    "industry_risks",
    "persona_pressures",
    "competitor_profiles",
    "content_pillars",
    "industry_stats"
  ];
  const out = {};
  for (const k of keys) {
    const list = safeArray(raw?.[k]).map(item => {
      if (!item) return null;
      const obj = typeof item === "string" ? { text: item } : { ...item };
      obj.text = typeof obj.text === "string" ? obj.text.trim() : "";
      if (!obj.text) return null;
      obj.id = obj.id || null;
      obj.source = obj.source || null;
      return obj;
    }).filter(Boolean);
    out[k] = list;
  }
  return out;
}

function initialiseInsights() {
  return {
    market_environment: [],
    buyer_pressures: [],
    demand_signals: [],
    adoption_barriers: [],
    risk_landscape: [],
    timing_drivers: [],
    opportunity_map: [],
    patterns: {
      need_clusters: [],
      blocker_clusters: [],
      signal_themes: []
    }
  };
}

// Simple helper to add items and avoid exact duplicates by JSON fingerprint
function pushUnique(arr, item) {
  if (!item) return;
  const key = JSON.stringify(item);

  let seen = seenMap.get(arr);
  if (!seen) {
    seen = new Set();
    seenMap.set(arr, seen);
  }

  if (seen.has(key)) return;
  seen.add(key);
  arr.push(item);
}

// Best-effort JSON loader: missing/malformed blobs become fallback
async function readJsonSafe(container, path, fallback = null) {
  try {
    const v = await getJson(container, path);
    return v ?? fallback;
  } catch {
    return fallback;
  }
}

// buildInsights: deterministic, non-generative Phase 1 insight engine.
// - Only derives from evidence.json, csv_normalized.json, needs_map.json, markdown_pack.json
// - No model calls, no narrative generation
// - Every item must be traceable back to source artefacts
async function buildInsights(container, prefix) {
  // ---------------------------------------------------------------------------
  // Phase 2 — Deterministic Insights Engine
  //
  // Authority:
  //   - evidence.json is the single source of truth
  //
  // Rules:
  //   - No raw markdown, CSV, or needs_map reads
  //   - No inference beyond admissible evidence
  //   - Every insight item must trace to claim_id
  // ---------------------------------------------------------------------------

  const evidenceRaw = await readJsonSafe(
    container,
    `${prefix}evidence.json`,
    null
  );

  if (!Array.isArray(evidenceRaw?.claims)) {
    throw new Error("[insights] evidence.json missing or invalid");
  }

  const claims = evidenceRaw.claims;
  const insights = initialiseInsights();

  // ---------------------------------------------------------------------------
  // Helper: filter claims by tier / group
  // ---------------------------------------------------------------------------

  const byTier = (tier, group = null) =>
    claims.filter(c =>
      c &&
      c.tier === tier &&
      (group ? c.tier_group === group : true)
    );

  // ---------------------------------------------------------------------------
  // 1) Market environment
  //   - Tier 1 markdown drivers
  //   - Tier 3 industry stats
  //   - Regulator claims
  // ---------------------------------------------------------------------------

  for (const c of byTier(1, "markdown_industry_drivers")) {
    pushUnique(insights.market_environment, {
      kind: "industry_driver",
      text: c.summary,
      claim_id: c.claim_id,
      source: {
        tier: c.tier,
        tier_group: c.tier_group,
        markdown_id: c.markdown_id || null
      }
    });
  }

  for (const c of byTier(3, "markdown_stats")) {
    pushUnique(insights.market_environment, {
      kind: "industry_stat",
      text: c.summary,
      claim_id: c.claim_id,
      source: {
        tier: c.tier,
        tier_group: c.tier_group
      }
    });
  }

  const regulatorRegex = /(ofcom|gov\.uk|ons|ico|dsit|regulator)/i;

  for (const c of claims) {
    const hay =
      `${c.source_type} ${c.title} ${c.summary} ${c.url}`.toLowerCase();

    if (!regulatorRegex.test(hay)) continue;

    pushUnique(insights.market_environment, {
      kind: "regulator_signal",
      text: c.summary || c.title,
      claim_id: c.claim_id,
      source: {
        tier: c.tier,
        tier_group: c.tier_group,
        url: c.url || null
      }
    });

    pushUnique(insights.risk_landscape, {
      kind: "regulator_risk",
      text: c.summary || c.title,
      claim_id: c.claim_id,
      source: {
        tier: c.tier,
        tier_group: c.tier_group,
        url: c.url || null
      }
    });
  }

  // ---------------------------------------------------------------------------
  // 2) Buyer pressures
  //   - Tier 1 persona markdown
  //   - Tier 4 coverage summary (derived structure)
  // ---------------------------------------------------------------------------

  for (const c of byTier(1, "markdown_persona")) {
    pushUnique(insights.buyer_pressures, {
      kind: "persona_pressure",
      text: c.summary,
      claim_id: c.claim_id,
      source: {
        tier: c.tier,
        tier_group: c.tier_group
      }
    });
  }

  for (const c of byTier(4, "coverage_summary")) {
    pushUnique(insights.buyer_pressures, {
      kind: "coverage_signal",
      text: c.summary,
      claim_id: c.claim_id,
      source: {
        tier: c.tier,
        tier_group: c.tier_group
      }
    });
  }

  // ---------------------------------------------------------------------------
  // 3) Demand signals & adoption barriers
  //   - Tier 0 CSV summary only
  // ---------------------------------------------------------------------------

  const csvClaim = byTier(0, "csv_summary")[0];

  if (csvClaim?.summary) {
    pushUnique(insights.demand_signals, {
      kind: "market_demand",
      text: csvClaim.summary,
      claim_id: csvClaim.claim_id,
      source: {
        tier: 0,
        tier_group: "csv_summary"
      }
    });
  }

  // ---------------------------------------------------------------------------
  // 4) Risk landscape
  //   - Tier 1 industry risks
  // ---------------------------------------------------------------------------

  for (const c of byTier(1, "markdown_industry_risks")) {
    pushUnique(insights.risk_landscape, {
      kind: "industry_risk",
      text: c.summary,
      claim_id: c.claim_id,
      source: {
        tier: c.tier,
        tier_group: c.tier_group
      }
    });
  }

  // ---------------------------------------------------------------------------
  // 5) Timing drivers
  //   - Tier 1 drivers mentioning time sensitivity
  // ---------------------------------------------------------------------------

  const timingRegex =
    /(renewal|contract|deadline|budget|fiscal|year-end|mandate|compliance)/i;

  for (const c of byTier(1, "markdown_industry_drivers")) {
    if (!timingRegex.test(c.summary)) continue;

    pushUnique(insights.timing_drivers, {
      kind: "timing_driver",
      text: c.summary,
      claim_id: c.claim_id,
      source: {
        tier: c.tier,
        tier_group: c.tier_group
      }
    });
  }

  // ---------------------------------------------------------------------------
  // 6) Opportunity map
  //   - Tier 4 derived coverage (no raw needs_map access)
  // ---------------------------------------------------------------------------

  for (const c of byTier(4, "coverage_summary")) {
    pushUnique(insights.opportunity_map, {
      kind: "coverage_opportunity",
      text: c.summary,
      claim_id: c.claim_id,
      source: {
        tier: c.tier,
        tier_group: c.tier_group
      }
    });
  }

  // ---------------------------------------------------------------------------
  // 7) Patterns (lightweight, evidence-backed only)
  // ---------------------------------------------------------------------------

  for (const c of claims) {
    if (!c.summary) continue;

    pushUnique(insights.patterns.signal_themes, {
      label: c.summary.slice(0, 120),
      count: 1,
      sources: [{ claim_id: c.claim_id }]
    });
  }

  // ---------------------------------------------------------------------------
  // Final validation + persist
  // ---------------------------------------------------------------------------

  validateAndWarn("insights", insights, console.log);

  await putJson(
    container,
    `${prefix}insights_v1/insights.json`,
    insights
  );

  return insights;
}

module.exports = {
  buildInsights
};
