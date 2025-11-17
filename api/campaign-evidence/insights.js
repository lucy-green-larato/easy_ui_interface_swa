// /api/campaign-evidence/insights.js 15-11-2025 v2
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

function safeArray(v) {
  return Array.isArray(v) ? v : [];
}

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

async function buildInsights(container, prefix) {
  // 1) Load inputs (best-effort)
  const evidenceRaw = await readJsonSafe(container, `${prefix}evidence.json`, null);
  const csvNorm = (await readJsonSafe(container, `${prefix}csv_normalized.json`, {})) || {};
  const needsMap = (await readJsonSafe(container, `${prefix}needs_map.json`, {})) || {};
  const markdownPackRaw = (await readJsonSafe(container, `${prefix}evidence_v2/markdown_pack.json`, {})) || {};

  const markdownPack = normaliseMarkdownPack(markdownPackRaw);

  let claims = [];
  if (Array.isArray(evidenceRaw?.claims)) {
    claims = evidenceRaw.claims;
  } else if (Array.isArray(evidenceRaw)) {
    claims = evidenceRaw;
  }

  const insights = initialiseInsights();

  // --- 2) Market environment (drivers + stats + regulator claims) ---

  for (const item of markdownPack.industry_drivers) {
    pushUnique(insights.market_environment, {
      kind: "markdown_driver",
      text: item.text,
      source: {
        source_type: "markdown",
        markdown_id: item.id || null,
        markdown_file: item.source?.file || null,
        markdown_heading: item.source?.heading || null,
        markdown_origin: item.source?.origin || null
      }
    });
  }

  for (const item of markdownPack.industry_stats) {
    pushUnique(insights.market_environment, {
      kind: "markdown_stat",
      text: item.text,
      from_markdown: {
        id: item.id || null,
        source: item.source || null
      }
    });
  }

  // Regulator / macro evidence claims → environment + risk
  const regulatorClaims = claims.filter(c => {
    if (!c || typeof c !== "object") return false;
    const hay = (
      String(c.source_type || "") + " " +
      String(c.title || "") + " " +
      String(c.summary || "") + " " +
      String(c.url || "")
    ).toLowerCase();
    return /(ofcom|gov\.uk|ons|ico|dsit|regulator)/.test(hay);
  });

  for (const c of regulatorClaims) {
    const text = String(
      (typeof c.summary === "string" && c.summary.trim()) ||
      (typeof c.title === "string" && c.title.trim()) ||
      (typeof c.url === "string" && c.url.trim()) ||
      (typeof c.source_type === "string" && c.source_type.trim()) ||
      ""
    ).trim();

    if (!text) continue;

    const envEntry = {
      kind: "regulator_claim",
      text,
      claim_id: c.claim_id || null,
      source_type: c.source_type || null,
      url: c.url || null
    };
    const riskEntry = { ...envEntry };

    pushUnique(insights.market_environment, envEntry);
    pushUnique(insights.risk_landscape, riskEntry);
  }

  // --- 3) Buyer pressures (persona pack + needs_map) ---

  for (const item of markdownPack.persona_pressures) {
    pushUnique(insights.buyer_pressures, {
      kind: "markdown_persona_pressure",
      text: item.text,
      from_markdown: {
        id: item.id || null,
        source: item.source || null
      }
    });
  }

  const nmItems = safeArray(needsMap.items);
  for (const n of nmItems) {
    const rawNeed =
      (typeof n?.need === "string" && n.need.trim())
        ? n.need
        : (typeof n?.label === "string" && n.label.trim()
          ? n.label
          : "");
    const need = rawNeed.trim();
    if (!need) continue;

    pushUnique(insights.buyer_pressures, {
      kind: "need_from_map",
      need,
      status: n.status || null,
      hits: Array.isArray(n.hits)
        ? n.hits.map(h => ({ name: (typeof h?.name === "string" && h.name.trim()) || null }))
        : [],
      source: {
        source_type: "needs_map",
        needs_index: idx,
        needs_status: n.status || null,
        needs_hits: Array.isArray(n.hits)
          ? n.hits.map(h => ({ name: h.name || null }))
          : []
      }
    });
  }

  // --- 4) Demand signals (CSV purchases) ---

  const sig = csvNorm && typeof csvNorm === "object" ? (csvNorm.signals || {}) : {};
  const glob = csvNorm && typeof csvNorm === "object" ? (csvNorm.global_signals || {}) : {};

  const demandSources = [
    { origin: "signals.top_purchases", values: safeArray(sig.top_purchases) },
    { origin: "global_signals.top_purchases", values: safeArray(glob.top_purchases) }
  ];

  for (const src of demandSources) {
    for (const v of src.values) {
      if (typeof v !== "string") continue;
      const label = v.trim();
      if (!label) continue;
      pushUnique(insights.demand_signals, {
        kind: "csv_purchase_intent",
        label,
        source: {
          field: src.origin
        }
      });
    }
  }

  // --- 5) Adoption barriers (CSV blockers) ---

  const blockerSources = [
    { origin: "signals.top_blockers", values: safeArray(sig.top_blockers) },
    { origin: "global_signals.top_blockers", values: safeArray(glob.top_blockers) }
  ];

  for (const src of blockerSources) {
    for (const v of src.values) {
      if (typeof v !== "string") continue;
      const label = v.trim();
      if (!label) continue;
      pushUnique(insights.adoption_barriers, {
        kind: "csv_blocker",
        label,
        source: {
          source_type: "csv",
          csv_field: src.origin || null,
          csv_index: idx ?? null
        }
      });
    }
  }

  // --- 6) Risk landscape (markdown + regulator claims already added above) ---

  for (const item of markdownPack.industry_risks) {
    pushUnique(insights.risk_landscape, {
      kind: "markdown_risk",
      text: item.text,
      from_markdown: {
        id: item.id || null,
        source: item.source || null
      }
    });
  }

  // --- 7) Timing drivers (subset of drivers mentioning time/renewals) ---

  const timingRegex = /(renewal|contract|deadline|budget|fiscal|year-end|mandate|compliance date)/i;
  for (const item of markdownPack.industry_drivers) {
    if (!timingRegex.test(item.text)) continue;
    pushUnique(insights.timing_drivers, {
      kind: "markdown_timing_driver",
      text: item.text,
      from_markdown: {
        id: item.id || null,
        source: item.source || null
      }
    });
  }

  // --- 8) Opportunity map (needs_map items directly) ---

  for (const n of nmItems) {
    const rawNeed =
      (typeof n?.need === "string" && n.need.trim())
        ? n.need
        : (typeof n?.label === "string" && n.label.trim()
          ? n.label
          : "");
    const need = rawNeed.trim();
    if (!need) continue;

    pushUnique(insights.opportunity_map, {
      need,
      status: n.status || null,
      hits: Array.isArray(n.hits)
        ? n.hits.map(h => ({ name: (typeof h?.name === "string" && h.name.trim()) || null }))
        : [],
      source: {
        source_type: "needs_map",
        needs_index: idx,
        needs_status: n.status || null,
        needs_hits: Array.isArray(n.hits)
          ? n.hits.map(h => ({ name: h.name || null }))
          : []
      }
    });
  }

  // --- 9) Patterns.need_clusters (group needs by label) ---

  if (nmItems.length) {
    const clusters = new Map();
    for (const n of nmItems) {
      const rawNeed =
        (typeof n?.need === "string" && n.need.trim())
          ? n.need
          : (typeof n?.label === "string" && n.label.trim()
            ? n.label
            : "");
      const label = rawNeed.trim();
      if (!label) continue;

      const key = label.toLowerCase();
      const entry = clusters.get(key) || { label, count: 0, statuses: new Set() };
      entry.count += 1;
      if (n.status) entry.statuses.add(String(n.status));
      clusters.set(key, entry);
    }
    for (const entry of clusters.values()) {
      insights.patterns.need_clusters.push({
        label: entry.label,
        count: entry.count,
        statuses: Array.from(entry.statuses),
        source: {
          from_needs_map: true
        }
      });
    }
  }

  // --- 10) Patterns.blocker_clusters (group blockers with explicit sources) ---

  const blockerClusterMap = new Map();

  for (const src of blockerSources) {
    for (const v of src.values) {
      if (typeof v !== "string") continue;
      const label = v.trim();
      if (!label) continue;

      const key = label.toLowerCase();
      const entry = blockerClusterMap.get(key) || {
        label,
        count: 0,
        fields: new Set()
      };
      entry.count += 1;
      entry.fields.add(src.origin);
      blockerClusterMap.set(key, entry);
    }
  }

  for (const entry of blockerClusterMap.values()) {
    insights.patterns.blocker_clusters.push({
      label: entry.label,
      count: entry.count,
      sources: Array.from(entry.fields).map(field => ({ field }))
    });
  }

  // Prepare a flat blockers list for themes (strings only)
  const allBlockers = [];
  for (const src of blockerSources) {
    for (const v of src.values) {
      if (typeof v !== "string") continue;
      const label = v.trim();
      if (!label) continue;
      allBlockers.push(label);
    }
  }

  // --- 11) Patterns.signal_themes (combined needs + blockers + purchases) ---

  const signalThemes = new Map();

  function bumpTheme(label, source) {
    const key = String(label || "").toLowerCase().trim();
    if (!key) return;
    const baseLabel = String(label || "").trim();
    if (!baseLabel) return;

    const entry = signalThemes.get(key) || {
      label: baseLabel,
      count: 0,
      sources: []
    };
    entry.count += 1;

    if (source && source.field) {
      const existingFields = new Set(entry.sources.map(s => s.field));
      if (!existingFields.has(source.field) && entry.sources.length < 16) {
        entry.sources.push(source);
      }
    }

    signalThemes.set(key, entry);
  }

  // Needs (supplier + global)
  for (const v of safeArray(sig.top_needs_supplier)) {
    if (typeof v !== "string") continue;
    const label = v.trim();
    if (!label) continue;
    bumpTheme(label, { field: "signals.top_needs_supplier" });
  }
  for (const v of safeArray(glob.top_needs_supplier)) {
    if (typeof v !== "string") continue;
    const label = v.trim();
    if (!label) continue;
    bumpTheme(label, { field: "global_signals.top_needs_supplier" });
  }

  // Purchases
  for (const v of safeArray(sig.top_purchases)) {
    if (typeof v !== "string") continue;
    const label = v.trim();
    if (!label) continue;
    bumpTheme(label, { field: "signals.top_purchases" });
  }
  for (const v of safeArray(glob.top_purchases)) {
    if (typeof v !== "string") continue;
    const label = v.trim();
    if (!label) continue;
    bumpTheme(label, { field: "global_signals.top_purchases" });
  }

  // Blockers (already normalised)
  for (const v of allBlockers) {
    bumpTheme(v, { field: "blockers" });
  }

  for (const entry of signalThemes.values()) {
    insights.patterns.signal_themes.push({
      label: entry.label,
      count: entry.count,
      sources: entry.sources
    });
  }

  // --- 12) Persist insights bundle ---

  await putJson(container, `${prefix}insights_v1/insights.json`, insights);
  return insights;
}

module.exports = {
  buildInsights
};
