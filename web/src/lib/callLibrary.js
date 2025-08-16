// Minimal, dependency-light. Works with your current layout AND the normalised v1 shape.

const BASE = "/content/call-library/v1"; // adjust if you serve from a different public path

export const canonical = {
  buyer(input) {
    const x = String(input).trim().toLowerCase();
    if (["innovator", "innovators"].includes(x)) return "innovator";
    if (["early adopter", "early adopters", "ea", "early-adopter"].includes(x)) return "early-adopter";
    if (["early majority", "early-majority", "em"].includes(x)) return "early-majority";
    if (["late majority", "late-majority", "lm"].includes(x)) return "late-majority";
    if (["sceptic", "sceptics", "skeptic", "skeptics"].includes(x)) return "sceptic";
    throw new Error(`Unknown buyer type: ${input}`);
  },
  mode(input) {
    const x = String(input).trim().toLowerCase();
    if (["partner", "channel"].includes(x)) return "partner";
    if (["direct"].includes(x)) return "direct";
    throw new Error(`Unknown sales mode: ${input}`);
  },
};

async function loadJson(rel) {
  const url = `${BASE}/${rel}`.replace(/\/+/g, "/");
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status} for ${url}`);
    err.status = res.status;
    throw err;
  }
  return res.json();
}

function zeroCountsFor(products) {
  const zeroBT = { "innovator":0,"early-adopter":0,"early-majority":0,"late-majority":0,"sceptic":0 };
  const counts = {};
  for (const p of products) {
    counts[p.id] = { direct: { ...zeroBT }, partner: { ...zeroBT } };
  }
  return counts;
}

/** getIndex() that accepts:
 *  - normalised index.json with {version,updated_at,products,counts}
 *  - your legacy index.json without counts (we'll synthesise zeros)
 */
export async function getIndex() {
  const raw = await loadJson("index.json");
  const products = raw.products || [];
  const counts = raw.counts || zeroCountsFor(products);
  const version = raw.version || "1.0.0";
  const updated_at = raw.updated_at || new Date().toISOString().slice(0,10);
  return { version, updated_at, products, counts };
}

function formatMetaLine(meta) {
  const label = meta.product_label || meta.product_id;
  return `Source: Larato Call Library · ${label} · ${meta.buyerType} · ${meta.sales_mode} · Updated ${meta.updated_at}`;
}

function toBuyerNeedsSlots(rec) {
  const s = rec.stages;
  const proof = (s.example && (s.example.proof_points_text || s.example.proof_points)) || [];
  const ctas  = (s.call_to_action && (s.call_to_action.ctas_text || s.call_to_action.ctas)) || [];
  return {
    "intel-priorities": (s.buyer_desire && s.buyer_desire.buyer_needs_summary) || [],
    "intel-pains": (s.buyer_pain && s.buyer_pain.buyer_needs_summary) || [],
    "intel-proof-points": proof,
    "intel-objections": (s.objections && (s.objections.anticipated || s.objections.buyer_needs_summary)) || [],
    "intel-ctas": ctas,
  };
}

/** Build a v1-shaped record from your legacy per-product file with buyer_types */
function coerceFromLegacy(raw, { product_id, mode, buyerType, sourcePath }) {
  // Map legacy buyer_types keys to canonical buyerType
  const keyMap = {
    "innovator":"innovators",
    "early-adopter":"early_adopters",
    "early-majority":"early_majority",
    "late-majority":"late_majority",
    "sceptic":"sceptics",
  };
  const btKey = keyMap[buyerType] || buyerType;

  const node = raw?.buyer_types?.[btKey];
  if (!node?.stages) return null;

  const meta = {
    product_id: raw?.meta?.product_id || product_id,
    product_label: raw?.meta?.product_label,
    sales_mode: raw?.meta?.sales_mode || mode,
    buyerType,
    version: raw?.meta?.version || "1.0.0",
    updated_at: raw?.meta?.updated_at || new Date().toISOString().slice(0,10),
    source_url: raw?.meta?.source_url || `${BASE}/${sourcePath}`,
  };

  // Normalise example/cta optional arrays to *_text fields
  const stages = {
    opening: node.stages.opening,
    buyer_pain: node.stages.buyer_pain,
    buyer_desire: node.stages.buyer_desire,
    example: {
      ...node.stages.example,
      proof_points_text: node.stages.example?.proof_points || node.stages.example?.proof_points_text || [],
      buyer_needs_summary: node.stages.example?.buyer_needs_summary || [],
    },
    objections: {
      ...node.stages.objections,
      buyer_needs_summary: node.stages.objections?.buyer_needs_summary || [],
    },
    call_to_action: {
      ...node.stages.call_to_action,
      ctas_text: node.stages.call_to_action?.ctas || node.stages.call_to_action?.ctas_text || [],
      buyer_needs_summary: node.stages.call_to_action?.buyer_needs_summary || [],
    },
  };

  return { meta, stages };
}

export async function getCallScript({ product, buyerType, mode }) {
  const product_id = String(product).trim().toLowerCase();
  const bt = canonical.buyer(buyerType);
  const m  = canonical.mode(mode);

  const attempts = [];
  const tryLoad = async (rel) => {
    attempts.push(rel);
    try { return await loadJson(rel); }
    catch (e) { if (e && typeof e.status === "number" && e.status === 404) return null; throw e; }
  };

  // 1) Preferred normalised v1 locations
  let raw = await tryLoad(`${product_id}/${m}/${bt}.json`);
  let usedFallback = false;

  if (!raw) { raw = await tryLoad(`${product_id}/${m}/early-majority.json`); usedFallback = !!raw; }
  if (!raw) { raw = await tryLoad(`${product_id}/direct/early-majority.json`); usedFallback = !!raw || usedFallback; }

  // 2) Legacy per-product files (your current structure)
  if (!raw) {
    // Try mode/<product>.json then <product>.json
    const legacyPaths = [`${m}/${product_id}.json`, `${product_id}.json`];
    for (const rel of legacyPaths) {
      const legacy = await tryLoad(rel);
      if (legacy) {
        const rec = coerceFromLegacy(legacy, { product_id, mode: m, buyerType: bt, sourcePath: rel });
        if (rec) {
          raw = rec;
          usedFallback = true;
          break;
        }
      }
    }
  }

  if (!raw) {
    console.warn(`[CallLibrary] Miss: ${attempts.join(" -> ")}`);
    const err = new Error("No library script found with fallback chain.");
    err.resolution = { attempts, hit: null, fallbackUsed: false };
    throw err;
  }

  const rec = raw.meta && raw.stages ? raw : { meta: raw.meta, stages: raw.stages };
  const metaLine = formatMetaLine(rec.meta);
  const buyerNeeds = toBuyerNeedsSlots(rec);

  console.info(`[CallLibrary] Hit: ${attempts[attempts.length - 1]} (fallback: ${usedFallback ? "yes" : "no"})`);

  return {
    ...rec,
    resolution: { attempts, hit: attempts[attempts.length - 1], fallbackUsed: usedFallback },
    metaLine,
    buyerNeeds,
    source: "library",
  };
}

export function diagnostic(result) {
  return {
    usedLibrary: result?.source === "library",
    resolution: result?.resolution ?? null,
  };
}
