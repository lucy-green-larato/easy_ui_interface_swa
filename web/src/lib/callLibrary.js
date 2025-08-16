// /src/lib/callLibrary.js
export const CALL_LIB_BASE = "/content/call-library/v1";

const FALLBACK_ORDER = Object.freeze([
  "early_majority",
  "early_adopters",
  "late_majority",
  "sceptics",
  "innovators"
]);

/** Load the top-level index (ids+labels for products, buyer_types, etc.) */
export async function getIndex() {
  const url = `${CALL_LIB_BASE}/index.json`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`Call library index not found (${res.status})`);
  return res.json();
}

/** Internal: load one product document for a sales mode */
async function loadCallDoc(productId, modeId) {
  const url = `${CALL_LIB_BASE}/${modeId}/${productId}.json`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`Call library not found: ${modeId}/${productId} (${res.status})`);
  return res.json();
}

/**
 * Get a ready-to-render script payload.
 * @param {{ product:string, buyerType:string, mode:'direct'|'partner' }} args
 */
export async function getCallScript({ product, buyerType, mode }) {
  const doc = await loadCallDoc(product, mode);

  // pick buyer type (with fallback order)
  let bt = buyerType;
  if (!doc.buyer_types?.[bt]) {
    bt = FALLBACK_ORDER.find(b => !!doc.buyer_types?.[b]);
    if (!bt) throw new Error(`No buyer_types available in ${mode}/${product}`);
  }
  const chosen = doc.buyer_types[bt];

  // map proof points / CTAs to text
  const ppIndex  = Object.fromEntries((doc.shared?.proof_points || []).map(p => [p.id, p.text]));
  const ctaIndex = Object.fromEntries((doc.shared?.ctas || []).map(c => [c.id, c.text]));

  const stageKeys = ["opening","buyer_pain","buyer_desire","example","objections","call_to_action"];
  const stages = {};
  for (const k of stageKeys) {
    const s = chosen.stages[k] || {};
    stages[k] = {
      ...s,
      proof_points_text: (s.proof_points || []).map(id => ppIndex[id]).filter(Boolean),
      ctas_text: (s.ctas || []).map(id => ctaIndex[id]).filter(Boolean)
    };
  }

  return {
    type: "call_script_v1",
    source: "larato_call_library",
    meta: doc.meta,
    tone: doc.shared?.tone || "",
    buyerType: bt,
    stages
  };
}

/* Optional helpers if you ever want to normalise in the UI */
export const canonical = {
  mode(v=""){ return String(v).toLowerCase().startsWith("p") ? "partner" : "direct"; },
  buyer(v=""){
    const s = String(v).toLowerCase();
    if (s.startsWith("innovator")) return "innovators";
    if (s.startsWith("early adopter")) return "early_adopters";
    if (s.startsWith("early majority")) return "early_majority";
    if (s.startsWith("late majority")) return "late_majority";
    if (s.startsWith("sceptic") || s.startsWith("skeptic")) return "sceptics";
    return "early_majority";
  }
};
