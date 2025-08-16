const CALL_LIB_BASE = "/content/call-library/v1";

const fallbackOrder = [
  "early_majority",
  "early_adopters",
  "late_majority",
  "sceptics",
  "innovators"
];

async function loadCallDoc(product, mode) {
  const url = `${CALL_LIB_BASE}/${mode}/${product}.json`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`Call library not found: ${mode}/${product}`);
  return await res.json();
}

export async function getCallScript({ product, buyerType, mode }) {
  const doc = await loadCallDoc(product, mode);

  let bt = buyerType;
  if (!doc.buyer_types[bt]) {
    bt = fallbackOrder.find(b => !!doc.buyer_types[b]);
    if (!bt) throw new Error(`No buyer_types available in ${mode}/${product}`);
  }
  const chosen = doc.buyer_types[bt];

  const ppIndex = Object.fromEntries(
    (doc.shared.proof_points || []).map(p => [p.id, p.text])
  );
  const ctaIndex = Object.fromEntries(
    (doc.shared.ctas || []).map(c => [c.id, c.text])
  );

  const stages = {};
  const stageKeys = [
    "opening","buyer_pain","buyer_desire","example","objections","call_to_action"
  ];
  stageKeys.forEach(k => {
    const s = chosen.stages[k];
    stages[k] = {
      ...s,
      proof_points_text: (s.proof_points || []).map(id => ppIndex[id]).filter(Boolean),
      ctas_text: (s.ctas || []).map(id => ctaIndex[id]).filter(Boolean)
    };
  });

  return {
    meta: doc.meta,
    tone: doc.shared.tone,
    buyerType: bt,
    stages
  };
}
