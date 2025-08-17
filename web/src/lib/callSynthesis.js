// /src/lib/callSynthesis.js
import { getCallScript } from "./callLibrary.js";

/* ================== Language & normalisation ================== */
const REPLACERS = [
  [/\borganization(s)?\b/gi, "organisation$1"],
  [/\borganize(d|s|r|ing)?\b/gi, "organise$1"],
  [/\boptimization\b/gi, "optimisation"],
  [/\boptimi(z|s)e\b/gi, "optimise"],
  [/\bcenter(s)?\b/gi, "centre$1"],
  [/\bmodeling\b/gi, "modelling"],
  [/\blicense\b/gi, "licence"],
  [/\butilize\b/gi, "utilise"],
  [/\bprogram(s)?\b/gi, "programme$1"]
];

function toUk(text = "") {
  let out = String(text || "");
  for (const [re, rep] of REPLACERS) out = out.replace(re, rep);
  return out.replace(/[ ]{2,}/g, " ").replace(/\n{3,}/g, "\n\n").replace(/!+/g, ".").trim();
}

function warm(text = "") {
  return text
    .replace(/\bI will\b/g, "I’ll")
    .replace(/\bwe will\b/gi, "we’ll")
    .replace(/\bdo not\b/gi, "don’t")
    .replace(/\bit is\b/gi, "it’s")
    .replace(/\bwe are\b/gi, "we’re");
}

function sentenceify(str = "") {
  return (str || "")
    .replace(/\s+([.,;:?!])/g, "$1")
    .replace(/\(\s+/g, "(")
    .replace(/\s+\)/g, ")")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function humanList(items = []) {
  const a = (items || []).filter(Boolean);
  if (!a.length) return "";
  if (a.length === 1) return a[0];
  return a.slice(0, -1).join(", ") + " and " + a[a.length - 1];
}

// Split text by newlines / semicolons / commas, dedupe (case-insensitive)
function normaliseList(text = "") {
  const parts = String(text)
    .split(/\r?\n|;|,/)
    .map(s => s.trim())
    .filter(Boolean);
  const seen = new Set();
  return parts.filter(p => (seen.has(p.toLowerCase()) ? false : (seen.add(p.toLowerCase()), true)));
}

// Turn opaque proof codes into readable claims; pass through plain text
function normaliseProofPoint(p = "") {
  const s = String(p || "").trim();
  const m = s.toLowerCase();
  if (m.includes("pp_sim_overspend_38")) return "SIM overspend dropped by 38 per cent";
  if (m.includes("pp_new_sites_under_7d")) return "new locations were connected in under seven days";
  if (m.includes("pp_control_no_headcount")) return "IT regained control without adding headcount";
  if (m.includes("pp_sla_tickets_down_41")) return "SLA tickets fell by 41 per cent";
  return s; // keep as-is if it already reads well
}

function mapProofPoints(arr = []) {
  return (arr || []).map(normaliseProofPoint).filter(Boolean);
}

// Trim by sentence to an approximate word target (keeps coherence)
function trimToWords(text = "", target = 0) {
  if (!target || target < 1) return text;
  const words = text.trim().split(/\s+/);
  if (words.length <= target * 1.15) return text; // small buffer
  const sentences = text.split(/(?<=[.?!])\s+/);
  const kept = [];
  let count = 0;
  for (const s of sentences) {
    const w = s.trim().split(/\s+/).filter(Boolean).length;
    if (count + w > target) break;
    kept.push(s);
    count += w;
  }
  return kept.join(" ").trim() || text;
}

function ensureCtaOptions(cta = "", fallbackDayPair = "Monday or Wednesday") {
  let out = String(cta || "").trim();
  if (!out) return "";
  if (!/[.?!]$/.test(out)) out += ".";
  if (!/\b(monday|tuesday|wednesday|thursday|friday)\b/i.test(out)) {
    out += " Would " + fallbackDayPair + " morning suit?";
  }
  return out;
}

/* ================== Content extraction helpers ================== */
function getStage(s, key) {
  const node = s && s[key];
  return (node && (node.talk_track || node.script || "") || "").trim();
}
function getArrayish(node, ...keys) {
  for (const k of keys) {
    const v = node && node[k];
    if (Array.isArray(v) && v.length) return v;
  }
  return [];
}
function extractPains(s) {
  const fromSummary = getArrayish(s?.buyer_pain, "buyer_needs_summary", "bullets");
  if (fromSummary.length) return fromSummary;
  // fall back: attempt to split the talk track into clauses
  const raw = getStage(s, "buyer_pain");
  return raw ? raw.split(/[.;•]\s+/).map(t => t.trim()).filter(Boolean) : [];
}
function extractDesires(s) {
  const fromSummary = getArrayish(s?.buyer_desire, "buyer_needs_summary", "bullets");
  if (fromSummary.length) return fromSummary;
  const raw = getStage(s, "buyer_desire");
  return raw ? raw.split(/[.;•]\s+/).map(t => t.trim()).filter(Boolean) : [];
}
function extractExampleAndProofs(s) {
  const ex = getStage(s, "example");
  const proofs = getArrayish(s?.example, "proof_points_text", "proof_points");
  return { example: ex, proofs: mapProofPoints(proofs) };
}
function extractObjections(s) {
  const txt = getStage(s, "objections");
  const anticipated = getArrayish(s?.objections, "anticipated", "buyer_needs_summary");
  return { objections: txt, anticipated };
}
function extractCta(s) {
  const txt = getStage(s, "call_to_action");
  const ctas = getArrayish(s?.call_to_action, "ctas_text", "ctas");
  return txt || (ctas[0] || "");
}

/* ================== Narrative composer ================== */
function composeNarrative({ record, variables, tone, form, lengthTarget }) {
  const s = record.stages || {};
  const meta = record.meta || {};
  const productLabel = meta.product_label || meta.product_id || (variables.product || "this area");
  const v = variables || {};
  const pName = (v.prospect_name || "").trim();
  const seller = (v.seller_name || "").trim();
  const sellerCo = (v.seller_company || "").trim();

  // 1) Greeting (human, name-led)
  let greet = pName ? `Hi ${pName}, it’s ${seller || "a colleague"}${sellerCo ? ` from ${sellerCo}` : ""}. `
                    : `Hi, it’s ${seller || "a colleague"}${sellerCo ? ` from ${sellerCo}` : ""}. `;
  if (/warm/i.test(tone)) greet = warm(greet);

  // 2) Observations drawn from pains (no assumptions about their current state)
  const pains = extractPains(s).slice(0, 3).map(sentenceify);
  const obs = pains.length
    ? `I’m calling because, speaking with similar organisations, we’re seeing ${humanList(pains)}—typically without changing what already works.`
    : `I’m calling because similar organisations are tightening control of mobile and fixed estates to reduce overspend and delays without reworking what already functions.`;

  // 3) Desired outcomes
  const desires = extractDesires(s).slice(0, 3).map(sentenceify);
  const desireLine = desires.length
    ? `Where teams are landing successfully is ${humanList(desires)}.`
    : `The aim is practical control—clarity on usage, simple provisioning and room to scale without extra admin.`;

  // 4) Weave USPs and “Other points” early so it feels tailored
  const usps = normaliseList(v.value_proposition || "");
  const other = normaliseList(v.context || "");
  const weaveEarly = [
    usps.length ? `From our side, the focus areas likely to add value are ${humanList(usps)}.` : "",
    other.length ? `If helpful, we can also touch on ${humanList(other)}; otherwise I’ll keep this to immediate priorities.` : ""
  ].filter(Boolean).join(" ");

  // 5) Positioning: light enhancement (not a rebuild)
  let lightLayer = `This is a light operational layer above your current services—you keep what works and adjust only what doesn’t.`;
  if (/warm/i.test(tone)) lightLayer = warm(lightLayer);

  // 6) Example + proofs
  const { example, proofs } = extractExampleAndProofs(s);
  const exLine = (example || "").trim();
  const proofsLine = proofs.length ? `In the first phase we saw ${humanList(proofs)}.` : "";

  // 7) Objections handled calmly
  const { objections, anticipated } = extractObjections(s);
  const objLine = objections
    ? objections
    : `If you have existing contracts or limited capacity for change, we phase in improvements as renewals arise and avoid wasted spend.`;
  const anticipatedLine = anticipated.length ? `Typical concerns we address: ${humanList(anticipated)}.` : "";

  // 8) CTA with two options
  let cta = ensureCtaOptions(extractCta(s) ||
    `A short ${productLabel} review tends to surface quick wins without disruption—shall we line that up?`, "Monday or Wednesday");

  // Build paragraphs (no headings; conversational flow)
  let paragraphs = [
    greet + `Thanks for taking the call—I'll be brief and make this useful.`,
    obs,
    desireLine,
    weaveEarly,
    exLine,
    proofsLine,
    lightLayer,
    objLine,
    anticipatedLine,
    cta
  ].filter(Boolean).map(sentenceify);

  // Tone & language polish
  let text = paragraphs.join("\n\n");
  text = toUk(/warm/i.test(tone) ? warm(text) : text);

  // Length control (150–650)
  const target = Math.max(150, Math.min(650, Number(lengthTarget || 300)));
  text = trimToWords(text, target);

  return text;
}

/* ================== Tips ================== */
const TIPS_POOL = [
  "Be concise. In the UK, long introductions lose people. Lead with why you are calling.",
  "Do not oversell. Understatement carries credibility; let facts and examples do the work.",
  "Use relatable UK cases. A story about a firm across five UK cities lands better than a generic global example.",
  "Match their pace. If the prospect is brisk, keep tight. If they are more conversational, give space.",
  "End with a clear, polite next step. Suggest specific times rather than leaving it open-ended.",
  "Open with value, not features. Busy decision-makers will only stay engaged if you show you understand their reality.",
  "Use credible stats sparingly. One or two strong numbers build authority — a flood of data feels like a lecture.",
  "Always anchor with a story. Prospects remember relatable examples more than abstract claims.",
  "Anticipate objections before they are raised. It shows confidence and saves time.",
  "Keep the CTA practical and time-bounded. Propose a clear, valuable next step.",
  "Tone matters. Speak in clear, confident British English, and adapt warmth and pace to the prospect."
];

// Always include three anchor tips you specified
function pickTips() {
  const anchors = [
    "Be concise. In the UK, long introductions lose people. Lead with why you are calling.",
    "Use relatable UK cases. A story about a firm across five UK cities lands better than a generic global example.",
    "End with a clear, polite next step. Suggest specific times rather than leaving it open-ended."
  ];
  return anchors.slice(0, 3);
}

/* ================== Public API ================== */
export async function generateCallFromLibrary({
  lookup,
  form = {},
  tone = "Professional (corporate)",  // or "Warm (professional)"
  length = 300,
  variables = {}
} = {}) {
  // 1) Fetch the curated record (but we will not echo its prose verbatim)
  const record = await getCallScript(lookup);

  // 2) Compose fresh narrative using library as source material
  let script = composeNarrative({
    record,
    variables,
    tone,
    form,
    lengthTarget: length
  });

  // 3) Build tips block
  const tips = pickTips();
  const tipsBlock = ["Helpful tips", ""].concat(tips.map(t => "- " + t)).join("\n");

  return {
    ...record,           // meta, stages, buyerNeeds, metaLine, resolution, etc.
    script_text: script, // final narrative (no headings)
    tips_text: tipsBlock,
    tips_list: tips
  };
}
