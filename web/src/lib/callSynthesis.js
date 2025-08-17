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
function normaliseList(text = "") {
  const parts = String(text).split(/\r?\n|;|,/).map(s => s.trim()).filter(Boolean);
  const seen = new Set();
  return parts.filter(p => (seen.has(p.toLowerCase()) ? false : (seen.add(p.toLowerCase()), true)));
}
function normaliseProofPoint(p = "") {
  const s = String(p || "").trim();
  const m = s.toLowerCase();
  if (m.includes("pp_sim_overspend_38")) return "SIM overspend dropped by 38 per cent";
  if (m.includes("pp_new_sites_under_7d")) return "new locations were connected in under seven days";
  if (m.includes("pp_control_no_headcount")) return "IT regained control without adding headcount";
  if (m.includes("pp_sla_tickets_down_41")) return "SLA tickets fell by 41 per cent";
  return s;
}
function mapProofPoints(arr = []) { return (arr || []).map(normaliseProofPoint).filter(Boolean); }
function trimToWords(text = "", target = 0) {
  if (!target || target < 1) return text;
  const words = text.trim().split(/\s+/);
  if (words.length <= target * 1.15) return text;
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

/* ================== Content extraction ================== */
function getStage(s, key) { return (s && s[key] && (s[key].talk_track || s[key].script) || "").trim(); }
function getArrayish(node, ...keys) { for (const k of keys) { const v = node && node[k]; if (Array.isArray(v) && v.length) return v; } return []; }
function extractPains(s) {
  const fromSummary = getArrayish(s?.buyer_pain, "buyer_needs_summary", "bullets");
  if (fromSummary.length) return fromSummary;
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

/* ================== Heuristics driven by optional inputs ================== */
const HINTS = [
  { key: /contract|term|renewal|migration|port/i, reassure: "We phase improvements in at natural renewals and port numbers cleanly, so nothing is wasted.", focus: "contract timing and migration risk" },
  { key: /cost|budget|overspend|finance|roi|saving/i, reassure: "Costs stay predictable — pooled policies and usage alerts keep Finance clear on spend.", focus: "cost control and measurable ROI" },
  { key: /security|secure|iso|gdpr|compliance|risk/i, reassure: "Controls align with UK regulatory and ISO practices; changes are auditable.", focus: "governance and compliance" },
  { key: /esim|provision|onboard|field/i, reassure: "Provisioning is same-day for most profiles; field teams are handled with pooled rules.", focus: "faster provisioning for teams" },
  { key: /report|visibility|dashboard|data/i, reassure: "Reporting is real-time and readable; IT and Finance see the same numbers.", focus: "shared reporting and visibility" }
];
function tailorByInputs(usps = [], other = []) {
  const notes = new Set();
  const reassures = new Set();
  const agenda = new Set();

  const hay = [ ...usps, ...other ].map(s => s.toLowerCase());
  for (const h of HINTS) {
    if (hay.some(t => h.key.test(t))) {
      if (h.reassure) reassures.add(h.reassure);
      if (h.focus) agenda.add(h.focus);
    }
  }
  if (usps.length) notes.add(`From our side, the areas most likely to add value are ${humanList(usps.slice(0,3))}.`);
  if (other.length) notes.add(`If useful, we can also touch on ${humanList(other.slice(0,3))}; otherwise we’ll keep strictly to priority items.`);

  return {
    weaveEarly: Array.from(notes).join(" "),
    extraReassurance: Array.from(reassures),
    agendaFocus: Array.from(agenda)
  };
}

/* ================== Narrative composer ================== */
function composeNarrative({ record, variables, tone, form, lengthTarget, withLabels = true }) {
  const s = record.stages || {};
  const meta = record.meta || {};
  const productLabel = meta.product_label || meta.product_id || (variables.product || "this area");
  const v = variables || {};
  const pName = (v.prospect_name || "").trim();
  const seller = (v.seller_name || "").trim();
  const sellerCo = (v.seller_company || "").trim();

  // Greeting
  let greet = pName ? `Hi ${pName}, it’s ${seller || "a colleague"}${sellerCo ? ` from ${sellerCo}` : ""}. `
                    : `Hi, it’s ${seller || "a colleague"}${sellerCo ? ` from ${sellerCo}` : ""}. `;
  if (/warm/i.test(tone)) greet = warm(greet);

  // Observations (from pains)
  const pains = extractPains(s).slice(0, 3).map(sentenceify);
  const obs = pains.length
    ? `Speaking with similar organisations, we’re seeing ${humanList(pains)} — typically without changing what already works.`
    : `Similar organisations are tightening control of mobile and fixed estates to reduce overspend and delays without reworking what already functions.`;

  // Desired outcomes
  const desires = extractDesires(s).slice(0, 3).map(sentenceify);
  const desireLine = desires.length
    ? `Where teams are landing successfully is ${humanList(desires)}.`
    : `The aim is practical control — clarity on usage, simple provisioning and headroom to grow.`;

  // Optional inputs
  const usps = normaliseList(v.value_proposition || "");
  const other = normaliseList(v.context || "");
  const { weaveEarly, extraReassurance, agendaFocus } = tailorByInputs(usps, other);

  // Positioning
  let lightLayer = `This is a light operational layer above your current services — you keep what works and adjust only what doesn’t.`;
  if (/warm/i.test(tone)) lightLayer = warm(lightLayer);

  // Example & proofs
  const { example, proofs } = extractExampleAndProofs(s);
  const exLine = (example || "").trim();
  const proofsLine = proofs.length ? `In the first phase we saw ${humanList(proofs)}.` : "";
  const proofsAlreadySaid = proofs.length && exLine && proofs.every(p => exLine.toLowerCase().includes(p.toLowerCase()));

  // Objections
  const { objections, anticipated } = extractObjections(s);
  const objLine = objections || `If you have existing contracts or limited capacity for change, we phase improvements in at renewals to avoid wasted spend.`;
  const anticipatedLine = anticipated.length ? `Typical concerns we handle: ${humanList(anticipated)}.` : "";
  const extraReassuranceLine = extraReassurance.length ? humanList(extraReassurance) + "." : "";

  // CTA (focused by agenda where possible)
  let ctaBase = extractCta(s) || `A short ${productLabel} review tends to surface quick wins without disruption — shall we line that up?`;
  if (agendaFocus.length) {
    ctaBase = `We can focus the review on ${humanList(agendaFocus)} and quantify impact quickly.`;
  }
  let cta = ensureCtaOptions(ctaBase, "Monday or Wednesday");

  // Assemble (with discreet labels)
  const blocks = [
    ["Opening", greet + "Thanks for taking the call — I’ll keep this useful and brief."],
    ["Market patterns", obs],
    ["Outcomes", desireLine],
    ["Tailoring notes", weaveEarly],
    ["Example", exLine],
    ["Results", proofsAlreadySaid ? "" : proofsLine],
    ["How it works", lightLayer],
    ["Reassurance", objLine],
    ["Likely concerns", anticipatedLine || extraReassuranceLine],
    ["Next step", cta]
  ].filter(([, txt]) => (txt && txt.trim().length));

  const textNoLabels = blocks.map(([, txt]) => sentenceify(txt)).join("\n\n");
  const labeled = blocks.map(([label, txt]) => `— ${label} —\n${sentenceify(txt)}`).join("\n\n");

  // Tone & length
  let out = withLabels ? labeled : textNoLabels;
  out = toUk(/warm/i.test(tone) ? warm(out) : out);
  const target = Math.max(150, Math.min(650, Number(lengthTarget || 300)));
  out = trimToWords(out, target);

  return { scriptPlain: textNoLabels, scriptLabeled: out };
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
function pickTips() {
  return [
    "Be concise. In the UK, long introductions lose people. Lead with why you are calling.",
    "Use relatable UK cases. A story about a firm across five UK cities lands better than a generic global example.",
    "End with a clear, polite next step. Suggest specific times rather than leaving it open-ended."
  ];
}

/* ================== Public API ================== */
export async function generateCallFromLibrary({
  lookup,
  form = {},
  tone = "Professional (corporate)",  // or "Warm (professional)"
  length = 300,
  variables = {},
  withLabels = true
} = {}) {
  const record = await getCallScript(lookup);

  const composed = composeNarrative({
    record,
    variables,
    tone,
    form,
    lengthTarget: length,
    withLabels
  });

  const tips = pickTips();
  const tipsBlock = ["Helpful tips", ""].concat(tips.map(t => "- " + t)).join("\n");

  return {
    ...record,
    script_text: composed.scriptPlain,
    script_text_labeled: composed.scriptLabeled,
    tips_text: tipsBlock,
    tips_list: tips
  };
}
