// /src/lib/callSynthesis.js
import { getCallScript } from "./callLibrary.js";

/* ---------- Language & style helpers (formal UK) ---------- */
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
  let out = String(text);
  for (const [re, rep] of REPLACERS) out = out.replace(re, rep);
  out = out.replace(/[ ]{2,}/g, " ").replace(/\n{3,}/g, "\n\n").trim();
  out = out.replace(/!+/g, "."); // avoid shouty punctuation
  return out;
}

function stripStageHeadings(text = "") {
  const re = /^(Opening|Buyer pain|Buyer desire|Example|Objections|Call to action)\s*[-–—]*\s*$/gim;
  return text.replace(re, "").replace(/\n{3,}/g, "\n\n").trim();
}

function sentenceify(str = "") {
  return str
    .replace(/\s+([.,;:?!])/g, "$1")
    .replace(/\(\s+/g, "(")
    .replace(/\s+\)/g, ")")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function ensureCtaTwoOptions(cta = "") {
  if (!cta) return "";
  const outHasTerminal = /[.?!]$/.test(cta);
  let out = cta.trim() + (outHasTerminal ? "" : ".");
  if (!/\b(monday|tuesday|wednesday|thursday|friday)\b/i.test(out)) {
    out += " Would Monday or Wednesday morning suit?";
  }
  return out;
}

// Gentle length limiter (keeps whole sentences)
function trimToWords(text = "", target = 0) {
  if (!target || target < 1) return text;
  const words = text.split(/\s+/);
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

/* ---------- Tone scaffolding ---------- */
function warmSofteners(text = "") {
  return text
    .replace(/\bI will\b/g, "I’ll")
    .replace(/\bwe will\b/gi, "we’ll")
    .replace(/\bdo not\b/gi, "don’t")
    .replace(/\bit is\b/gi, "it’s")
    .replace(/\bwe are\b/gi, "we’re");
}

function makeObservationLine(tone = "Professional", sectorHint = "") {
  const base =
    "We are seeing similar organisations tighten control of mobile and fixed estates to reduce overspend and delays without reworking what already functions.";
  const withSector = sectorHint ? base + " In " + sectorHint + ", the pattern is similar." : base;
  return /warm/i.test(tone) ? warmSofteners(withSector) : withSector;
}

function makeEnhancementLine(tone = "Professional") {
  const line =
    "This is a light operational layer—an orchestration step above your current services—so you keep what works and adjust only what does not.";
  return /warm/i.test(tone) ? warmSofteners(line) : line;
}

function greet(variables = {}, tone = "Professional") {
  const p = (variables.prospect_name || "").trim();
  const seller = (variables.seller_name || "").trim();
  const co = (variables.seller_company || "").trim();
  const who = [seller || "a colleague", co && "from " + co].filter(Boolean).join(" ");
  let line = p ? "Hi " + p + ", it’s " + who + ". " : "Hi, it’s " + who + ". ";
  if (/warm/i.test(tone)) line = warmSofteners(line);
  return line;
}

/* ---------- Helpers to use USPs and “Other points” naturally ---------- */
function normaliseList(text = "") {
  // split by newlines, semicolons, or commas; keep short items
  const parts = String(text)
    .split(/\r?\n|;|,/)
    .map(s => s.trim())
    .filter(Boolean);
  // dedupe while preserving order
  const seen = new Set();
  return parts.filter(p => (seen.has(p.toLowerCase()) ? false : (seen.add(p.toLowerCase()), true)));
}

function weaveUserInputs({ usps = [], other = [] }, tone = "Professional") {
  const lines = [];
  if (usps.length) {
    const lead = "From our side, the areas we are confident add value are";
    const content = usps.join("; ");
    lines.push(lead + ": " + content + ".");
  }
  if (other.length) {
    const lead = "If it is useful, we can also touch on";
    const content = other.join("; ");
    lines.push(lead + " " + content + "; otherwise, we can keep this strictly to immediate priorities.");
  }
  let out = lines.join(" ");
  if (/warm/i.test(tone)) out = warmSofteners(out);
  return out;
}

/* ---------- Narrative composer (no headings; UK) ---------- */
function composeNarrativeFromStages(stages = {}, opts = {}) {
  const v = opts.variables || {};
  const tone = opts.tone || "Professional (corporate)";
  const s = stages;

  const hello = greet(v, tone);

  const opening =
    (s.opening && (s.opening.talk_track || s.opening.script) || "").trim() ||
    "Thanks for taking the call; I will keep this brief and useful.";
  const openingPara = /warm/i.test(tone) ? warmSofteners(opening) : opening;

  const observation = makeObservationLine(tone, (opts.form && opts.form.sector) || "");

  const pain = (s.buyer_pain && (s.buyer_pain.talk_track || s.buyer_pain.script) || "").trim();

  const desire =
    (s.buyer_desire && (s.buyer_desire.talk_track || s.buyer_desire.script) || "").trim() ||
    "The goal is practical control—clarity on usage, simple provisioning and room to scale without extra admin.";

  // Weave USPs and “Other points to cover”
  const usps = normaliseList(v.value_proposition || "");
  const other = normaliseList(v.context || "");
  const weave = weaveUserInputs({ usps, other }, tone);

  const example = (s.example && (s.example.talk_track || s.example.script) || "").trim();
  const proofs = (s.example && (s.example.proof_points_text || s.example.proof_points) || []);
  const exampleLine = [example, proofs && proofs.length ? "For example: " + proofs.join("; ") + "." : ""]
    .filter(Boolean)
    .join(" ");

  const lightLayer = makeEnhancementLine(tone);

  const objections = (s.objections && (s.objections.talk_track || s.objections.script) || "").trim();
  const anticipated = (s.objections && s.objections.anticipated) || [];
  const objectionsLine = [objections, anticipated.length ? "Common concerns we address: " + anticipated.join("; ") + "." : ""]
    .filter(Boolean)
    .join(" ");

  let cta = (s.call_to_action && (s.call_to_action.talk_track || s.call_to_action.script) || "").trim();
  const ctas = (s.call_to_action && (s.call_to_action.ctas_text || s.call_to_action.ctas) || []);
  if (!cta && ctas.length) cta = String(ctas[0]);
  cta = ensureCtaTwoOptions(cta);

  const paras = [
    openingPara,
    observation,
    pain,
    desire,
    weave,         // ← user-provided USPs / other points folded in naturally
    exampleLine,
    lightLayer,
    objectionsLine,
    cta
  ].filter(Boolean);

  let text = [hello + paras[0], ...paras.slice(1)]
    .map(p => sentenceify(stripStageHeadings(/warm/i.test(tone) ? warmSofteners(p) : p)))
    .join("\n\n");

  text = toUk(text);
  return text;
}

/* ---------- Tips (3 items from your preferred list) ---------- */
const TIPS_POOL = [
  "Your call opener is critical. Be concise. Lead with why you are calling. Focus on the value for the prospect.",
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

function pickTips(count = 3) {
  // deterministic pick that includes at least one of your “lead with why” / “UK cases” / “clear next step”
  const anchors = [
    "Your call opener is critical. Be concise. Lead with why you are calling. Focus on the value for the prospect.",
    "Use relatable UK cases. A story about a firm across five UK cities lands better than a generic global example.",
    "End with a clear, polite next step. Suggest specific times rather than leaving it open-ended."
  ];
  const rest = TIPS_POOL.filter(t => !anchors.includes(t));
  const picked = [anchors[0], anchors[1]]; // first two anchors
  // add one more either the third anchor or from rest
  picked.push(anchors[2]);
  return picked.slice(0, count);
}

/* ---------- Public API ---------- */
export async function generateCallFromLibrary({
  lookup,
  form = {},
  tone = "Professional (corporate)", // or "Warm (professional)"
  length = 300,
  variables = {}
} = {}) {
  const record = await getCallScript(lookup);

  let script = composeNarrativeFromStages(record.stages, { variables, tone, form });

  if (Number.isFinite(length) && length > 0) {
    // clamp 150–650 hard to reflect the UI
    const target = Math.max(150, Math.min(650, length));
    script = trimToWords(script, target);
  }

  const tips = pickTips(3);
  const tipsBlock = ["Helpful tips", ""].concat(tips.map(t => "- " + t)).join("\n");

  return {
    ...record,                 // { meta, stages, buyerNeeds, metaLine, source, resolution }
    script_text: script,       // narrative only
    tips_text: tipsBlock,      // tips markdown block
    tips_list: tips,           // array for UI list
  };
}
