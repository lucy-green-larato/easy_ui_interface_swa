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
  [/\bprogram(s)?\b/gi, "programme$1"],

  // tone clean-up
  [/\brobust\b/gi, "reliable"],
  [/\broll[\s-]?out\b/gi, "roll-out"],
  [/\blocked in\b/gi, "locked-in"],
  [/\bplay out\b/gi, "unfold"],
  [/\bpile-?up\b/gi, "backlog"],
  [/\bflex\b/gi, "adjust"],
  [/\bguys\b/gi, "team"],
];

function toUk(text = "") {
  let out = String(text);
  for (const [re, rep] of REPLACERS) out = out.replace(re, rep);
  out = out.replace(/[ ]{2,}/g, " ").replace(/\n{3,}/g, "\n\n").trim();
  out = out.replace(/!+/g, "."); // no shouty punctuation
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
  const hasOptions = /\b(monday|tuesday|wednesday|thursday|friday)\b/i.test(cta);
  let out = cta.trim();
  if (!/[.?!]$/.test(out)) out += ".";
  if (!hasOptions) out += " Would Monday or Wednesday morning suit?";
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

/* ---------- Openers & tone scaffolding ---------- */
function warmSofteners(text = "") {
  return text
    .replace(/\bI will\b/g, "I’ll")
    .replace(/\bwe will\b/gi, "we’ll")
    .replace(/\bdo not\b/gi, "don’t")
    .replace(/\bit is\b/gi, "it’s")
    .replace(/\bwe are\b/gi, "we’re");
}

function makeObservationLine(mode = "Professional", sectorHint = "") {
  const base =
    "We’re noticing similar organisations are tightening control of mobile and fixed estates to reduce overspend and delays without reworking what already functions.";
  const withSector = sectorHint ? `${base} In ${sectorHint}, the pattern is similar.` : base;
  return /warm/i.test(mode)
    ? warmSofteners(withSector)
    : withSector;
}

function makeEnhancementLine(mode = "Professional") {
  const line =
    "This is a light operational layer—an orchestration step above your current services—so you keep what works and only adjust what doesn’t.";
  return /warm/i.test(mode) ? warmSofteners(line) : line;
}

function greet(variables = {}, mode = "Professional") {
  const p = (variables.prospect_name || "").trim();
  const seller = (variables.seller_name || "").trim();
  const co = (variables.seller_company || "").trim();
  const who = [seller || "a colleague", co && `from ${co}`].filter(Boolean).join(" ");
  let line = p ? `Hi ${p}, it’s ${who}. ` : `Hi, it’s ${who}. `;
  if (/warm/i.test(mode)) line = warmSofteners(line);
  return line;
}

/* ---------- Narrative composer (no headings; UK) ---------- */
function composeNarrativeFromStages(stages = {}, opts = {}) {
  const v = opts.variables || {};
  const tone = opts.tone || "Professional (corporate)";
  const s = stages;

  // Build paragraphs in a conversational order
  const hello = greet(v, tone);

  const opening =
    (s.opening?.talk_track || s.opening?.script || "").trim() ||
    "Thanks for taking the call—I'll keep this brief and useful.";

  // Observation (no assumptions)
  const observation = makeObservationLine(tone, (opts.form && opts.form.sector) || "");

  // Pains & desires
  const pain = (s.buyer_pain?.talk_track || s.buyer_pain?.script || "").trim();
  const desire = (s.buyer_desire?.talk_track || s.buyer_desire?.script || "").trim() ||
                 "The goal is practical control—clarity on usage, simple provisioning and room to scale without extra admin.";

  // Example + outcomes
  const example = (s.example?.talk_track || s.example?.script || "").trim();
  const proofs = (s.example?.proof_points_text || s.example?.proof_points || []);
  const exampleLine = [example, proofs.length ? `For example: ${proofs.join("; ")}.` : ""]
    .filter(Boolean)
    .join(" ");

  // Enhancement framing (lightweight)
  const lightLayer = makeEnhancementLine(tone);

  // Objections calm
  const objections = (s.objections?.talk_track || s.objections?.script || "").trim();
  const anticipated = (s.objections?.anticipated || []);
  const objectionsLine = [objections, anticipated.length ? `Common concerns we address: ${anticipated.join("; ")}.` : ""]
    .filter(Boolean)
    .join(" ");

  // CTA with two options
  let cta = (s.call_to_action?.talk_track || s.call_to_action?.script || "").trim();
  const ctas = (s.call_to_action?.ctas_text || s.call_to_action?.ctas || []);
  if (!cta && ctas.length) cta = String(ctas[0]);
  cta = ensureCtaTwoOptions(cta);

  // Warm softeners (measured, human) if requested
  const soften = /warm/i.test(tone);
  const paras = [
    hello + (soffen(opening, soften)),
    observation,
    pain,
    desire,
    exampleLine,
    lightLayer,
    objectionsLine,
    cta,
  ].filter(Boolean);

  let text = paras.map(p => sentenceify(stripStageHeadings(soften ? warmSofteners(p) : p))).join("\n\n");
  text = toUk(text);
  return text;
}

function soften(str, doSoften) {
  return doSoften ? warmSofteners(str) : str;
}

/* ---------- Tips (8–10 practical points) ---------- */
function buildSalesTips(tone = "Professional (corporate)") {
  const warm = /warm/i.test(tone);
  const tips = [
    "Open with observed market patterns, not assumptions about their situation.",
    "State relevance in plain language; avoid jargon and US idiom.",
    "Keep sentences concise; allow short pauses for the listener to respond.",
    "Frame your offer as an operational enhancement that preserves what already works.",
    "Use one specific example with clear, measurable outcomes.",
    "Address likely objections calmly and factually; no pressure language.",
    "Ask one clear next step and offer two time options.",
    "Confirm who else should attend and what would make the session valuable.",
    "Note any benchmarks promised and send them promptly after the call.",
    "Close politely; thank them for their time even if they decline.",
  ];
  if (warm) {
    tips.splice(2, 0, "Use a warm, respectful tone, but remain formal and precise.");
    tips.splice(7, 0, "Invite brief input: “Does that reflect what you see your side?”");
  }
  return tips.slice(0, 10);
}

/* ---------- Public API ---------- */
/**
 * generateCallFromLibrary
 * @param {Object} options
 * @param {Object} options.lookup  { product, buyerType, mode }
 * @param {Object} [options.form]  { company, role, sector, scenario, meeting_type }
 * @param {string} [options.tone]  "Professional (corporate)" | "Warm (professional)" | "Warm (relaxed)" | ""
 * @param {number} [options.length] soft word target (e.g., 100)
 * @param {Object} [options.variables] { seller_name, seller_company, prospect_name }
 * @returns {Promise<{stages, meta, buyerNeeds, metaLine, script_text, tips_text, combined_text, source, resolution}>}
 */
export async function generateCallFromLibrary({
  lookup,
  form = {},
  tone = "Professional (corporate)",
  length = 150, // 5-minute target is ~650–750 words; tool usually aims shorter; adjust per UI
  variables = {},
} = {}) {
  // Pull the canonical library record
  const record = await getCallScript(lookup);

  // Compose narrative
  let script = composeNarrativeFromStages(record.stages, { variables, tone, form });

  // Respect requested length softly
  if (Number.isFinite(length) && length > 0) {
    script = trimToWords(script, Math.max(60, Math.min(800, length)));
  }

  // Build tips
  const tips = buildSalesTips(tone);
  const tipsBlock = ["Sales tips for colleagues conducting similar calls", "", ...tips.map(t => `• ${t}`)].join("\n");

  // Combined text if you want one-shot rendering
  const combined = [script.trim(), "", tipsBlock].join("\n");

  return {
    ...record,                 // { meta, stages, buyerNeeds, metaLine, source, resolution }
    script_text: script,       // ready-to-render narrative (no headings)
    tips_text: tipsBlock,      // the 8–10 coaching points
    combined_text: combined,   // script + tips, convenient for <pre>
  };
}
