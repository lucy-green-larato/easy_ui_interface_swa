// /src/lib/callSynthesis.js
import { getCallScript } from "./callLibrary.js";

/* ---------- Tone & language utilities (formal UK) ---------- */
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
  [/\brobust\b/gi, "reliable"],
  [/\brollout\b/gi, "roll-out"],
  [/\blocked in\b/gi, "locked-in"],
  [/\bplay out\b/gi, "unfold"],
  [/\bpile-?up\b/gi, "backlog"],
  [/\bflex\b/gi, "adjust"],
  [/\bguys\b/gi, "team"],
];

function toUk(text = "") {
  let out = String(text);
  for (const [re, rep] of REPLACERS) out = out.replace(re, rep);
  // tidy whitespace & spacing
  out = out.replace(/[ ]{2,}/g, " ").replace(/\n{3,}/g, "\n\n").trim();
  // remove shouty punctuation
  out = out.replace(/!+/g, ".");
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
  // If no day/time already present, add the two options
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
  // cut by sentences until under target
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

/* ---------- Narrative composer (no headings, formal UK) ---------- */
function composeNarrativeFromStages(stages = {}, opts = {}) {
  const v = opts.variables || {}; // may include seller_name, seller_company, prospect_name
  const s = stages;

  // Greeting with names if passed by the UI; otherwise neutral
  const hello = (() => {
    const p = (v.prospect_name || "").trim();
    const seller = (v.seller_name || "").trim();
    const co = (v.seller_company || "").trim();
    if (p || seller || co) {
      const who = [seller || "a colleague", co && `from ${co}`].filter(Boolean).join(" ");
      return p ? `Hi ${p}, it’s ${who}. ` : `Hi, it’s ${who}. `;
    }
    return "Hello, thanks for taking the call. ";
  })();

  const opening =
    (s.opening?.talk_track || s.opening?.script || "").trim() ||
    "I’ll keep this brief and useful.";

  const pain = (s.buyer_pain?.talk_track || s.buyer_pain?.script || "").trim();
  const desire = (s.buyer_desire?.talk_track || s.buyer_desire?.script || "").trim();

  const example = (s.example?.talk_track || s.example?.script || "").trim();
  const proofs = (s.example?.proof_points_text || s.example?.proof_points || []);
  const exampleLine = [example, proofs.length ? `For example: ${proofs.join("; ")}.` : ""]
    .filter(Boolean)
    .join(" ");

  const objections = (s.objections?.talk_track || s.objections?.script || "").trim();
  const anticipated = (s.objections?.anticipated || []);
  const objectionsLine = [objections, anticipated.length ? `Common concerns we handle: ${anticipated.join("; ")}.` : ""]
    .filter(Boolean)
    .join(" ");

  let cta = (s.call_to_action?.talk_track || s.call_to_action?.script || "").trim();
  const ctas = (s.call_to_action?.ctas_text || s.call_to_action?.ctas || []);
  if (!cta && ctas.length) cta = String(ctas[0]);
  cta = ensureCtaTwoOptions(cta);

  // Join paragraphs
  const paras = [
    hello + opening,
    pain,
    desire,
    exampleLine,
    objectionsLine,
    cta,
  ].filter(Boolean);

  // Clean up, UK normalise, de-heading, sentence tidy
  let text = paras.map(p => sentenceify(stripStageHeadings(p))).join("\n\n");
  text = toUk(text);
  return text;
}

/* ---------- Public API ---------- */
/**
 * generateCallFromLibrary
 * @param {Object} options
 * @param {Object} options.lookup  { product, buyerType, mode }
 * @param {Object} [options.form]  { company, role, scenario, meeting_type }  // optional context
 * @param {string} [options.tone]  "Professional (corporate)" | "Warm (relaxed)" | ""
 * @param {number} [options.length] soft word target (e.g., 100)
 * @param {Object} [options.variables] optional names for greeting { seller_name, seller_company, prospect_name }
 * @returns {Promise<{stages, metaLine, buyerNeeds, script_text, source, resolution}>}
 */
export async function generateCallFromLibrary({
  lookup,
  form = {},
  tone = "Professional (corporate)",
  length = 120,
  variables = {},
} = {}) {
  // Pull the canonical library record (with meta, stages and buyerNeeds)
  const record = await getCallScript(lookup);

  // Compose a narrative in the requested tone (rules-based, no headings)
  let script = composeNarrativeFromStages(record.stages, { variables, tone });

  // Formal tone is default; for "Warm" we make minimal softeners
  if (/warm/i.test(tone)) {
    script = script
      .replace(/\bI will\b/g, "I’ll")
      .replace(/\bwe will\b/gi, "we’ll")
      .replace(/\bdo not\b/gi, "don’t");
  }

  // Respect requested length as a soft limit
  if (Number.isFinite(length) && length > 0) {
    script = trimToWords(script, Math.max(40, Math.min(600, length)));
  }

  return {
    ...record,                 // { meta, stages, buyerNeeds, metaLine, source, resolution }
    script_text: script,       // ready-to-render narrative (no headings)
  };
}
