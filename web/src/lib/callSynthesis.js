// /src/lib/callSynthesis.js
// Synthesis that turns library stages + form variables into a UK-ready talk track.

import { getCallScript } from "./callLibrary.js";

/* ------------------------------ Utilities ------------------------------ */

const toArray = (v) => {
  if (!v) return [];
  if (Array.isArray(v)) return v.map(String).map(s => s.trim()).filter(Boolean);
  return String(v)
    .split(/\r?\n|[;,]/g)
    .map(s => s.trim())
    .filter(Boolean);
};

const joinNatural = (arr) => {
  const a = toArray(arr);
  if (a.length === 0) return "";
  if (a.length === 1) return a[0];
  if (a.length === 2) return `${a[0]} and ${a[1]}`;
  return `${a.slice(0, -1).join(", ")} and ${a[a.length - 1]}`;
};

function toUkEnglish(text) {
  if (!text) return "";
  const map = [
    [/\borganization(s)?\b/gi, "organisation$1"],
    [/\borganize(d|s|r|ing)?\b/gi, "organise$1"],
    [/\boptimization(s)?\b/gi, "optimisation$1"],
    [/\boptimi(z|s)e\b/gi, "optimise"],
    [/\bcenter(s)?\b/gi, "centre$1"],
    [/\bmodeling\b/gi, "modelling"],
    [/\blicense(s|d|r)?\b/gi, "licence$1"],
    [/\bprogram(me)?\b/gi, "programme"],
    [/\bblocker(s)?\b/gi, "impediment$1"],
    [/\broll ?out(s|ed|ing)?\b/gi, "deployment$1"],
    [/\bplay out\b/gi, "develop"],
    [/\bpile-?up(s)?\b/gi, "backlog$1"],
    [/\bflex\b/gi, "adapt"],
  ];
  let out = text;
  for (const [re, rep] of map) out = out.replace(re, rep);
  return out;
}

function sentenceClean(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function sentencesFrom(text) {
  // Split on sentence boundaries but keep initials, numbers etc. simple.
  return String(text || "")
    .replace(/\n{2,}/g, "\n")
    .split(/(?<=[.!?])\s+(?=[A-Z0-9“"])/)
    .map(sentenceClean)
    .filter(Boolean);
}

function clipToWords(text, targetWords) {
  const sents = sentencesFrom(text);
  if (!targetWords || targetWords <= 0) return sents.join(" ");
  const tgt = Math.max(60, Number(targetWords));
  const out = [];
  let count = 0;
  for (const s of sents) {
    const words = s.split(/\s+/).filter(Boolean).length;
    if (count && count + words > tgt * 1.15) break; // soft cap ~+15%
    out.push(s);
    count += words;
  }
  return out.join(" ");
}

/* ------------------------------ Tips ------------------------------ */

// Prioritised tips (we will guarantee at least one of the first three appears)
const TIPS_PRIORITY = [
  "Be concise. In the UK, long introductions lose people. Lead with why you are calling.",
  "Do not oversell. Understatement carries credibility; let facts and examples do the work.",
  "Use relatable UK cases. A story about a firm across five UK cities lands better than a generic global example.",
];

const TIPS_EXTRA = [
  "Match their pace. If the prospect is brisk, keep tight. If they are more conversational, give space.",
  "End with a clear, polite next step. Suggest specific times rather than leaving it open-ended.",
  "Open with value, not features. Busy decision-makers stay engaged when you show you understand their reality.",
  "Use credible stats sparingly. One or two strong numbers build authority — a flood of data feels like a lecture.",
  "Always anchor with a story. Prospects remember relatable examples more than abstract claims.",
  "Anticipate objections before they are raised; it shows confidence and saves time.",
  "Keep the CTA practical and time-bounded; propose a clear, valuable next step.",
  "Tone matters. Use clear, confident British English; adjust warmth and pace to the prospect’s energy.",
];

function pickTips(isWarm) {
  // Ensure at least one priority tip, then fill to 3 total.
  const out = [];
  // 1 from priority
  out.push(TIPS_PRIORITY[0]);
  // 2 from the remaining pool (shuffle-light)
  const pool = [...TIPS_PRIORITY.slice(1), ...TIPS_EXTRA];
  for (let i = pool.length - 1; i > 0; i--) {
    const j = (Math.random() * (i + 1)) | 0; [pool[i], pool[j]] = [pool[j], pool[i]];
  }
  for (const tip of pool) {
    if (out.length >= 3) break;
    if (!out.includes(tip)) out.push(tip);
  }
  return out.slice(0, 3);
}

/* ------------------------------ Composition helpers ------------------------------ */

function marketPatterns(painsArr) {
  const pains = toArray(painsArr);
  if (pains.length === 0) return "";
  const some = pains.slice(0, 3);
  return `In similar organisations we are seeing issues such as ${joinNatural(some).toLowerCase()}—usually without changing what already works.`;
}

function exampleLine(exampleStage) {
  const ex = sentenceClean(exampleStage?.talk_track || exampleStage?.script || "");
  const proofs = toArray(exampleStage?.proof_points_text || exampleStage?.proof_points);
  if (!ex && proofs.length === 0) return "";
  const proofClause = proofs.length ? ` In the first ninety days: ${joinNatural(proofs).replace(/%/g, " per cent")}.` : "";
  return `${ex}${proofClause}`;
}

function objectionsLine(objectionsStage) {
  const base = sentenceClean(objectionsStage?.talk_track || objectionsStage?.script || "");
  const anticip = toArray(objectionsStage?.anticipated);
  const extra = anticip.length ? ` Common concerns we handle include ${joinNatural(anticip).toLowerCase()}.` : "";
  return `${base}${extra}`;
}

function buildGreeting(v) {
  const prospect = (v.prospect_name || "").trim();
  const seller = (v.seller_name || "").trim();
  const company = (v.seller_company || "").trim();

  // Gentle contextual nods (e.g., "back from holiday")
  const ctx = toArray(v.context).map(s => s.toLowerCase());
  const holiday = ctx.find(x => x.includes("back from holiday") || x.includes("holiday"));

  const who = seller ? (company ? `${seller} from ${company}` : seller) : (company ? `a colleague from ${company}` : "a colleague");
  const greet = prospect ? `Hi ${prospect}, it’s ${who}.` : `Hi, it’s ${who}.`;
  const grace = holiday ? " I hope the return from holiday has not left you with too much to catch up on." : "";

  return `${greet}${grace} Thanks for taking the call — I will keep this brief and useful.`;
}

function tailoringNotes(v) {
  const usps = toArray(v.value_proposition);
  const ctx = toArray(v.context);
  const left = usps.length ? `From our side, the areas most likely to add value are ${joinNatural(usps).toLowerCase()}.` : "";
  const right = ctx.length ? ` If useful, we can also touch on ${joinNatural(ctx).toLowerCase()}; otherwise I will keep to immediate priorities.` : "";
  return (left || right) ? (left + right).trim() : "";
}

/* ------------------------------ Corporate (formal) ------------------------------ */

function composeCorporateNarrative(stages, v) {
  const s = stages || {};
  const intro = buildGreeting(v);

  const patterns = marketPatterns(s.buyer_pain?.buyer_needs_summary || [
    "ad-hoc SIM spend without central oversight",
    "long fixed-term lines",
    "provisioning delays across deployments",
    "multiple suppliers with differing SLAs",
  ]);

  const relevance = "I do not know whether any of that applies in your organisation; it may be useful to share how peers are keeping control without rebuilding anything.";
  const desire = sentenceClean(s.buyer_desire?.talk_track || s.buyer_desire?.script ||
    "A light management layer sits across the current mobile and fixed estate, consolidates administration, enables pooled mobile data, and allows bandwidth to scale on demand. Reporting is clear enough for Finance to use without exporting spreadsheets. This is an operational enhancement rather than a change programme: you retain what works and adjust only what does not.");
  const tailor = tailoringNotes(v);
  const example = exampleLine(s.example);
  const objections = objectionsLine(s.objections);
  const cta = sentenceClean(
    s.call_to_action?.talk_track ||
    s.call_to_action?.script ||
    "If the above aligns with what you are seeking, the practical next step is a forty-five-minute Connectivity Review with your IT and operations leads. Would Monday morning suit, or would Wednesday morning be better?"
  );

  const blocks = [intro, patterns, relevance, desire, tailor, example, objections, cta]
    .map(toUkEnglish)
    .filter(Boolean);

  // Optional discreet section markers for readability in the UI
  const labelled = [
    ["— Opening —", blocks[0]],
    ["— Market patterns —", blocks[1]],
    ["— Relevance —", blocks[2]],
    ["— Approach —", blocks[3]],
    tailor && ["— Tailoring notes —", blocks[4]],
    ["— Example —", example ? toUkEnglish(example) : ""],
    ["— Reassurance —", objections ? toUkEnglish(objections) : ""],
    ["— Next step —", toUkEnglish(cta)],
  ].filter(Boolean).map(([h, t]) => `${h}\n${t}`);

  return labelled.join("\n\n");
}

/* ------------------------------ Warm (professional, human) ------------------------------ */

function composeWarmNarrative(stages, v) {
  const s = stages || {};
  const intro = buildGreeting(v);

  const patterns = marketPatterns(s.buyer_pain?.buyer_needs_summary || [
    "ad-hoc SIM spend",
    "fixed contracts that are slow to change",
    "delays in provisioning that hold up teams",
  ]);

  const relevance = "None of this may be live for you; I can share briefly what has helped similar teams keep things moving without disruption.";
  const desire = sentenceClean(s.buyer_desire?.talk_track || s.buyer_desire?.script ||
    "A light layer over the current mobile and fixed services brings pooled data, simpler changes, and reporting that Finance and IT can both use. It is about pace and control rather than a rebuild.");
  const tailor = tailoringNotes(v);
  const example = exampleLine(s.example);
  const objections = objectionsLine(s.objections);
  const cta = "If that sounds useful, the next step is a short, structured review with your IT and ops leads — forty-five minutes. Would Monday or Wednesday morning work best?";

  const blocks = [intro, patterns, relevance, desire, tailor, example, objections, cta]
    .map(toUkEnglish)
    .filter(Boolean);

  const labelled = [
    ["— Opening —", blocks[0]],
    ["— Market patterns —", blocks[1]],
    ["— Relevance —", blocks[2]],
    ["— Approach —", blocks[3]],
    tailor && ["— Tailoring notes —", blocks[4]],
    ["— Example —", example ? toUkEnglish(example) : ""],
    ["— Reassurance —", objections ? toUkEnglish(objections) : ""],
    ["— Next step —", toUkEnglish(cta)],
  ].filter(Boolean).map(([h, t]) => `${h}\n${t}`);

  return labelled.join("\n\n");
}

/* ------------------------------ Main entry ------------------------------ */

export async function generateCallFromLibrary({ lookup, form, tone, length, variables }) {
  // 1) Resolve library content
  const lib = await getCallScript(lookup);

  // 2) Compose narrative in the requested tone
  const t = String(tone || "").toLowerCase();
  const isWarm = t.includes("warm"); // anything else -> corporate
  const body = isWarm
    ? composeWarmNarrative(lib.stages, variables || {})
    : composeCorporateNarrative(lib.stages, variables || {});

  // 3) Length management
  const target = Number(length) || 300;
  const script_text = clipToWords(body, target);

  // 4) Tips (exactly three)
  const tips_list = pickTips(isWarm);

  return {
    script_text,
    tips_list,
    stages: lib.stages,
    metaLine: lib.metaLine,
    buyerNeeds: lib.buyerNeeds || {},
