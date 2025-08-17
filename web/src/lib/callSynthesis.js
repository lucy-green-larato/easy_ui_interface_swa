// /src/lib/callSynthesis.js
// Compose a natural UK B2B cold-call narrative from the curated library.
// Produces: { stages, metaLine, buyerNeeds, script_text, tips_list, source: "library" }

import { getCallScript } from "./callLibrary.js";

// ------- Utilities (lightweight, deterministic) -------

const ukSpelling = (text="") => {
  const map = [
    [/organization(s)?/gi, "organisation$1"],
    [/organize(d|s|r|ing)?/gi, "organise$1"],
    [/optimization/gi, "optimisation"],
    [/optimi(z|s)e(?!d|r|s|ing)/gi, "optimise"],
    [/center(s)?/gi, "centre$1"],
    [/modeling/gi, "modelling"],
    [/license/gi, "licence"],
    [/utilize/gi, "utilise"],
    [/program(s)?/gi, "programme$1"],
    [/behavior/gi, "behaviour"],
    [/advisor(s)?/gi, "adviser$1"],
    [/color/gi, "colour"],
    [/meter(s)?/gi, "metre$1"]
  ];
  return map.reduce((t, [re, rep]) => t.replace(re, rep), String(text || ""));
};

const clean = (s="") => ukSpelling(String(s).replace(/\s+\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim());

const clipToWords = (text="", target=300) => {
  const hardMin = Math.max(120, Math.floor(target * 0.6));
  const hardMax = Math.max(target, Math.floor(target * 1.15));
  const words = text.trim().split(/\s+/);
  if (words.length <= hardMax) return text.trim();
  // Prefer sentence boundary clipping
  const sentences = text.split(/(?<=\.)\s+/);
  let out = "";
  for (const s of sentences) {
    if ((out + " " + s).trim().split(/\s+/).length > hardMax) break;
    out = (out ? out + " " : "") + s;
  }
  if (out.split(/\s+/).length >= hardMin) return out.trim();
  // Fallback: hard word trim
  return words.slice(0, hardMax).join(" ").trim();
};

const asInline = (arr) => Array.isArray(arr) ? arr.filter(Boolean).map(String).join("; ") : "";

const sentence = (s) => {
  const t = String(s || "").trim();
  if (!t) return "";
  return /[.?!]$/.test(t) ? t : `${t}.`;
};

// Subtle, non-salesy weaving of USPs / extra points
function weaveTailoring({ usps="", extras="" }) {
  const usp = String(usps || "").trim();
  const ext = String(extras || "").trim();
  const bits = [];
  if (usp) bits.push(`In practice we focus on the elements that reduce friction fastest — ${usp.replace(/[;,\n]+/g, ", ").replace(/\s+/g, " ")}`);
  if (ext) bits.push(`If it is useful, we can also touch briefly on ${ext.toLowerCase().replace(/[;,\n]+/g, ", ").replace(/\s+/g, " ")}`);
  return bits.length ? bits.map(sentence).join(" ") : "";
}

// ------- Tips (corporate + warm) -------

const TIPS_CORPORATE = [
  "Prepare two sector-relevant patterns you have observed and state them as observations, not diagnoses; invite the prospect to confirm or correct.",
  "Use the prospect’s time carefully: one sentence to introduce yourself and relevance, then move to the value of the conversation.",
  "Avoid assumptions about growth, problems or budgets; use phrases such as “in similar organisations we’ve seen…” and “you may recognise some of this”.",
  "Frame the offer as an operational enhancement that sits above the current estate; emphasise continuity and low disruption before features.",
  "Quote one specific client example with two to four quantified results and a credible time frame; keep the story under thirty seconds.",
  "Handle objections factually and briefly: contracts (phase in at renewal), change (orchestration layer, not a rebuild), and scale (smaller teams benefit from automation).",
  "Maintain a formal, courteous tone; avoid slang and Americanisms; prefer “organisation”, “programme”, “licence”, “tied into”, “deployment”, and “impediment”.",
  "Close with a clear double alternative: offer two precise time options and anchor the value of the session before asking.",
  "Agree attendees while on the call (IT and operations/finance), then email a one-paragraph summary, the agreed option, and the short agenda.",
  "If the prospect is not ready, offer a short toolkit and a two-line summary, and propose a light follow-up date; remain helpful, not insistent."
];

const TIPS_WARM = [
  "Open with one concise observation from similar UK organisations; invite the prospect to react.",
  "Keep language clear, plain and courteous; avoid slang and Americanisms.",
  "Position the offer as a small operational lift above what already works; continuity first, features later.",
  "Use one relatable UK example with measurable outcomes; keep it tight.",
  "Acknowledge common concerns calmly — contracts, change, and team size.",
  "Suggest a practical next step with two time options; confirm attendees.",
  "Follow up with a short note and a useful attachment; keep momentum without pressure.",
  "Match their pace — brisk for brisk, more spacious if they are reflective."
];

// ------- Narrative composer (matches your “good output”) -------

function composeCorporateNarrative(stages, vars) {
  const s = stages || {};
  const pain = clean(s.buyer_pain?.talk_track || s.buyer_pain?.script || "");
  const desire = clean(s.buyer_desire?.talk_track || s.buyer_desire?.script || "");
  const opening = clean(
    "When we speak with operations and technology leaders across the UK, a consistent pattern appears: " +
    (pain || "the pace of change often outstrips what estates were designed to handle.")
  );

  const approachCore = clean(
    "The most effective approach we see is a light-touch management layer that sits across the current mobile and fixed estate. " +
    (desire || "It consolidates administration, brings pooled data to reduce waste, allows bandwidth to flex with demand, and keeps reporting usable for Finance without exporting spreadsheets.") +
    " In practice this is an operational enhancement rather than a change programme: you retain what is working and only adjust what is holding you back."
  );

  const tailoring = weaveTailoring({ usps: vars?.value_proposition, extras: vars?.context });

  const exampleBody = clean(
    s.example?.talk_track || s.example?.script || ""
  );
  const proof = asInline(s.example?.proof_points_text || s.example?.proof_points || []);
  const examplePara = [exampleBody, proof && `In the first ninety days, ${proof}.`]
    .filter(Boolean).map(sentence).join(" ");

  const objections = clean(
    s.objections?.talk_track || s.objections?.script || ""
  );

  const ctaBase = clean(
    s.call_to_action?.talk_track || s.call_to_action?.script || 
    "If the above aligns with the improvements you are seeking, the most useful next step is a short, structured review. We run a forty-five-minute Connectivity Review with your IT and operations leads to produce a usage and cost heat-map, a concise list of savings that can be achieved without disruption, and a tailored pilot that proves return within a quarter."
  );
  const ctaClose = "Would Monday morning suit, or would Wednesday morning be better?";

  // Assemble in the same cadence as your “good output”
  const paragraphs = [
    opening + " " + "I do not know whether any of this applies in your organisation, but it may be useful to share what peers are doing to keep control without rebuilding anything.",
    approachCore + (tailoring ? " " + tailoring : ""),
    examplePara,
    objections,
    ctaBase + " " + (vars?.context ? sentence(`We can include ${String(vars.context).toLowerCase()} if that is a current theme`) : "") + " " + ctaClose
  ];

  return paragraphs.filter(Boolean).join("\n\n").trim();
}

function composeWarmNarrative(stages, vars) {
  // Same structure, a touch more human warmth, but still formal
  const s = stages || {};
  const pain = clean(s.buyer_pain?.talk_track || s.buyer_pain?.script || "");
  const desire = clean(s.buyer_desire?.talk_track || s.buyer_desire?.script || "");
  const opening = clean(
    "Speaking with similar UK organisations, we’re seeing a familiar pattern: " +
    (pain || "the estate hasn’t kept pace with the business.")
  );
  const approachCore = clean(
    "What tends to work best is a light management layer across what you already run. " +
    (desire || "It brings pooled data to cut waste, lets bandwidth scale when needed, and keeps reporting straightforward for Finance.") +
    " It is an operational enhancement, not a rebuild."
  );
  const tailoring = weaveTailoring({ usps: vars?.value_proposition, extras: vars?.context });
  const exampleBody = clean(s.example?.talk_track || s.example?.script || "");
  const proof = asInline(s.example?.proof_points_text || s.example?.proof_points || []);
  const examplePara = [exampleBody, proof && `In the first ninety days, ${proof}.`]
    .filter(Boolean).map(sentence).join(" ");
  const objections = clean(s.objections?.talk_track || s.objections?.script || "");
  const ctaBase = clean(
    s.call_to_action?.talk_track || s.call_to_action?.script ||
    "If that sounds useful, the next step is a forty-five-minute Connectivity Review with IT and operations: usage and cost heat-map, immediate saving opportunities, and a small pilot to prove value."
  );
  const ctaClose = "Would Monday morning work, or would Wednesday be better?";

  const paragraphs = [
    opening + " " + "I can’t speak for your set-up, so treat this as context rather than a diagnosis.",
    approachCore + (tailoring ? " " + tailoring : ""),
    examplePara,
    objections,
    ctaBase + " " + (vars?.context ? sentence(`We can include ${String(vars.context).toLowerCase()} if that is timely`) : "") + " " + ctaClose
  ];
  return paragraphs.filter(Boolean).join("\n\n").trim();
}

// ------- Public API -------

export async function generateCallFromLibrary({ lookup, form, tone="", length=300, variables={} }) {
  // 1) Get the curated record (resolves normalised + legacy paths)
  const lib = await getCallScript(lookup);

  // 2) Compose narrative in the requested tone
const t = String(tone || "").toLowerCase();
const isWarm = t.includes("warm"); // explicit "warm" takes priority
const body = isWarm
  ? composeWarmNarrative(lib.stages, variables)
  : composeCorporateNarrative(lib.stages, variables);

// 3) Length management (soft cap, keeps sentences intact)
const target = Number(length) || 300;
const script_text = clipToWords(body, target);

// 4) Tips pool
const tips_list = (isWarm ? TIPS_WARM : TIPS_CORPORATE).slice();

  return {
    stages: lib.stages,
    metaLine: lib.metaLine,
    buyerNeeds: lib.buyerNeeds,
    script_text,
    tips_list,
    source: "library"
  };
}
