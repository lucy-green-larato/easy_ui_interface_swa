// File: /src/lib/callSynthesis.js
// Browser ESM. Sits next to callLibrary.js and imports it.
import { getCallScript } from "./callLibrary.js";

/** @typedef {"professional"|"warm"|"neutral"} Tone */

/* ========= Style guide (your exact intent) ========= */
const STYLE_GUIDE =
  "Empathy and Value First — Start with empathy, show understanding of the buyer’s reality; focus on value delivered, not features; stay conversational and credible in British English; be concise; always prefer proof/outcomes; include small sector- or role-relevant touches if provided";

/* ========= Tone + duration helpers ========= */
const canonTone = (t = "") => {
  const x = String(t).toLowerCase().trim();
  if (/^(pro|corporate|formal)/.test(x)) return "professional";
  if (/^(warm|friendly|relaxed|casual)/.test(x)) return "warm";
  return "neutral";
};

// Words-per-minute guidance (we target ~75% for think-time)
const WPM = 140;
function resolveLengthPref(pref) {
  const s = String(pref || "").toLowerCase();
  const m = s.match(/(\d+)\s*m/);
  if (m) {
    const minutes = Math.max(1, parseInt(m[1], 10));
    const targetWords = Math.round(minutes * WPM * 0.75);
    const questions = minutes <= 2 ? [2,3] : minutes <= 4 ? [3,5] :
                      minutes <= 6 ? [5,7] : [6,9];
    const proofCount = minutes >= 5 ? 2 : 1;
    const recap = minutes >= 3;
    const followups = minutes >= 5 ? 2 : 1;
    return { targetWords, questions, proofCount, recap, followups };
  }
  // Accept "~150 words"
  const num = parseInt(s.replace(/[^\d]/g, ''), 10) || 100;
  const questions = num <= 120 ? [2,3] : num <= 300 ? [3,5] :
                    num <= 700 ? [5,7] : [6,9];
  const proofCount = num >= 550 ? 2 : 1;
  const recap = num >= 250;
  const followups = num >= 350 ? 2 : 1;
  return { targetWords: num, questions, proofCount, recap, followups };
}

/* ========= Shared proof/CTA resolution ========= */
const byId = (arr = []) => {
  const map = new Map();
  for (const item of arr) if (item && typeof item === "object" && item.id) map.set(item.id, item);
  return map;
};
function resolveRefs(idsOrText = [], table = new Map()) {
  const out = [];
  for (const x of idsOrText) {
    if (!x) continue;
    if (typeof x === "string" && table.has(x)) {
      const t = table.get(x)?.text || "";
      if (t) out.push(t);
    } else if (typeof x === "string") {
      if (!/^pp_|^cta_/i.test(x)) out.push(x); // avoid raw IDs
    } else if (typeof x === "object" && x.text) {
      out.push(x.text);
    }
  }
  return out;
}

/* ========= Facts pack from curated record ========= */
function factsFromLibrary(rec, form) {
  const s = rec?.stages || {};
  const sharedPP = byId(rec?.shared?.proof_points || []);
  const sharedCT = byId(rec?.shared?.ctas || []);

  const proofStage = s.example?.proof_points_text || s.example?.proof_points || [];
  const ctasStage  = s.call_to_action?.ctas_text   || s.call_to_action?.ctas   || [];

  return {
    product: rec?.meta?.product_label || rec?.meta?.product_id || "",
    buyerType: rec?.meta?.buyerType || "",
    salesMode: rec?.meta?.sales_mode || "",
    opening: (s.opening?.buyer_needs_summary || []).join(", "),
    pain: (s.buyer_pain?.buyer_needs_summary || []).join(", "),
    desire: (s.buyer_desire?.buyer_needs_summary || []).join(", "),
    proofPoints: resolveRefs(proofStage, sharedPP),
    objections: resolveRefs(s.objections?.anticipated || s.objections?.buyer_needs_summary || []),
    ctas: resolveRefs(ctasStage, sharedCT),
    form: {
      seller_name:     form?.seller_name     || "",
      seller_company:  form?.seller_company  || "",
      prospect_name:   form?.prospect_name   || "",
      company:         form?.company         || "",
      role:            form?.role            || "",
      sector:          form?.sector          || "",
      size:            form?.company_size    || "",
      scenario:        form?.scenario        || "",
      meeting_type:    form?.meeting_type    || "first call"
    }
  };
}

/* ========= Conversational utilities ========= */
const englishList = (arr = [], n = 3) => {
  const items = arr.filter(Boolean).slice(0, n);
  if (items.length <= 1) return items.join("");
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(", ")}, and ${items[items.length - 1]}`;
};

function makeGreeting({ prospect, seller, sellerCo, tone }) {
  const hello = tone === "professional" ? `Hello ${prospect || "there"},` : `Hi ${prospect || "there"},`;
  const from  = tone === "professional" ? `it's ${seller || "a colleague"} from ${sellerCo || "our team"}.` : `it’s ${seller || "a colleague"} at ${sellerCo || "our side"}.`;
  const check = tone === "professional" ? `Is now still a good time?` : `Have you got a minute?`;
  return `${hello} ${from} ${check}`;
}

// Tiny context niceties
function smallTalkFromContext(context = "", tone) {
  const c = context.toLowerCase();
  const polite = (pro, warm) => (tone === "professional" ? pro : warm);

  if (/\b(back|just\s+back)\s+from\s+(holiday|leave|vacation)\b/.test(c) || /\bholiday\b/.test(c)) {
    return polite("I hope you had a good holiday.", "Hope you had a good holiday.");
  }
  if (/\bcongrat(ulation|s)|promoted|promotion|award|shortlist(ed)?\b/.test(c)) {
    return polite("Congratulations on the recent news.", "Congrats on the good news!");
  }
  if (/\b(conf(erence)?|summit|expo|event)\b/.test(c)) {
    return polite("How did the event go?", "How was the event?");
  }
  if (/\btravel|flight|train|back\s+in\s+the\s+office\b/.test(c)) {
    return polite("I hope the travel wasn’t too painful.", "Hope the travel wasn’t too painful.");
  }
  return "";
}

/* ========= Checklist builder ========= */
function buildChecklist(facts) {
  return {
    prospect_name: facts.form.prospect_name || "",
    prospect_role: facts.form.role || "",
    company: facts.form.company || "",
    buyer_type: facts.buyerType || "",
    pains_discussed: [],             // to capture during/after call
    business_goals: [],              // "
    case_study_used: "",             // "
    objections: [],                  // "
    next_step: "",                   // "
    followup_resources: []           // "
  };
}

function checklistAsText(chk) {
  const lines = [
    "— Checklist (for you) —",
    `Prospect: ${chk.prospect_name || "—"} (${chk.prospect_role || "—"}), ${chk.company || "—"}`,
    `Buyer type: ${chk.buyer_type || "—"}`,
    `Main pains: ${chk.pains_discussed?.join("; ") || "—"}`,
    `Business goals: ${chk.business_goals?.join("; ") || "—"}`,
    `Case/example shared: ${chk.case_study_used || "—"}`,
    `Objections: ${chk.objections?.join("; ") || "—"}`,
    `Agreed next step: ${chk.next_step || "—"}`,
    `Follow-up resources: ${chk.followup_resources?.join("; ") || "—"}`
  ];
  return lines.join("\n");
}

/* ========= Rule-based generator (no headings; empathy/value-first; greeting + nicety) ========= */
function ruleBased(rec, facts, tone, preset) {
  const { questions = [3,5], proofCount = 1, recap = true, followups = 1 } = preset || {};
  const listN = proofCount === 2 ? 4 : 3;

  const intro =
    tone === "professional"
      ? "Thanks for making the time."
      : "Thanks for taking a moment today.";

  const frame =
    tone === "professional"
      ? "I’ll keep this focused on outcomes and whether there’s a quick win."
      : "I’ll keep this practical and look for a quick win.";

  const role   = facts.form.role?.trim();
  const sector = facts.form.sector?.trim();
  const touch = role && sector
    ? `Given you’re a ${role} in ${sector}, `
    : role
    ? `Given your role as ${role}, `
    : sector
    ? `In ${sector}, `
    : "";

  const org = facts.form.company || "your organisation";
  const openingNeeds = englishList((facts.opening || "").split(",").map(s => s.trim()).filter(Boolean), listN);
  const pains        = englishList((facts.pain || "").split(",").map(s => s.trim()).filter(Boolean), listN);
  const desires      = englishList((facts.desire || "").split(",").map(s => s.trim()).filter(Boolean), listN);
  const pp           = englishList(facts.proofPoints, Math.min(proofCount, (facts.proofPoints || []).length) || 1);
  const ctas         = englishList(facts.ctas, 2);

  const greet = makeGreeting({
    prospect: facts.form.prospect_name,
    seller: facts.form.seller_name,
    sellerCo: facts.form.seller_company,
    tone
  });
  const smalltalk = smallTalkFromContext(facts.form.scenario || "", tone);

  const p0 = [greet, smalltalk].filter(Boolean).join(" ");
  const p1 = [
    `${intro} ${frame}`,
    openingNeeds ? `${touch}we tend to see priorities like ${openingNeeds}.` : (touch ? touch.trim() : "")
  ].filter(Boolean).join(" ");

  const p2 = pains
    ? `Where it often bites is ${pains}. Does that mirror what you’re seeing at ${org}?`
    : `Before we dive in, where does connectivity pinch most at ${org}?`;

  const p3 = desires
    ? `What good looks like is ${desires}. If we validated one thing quickly, what would help your ${role || "team"} most?`
    : `If we proved one thing quickly, what would help your ${role || "team"} most?`;

  const p3a = followups >= 1 ? `Where are the biggest swings—sites, roles, or suppliers?` : "";
  const p3b = followups >= 2 ? `If we could remove one blocker this quarter, which would move the dial most?` : "";

  const p4 = pp
    ? `Peers have achieved ${pp}. We’d take a phased pilot so you can measure outcomes without disruption.`
    : `We usually start with a phased pilot so you can measure outcomes without disruption.`;

  const p5recap = recap ? `From what you’ve said, it sounds like we should validate the hotspots first, then scale if it stacks up.` : "";

  const p5 = facts.objections?.length
    ? `Typical concerns are ${englishList(facts.objections, 3)}. We handle that with a staged pilot, keep existing numbers and hardware, and make contracts portable.`
    : `If there are concerns, we de-risk with a staged pilot, keep existing numbers and hardware, and make contracts portable.`;

  const p6 = ctas
    ? `A sensible next step is ${ctas}. Would next week work to set that up?`
    : `A sensible next step is a short review to baseline usage and costs and line up a small pilot. Would next week work?`;

  const paras = [p0, p1, p2, p3, p3a, p3b, p4, p5recap, p5, p6].filter(Boolean);

  return {
    stages: {
      opening:        { talk_track: paras[0] || "" },
      buyer_pain:     { talk_track: paras[2] || "" },
      buyer_desire:   { talk_track: [paras[3], paras[4], paras[5]].filter(Boolean).join(" ") },
      example:        { talk_track: paras[6] || "" },
      objections:     { talk_track: [paras[7], paras[8]].filter(Boolean).join(" ").trim() },
      call_to_action: { talk_track: paras[9] || "" }
    },
    meta: { tone, source: "generated-from-library" },
    checklist: buildChecklist(facts),
    checklist_text: checklistAsText(buildChecklist(facts)) // optional for easy rendering
  };
}

/* ========= Main: model first (if provided), else rules ========= */
export async function generateCallFromLibrary({ lookup, form, tone, length, generate }) {
  const rec = await getCallScript(lookup);

  const t = canonTone(tone);
  const facts = factsFromLibrary(rec, form);
  const preset = resolveLengthPref(length);

  if (typeof generate === "function") {
    const system = [
      "You are Larato’s B2B calling assistant.",
      STYLE_GUIDE,
      "Write a first-call SCRIPT using the 6-step Larato method (Opening, Buyer Pain, Buyer Desire, Example, Objections, Call To Action).",
      "Important output rules:",
      "- Start with a natural greeting that uses names and the seller’s company, e.g., ‘Hello Robert, it’s Lucy from Larato. Is now still a good time?’",
      "- If the context suggests a nicety (e.g., holiday), add ONE short line after the greeting (e.g., ‘Hope you had a good holiday.’).",
      "- Do NOT include headings or numbered step labels.",
      "- Do NOT output bullets; write as short conversational paragraphs.",
      "- NEVER echo internal IDs (e.g., pp_* or cta_*). Use natural language only.",
      "- British English; businesslike for professional tone; friendly and plain for warm tone.",
      `- Use ${preset.questions[0]}–${preset.questions[1]} short questions across the whole script (not one per step).`,
      `- Keep total length around ${preset.targetWords} words (±15%).`,
      "- Use only the library facts provided; if something is missing, stay generic but plausible.",
      // >>> Include the checklist in the JSON output <<<
      `Return JSON only: {"stages":{"opening":{"talk_track":""},"buyer_pain":{"talk_track":""},"buyer_desire":{"talk_track":""},"example":{"talk_track":""},"objections":{"talk_track":""},"call_to_action":{"talk_track":""}},"meta":{"tone":"","source":"generated-from-library"},"checklist":{"prospect_name":"","prospect_role":"","company":"","buyer_type":"","pains_discussed":[],"business_goals":[],"case_study_used":"","objections":[],"next_step":"","followup_resources":[]}}`
    ].join("\n");

    const user = [
      `TONE: ${t}`,
      `TARGET_LENGTH_WORDS: ${preset.targetWords}`,
      `PRODUCT: ${facts.product}`,
      `BUYER TYPE: ${facts.buyerType}`,
      `SALES MODE: ${facts.salesMode}`,
      `FORM: seller_name=${facts.form.seller_name} seller_company=${facts.form.seller_company} prospect_name=${facts.form.prospect_name} prospect_company=${facts.form.company} role=${facts.form.role} sector=${facts.form.sector} size=${facts.form.size} scenario=${facts.form.scenario} meeting=${facts.form.meeting_type}`,
      "",
      "LIBRARY FACTS (paraphrase naturally; include small role/sector touches when relevant):",
      `OPENING NEEDS: ${facts.opening}`,
      `PAINS: ${facts.pain}`,
      `DESIRES: ${facts.desire}`,
      `PROOF POINTS (human text): ${facts.proofPoints.join(" | ")}`,
      `OBJECTIONS: ${facts.objections.join(" | ")}`,
      `CTAS (human text): ${facts.ctas.join(" | ")}`,
      "",
      "REMINDER: Begin with a greeting line using names/company, then (if relevant) one short nicety from context. No headings/bullets."
    ].join("\n");

    try {
      const raw = await generate([
        { role: "system", content: system },
        { role: "user", content: user }
      ]);
      const jsonStr = String(raw).replace(/```json|```/g, "").trim();
      const data = JSON.parse(jsonStr);

      const stages = data?.stages || {};
      for (const k of ["opening","buyer_pain","buyer_desire","example","objections","call_to_action"]) {
        if (!stages[k]) stages[k] = { talk_track: "" };
        if (typeof stages[k].talk_track !== "string") stages[k].talk_track = String(stages[k].talk_track || "");
      }

      const checklist = data?.checklist || buildChecklist(facts);

      return {
        stages,
        meta: { ...(data.meta || {}), tone: t, source: "generated-from-library" },
        metaLine: `${rec.metaLine} · Tone: ${t} · Source: model`,
        baseMeta: rec.meta,
        buyerNeeds: rec.buyerNeeds,
        checklist,
        checklist_text: checklistAsText(checklist),
        source: "generated-from-library"
      };
    } catch {
      // fall through to rules
    }
  }

  // Rules
  const data = ruleBased(rec, facts, t, preset);
  return {
    ...data,
    metaLine: `${rec.metaLine} · Tone: ${t} · Source: rules`,
    baseMeta: rec.meta,
    buyerNeeds: rec.buyerNeeds,
    source: "generated-from-library"
  };
}
