// File: /src/lib/callSynthesis.js
// Browser ESM. Sits next to callLibrary.js and imports it.
import { getCallScript } from "./callLibrary.js";

/** @typedef {"professional"|"warm"|"neutral"} Tone */

/** Canonicalise tone labels from UI ("Professional (corporate)" / "Warm (relaxed)" / blank) */
const canonTone = (t) => {
  const x = String(t || "").toLowerCase();
  if (x.startsWith("pro")) return "professional";
  if (x.startsWith("warm")) return "warm";
  return "neutral";
};

/** Pick how much content to surface based on target length */
function pickBudget(length) {
  const n = Number(length || 100);
  if (n <= 70) return { list: 2, askShort: true };
  if (n >= 140) return { list: 4, askShort: false };
  return { list: 3, askShort: false };
}

/** Build a compact facts pack from the curated library record */
function factsFromLibrary(rec, form) {
  const s = rec?.stages || {};
  const proof = s.example?.proof_points_text || s.example?.proof_points || [];
  const ctas  = s.call_to_action?.ctas_text || s.call_to_action?.ctas || [];
  return {
    product: rec?.meta?.product_label || rec?.meta?.product_id || "",
    buyerType: rec?.meta?.buyerType || "",
    salesMode: rec?.meta?.sales_mode || "",
    opening: (s.opening?.buyer_needs_summary || []).join(" · "),
    pain: (s.buyer_pain?.buyer_needs_summary || []).join(" · "),
    desire: (s.buyer_desire?.buyer_needs_summary || []).join(" · "),
    proofPoints: Array.isArray(proof) ? proof.filter(Boolean) : [],
    objections: Array.isArray(s.objections?.anticipated) ? s.objections.anticipated.filter(Boolean) : (s.objections?.buyer_needs_summary || []),
    ctas: Array.isArray(ctas) ? ctas.filter(Boolean) : (s.call_to_action?.buyer_needs_summary || []),
    form: {
      company: form?.company || "",
      role: form?.role || "",
      sector: form?.sector || "",
      size: form?.company_size || "",
      scenario: form?.scenario || "",
      meeting_type: form?.meeting_type || "first call"
    }
  };
}

/** Simple sentence helpers */
const q = (s) => (s.endsWith("?") ? s : s.replace(/[.]+$/,"") + "?");
const dot = (s) => (s.endsWith(".") || s.endsWith("?")) ? s : s + ".";

/** Minimal deterministic fallback (works offline / without any API), with length shaping */
function ruleBased(rec, facts, tone, length) {
  const budget = pickBudget(length);
  const soften = tone === "warm"
    ? "Thanks for taking a moment today."
    : "Appreciate your time.";
  const style  = tone === "professional"
    ? "Let’s stay outcome-focused."
    : "Let’s keep this practical.";
  const org = facts.form.company || "your organisation";

  const list = (arr) => (arr || []).slice(0, budget.list).join("; ");
  const ask1 = budget.askShort ? "How does this show up day to day?" : `How does this show up at ${org}?`;
  const ask2 = budget.askShort ? "Where does it bite most?" : "Where does this bite most—field teams, new sites, or finance visibility?";
  const ask3 = budget.askShort ? "Which outcomes matter first?" : "Which two outcomes would matter most in the next quarter?";
  const ask4 = budget.askShort ? "Would a light pilot help?" : "Would a similar phased approach work here?";
  const ask5 = budget.askShort ? "What would build confidence?" : "What would you need to feel confident?";
  const ask6 = budget.askShort ? "Would next week suit?" : "Would a brief session next week suit?";

  return {
    stages: {
      opening: { talk_track:
        dot(`${soften} ${style} We often hear: ${facts.opening || "pressure on cost and productivity with limited visibility."} ${q(ask1)}`)
      },
      buyer_pain: { talk_track:
        dot(`Common pressure points: ${list((facts.pain || "").split(" · ").filter(Boolean)) || "cost leakage, slow sites, out-of-bundle charges"}. ${q(ask2)}`)
      },
      buyer_desire: { talk_track:
        dot(`Teams typically want: ${list((facts.desire || "").split(" · ").filter(Boolean)) || "measurable improvements, clear controls, simple reporting"}. ${q(ask3)}`)
      },
      example: { talk_track:
        facts.proofPoints?.length
          ? dot(`For context, peers achieved: ${list(facts.proofPoints)} ${q(ask4)}`)
          : dot(`We usually start with a small, low-risk pilot to prove value. ${q(ask4)}`)
      },
      objections: { talk_track:
        (facts.objections?.length
          ? dot(`Usual concerns: ${list(facts.objections)} We mitigate with staged pilots and portability—${q(ask5)}`)
          : dot(`If there are concerns, we mitigate with a phased pilot and clear exit—${q(ask5)}`))
      },
      call_to_action: { talk_track:
        (facts.ctas?.length
          ? dot(`Next sensible step: ${list(facts.ctas)} ${q(ask6)}`)
          : dot(`A short review to baseline today and highlight quick wins is a safe next step. ${q(ask6)}`))
      }
    },
    meta: { tone, source: "generated-from-library" }
  };
}

/**
 * Generate a call script from curated content.
 * Pass an optional `generate(messages)->text` that returns a JSON string when using a server LLM.
 */
export async function generateCallFromLibrary({ lookup, form, tone, length, generate }) {
  // 1) Pull curated record (uses your existing callLibrary.js)
  const rec = await getCallScript(lookup);

  // 2) Build facts for prompting
  const t = canonTone(tone);
  const facts = factsFromLibrary(rec, form);

  // 3) If an LLM generator is supplied, call it; otherwise use the rules
  if (typeof generate === "function") {
    const system = [
      "You are Larato’s B2B calling assistant.",
      "Write a concise first-call SCRIPT using the 6-step Larato method:",
      "1) Opening 2) Buyer Pain 3) Buyer Desire 4) Example 5) Objections 6) Call To Action.",
      "Constraints:",
      "- British English; empathy & value-first; adapt to requested tone; do NOT copy verbatim.",
      "- 2–4 sentences per step; include 1–2 short questions where helpful.",
      `- Keep total length around ${Number(length || 100)} words (±15%).`,
      "- Use only the library facts provided; if a fact is missing, stay generic but plausible.",
      "Return JSON only: { \"stages\": {\"opening\":{\"talk_track\":\"...\"}, ...}, \"meta\": {\"tone\":\"...\",\"source\":\"generated-from-library\"} }"
    ].join("\n");

    const user = [
      `TONE: ${t}`,
      `TARGET_LENGTH_WORDS: ${Number(length || 100)}`,
      `PRODUCT: ${facts.product}`,
      `BUYER TYPE: ${facts.buyerType}`,
      `SALES MODE: ${facts.salesMode}`,
      `FORM: company=${facts.form.company} role=${facts.form.role} sector=${facts.form.sector} size=${facts.form.size} scenario=${facts.form.scenario} meeting=${facts.form.meeting_type}`,
      "",
      "LIBRARY FACTS:",
      `OPENING: ${facts.opening}`,
      `PAIN: ${facts.pain}`,
      `DESIRE: ${facts.desire}`,
      `PROOF: ${facts.proofPoints.join(" | ")}`,
      `OBJECTIONS: ${facts.objections.join(" | ")}`,
      `CTAS: ${facts.ctas.join(" | ")}`
    ].join("\n");

    try {
      const raw = await generate([
        { role: "system", content: system },
        { role: "user", content: user }
      ]);
      const jsonStr = String(raw).replace(/```json|```/g, "").trim();
      const data = JSON.parse(jsonStr);

      // Defensive normalisation: ensure each stage has talk_track
      const stages = data?.stages || {};
      for (const k of ["opening","buyer_pain","buyer_desire","example","objections","call_to_action"]) {
        if (!stages[k]) stages[k] = { talk_track: "" };
        if (typeof stages[k].talk_track !== "string") stages[k].talk_track = String(stages[k].talk_track || "");
      }

      return {
        stages,
        meta: { ...(data.meta || {}), tone: t, source: "generated-from-library" },
        metaLine: `${rec.metaLine} · Tone: ${t} · Source: model`,
        baseMeta: rec.meta,
        buyerNeeds: rec.buyerNeeds,
        source: "generated-from-library"
      };
    } catch {
      // fall through to rules
    }
  }

  // Rule-based synthesis
  const data = ruleBased(rec, facts, t, length);
  return {
    ...data,
    metaLine: `${rec.metaLine} · Tone: ${t} · Source: rules`,
    baseMeta: rec.meta,
    buyerNeeds: rec.buyerNeeds,
    source: "generated-from-library"
  };
}
