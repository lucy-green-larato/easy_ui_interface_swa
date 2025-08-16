// Browser ESM. Sits next to callLibrary.js and imports it.
import { getCallScript } from "./callLibrary.js";

/** @typedef {"professional"|"warm"|"neutral"} Tone */
const canonTone = (t) => {
  const x = String(t || "").toLowerCase();
  if (x.startsWith("pro")) return "professional";
  if (x.startsWith("warm")) return "warm";
  return "neutral";
};

// Build a compact facts pack from the curated library record
function factsFromLibrary(rec, form) {
  const s = rec.stages || {};
  const proof = s.example?.proof_points_text || s.example?.proof_points || [];
  const ctas  = s.call_to_action?.ctas_text || s.call_to_action?.ctas || [];
  return {
    product: rec.meta.product_label || rec.meta.product_id,
    buyerType: rec.meta.buyerType,
    salesMode: rec.meta.sales_mode,
    opening: s.opening?.buyer_needs_summary?.join(" · ") || "",
    pain: s.buyer_pain?.buyer_needs_summary?.join(" · ") || "",
    desire: s.buyer_desire?.buyer_needs_summary?.join(" · ") || "",
    proofPoints: Array.isArray(proof) ? proof : [],
    objections: s.objections?.anticipated || [],
    ctas: Array.isArray(ctas) ? ctas : [],
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

// Minimal deterministic fallback (works offline / without any API)
function ruleBased(rec, facts, tone) {
  const soften = tone === "warm" ? "Thanks for taking a moment today." : "Appreciate your time.";
  const style  = tone === "professional" ? "Let’s keep this business-focused." : "Let’s keep this practical.";
  return {
    stages: {
      opening: { talk_track:
        `${soften} ${style} We often hear: ${facts.opening}. Briefly—how does this show up at ${facts.form.company || "your organisation"}?`
      },
      buyer_pain: { talk_track:
        `Common pressure points: ${facts.pain}. Where does this bite most—field teams, new sites, or finance visibility?`
      },
      buyer_desire: { talk_track:
        `Teams want: ${facts.desire}. Which two outcomes would matter most in the next quarter?`
      },
      example: { talk_track:
        facts.proofPoints.length
          ? `For context, peers achieved: ${facts.proofPoints.join("; ")}. Would a similar phased approach work here?`
          : `We usually start with a small, low-risk pilot to prove value. Would that be acceptable?`
      },
      objections: { talk_track:
        (facts.objections.length
          ? `Usual concerns: ${facts.objections.join("; ")}. We handle this with phased pilots and portability—what would you need to feel confident?`
          : `If you have concerns, we mitigate with a phased pilot and clear exit—what would you need to feel confident?`)
      },
      call_to_action: { talk_track:
        (facts.ctas.length
          ? `Next sensible step: ${facts.ctas.join("; ")}. Would a brief session next week suit?`
          : `Next step could be a short review to baseline today and highlight quick wins. Does early next week work?`)
      }
    },
    meta: { tone, source: "generated-from-library" }
  };
}

/**
 * Generate a call script from curated content.
 * If you have a server LLM endpoint, pass a generate(messages)->text fn.
 */
export async function generateCallFromLibrary({ lookup, form, tone, generate }) {
  // 1) Pull curated record (uses your existing callLibrary.js)
  const rec = await getCallScript(lookup);

  // 2) Build facts for prompting
  const t = canonTone(tone);
  const facts = factsFromLibrary(rec, form);

  // 3) If an LLM generator is supplied, call it; else fallback rules
  if (typeof generate === "function") {
    const system = [
      "You are Larato’s B2B calling assistant.",
      "Write a concise first-call SCRIPT using the 6-step Larato method:",
      "1) Opening 2) Buyer Pain 3) Buyer Desire 4) Example 5) Objections 6) Call To Action.",
      "Constraints:",
      "- British English; empathy & value-first; adapt to requested tone; do NOT copy verbatim.",
      "- 2–4 sentences per step; include 1–2 short questions where helpful.",
      "- Use only the library facts provided; if a fact is missing, stay generic but plausible.",
      "Return JSON only: { \"stages\": {\"opening\":{\"talk_track\":\"...\"}, ...}, \"meta\": {\"tone\":\"...\",\"source\":\"generated-from-library\"} }"
    ].join("\n");

    const user = [
      `TONE: ${t}`,
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
      return {
        ...data,
        metaLine: rec.metaLine + ` · Tone: ${t} · Source: model`,
        baseMeta: rec.meta,
        buyerNeeds: rec.buyerNeeds,
        source: "generated-from-library"
      };
    } catch {
      // fall through to rules
    }
  }

  const data = ruleBased(rec, facts, t);
  return {
    ...data,
    metaLine: rec.metaLine + ` · Tone: ${t} · Source: rules`,
    baseMeta: rec.meta,
    buyerNeeds: rec.buyerNeeds,
    source: "generated-from-library"
  };
}
