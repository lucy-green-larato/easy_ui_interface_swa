// Minimal, dependency-light synthesis from library -> call script
// Uses your existing getCallScript() to fetch curated ingredients,
// then calls your generator (OpenAI/Azure/etc) with a tight prompt.
// If no API configured, it falls back to a deterministic template.

import { getCallScript } from "./callLibrary"; // your existing loader

type Tone = "professional" | "warm" | "neutral";
type Lookup = { product: string; buyerType: string; mode: string };

// You can wire this to your existing /api/generate endpoint if you have one.
async function defaultGenerate(messages: { role: "system" | "user"; content: string }[]) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error("NO_LLM");
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${apiKey}` },
    body: JSON.stringify({
      model: "gpt-4o-mini", // replace with your model (or Azure endpoint)
      temperature: 0.4,
      messages
    })
  });
  const data = await res.json();
  const text = data?.choices?.[0]?.message?.content?.trim();
  if (!text) throw new Error("LLM_EMPTY");
  return text;
}

function canonTone(input?: string): Tone {
  const x = (input || "").toLowerCase();
  if (x.startsWith("pro")) return "professional";
  if (x.startsWith("warm")) return "warm";
  return "neutral";
}

// Build a compact facts pack from the library record
function factsFromLibrary(rec: any, form: Record<string, any>) {
  const s = rec.stages;
  const proof = s?.example?.proof_points_text || s?.example?.proof_points || [];
  const ctas  = s?.call_to_action?.ctas_text || s?.call_to_action?.ctas || [];
  return {
    product: rec.meta.product_label || rec.meta.product_id,
    buyerType: rec.meta.buyerType,
    salesMode: rec.meta.sales_mode,
    // Single-sentence essence for each stage to steer the model
    opening: s?.opening?.buyer_needs_summary?.join(" · ") || "",
    pain: s?.buyer_pain?.buyer_needs_summary?.join(" · ") || "",
    desire: s?.buyer_desire?.buyer_needs_summary?.join(" · ") || "",
    proofPoints: Array.isArray(proof) ? proof : [],
    objections: s?.objections?.anticipated || [],
    ctas: Array.isArray(ctas) ? ctas : [],
    // From your left column form:
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

function buildPrompt({ rec, facts, tone }: { rec: any; facts: ReturnType<typeof factsFromLibrary>; tone: Tone }) {
  const system = [
    "You are Larato’s B2B calling assistant.",
    "Write a concise first-call SCRIPT using the 6-step Larato method:",
    "1) Opening 2) Buyer Pain 3) Buyer Desire 4) Example 5) Objections 6) Call To Action.",
    "Constraints:",
    "- British English.",
    "- Empathy and value-first; credible, conversational (no hype).",
    "- Adapt to requested TONE.",
    "- Use the library facts as inputs; do NOT copy sentences verbatim.",
    "- Keep each step to 2–4 sentences and 1–2 short questions where helpful.",
    "- Keep proof/CTAs grounded in the provided facts; if absent, be generic but plausible.",
    "- Avoid US spellings and avoid jargon unless explained.",
    "Output JSON only with shape:",
    "{ \"stages\": { \"opening\": {\"talk_track\":\"...\"}, \"buyer_pain\": {...}, \"buyer_desire\": {...}, \"example\": {...}, \"objections\": {...}, \"call_to_action\": {...} },",
    "  \"meta\": { \"tone\": \"...\", \"source\": \"generated-from-library\" } }"
  ].join("\n");

  const user = [
    `TONE: ${tone}`,
    `PRODUCT: ${facts.product}`,
    `BUYER TYPE: ${facts.buyerType}`,
    `SALES MODE: ${facts.salesMode}`,
    `FORM CONTEXT: company=${facts.form.company} role=${facts.form.role} sector=${facts.form.sector} size=${facts.form.size} scenario=${facts.form.scenario} meeting=${facts.form.meeting_type}`,
    "",
    "LIBRARY FACTS (short prompts):",
    `OPENING: ${facts.opening}`,
    `PAIN: ${facts.pain}`,
    `DESIRE: ${facts.desire}`,
    `PROOF: ${facts.proofPoints.join(" | ")}`,
    `OBJECTIONS: ${facts.objections.join(" | ")}`,
    `CTAS: ${facts.ctas.join(" | ")}`,
  ].join("\n");

  return [
    { role: "system" as const, content: system },
    { role: "user" as const, content: user }
  ];
}

// Deterministic fallback if no LLM (keeps you productive in dev/offline)
function ruleBasedFallback(rec: any, facts: any, tone: Tone) {
  const soften = tone === "warm" ? "Thanks for taking a moment today." : "Appreciate your time.";
  const style  = tone === "professional" ? "Let’s keep this business-focused." : "Let’s keep this practical.";
  return {
    stages: {
      opening: { talk_track:
        `${soften} ${style} We often hear: ${facts.opening}. Briefly confirm how this shows up for you at ${facts.form.company || "your organisation"}?`
      },
      buyer_pain: { talk_track:
        `Common pressure points: ${facts.pain}. Where does this bite most—field teams, new sites, or finance visibility?`
      },
      buyer_desire: { talk_track:
        `Teams tell us they want: ${facts.desire}. Which two outcomes would matter most in the next quarter?`
      },
      example: { talk_track:
        facts.proofPoints.length
          ? `For context, peers achieved: ${facts.proofPoints.join("; ")}. Would a similar phased approach work here?`
          : `We typically start with a small, low-risk pilot to prove value. Would that be acceptable?`
      },
      objections: { talk_track:
        facts.objections.length
          ? `Usual concerns: ${facts.objections.join("; ")}. We handle this with phased pilots and portability—what would you need to feel confident?`
          : `If you have concerns, we mitigate with a phased pilot and clear exit—what would you need to feel confident?`
      },
      call_to_action: { talk_track:
        facts.ctas.length
          ? `Next sensible step: ${facts.ctas.join("; ")}. Would a brief session next week suit?`
          : `Next step could be a short review to baseline today and highlight quick wins. Does early next week work?`
      }
    },
    meta: { tone, source: "generated-from-library" }
  };
}

export async function generateCallFromLibrary(
  { lookup, form, tone, generate = defaultGenerate }:
  { lookup: Lookup; form: Record<string, any>; tone?: string; generate?: typeof defaultGenerate }
) {
  const rec = await getCallScript(lookup); // curated input + buyerNeeds/meta
  const t = canonTone(tone);
  const facts = factsFromLibrary(rec, form);

  try {
    const messages = buildPrompt({ rec, facts, tone: t });
    const raw = await generate(messages);
    // Try to parse JSON; if model returned markdown, strip fences
    const jsonStr = raw.replace(/```json|```/g, "").trim();
    const data = JSON.parse(jsonStr);
    return {
      ...data,
      metaLine: rec.metaLine + ` · Tone: ${t} · Source: model`,
      baseMeta: rec.meta,
      buyerNeeds: rec.buyerNeeds,
      source: "generated-from-library"
    };
  } catch (e) {
    // Fallback deterministic template (no external calls)
    const data = ruleBasedFallback(rec, facts, t);
    return {
      ...data,
      metaLine: rec.metaLine + ` · Tone: ${t} · Source: rules`,
      baseMeta: rec.meta,
      buyerNeeds: rec.buyerNeeds,
      source: "generated-from-library"
    };
  }
}
