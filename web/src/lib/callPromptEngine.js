// /src/lib/callPromptEngine.js
// Prompt-library engine: takes a loaded Markdown template and interpolates variables.
// British English; no assumptive closes; salesperson can choose the next step.

function toSentenceList(input = "") {
  const txt = String(input || "").trim();
  if (!txt) return [];
  // split on new lines / semicolons / commas (light touch)
  return txt
    .split(/\r?\n|;|(?:,\s+)/)
    .map(s => s.trim())
    .filter(Boolean);
}

function canonicalBuyerLabel(id = "") {
  const s = String(id).toLowerCase();
  if (s.includes("innovator")) return "Innovators";
  if (s.includes("early-adopter")) return "Early Adopters";
  if (s.includes("early_majority") || s.includes("early-majority")) return "Early Majority";
  if (s.includes("late_majority") || s.includes("late-majority")) return "Late Majority";
  if (s.includes("sceptic") || s.includes("skeptic")) return "Sceptics";
  // fallback: title case
  return String(id || "")
    .replace(/(^|[-_ ])(\w)/g, (_, sp, ch) => (sp ? " " : "") + ch.toUpperCase())
    .trim();
}

function tidy(lines) {
  // collapse 3+ newlines to max 2; trim edges
  return String(lines || "").replace(/\n{3,}/g, "\n\n").trim();
}

function ensureThanksClose(text) {
  // Guarantee the script ends with the required thanks line.
  const t = String(text || "").trim();
  if (/thank you for your time\.?$/i.test(t)) return t;
  return (t + (t.endsWith("\n") ? "" : "\n") + "Thank you for your time.").trim();
}

function interpolateBasic(template, vars) {
  // Simple {{token}} replacement for scalar tokens we support directly.
  // We intentionally DO NOT directly dump value_proposition or next_step here.
  return template
    .replace(/\{\{\s*seller\.name\s*\}\}/g, vars.seller?.name ?? "")
    .replace(/\{\{\s*seller\.company\s*\}\}/g, vars.seller?.company ?? "")
    .replace(/\{\{\s*prospect\.name\s*\}\}/g, vars.prospect?.name ?? "")
    .replace(/\{\{\s*prospect\.role\s*\}\}/g, vars.prospect?.role ?? "")
    .replace(/\{\{\s*prospect\.company\s*\}\}/g, vars.prospect?.company ?? "")
    .replace(/\{\{\s*product\.id\s*\}\}/g, vars.product?.id ?? "")
    .replace(/\{\{\s*product\.label\s*\}\}/g, vars.product?.label ?? "")
    .replace(/\{\{\s*buyer\.type\s*\}\}/g, vars.buyer ?? "");
}

function weaveValueProposition(sectionText, valueProp, productLabel) {
  const vp = String(valueProp || "").trim();
  if (!vp) {
    // Remove token if present; leave section as-is.
    return sectionText.replace(/\{\{\s*value_proposition\s*\}\}/gi, "").trim();
  }

  const bullets = toSentenceList(vp);
  const blended = bullets.length
    ? `In brief, what tends to resonate with teams like yours is ${bullets.length === 1 ? bullets[0] : bullets.slice(0, -1).join(", ") + " and " + bullets.slice(-1)}.`
    : `In short, the way we create value with ${productLabel || "this solution"} is practical and measurable.`;

  // Replace token with a naturally phrased line; append if token absent.
  let out = sectionText.replace(/\{\{\s*value_proposition\s*\}\}/gi, blended);
  if (out === sectionText) out = `${sectionText}\n\n${blended}`;
  return out.trim();
}

function weaveNextStep(sectionText, nextStep) {
  const ask = String(nextStep || "").trim();
  const blended = ask
    ? `Given what we have covered, my suggestion is: ${ask}`
    : `If it helps, I can share a concise summary and we can take it from there.`;

  let out = sectionText.replace(/\{\{\s*next_step\s*\}\}/gi, blended);
  if (out === sectionText) out = `${sectionText}\n\n${blended}`;
  // Do not add calendar specifics; the salesperson’s copy is taken verbatim but framed.
  return out.trim();
}

// Canonical headings we support
const CANONICAL_ORDER = [
  "Opening",
  "Buyer Pain",
  "Buyer Desire",
  "Example Illustration",
  "Handling Objections",
  "Next Step",
  "Close"
];

// Aliases (case-insensitive) that we normalise into the canonical headings above
const ALIASES = new Map(
  Object.entries({
    "opener": "Opening",
    "context bridge": "Buyer Pain",
    "buyer pains": "Buyer Pain",
    "value moment": "Buyer Desire",
    "exploration nudge": "Buyer Desire",
    "value story": "Example Illustration",
    "example": "Example Illustration",
    "objections & responses": "Handling Objections",
    "objections": "Handling Objections",
    "call to action": "Next Step",
    "next step (salesperson-chosen)": "Next Step",
    "next step (salesperson chosen)": "Next Step" // just in case
  })
);

function normaliseHeadingName(raw = "") {
  const k = String(raw).trim().toLowerCase();
  if (CANONICAL_ORDER.map(s => s.toLowerCase()).includes(k)) {
    // exact canonical
    return CANONICAL_ORDER.find(s => s.toLowerCase() === k);
  }
  if (ALIASES.has(k)) return ALIASES.get(k);
  // best-effort: title case the raw
  return String(raw).replace(/\b\w/g, c => c.toUpperCase()).trim();
}

function splitSections(markdown) {
  // Accept both H1 and H2 headings with any of the supported/alias names
  const lines = String(markdown || "").replace(/\r\n/g, "\n").split("\n");
  const sections = {};
  let current = null;

  for (const line of lines) {
    const m = /^#{1,2}\s+(.+?)\s*$/.exec(line); // # or ## heading
    if (m) {
      const canonical = normaliseHeadingName(m[1]);
      current = canonical;
      if (!sections[current]) sections[current] = [];
      continue;
    }
    if (current) sections[current].push(line);
  }

  // Join arrays to strings; ensure all canonical sections exist
  for (const k of CANONICAL_ORDER) sections[k] = tidy((sections[k] || []).join("\n"));
  return { sections, order: CANONICAL_ORDER.slice() };
}

function reassemble({ sections, order }) {
  return order
    .map(h => `# ${h}\n${(sections[h] || "").trim()}`)
    .join("\n\n")
    .trim();
}

function tipsBank() {
  return [
    "Open with something you've seen in the market, not a guess about their business.",
    "Check that you understand what their business does before you call.",
    "Introduce yourself briefly, then talk about what’s happening in their world, not yours.",
    "Use one strong, relevant example of the value on offer.",
    "Show you understand likely concerns and offer a simple way forward.",
    "When closing, suggest a clear outcome and agree next steps.",
    "Ask clear questions and give the prospect space to think.",
    "Know the value of your pitch — not just the words.",
    "Be natural. Be yourself.",
    "After a call, jot down how the prospect received it.",
    "Keep good call notes. They are gold dust."
  ];
}

function pickRandom(arr, n) {
  const copy = [...arr];
  const out = [];
  while (copy.length && out.length < n) {
    const i = Math.floor(Math.random() * copy.length);
    out.push(copy.splice(i, 1)[0]);
  }
  return out;
}

/**
 * Generate a call script from a markdown template and form inputs.
 * @param {Object} cfg
 *   - input: {
 *       mode, product:{id,label}, buyer, seller:{name,company}, prospect:{name,role,company},
 *       value_proposition, next_step, template_text
 *     }
 *   - tone: 'consultative' | 'warm' (informal hint, kept light)
 *   - lengthHint: number|string (approx. target word budget; used gently)
 * @returns {{script_text:string, script_text_labeled:string, metaLine:string, tips_list:string[]}}
 */
export function generatePromptBasedCallScript({ input, tone = "consultative", lengthHint = 300 }) {
  const tpl = String(input?.template_text || "");
  if (!tpl.trim()) throw new Error("Template text missing.");

  const { sections, order } = splitSections(tpl);

  // Basic token interpolation (not value/next_step)
  const baseInterpolated = interpolateBasic(
    reassemble({ sections, order }),
    {
      seller: input?.seller || {},
      prospect: input?.prospect || {},
      product: input?.product || {},
      buyer: input?.buyer || ""
    }
  );

  // Re-split to work section-by-section for weaving
  const { sections: S2, order: O2 } = splitSections(baseInterpolated);

  // Weave value proposition into Buyer Desire
  S2["Buyer Desire"] = weaveValueProposition(
    S2["Buyer Desire"],
    input?.value_proposition || "",
    input?.product?.label || input?.product?.id || "this solution"
  );

  // Weave next step into Next Step
  S2["Next Step"] = weaveNextStep(
    S2["Next Step"],
    input?.next_step || ""
  );

  // Close: ensure mandated thank-you
  S2["Close"] = ensureThanksClose(S2["Close"]);

  // Light-touch length hint: trim long sections slightly
  const budget = parseInt(String(lengthHint), 10) || 300;
  const softTrim = (txt) => {
    const words = String(txt || "").split(/\s+/);
    if (words.length <= budget * 1.4) return txt; // generous
    return words.slice(0, Math.max(80, Math.floor(budget * 0.9))).join(" ") + "…";
    };
  S2["Buyer Desire"] = softTrim(S2["Buyer Desire"]);
  S2["Example Illustration"] = softTrim(S2["Example Illustration"]);

  const finalText = reassemble({ sections: S2, order: O2 });

  // Meta line for the UI
  const buyerNice = canonicalBuyerLabel(input?.buyer || "");
  const modeNice = String(input?.mode || "direct").charAt(0).toUpperCase() + String(input?.mode || "direct").slice(1);
  const metaLine = `Library · ${input?.product?.label || input?.product?.id || "—"} · ${buyerNice} · ${modeNice}`;

  // Tips: 1 fixed anchor + 2 random (updated phrasing)
  const anchor = "Make one clear ask and agree the next step.";
  const extra = pickRandom(tipsBank(), 2);
  const tips_list = [anchor, ...extra];

  return {
    script_text: finalText,
    script_text_labeled: finalText, // already includes headings
    metaLine,
    tips_list
  };
}
