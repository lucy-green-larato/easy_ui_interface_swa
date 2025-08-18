// /src/lib/callPromptEngine.js
// Prompt-library engine: takes a loaded Markdown template and interpolates variables.
// British English; no assumptive closes; salesperson chooses the next step.

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
  return id.replace(/(^|[-_ ])(\w)/g, (_, sp, ch) => (sp ? " " : "") + ch.toUpperCase()).trim();
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

  // Replace token with a naturally phrased line.
  let out = sectionText.replace(/\{\{\s*value_proposition\s*\}\}/gi, blended);
  if (out === sectionText) {
    // No token in the template — append politely.
    out = `${sectionText}\n\n${blended}`;
  }
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

function splitSections(markdown) {
  // Split by H1 headings of the required format (# Title)
  const lines = String(markdown || "").replace(/\r\n/g, "\n").split("\n");
  const order = [
    "Opener",
    "Context bridge",
    "Value moment",
    "Exploration nudge",
    "Objections",
    "Next step (salesperson-chosen)",
    "Close"
  ];
  const sections = {};
  let current = null;

  for (const line of lines) {
    const m = /^#\s+(.+)\s*$/.exec(line);
    if (m) {
      current = m[1].trim();
      if (!sections[current]) sections[current] = [];
      continue;
    }
    if (current) sections[current].push(line);
  }

  // Join arrays to strings; ensure missing sections exist
  for (const k of order) sections[k] = tidy((sections[k] || []).join("\n"));
  return { sections, order };
}

function reassemble({ sections, order }) {
  return order
    .map(h => `# ${h}\n${(sections[h] || "").trim()}`)
    .join("\n\n")
    .trim();
}

function tipsBank() {
  return [
    "Lead with relevance and empathy, not features.",
    "Keep one specific ask — no laundry list.",
    "Acknowledge constraints briefly if raised; keep the ask.",
    "Avoid calendar specifics — propose the step, not the slot.",
    "Use role-specific proof in one line; keep it credible.",
    "Mirror their language; stay concise and respectful."
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
  if (!tpl.trim()) {
    throw new Error("Template text missing.");
  }

  const { sections, order } = splitSections(tpl);

  // Basic token interpolation (not value/next_step)
  let pass1 = {};
  Object.assign(pass1, sections);
  const baseInterpolated = interpolateBasic(reassemble({ sections: pass1, order }), {
    seller: input?.seller || {},
    prospect: input?.prospect || {},
    product: input?.product || {},
    buyer: input?.buyer || ""
  });

  // Re-split to work section-by-section for weaving
  const { sections: S2, order: O2 } = splitSections(baseInterpolated);

  // Weave value proposition and next step
  S2["Value moment"] = weaveValueProposition(
    S2["Value moment"],
    input?.value_proposition || "",
    input?.product?.label || input?.product?.id || "this solution"
  );

  S2["Next step (salesperson-chosen)"] = weaveNextStep(
    S2["Next step (salesperson-chosen)"],
    input?.next_step || ""
  );

  // Close: ensure mandated thank-you
  S2["Close"] = ensureThanksClose(S2["Close"]);

  // Light touch length hint: if user asked for ~150 words, trim long sections slightly.
  const budget = parseInt(String(lengthHint), 10) || 300;
  const softTrim = (txt) => {
    const words = txt.split(/\s+/);
    if (words.length <= budget * 1.4) return txt; // generous
    return words.slice(0, Math.max(80, Math.floor(budget * 0.9))).join(" ") + "…";
  };

  // Apply soft trim to “Value moment” and “Exploration nudge” if very long
  S2["Value moment"] = softTrim(S2["Value moment"]);
  S2["Exploration nudge"] = softTrim(S2["Exploration nudge"]);

  const finalText = reassemble({ sections: S2, order: O2 });

  // Meta line for the UI
  const buyerNice = canonicalBuyerLabel(input?.buyer || "");
  const modeNice = String(input?.mode || "direct").charAt(0).toUpperCase() + String(input?.mode || "direct").slice(1);
  const metaLine = `Library · ${input?.product?.label || input?.product?.id || "—"} · ${buyerNice} · ${modeNice}`;

  // Tips: 1 fixed anchor + 2 random
  const anchor = "Make one clear ask (salesperson-chosen).";
  const extra = pickRandom(tipsBank(), 2);
  const tips_list = [anchor, ...extra];

  return {
    script_text: finalText,
    script_text_labeled: finalText, // already includes headings
    metaLine,
    tips_list
  };
}
