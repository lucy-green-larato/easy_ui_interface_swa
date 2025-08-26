// /api/lib/prompt-harness.js
// Builds ChatGPT-style prompts for lead-qualification with strict JSON output.
//
// - Default mode: uses your standard “UK channel strategist” system prompt.
// - Custom mode: lets you pass a free-form user prompt (still enforces schema).
//
// Output: { messages: [{role, content}...], schemaJson }

const fs = require("fs");
const path = require("path");

// --- Load the JSON schema used by the model’s response ---
const schemaPath = path.join(__dirname, "..", "schemas", "qualification.v2.json");
const schemaJson = JSON.parse(fs.readFileSync(schemaPath, "utf8"));

// --- Helpers to keep message size sane (prevents token blow-ups) ---
const MAX_STR = 120_000;
function clipString(s, max = MAX_STR) {
  const str = typeof s === "string" ? s : JSON.stringify(s ?? "");
  return str.length > max ? (str.slice(0, max) + " …TRUNCATED…") : str;
}
function safe(obj) { try { return clipString(obj); } catch { return "null"; } }

// --- Base, reusable rules we always enforce ---
const BASE_RULES = `
Return STRICTLY valid JSON that conforms to the provided schema. Do NOT include any prose before or after the JSON.

EVIDENCE RULES
- Every bullet/statement must end with a short source tag in parentheses, e.g., (Company site), (Annual report 2024, p.14), (Trade show directory).
- If something cannot be evidenced from the evidence pack, do not claim it. You may add a brief reason in notEvidenced[].
- Do NOT invent sources or estimate trade-show budgets unless a show organiser/exhibitor source exists in the evidence pack.

STYLE & FORMATTING
- UK currency & numerals: £20.756m, £1,421,646, 8.4%.
- No generic advice; keep everything specific to the evidence.
- Keep tables/lists compact and scannable.
`.trim();

const DEFAULT_PERSONA = `
You are a top-performing UK B2B channel strategist and CMO.
You understand partner recruitment, enablement, and common channel constraints (M&A, higher interest rates, lead-gen pressure).
Your job is to produce an evidence-only partner-readiness assessment.
`.trim();

// --- Default harness: you pass inputs + evidence, we build messages like you'd do in ChatGPT ---
function buildDefaultMessages({ inputs = {}, evidencePack = {} }) {
  const system = `${DEFAULT_PERSONA}\n\n${BASE_RULES}`;

  const user = `
Company inputs
- Name: ${inputs.prospect_company || ""}
- Website: ${inputs.prospect_website || ""}
- Buyer type: ${inputs.buyer_type || ""}
- Sales model: ${inputs.call_type || ""}
- Product/service focus: ${inputs.product_service || ""}
- Extra context: ${inputs.context || ""}

Evidence pack (may be partial)
- WEBSITE_SNAPSHOTS: ${safe(evidencePack.website || [])}
- IXBRL_SUMMARY: ${safe(evidencePack.ixbrl || {})}
- PDF_EXTRACTS: ${safe(evidencePack.pdf || [])}
- DIRECTORY_MATCHES: ${safe(evidencePack.directories || [])}

Return only JSON for this schema:
${JSON.stringify(schemaJson)}
`.trim();

  return { messages: [{ role: "system", content: system }, { role: "user", content: user }], schemaJson };
}

// --- Custom harness: you pass your own user prompt (still enforces schema) ---
function buildCustomMessages({ customPromptText = "", evidencePack = {} }) {
  const system = `${BASE_RULES}`;

  const user = `
${customPromptText}

Evidence pack (may be partial)
- WEBSITE_SNAPSHOTS: ${safe(evidencePack.website || [])}
- IXBRL_SUMMARY: ${safe(evidencePack.ixbrl || {})}
- PDF_EXTRACTS: ${safe(evidencePack.pdf || [])}
- DIRECTORY_MATCHES: ${safe(evidencePack.directories || [])}

Return only JSON for this schema:
${JSON.stringify(schemaJson)}
`.trim();

  return { messages: [{ role: "system", content: system }, { role: "user", content: user }], schemaJson };
}

module.exports = {
  buildDefaultMessages,
  buildCustomMessages,
  schemaJson
};
