// /api/lib/prompt-harness.js 2025-10-26 v 7
// Exports:
//   - buildDefaultMessages({ inputs, evidencePack })
//   - buildCustomMessages({ customPromptText, evidencePack })
//   - schemaJson  (from loaded/default schema)
//   - generate({ schemaPath?, packs?, input?, options? }) -> ALWAYS returns a JSON object
//
// Behaviours:
// - If schemaPath is provided, that schema is used instead.
// - Uses Azure OpenAI **Chat Completions** (2024-08-01-preview) with JSON Schema mode.
// - (Responses API not enabled in this version; we always call Chat Completions.)
// - Env can be overridden by options.azure { endpoint, apiKey, apiVersion, deployment }.
// - Node 18/20 global fetch; no extra deps.

const fs = require("fs");
const path = require("path");

// ---- Env defaults (override with options.azure below) ----
const ENV_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT;   // e.g., https://sales-tools.openai.azure.com
const ENV_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT; // e.g., sales-tools
const ENV_API_VER = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";
const ENV_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const LLM_TIMEOUT_MS_DEFAULT = Number(process.env.LLM_TIMEOUT_MS || "45000");
const ENV_MAX_TOKENS = Number(process.env.LLM_MAX_TOKENS || process.env.AZURE_OPENAI_MAX_TOKENS || "4096");
const ENV_MAX_EVIDENCE_CHARS = (() => {
  const raw = process.env.PROMPT_MAX_EVIDENCE_CHARS || process.env.MAX_STR;
  const n = Number(raw);
  // Guardrails: allow 50k–1.5M chars, default 380k
  if (!Number.isFinite(n)) return 380_000;
  return Math.min(Math.max(n, 50_000), 1_500_000);
})();
if (process.env.NODE_ENV !== "production" && !global.__LOGGED_PROMPT_MAX__) {
  try { console.log(`[prompt-harness] PROMPT_MAX_EVIDENCE_CHARS=${MAX_STR}`); } catch { }
  global.__LOGGED_PROMPT_MAX__ = true;
}
// ---- Default schema path ----
const DEFAULT_SCHEMA_PATH = path.join(__dirname, "..", "schemas", "campaign.schema.json");

function loadJsonFile(absOrRelPath) {
  const abs = path.isAbsolute(absOrRelPath)
    ? absOrRelPath
    : path.join(process.cwd(), "api", absOrRelPath);
  const raw = fs.readFileSync(abs, "utf8");
  return JSON.parse(raw);
}

function tryLoadDefaultSchema() {
  try { return JSON.parse(fs.readFileSync(DEFAULT_SCHEMA_PATH, "utf8")); }
  catch { return null; }
}

// Now driven by env (PROMPT_MAX_EVIDENCE_CHARS or MAX_STR).
const MAX_STR = ENV_MAX_EVIDENCE_CHARS;

function clipString(s, max = MAX_STR) {
  const str = typeof s === "string" ? s : JSON.stringify(s ?? "");
  if (str.length <= max) return str;
  // Keep head + tail to preserve useful context; avoids losing titles/attributions
  const keep = Math.floor(max / 2);
  return str.slice(0, keep) + " …TRUNCATED… " + str.slice(-keep);
}

function safe(obj, max = MAX_STR) {
  try { return clipString(obj, max); }
  catch { return "null"; }
}


// ---- Personas & rules ----
const BASE_RULES = `
Return STRICTLY valid JSON that conforms to the provided schema. Do NOT include any prose before or after the JSON.

EVIDENCE RULES
- Every bullet/statement must end with a short source tag in parentheses, e.g., (Company site), (Annual report 2024, p.14), (Trade press).
- If something cannot be evidenced from the evidence pack, do not claim it. You may add a brief reason in notEvidenced[].
- Do NOT invent sources or estimate trade-show budgets unless a show organiser/exhibitor source exists in the evidence pack.

SOURCE MIX (hard requirement; reject internally until met)
- Minimum mix across the whole JSON:
  • ≥4 claims from the **company website** or product materials (tag as (Company site) or the exact page title).  
  • ≥1 signal from **LinkedIn** (company page/post) if provided in evidencePack.  
  • ≥2 claims from **regulator/government** (e.g., Ofcom/ONS/DSIT); prefer ≤6 months old.  
- “Trade press” is allowed, but NEVER as the only category. If quotas are not met with given evidence, leave fields blank and add an explanation in notEvidenced[].

CSV FUSION RULES
- Treat CSV fields as ground truth about ICP focus and messaging emphasis. You MUST reflect:
  • SimplifiedIndustry → ICP phrasing and examples,
  • TopPurchases → offer & assets,
  • TopBlockers → pains & objections,
  • TopNeedsSupplier → nonnegotiables & differentiators.
- Do NOT use AdopterProfile or TopConnectivity even if present.
- If CSV is missing, say why in notEvidenced[].

STYLE & FORMATTING
- UK currency & numerals: £20.756m, £1,421,646, 8.4%.
- No generic advice; keep everything specific to the evidence.
- Keep tables/lists compact and scannable.
`.trim();

const DOC_SPEC = `
DOCUMENT STRUCTURE (your output JSON must fill these keys with cited content)
- executive_summary: 4–8 bullets. Each ends with a parenthesised source tag; include at least one Company site and one regulator claim.
- evidence_log: array of { claim_id, summary, source_type, url, title, quote? }. Reuse ClaimIDs in other sections.
- case_studies: 2–4 named examples (prefer company materials; clearly mark proxies if used).
- positioning_and_differentiation: value_prop; ≥3 differentiators (each cited). Map CSV TopNeedsSupplier into “nonnegotiables”.
- messaging_matrix: rows per persona with pain (from CSV TopBlockers), value_statement (cited), proof (cited ClaimIDs/Case), CTA.
- offer_strategy.landing_page: hero; why_it_matters[] (regulator/company claims); what_you_get[]; how_it_works[]; outcomes[]; proof[]; cta.
- channel_plan.emails: 4 emails. subject ≤80 chars; body 90–120 words; each body includes at least one citation.
- sales_enablement: ≥5 discovery_questions; ≥3 objection_cards with reframe_with_claimid + proof.
- measurement_and_learning, compliance_and_governance, risks_and_contingencies, one_pager_summary: all evidence-anchored.
- notEvidenced: enumerate anything you could not back with the pack/CSV.

VALIDATION GATE (self-check before emitting JSON)
- Reject internally until: source quotas met; CSV fields referenced in Executive Summary, Messaging, Offer, and Objections; at least one Company site ClaimID present in evidence_log and reused elsewhere.
`.trim();

const PARTNER_PERSONA = `
You are a top-performing UK B2B channel strategist (tech markets) and CMO. 
You understand partner recruitment, enablement, and common channel constraints (M&A, higher interest rates, lead-gen pressure).
Your job is to produce an evidence-only campaign that adds value to the partners by making them more competitive, improving their customer service, and differentiating them in the market.
`.trim();

const DIRECT_PERSONA = `
You are a top-performing UK B2B tech market strategist and CMO. 
You understand the challenges that your customers face (need better productivity, understand how to invest in the right technologies and why)
Your job is to produce an evidence-only campaign that adds value to direct customers by making them more efficient and productive, improving customer service they provide, and differentiating them in the market. (Key technologies: cybersecurity, artificial intelligence, IoT, mobile data connectivity)
`.trim();

const DEFAULT_PERSONA = PARTNER_PERSONA;
function selectPersona(input = {}) {
  const raw = (
    input.sales_model ??
    input.salesModel ??
    input.call_type ??
    input?.filters?.sales_model ??
    input?.filters?.salesModel ??
    input?.filters?.call_type ??
    ""
  ).toString().toLowerCase().trim();

  if (raw.includes("partner") || raw.includes("channel") || raw.includes("indirect")) return PARTNER_PERSONA;
  if (raw.includes("direct") || raw.includes("field") || raw.includes("inside")) return DIRECT_PERSONA;
  return DEFAULT_PERSONA;
}

// ---- Builders ----
function buildDefaultMessages({ inputs = {}, evidencePack = {} }) {
  const system = `${selectPersona(inputs)}\n\n${BASE_RULES}`;

  const user = `
Company inputs
- Name: ${inputs.prospect_company || ""}
- Website: ${inputs.prospect_website || ""}
- Buyer type: ${inputs.buyer_type || ""}
- Sales model: ${inputs.sales_model || inputs.salesModel || inputs.call_type || ""}
- Product/service focus: ${inputs.product_service || ""}
- Extra context: ${inputs.context || ""}

Evidence pack (prioritise Company site → LinkedIn → Regulator/Gov → Other)
- COMPANY_WEBSITE_HTML_OR_TEXT: ${safe(evidencePack.website || [])}
- LINKEDIN_COMPANY: ${safe(evidencePack.linkedin || [])}
- IXBRL_SUMMARY: ${safe(evidencePack.ixbrl || {})}
- PDF_EXTRACTS: ${safe(evidencePack.pdf || [])}
- DIRECTORY_MATCHES: ${safe(evidencePack.directories || [])}
- CSV_SUMMARY: ${safe(evidencePack.csv || inputs?.csvSummary || {})}

Return only JSON for this schema:
${JSON.stringify(schemaJson)}
`.trim();

  return { messages: [{ role: "system", content: system }, { role: "user", content: user }], schemaJson };
}

function buildCustomMessages({ customPromptText = "", evidencePack = {} }) {
  const system = `${BASE_RULES}\n\n${DOC_SPEC}`;
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

// ---- Schema holder ----
let schemaJson = tryLoadDefaultSchema() || { title: "campaign", type: "object" };

// ---- Core generate() ----
async function generate({ schemaPath, packs, input, evidencePack = {}, options = {} }) {
  // Load/override schema
  if (schemaPath) {
    const loaded = loadJsonFile(schemaPath);
    if (!loaded || typeof loaded !== "object") throw new Error("Invalid schema");
    schemaJson = loaded;
  }

  // Effective config: env defaults overridden by options.azure
  const azure = {
    endpoint: options.azure?.endpoint || ENV_ENDPOINT,
    apiKey: options.azure?.apiKey || ENV_API_KEY,
    apiVersion: options.azure?.apiVersion || ENV_API_VER,
    deployment: options.azure?.deployment || ENV_DEPLOYMENT,
    api: (options.azure?.api || "chat").toLowerCase() // "chat" | "responses"
  };

  if (!azure.endpoint || !azure.deployment) {
    throw new Error("OpenAI endpoint/deployment not configured");
  }
  if (!azure.apiKey) {
    throw new Error("AZURE_OPENAI_API_KEY not configured");
  }

  // Messages payload (evidence-rich)
  const useCustom = !!(packs && typeof packs.promptText === "string" && packs.promptText.trim());
  const { messages: builtMessages } = useCustom
    ? buildCustomMessages({ customPromptText: packs.promptText, evidencePack })
    : buildDefaultMessages({ inputs: input || {}, evidencePack });

  // Optionally augment system message with persona + DOC_SPEC + enabled packs line
  const packsLine =
    packs?.enabled && Array.isArray(packs.enabled) && packs.enabled.length
      ? `\nEnabled packs: ${packs.enabled.join(", ")}`
      : "";

  // Rebuild system message to include Persona + BASE_RULES + DOC_SPEC + packs line
  const personaText = selectPersona(input);
  const systemFull = [
    personaText,
    BASE_RULES,
    DOC_SPEC,
    packsLine
  ].filter(Boolean).join("\n\n");

  const messages = [
    { role: "system", content: systemFull },
    // keep the evidence-rich user content from the builder (index 1)
    builtMessages[1] || { role: "user", content: "Return JSON." }
  ];
  // Retry helper
  const attempts = Math.max(1, Number(options.retry?.attempts || 1));
  const backoffMs = Math.max(0, Number(options.retry?.backoffMs || 0));

  try {
    // Build request body
    const reqBody = {
      messages,
      temperature: 0,
      top_p: 0.9,
      max_tokens: Number.isFinite(options.maxTokens) ? options.maxTokens : ENV_MAX_TOKENS,
      response_format: {
        type: "json_schema",
        json_schema: {
          name: (schemaJson.title || "campaign_schema").replace(/[^\w\-]/g, "_"),
          schema: schemaJson,
          strict: true
        }
      },
      ...(options.seed ? { seed: Number(options.seed) } : {})
    };

    // Decide API path (force Chat Completions; do not use Responses)
    const base = azure.endpoint.replace(/\/+$/, "");
    const dep = encodeURIComponent(azure.deployment);
    const ver = encodeURIComponent(azure.apiVersion);
    const url = `${base}/openai/deployments/${dep}/chat/completions?api-version=${ver}`;

    // Attempt loop
    let lastErr;
    for (let i = 0; i < attempts; i++) {
      // Per-attempt timeout & controller
      const timeoutMsRaw = options.timeoutMs ?? LLM_TIMEOUT_MS_DEFAULT;
      const timeoutMs = Number.isFinite(Number(timeoutMsRaw)) && Number(timeoutMsRaw) > 0
        ? Number(timeoutMsRaw)
        : LLM_TIMEOUT_MS_DEFAULT;
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort("llm_timeout"), timeoutMs);

      try {
        const res = await fetch(url, {
          method: "POST",
          signal: controller.signal,
          headers: {
            "Content-Type": "application/json",
            "api-key": azure.apiKey
          },
          body: JSON.stringify(reqBody)
        });

        if (!res.ok) {
          const text = await res.text().catch(() => "");
          throw new Error(`OpenAI error ${res.status}: ${text || res.statusText}`);
        }

        const json = await res.json();
        const content = json?.choices?.[0]?.message?.content;

        if (!content) throw new Error("Empty model response");

        let obj;
        try {
          obj = JSON.parse(content);
        } catch {
          throw new Error("Model returned non-JSON content");
        }
        if (obj === null || typeof obj !== "object" || Array.isArray(obj)) {
          throw new Error("Model returned non-object JSON");
        }
        return obj;
      } catch (e) {
        lastErr = e;
        if (i < attempts - 1 && backoffMs) {
          await new Promise(r => setTimeout(r, backoffMs));
        }
      } finally {
        clearTimeout(timer);
      }
    }
    throw lastErr || new Error("Unknown LLM error");
  } finally {
    // no global timer
  }
}
Object.defineProperty(module.exports, "schemaJson", {
  enumerable: true,
  get() { return schemaJson; }
});

module.exports.buildDefaultMessages = buildDefaultMessages;
module.exports.buildCustomMessages = buildCustomMessages;
// schemaJson is exposed via the getter above
module.exports.generate = generate;
