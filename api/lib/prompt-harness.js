// /api/lib/prompt-harness.js 2025-10-25 v 6
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

// ---- Default schema path ----
const DEFAULT_SCHEMA_PATH = path.join(__dirname, "..", "schemas", "qualification.v2.json");

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

// ---- Helpers to keep message size sane ----
const MAX_STR = 120_000;
function clipString(s, max = MAX_STR) {
  const str = typeof s === "string" ? s : JSON.stringify(s ?? "");
  return str.length > max ? (str.slice(0, max) + " …TRUNCATED…") : str;
}
function safe(obj) { try { return clipString(obj); } catch { return "null"; } }

// ---- Personas & rules ----
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

// ---- Schema holder ----
let schemaJson = tryLoadDefaultSchema() || { title: "campaign", type: "object" };

// ---- Core generate() ----
async function generate({ schemaPath, packs, input, options = {} }) {
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

  // Messages payload (compact)
  const packsLine =
    packs?.enabled && Array.isArray(packs.enabled) && packs.enabled.length
      ? `\nEnabled packs: ${packs.enabled.join(", ")}`
      : "";
  const system = `${selectPersona(input)}\n\n${BASE_RULES}${packsLine}`;
  const userPayload = {
    task: "Generate JSON that STRICTLY conforms to the provided schema.",
    input: input || {},
    packs: packs || {},
    schema_hint: schemaJson?.title || "campaign",
  };
  const messages = [
    { role: "system", content: system },
    { role: "user", content: JSON.stringify(userPayload) }
  ];

  // Timeouts
  const timeoutMsRaw = options.timeoutMs ?? LLM_TIMEOUT_MS_DEFAULT;
  const timeoutMs = Number.isFinite(Number(timeoutMsRaw)) && Number(timeoutMsRaw) > 0
    ? Number(timeoutMsRaw)
    : LLM_TIMEOUT_MS_DEFAULT;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort("llm_timeout"), timeoutMs);

  // Retry helper
  const attempts = Math.max(1, Number(options.retry?.attempts || 1));
  const backoffMs = Math.max(0, Number(options.retry?.backoffMs || 0));

  try {
    // Build request body
    const reqBody = {
      messages,
      temperature: 0,
      response_format: {
        type: "json_schema",
        json_schema: {
          name: (schemaJson.title || "campaign_schema").replace(/[^\w\-]/g, "_"),
          schema: schemaJson,
          strict: true
        }
      }
    };

    // Decide API path (force Chat Completions; do not use Responses)
    const base = azure.endpoint.replace(/\/+$/, "");
    const dep = encodeURIComponent(azure.deployment);
    const ver = encodeURIComponent(azure.apiVersion);
    const url = `${base}/openai/deployments/${dep}/chat/completions?api-version=${ver}`;

    // Attempt loop
    let lastErr;
    for (let i = 0; i < attempts; i++) {
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
      }
    }
    throw lastErr || new Error("Unknown LLM error");
  } finally {
    clearTimeout(timeout);
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
