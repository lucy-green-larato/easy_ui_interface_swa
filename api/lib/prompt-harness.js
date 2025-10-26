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

FAIL-SAFE
- Never return empty sections. If evidence is sparse, (1) use company website + LinkedIn + USER_USPS, (2) mark any unsubstantiated elements in notEvidenced[], but (3) still produce full JSON objects/arrays to meet the schema.
`.trim();

const DOC_SPEC = `
DOCUMENT STRUCTURE (JSON keys and expectations)

CAMPAIGN NAMING
- Create a programme name in the format: "Demand programme for <market> by <company>". 
- Write this as the FIRST line inside meta.icp_from_csv (prefix with "Campaign: "), then include any other ICP notes below it.
  Example: meta.icp_from_csv = "Campaign: Demand programme for UK Construction by Comms 365\\nPrimary ICPs: …"

EXECUTIVE SUMMARY (format required)
- 1 short paragraph (≈100–130 words) that introduces the problem, the buyer context, and the company's USP-backed solution. 
- Follow with a "Why now:" list of 2–4 **evidenced bullets** (each ends with a citation tag).- Prioritise evidence from Company site & LinkedIn where applicable, then CSV (tag "(CSV)"), then Regulators/Gov (Ofcom, ONS, DSIT), then annual reports/IXBRL, then PDF extracts, then Trade press/Directory.

EVIDENCE LOG (mix of sources is mandatory)
- At least **2 items** must come from Company site or LinkedIn (source_type = "Company site" or "LinkedIn").
- At least **3 items** must come from external/regulatory/public sources (Ofcom/ONS/DSIT/Annual report/PDF/Trade press/Directory).
- Each item: claim_id (CLM-###), summary (cited), source_type, url (if applicable), title, and optional quote.

POSITIONING & USPs
- value_prop concise and UK-specific. 
- differentiators (≥3) MUST include at least **2 items sourced from USER_USPS** (verbatim or tight paraphrase) and be cited to Company site/LinkedIn where possible (else include brief note in notEvidenced[]).

OFFER & CHANNELS
- offer_strategy.landing_page: hero; why_it_matters[] (cited); what_you_get[] (cited; reflect TopPurchases + USER_USPS); how_it_works[]; outcomes[] (cited); proof[] (cited); cta.
- channel_plan.emails: exactly 4 emails; subject ≤80 chars; body ≈90–120 words; each body includes ≥1 citation (prefer Company site/LinkedIn/CSV).
- LinkedIn section: connect_note, insight_post, dm each with at least one specific assertion or proof tied to the CSV or site.

SALES ENABLEMENT, MEASUREMENT, COMPLIANCE, RISKS
- sales_enablement: ≥5 discovery_questions; ≥3 objection_cards mapping TopBlockers → reframe_with_claimid + proof (both cited).
- measurement_and_learning: concrete KPIs; weekly_test_plan tied to channels; UTM & CRM notes.
- compliance_and_governance: practical, specific lists; approval_log_note short.
- risks_and_contingencies: ≥2 cited items.

CSV FUSION RULES (CSV is current market research; treat as ground truth for targeting/messaging)
- You MUST reflect CSV mappings:
  • SimplifiedIndustry → ICP phrasing, persona contexts, examples.  
  • TopPurchases → offer_strategy.what_you_get and assets_checklist.  
  • TopBlockers → persona pains + objection_cards.  
  • TopNeedsSupplier → nonnegotiables + differentiators.
- Do NOT use AdopterProfile or TopConnectivity even if present.
- Cite CSV-derived statements with (CSV). If a CSV value conflicts with public sources, keep CSV (ground truth for targeting) and note the discrepancy in notEvidenced[].

USP FUSION RULES (must accept and use USER_USPS)
- Incorporate USER_USPS into: differentiators (≥2), nonnegotiables (≥2), landing_page.hero & what_you_get.
- If a USP has no public corroboration, still include it (it is supplied input); cite Company site/LinkedIn if present; otherwise list a one-line rationale in notEvidenced[].

FAIL-SAFE OUTPUT
- Never leave sections empty. If evidence is sparse, prioritise Company site + LinkedIn + USER_USPS + CSV; mark any gaps in notEvidenced[] and still produce full, schema-valid JSON.

ALLOWED CITATION TAGS
- (Company site), (LinkedIn), (CSV), (Ofcom), (ONS), (DSIT), (Annual report 2024, p.xx), (IXBRL), (PDF extract), (Trade press), (Directory).
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
  // ---- System message: persona + rules + document spec (non-negotiable) ----
  const system = `${selectPersona(inputs)}\n\n${BASE_RULES}\n\n${DOC_SPEC}`;

  // ---- Normalise common input aliases (defensive) ----
  const name    = inputs.prospect_company   || inputs.company_name     || "";
  const website = inputs.prospect_website   || inputs.company_website  || "";
  const linkedin= inputs.prospect_linkedin  || inputs.company_linkedin || "";

  const salesModel =
    inputs.sales_model ?? inputs.salesModel ?? inputs.call_type ?? "";

  // USER_USPS: array of strings (your UI should send this as inputs.user_usps)
  const userUsps = Array.isArray(inputs.user_usps) ? inputs.user_usps : (inputs.user_usps ? [inputs.user_usps] : []);

  // CSV meta + preview rows (short, so we don’t blow the token budget)
  const csvMeta = inputs.csv_meta || evidencePack.csv_meta || evidencePack.csvSummary || inputs.csvSummary || {};
  const csvRowsPreview = Array.isArray(inputs.csv_rows) ? inputs.csv_rows.slice(0, 5)
                        : Array.isArray(evidencePack.csv_rows) ? evidencePack.csv_rows.slice(0, 5)
                        : [];

  // ---- User message: ALL sources, in the priority order we want the model to follow ----
  const user = `
Company inputs
- Name: ${name}
- Website: ${website}
- LinkedIn: ${linkedin}
- Buyer type: ${inputs.buyer_type || ""}
- Sales model: ${salesModel}
- Product/service focus: ${inputs.product_service || ""}
- USER_USPS: ${safe(userUsps)}
- Persona hints (optional): ${safe(inputs.persona_hints || [])}
- Extra context: ${inputs.context || ""}

CSV (current market research; treat as ground truth per CSV FUSION RULES)
- CSV_META: ${safe(csvMeta)}
- CSV_ROWS (preview): ${safe(csvRowsPreview)}

Evidence pack (use priority: Company site/LinkedIn → CSV → Regulator/Gov → Annual report/IXBRL → PDF → Trade press/Directory)
- COMPANY_WEBSITE_HTML_OR_TEXT: ${safe(evidencePack.website || [])}
- LINKEDIN_COMPANY: ${safe(evidencePack.linkedin || [])}
- IXBRL_SUMMARY: ${safe(evidencePack.ixbrl || {})}
- PDF_EXTRACTS: ${safe(evidencePack.pdf || [])}
- DIRECTORY_MATCHES: ${safe(evidencePack.directories || [])}

Return only JSON for this schema:
${JSON.stringify(schemaJson)}
`.trim();

  return {
    messages: [
      { role: "system", content: system },
      { role: "user",   content: user    }
    ],
    schemaJson
  };
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
