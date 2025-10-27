// /api/lib/prompt-harness.js 2025-10-27 v 9
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
const ENV_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT;
const ENV_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT;
const ENV_API_VER = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";
const ENV_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const LLM_TIMEOUT_MS_DEFAULT = Number(process.env.LLM_TIMEOUT_MS || "45000");

const ENV_MAX_TOKENS = Number(process.env.LLM_MAX_TOKENS || process.env.AZURE_OPENAI_MAX_TOKENS || "4096");

// Evidence clipping limit driven by env
const ENV_MAX_EVIDENCE_CHARS = (() => {
  const raw = process.env.PROMPT_MAX_EVIDENCE_CHARS || process.env.MAX_STR;
  const n = Number(raw);
  if (!Number.isFinite(n)) return 380_000;
  return Math.min(Math.max(n, 50_000), 1_500_000);
})();

// Use env-driven limit
const MAX_STR = ENV_MAX_EVIDENCE_CHARS;

// dev-only visibility
if (process.env.NODE_ENV !== "production" && !global.__LOGGED_PROMPT_MAX__) {
  try { console.log(`[prompt-harness] PROMPT_MAX_EVIDENCE_CHARS=${ENV_MAX_EVIDENCE_CHARS}`); } catch { }
  global.__LOGGED_PROMPT_MAX__ = true;
}

// ---- Default schema path ----
const DEFAULT_SCHEMA_PATH = path.join(__dirname, "..", "schemas", "campaign.schema.json");

function loadJsonFile(absOrRelPath) {
  const abs = path.isAbsolute(absOrRelPath)
    ? absOrRelPath
    : path.join(process.cwd(), "api", absOrRelPath);
  const raw = fs.readFileSync(abs, "utf8");
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Schema must be a JSON object");
  }
  return parsed;
}

function tryLoadDefaultSchema() {
  try { return JSON.parse(fs.readFileSync(DEFAULT_SCHEMA_PATH, "utf8")); }
  catch { return null; }
}

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
- Every bullet/statement must end with a short source tag in parentheses, e.g., (Company site), (Ofcom), (ONS), (DSIT), (PDF extract), (Trade press), (Directory).
- Evidence log items must be SPECIFIC:
  • Include concrete numbers, dates, and named entities (e.g., “Connected Nations 2024”, “Cyber Security Breaches Survey 2025”).
  • Prefer deep links to the exact page, not homepages.
  • For company site claims, reference the exact product page (title + URL), and include a short verbatim quote in 'quote' when available.
  • Avoid generic marketing claims with no figures; omit if not evidenced.

- STRICT NO-FABRICATION FOR CASE STUDIES:
  • Only include case studies that are evidenced by the evidence pack OR are direct pages on the same host as the provided company website.
  • If you cannot find any genuine case studies, return an empty array for the case_study_library section (do not invent customers or URLs).
  • Use deep links (product/case-study pages), not homepages. Include page title and exact URL.

SOURCE MIX (hard requirement; map to schema-safe source_type)
- Ensure ≥2 items from Company site (product pages), and ≥3 items from external regulators/government (Ofcom/ONS/DSIT) where applicable.

CSV & USPs
- Treat CSV as current market research for targeting/messaging (tag inline as (CSV)). The schema does not capture CSV as source_type; keep "Company site"/regulator/etc. in evidence_log.source_type and reserve "(CSV)" for inline tags.
- USER_USPS are accepted inputs; use them directly where relevant and tag inline with (Company site) if corroborated, otherwise leave the inline tag off and incorporate them as inputs.

STYLE & FORMATTING
- UK currency & numerals: £20.756m, £1,421,646, 8.4%.
- No generic advice; keep everything specific to the evidence.
- Keep tables/lists compact and scannable.
`.trim();

const DOC_SPEC = `
DOCUMENT STRUCTURE (JSON keys and expectations)

CAMPAIGN NAMING
- Put a programme name at the TOP of meta.icp_from_csv, first line only, formatted:
  "Campaign: Demand programme for <market> by <company>"
  Then append any ICP notes below (newline-separated).

EXECUTIVE SUMMARY
- executive_summary is an array of strings.
  • Item #1 (paragraph, ~110–140 words): name the company and explicitly reference its relevant products/services found on the website (use exact product names, e.g., “Bonded Internet”, “SD-One”, “Continuum”, “Continuum Constellation”) and tie them to outcomes for the specified buyer context (e.g., UK construction).
  • Items #2–#5: “Why now” bullets (each ends with a citation tag like (Ofcom), (ONS), (DSIT), (Company site)). Prioritise regulator/government sources for market signals; include concrete numbers/percentages/dates.

  EVIDENCE LOG
- evidence_log entries MUST satisfy the schema:
  { claim_id: "CLM-###", summary (cited), source_type (enum-compliant), url, title, quote? }.
- Ensure at least 2 items are clearly from the company (Company site source_type), and ≥3 items from external sources (Ofcom/ONS/DSIT/PDF extract/Trade press/Directory).
- When using LinkedIn, set source_type="Company site" but keep the inline "(LinkedIn)" tag inside the summary.

CASE STUDY LIBRARY
- case_study_library is an array. Each item must be sourced from the company website (same host as prospect_website) or appear in the evidence pack.
- Required fields per item: { customer, industry?, headline, bullets[] (2–4), url, source_type="Company site" }.
- Do NOT invent customers or URLs. If none are found, return [].

POSITIONING & DIFFERENTIATION
- Provide a concise value_prop for the UK context.
- differentiators: ≥3 items; incorporate ≥2 USER_USPS if given (cite Company site if possible; otherwise keep without citation).
- competitor_set: 5 vendors with reason_in_set and URL.

MESSAGING MATRIX
- nonnegotiables: ≥3.
- matrix rows: persona, pain, value_statement (cited), proof (cited), cta.
- Use CSV fields to drive wording: SimplifiedIndustry → persona context; TopBlockers → pains; TopNeedsSupplier → nonnegotiables; TopPurchases → value/what_you_get.

OFFER STRATEGY
- landing_page: hero, why_it_matters[] (cited), what_you_get[] (cited; reflect CSV + USER_USPS), how_it_works[], outcomes[] (cited), proof[] (cited), cta.
- assets_checklist: ≥5 items tied to the above.

CHANNEL PLAN
- emails: exactly 4; subject ≤80 chars; body ~90–120 words; each body includes ≥1 citation (prefer Company site/(LinkedIn)/CSV).
- linkedin.{connect_note, insight_post, dm, comment_strategy} with specific assertions tied to CSV/site.

SALES ENABLEMENT / MEASUREMENT / COMPLIANCE / RISKS
- discovery_questions: ≥5.
- objection_cards: ≥3 mapping TopBlockers → reframe_with_claimid + proof (both cited inline).
- measurement_and_learning: concrete KPIs; weekly_test_plan; utm_and_crm; evidence_freshness_rule.
- compliance_and_governance: substantiation_file; gdpr_pecr_checklist; brand_accessibility_checks; approval_log_note.
- risks_and_contingencies: ≥2 items (cited if external).

CSV MAPPING (when buyer_industry is provided)
- ONLY use csvSummary.* derived from the specified buyer_industry.
- Use IT spend distribution to set commercial tone/priority (e.g., “most spend 7–10%”).
- Map:
  • TopBlockers  → pains, objections, and risk language.
  • TopPurchases → offer & “what you get” (tie directly to supplier’s product names from the website).
  • TopNeedsSupplier → nonnegotiables & differentiators (tie to supplier capabilities).
- If buyer_industry is NOT provided, ignore csvSummary entirely (do not average across multiple industries).
- Do NOT use AdopterProfile or TopConnectivity.

ALLOWED INLINE CITATION TAGS (IN TEXT, not source_type): (Company site), (LinkedIn), (CSV), (Ofcom), (ONS), (DSIT), (PDF extract), (Trade press), (Directory).
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
function buildDefaultMessages({ inputs = {}, evidencePack = {}, packs = {}, useDocSpec = false }) {
  const persona = selectPersona(inputs);
  const packsLine = (packs?.enabled && Array.isArray(packs.enabled) && packs.enabled.length)
    ? `\nEnabled packs: ${packs.enabled.join(", ")}`
    : "";

  const system = `${persona}\n\n${BASE_RULES}${useDocSpec ? `\n\n${DOC_SPEC}` : ""}${packsLine}`;

  const user = `
Company inputs
- Name: ${inputs.prospect_company || ""}
- Website: ${inputs.prospect_website || ""}
- Buyer type: ${inputs.buyer_type || ""}
- Sales model: ${inputs.sales_model || inputs.salesModel || inputs.call_type || ""}
- Product/service focus: ${inputs.product_service || ""}
- USER_USPS (comma-separated): ${inputs.user_usps || inputs.usps || ""}
- Personas (comma-separated): ${inputs.personas || ""}
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
async function generate({ schemaPath, packs = {}, input = {}, evidencePack = {}, options = {} }) {
  // Param-name-agnostic: works whether function signature is (args) or ({...})
  const __firstArg = (arguments && arguments.length) ? arguments[0] : undefined;
  const ep = (__firstArg && __firstArg.evidencePack) || {};
  console.log("[harness] evidencePack counts", {
    website: Array.isArray(ep.website) ? ep.website.length : 0,
    linkedin: Array.isArray(ep.linkedin) ? ep.linkedin.length : 0,
    pdf: Array.isArray(ep.pdf) ? ep.pdf.length : 0,
    directories: Array.isArray(ep.directories) ? ep.directories.length : 0,
    ixbrlKeys: ep.ixbrl ? Object.keys(ep.ixbrl).length : 0,
    csvKeys: ep.csv ? Object.keys(ep.csv).length : 0
  });
  // --- 1) Load schema WITHOUT mutating the module global (thread/concurrency safe)
  let effectiveSchema = schemaJson;
  if (schemaPath) {
    const loaded = loadJsonFile(schemaPath);
    if (!loaded || typeof loaded !== "object") throw new Error("Invalid schema");
    effectiveSchema = loaded;
  }

  // --- 2) Azure config (env with per-call overrides)
  const azure = {
    endpoint: options.azure?.endpoint || ENV_ENDPOINT,
    apiKey: options.azure?.apiKey || ENV_API_KEY,
    apiVersion: options.azure?.apiVersion || ENV_API_VER,
    deployment: options.azure?.deployment || ENV_DEPLOYMENT,
    api: (options.azure?.api || "chat").toLowerCase()
  };
  if (!azure.endpoint) throw new Error("AZURE_OPENAI_ENDPOINT not configured");
  if (!azure.deployment) throw new Error("AZURE_OPENAI_DEPLOYMENT not configured");
  if (!azure.apiKey) throw new Error("AZURE_OPENAI_API_KEY not configured");

  // --- 3) Build messages (prefer a builder that includes DOC_SPEC/CSV rules)
  let messages;
  if (Array.isArray(options.messages) && options.messages.length) {
    messages = options.messages;
  } else if (typeof buildDefaultMessages === "function") {
    // Pass through packs and ask the builder to include DOC_SPEC if it supports the flag
    const built = buildDefaultMessages({
      inputs: input,
      evidencePack,
      packs,           // <- don’t drop this
      useDocSpec: true // <- your builder can append DOC_SPEC/CSV fusion rules if it supports this flag
    });
    messages = built.messages;
  } else {
    // Fallback inline build (keeps you safe even if builder signature lags)
    // Fallback inline build (if builders aren't available)
    const personaText = selectPersona(input);
    const packsLine = (packs?.enabled && Array.isArray(packs.enabled) && packs.enabled.length)
      ? `\nEnabled packs: ${packs.enabled.join(", ")}`
      : "";

    const system = [
      personaText,
      BASE_RULES,
      DOC_SPEC
    ].filter(Boolean).join("\n\n") + packsLine;

    const evidenceText = [
      "Evidence pack (prioritise Company site → LinkedIn → Regulator/Gov → Other)",
      `- COMPANY_WEBSITE_HTML_OR_TEXT: ${safe(evidencePack.website || [])}`,
      `- LINKEDIN_COMPANY: ${safe(evidencePack.linkedin || [])}`,
      `- IXBRL_SUMMARY: ${safe(evidencePack.ixbrl || {})}`,
      `- PDF_EXTRACTS: ${safe(evidencePack.pdf || [])}`,
      `- DIRECTORY_MATCHES: ${safe(evidencePack.directories || [])}`,
      `- CSV_SUMMARY: ${safe(evidencePack.csv || input?.csvSummary || {})}`
    ].join("\n");

    const userText = [
      "Company inputs",
      `- Name: ${input.prospect_company || ""}`,
      `- Website: ${input.prospect_website || ""}`,
      `- Buyer type: ${input.buyer_type || ""}`,
      `- Sales model: ${input.sales_model || input.salesModel || input.call_type || ""}`,
      `- Product/service focus: ${input.product_service || ""}`,
      `- USER_USPS (comma-separated): ${input.user_usps || input.usps || ""}`,
      `- Personas (comma-separated): ${input.personas || ""}`,
      `- Extra context: ${input.context || ""}`,
      "",
      evidenceText,
      "",
      "Return only JSON for this schema:",
      JSON.stringify(effectiveSchema)
    ].join("\n");

    messages = [
      { role: "system", content: system },
      { role: "user", content: userText }
    ];
  }
  // --- 4) Retry/jitter + behavior knobs
  const attempts = Math.max(1, Number(options.retry?.attempts ?? 2));
  const backoffMs = Math.max(0, Number(options.retry?.backoffMs ?? 600));

  const maxTokens = Number.isFinite(Number(options.maxTokens))
    ? Number(options.maxTokens)
    : (ENV_MAX_TOKENS || 3072);
  const temperature = (typeof options.temperature === "number") ? options.temperature : 0;
  const seedEnv = (process.env.LLM_SEED != null ? Number(process.env.LLM_SEED) : undefined);
  const seed = (options.seed != null ? Number(options.seed) : seedEnv);

  // --- 5) Endpoint URL
  const base = azure.endpoint.replace(/\/+$/, "");
  const dep = encodeURIComponent(azure.deployment);
  const ver = encodeURIComponent(azure.apiVersion);
  const url = `${base}/openai/deployments/${dep}/chat/completions?api-version=${ver}`;

  // --- 6) Fresh body per attempt (adds a tiny non-semantic attempt hint in system on retries)
  const mkReqBody = (attemptIndex) => {
    const bumped = (attemptIndex > 0)
      ? messages.map(m => (m.role === "system")
        ? { ...m, content: `${m.content}\n\n[attempt:${attemptIndex + 1}]` }
        : m)
      : messages;

    const body = {
      messages: bumped,
      temperature,
      max_tokens: maxTokens,
      response_format: {
        type: "json_schema",
        json_schema: {
          name: (effectiveSchema.title || "campaign_schema").replace(/[^\w\-]/g, "_"),
          schema: effectiveSchema,
          strict: true
        }
      }
    };
    // Only pass seed if explicitly allowed (some Azure deployments 400 on unknown param)
    if (options.useSeed === true && seed != null && Number.isFinite(seed)) {
      body.seed = Number(seed);
    }
    return body;
  };

  // --- 7) Attempt loop with jittered linear backoff
  let lastErr;
  for (let i = 0; i < attempts; i++) {
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
        headers: { "Content-Type": "application/json", "api-key": azure.apiKey },
        body: JSON.stringify(mkReqBody(i))
      });

      const raw = await res.text();
      if (!res.ok) {
        const msg = raw && raw.length > 2000 ? raw.slice(0, 2000) + "…(truncated)" : (raw || res.statusText);
        throw new Error(`OpenAI error ${res.status}: ${msg}`);
      }

      let wire;
      try { wire = raw ? JSON.parse(raw) : null; } catch { wire = null; }
      const content = wire?.choices?.[0]?.message?.content;
      if (!content) throw new Error("Empty model response");

      // Fence-safe parse
      let cleaned = String(content).trim();
      if (/^```/i.test(cleaned)) {
        cleaned = cleaned.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, "");
      }

      const obj = JSON.parse(cleaned);
      if (obj === null || typeof obj !== "object" || Array.isArray(obj)) {
        throw new Error("Model returned non-object JSON");
      }
      return obj;
    } catch (e) {
      // Normalize and tag the error so the caller can distinguish timeout vs generic LLM error
      const err = (e instanceof Error) ? e : new Error(String(e));
      const msg = String(err.message || e || "");
      const aborted = (err.name === "AbortError") || /aborted|abort|timeout/i.test(msg);
      if (!("code" in err)) {
        Object.defineProperty(err, "code", { value: aborted ? "llm_timeout" : "llm_error", enumerable: true });
      } else if (!err.code) {
        err.code = aborted ? "llm_timeout" : "llm_error";
      }
      lastErr = err;

      if (i < attempts - 1 && backoffMs) {
        const jitter = Math.floor(Math.random() * 250); // 0–250ms
        const step = backoffMs * (i + 1);
        await new Promise(r => setTimeout(r, step + jitter));
      }
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastErr || new Error("Unknown LLM error");
}

Object.defineProperty(module.exports, "schemaJson", {
  enumerable: true,
  get() { return schemaJson; }
});

module.exports.buildDefaultMessages = buildDefaultMessages;
module.exports.buildCustomMessages = buildCustomMessages;
// schemaJson is exposed via the getter above
module.exports.generate = generate;
