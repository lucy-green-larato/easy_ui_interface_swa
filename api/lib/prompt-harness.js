// /api/lib/prompt-harness.js 2025-11-03 v 10
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
function loadIndustrySources(industryRaw) {
  try {
    const industry = String(industryRaw || "").toLowerCase().replace(/\s+/g, "-");
    const baseDir = path.join(__dirname, "..", "packs", "industry-sources");
    const general = fs.existsSync(path.join(baseDir, "sources.md"))
      ? fs.readFileSync(path.join(baseDir, "sources.md"), "utf8")
      : "";
    const sectPath = path.join(baseDir, `${industry}.md`);
    const sector = fs.existsSync(sectPath) ? fs.readFileSync(sectPath, "utf8") : "";
    return { general, sector };
  } catch { return { general: "", sector: "" }; }
}

// ---- Env defaults (override with options.azure below) ----
const ENV_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT;
const ENV_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT;
const ENV_API_VER = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";
const ENV_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const LLM_TIMEOUT_MS_DEFAULT = Number(process.env.LLM_TIMEOUT_MS || "45000");

const ENV_MAX_TOKENS = Number(process.env.LLM_MAX_TOKENS || process.env.AZURE_OPENAI_MAX_TOKENS || "8192");

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

// ---- Robust JSON extraction / repair helpers ----
function extractJsonCandidate(s) {
  if (!s) return "";
  // Prefer fenced ```json … ```
  const fence = s.match(/```json\s*([\s\S]*?)```/i);
  if (fence && fence[1]) return fence[1].trim();

  // Otherwise, take the first plausible top-level object
  const start = s.indexOf("{");
  const end = s.lastIndexOf("}");
  if (start !== -1 && end !== -1 && end > start) return s.slice(start, end + 1).trim();
  return s.trim();
}

function tryParseJson(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function parseModelJsonOrRepair(rawText) {
  let candidate = extractJsonCandidate(String(rawText || ""));

  // 1) First attempt (fast path)
  let obj = tryParseJson(candidate);
  if (obj) return obj;

  // 2) Minimal, safe repairs for common LLM issues
  let repaired = candidate
    .replace(/^\uFEFF/, "")                           // strip BOM
    .replace(/"([^"\\]*(\\.[^"\\]*)*)"/g, m =>       // collapse CR/LF inside quoted strings
      m.replace(/\r?\n/g, "\\n"))
    .replace(/,\s*([}\]])/g, "$1");                   // remove trailing commas

  obj = tryParseJson(repaired);
  if (obj) return obj;

  const err = new Error("draft_json_parse_error: unrecoverable JSON");
  err.code = "draft_json_parse_error";
  err.details = {
    length: candidate.length,
    head: candidate.slice(0, 1200),
    tail: candidate.slice(-1200)
  };
  throw err;
}

// ---- Personas & rules ----
const BASE_RULES = `
Return STRICTLY valid JSON that conforms to the provided schema. Do NOT include any prose before or after the JSON.
PLACEHOLDERS
- Never invent placeholder names (e.g., "Product A/B", "Vendor A–E"). If a real product/vendor name or page is unknown, omit the item.
- Do not use example.com or dummy URLs. Prefer deterministic evidence supplied by the worker (industry sources, CSV population, company site).

EVIDENCE RULES
- Every bullet/statement must end with a short source tag in parentheses, e.g., (Company site), (Ofcom), (ONS), (DSIT), (PDF extract), (Trade press), (Directory).
- Evidence log items must be SPECIFIC:
  • Include concrete numbers, dates, and named entities (e.g., “Connected Nations 2024”, “Cyber Security Breaches Survey 2025”).
  • Prefer deep links to the exact page, not homepages.
  • For company site claims, reference the exact product page (title + URL), and include a short verbatim quote in 'quote' when available.
  • Avoid generic marketing claims with no figures; omit if not evidenced.

CAMPAIGN CREATION RULES
- When creating the campaign outline or final messaging, always map the company’s products (from product_names) to the buyer needs and blockers (from csv_signals). Each section must reference how at least one of the products addresses a specific buyer need or purchase driver.

- STRICT NO-FABRICATION FOR CASE STUDIES:
  • Only include case studies that are evidenced by the evidence pack OR are direct pages on the same host as the provided company website.
  • If you cannot find any genuine case studies, return an empty array for the case_study_library section (do not invent customers or URLs).
  • Use deep links (product/case-study pages), not homepages. Include page title and exact URL.

SOURCE MIX (hard requirement; map to schema-safe source_type)
- Ensure ≥2 items from Company site (product pages), and ≥3 items from external regulators/government (Ofcom/ONS/DSIT) where applicable.

CSV & USPs
- Treat CSV as current market research for targeting/messaging (tag inline as (CSV)). The schema does not capture CSV as source_type; keep "Company site"/regulator/etc. in evidence_log.source_type and reserve "(CSV)" for inline tags.
- SUPPLIER_USPS are accepted inputs; use them directly where relevant and tag inline with (Company site) if corroborated, otherwise leave the inline tag off and incorporate them as inputs.

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
  
SIZE LIMITS
- Keep arrays compact so the whole document fits: evidence_log ≤ 14 items, case_study_library ≤ 3, competitor_set = 5, discovery_questions 5–7, objection_cards 3, emails exactly 4. Email bodies ~90–120 words each.

EXECUTIVE SUMMARY
- executive_summary is an array of strings.
  • Item #1 (paragraph, ~110–140 words): name the company and explicitly reference its relevant products/services found on the website (use exact product names, e.g., “Bonded Internet”, “SD-One”, “Continuum”, “Continuum Constellation”) and tie them to outcomes for the specified buyer context (e.g., UK construction).
  • Items #2–#5: “Why now” bullets (each ends with a citation tag like (Ofcom), (ONS), (DSIT), (Company site)). Prioritise regulator/government sources for market signals; include concrete numbers/percentages/dates.

  EVIDENCE LOG
- evidence_log entries MUST satisfy the schema:
  { claim_id: "CLM-###", summary (cited), source_type (enum-compliant), url, title, quote? }.
- Ensure at least 2 items are clearly from the company (Company site source_type), and ≥3 items from external sources (Ofcom/ONS/DSIT/PDF extract/Trade press/Directory).
- When using LinkedIn, set source_type="Company site" but keep the inline "(LinkedIn)" tag inside the summary.
- Include an addressable-market item from CSV (use the pattern:
  "Addressable market: <rows> companies in <csvFileName>. <focusComment>" and source_type="Directory", title="CSV population", url=<csvFileName>).
- [ ] No placeholder items ("Product A/B", "Vendor A–E") and no dummy URLs.


  MANDATORY EVIDENCE CHECKLIST (reject output if missing):
- [ ] CSV addressable market (Directory)
- [ ] ≥1 regulator/government source relevant to the industry (e.g., Ofcom/ONS/DSIT/NCSC)
- [ ] ≥2 Company site items (product/capability pages)
- [ ] All user-supplied competitors + ≥1 inferred alternatives (with product-page deep links)
- [ ] ≥1 LinkedIn items (use post permalinks; mark "(LinkedIn)" in summary)

CASE STUDY LIBRARY
- case_study_library is an array. Each item must be sourced from the company website (same host as prospect_website) or appear in the evidence pack.
- Required fields per item: { customer, industry?, headline, bullets[] (2–4), url, source_type="Company site" }.
- Do NOT invent customers or URLs. If none are found, return [].

SALES MODEL EVIDENCE
- If Sales model is "direct": do NOT include partner recruitment/enablement claims in evidence_log.
  Focus evidence on direct-customer proof (product pages, service specs, case studies, pricing/SLAs).
- If Sales model is "partner"/"channel"/"indirect": do NOT include direct-only onboarding unless it demonstrates partner value
  (e.g., incentives, enablement, co-sell, margin).

COMPETITOR COVERAGE
- Include all user-supplied competitors AND infer at least two additional relevant alternatives
  (similar products into similar markets solving CSV buyer problems). Prefer deep links to product pages.

INDUSTRY SOURCES
- Prefer reputable UK sources (use INDUSTRY_SOURCES_GENERAL_MD/SECTOR_MD as the shortlist) for market statistics.

POSITIONING & DIFFERENTIATION
- Provide a concise value_prop for the UK context.
- differentiators: ≥3 items; incorporate ≥2 SUPPLIER_USPS if given (cite Company site if possible; otherwise keep without citation).
- competitor_set: 5 vendors with reason_in_set and URL.

MESSAGING MATRIX
- nonnegotiables: ≥3.
- matrix rows: persona, pain, value_statement (cited), proof (cited), cta.
- Use CSV fields to drive wording: SimplifiedIndustry → persona context; TopBlockers → pains; TopNeedsSupplier → nonnegotiables; TopPurchases → value/what_you_get.

OFFER STRATEGY
- landing_page: hero, why_it_matters[] (cited), what_you_get[] (cited; reflect CSV + SUPPLIER_USPS), how_it_works[], outcomes[] (cited), proof[] (cited), cta.
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
- Sales model (strict): ${inputs.sales_model || inputs.salesModel || inputs.call_type || ""}
- USER_NOTES (integrate explicitly): ${safe(inputs.notes || "")}
- Product/service focus: ${inputs.product_service || ""}
- SUPPLIER_USPS (comma-separated): ${inputs.supplier_usps || inputs.usps || ""}
- Personas (comma-separated): ${inputs.personas || ""}
- Extra context: ${inputs.context || ""}

Evidence pack (prioritise Company site → LinkedIn → Regulator/Gov → Other)
- COMPANY_WEBSITE_HTML_OR_TEXT: ${safe(evidencePack.website || [])}
- LINKEDIN_COMPANY: ${safe(evidencePack.linkedin || [])}
- IXBRL_SUMMARY: ${safe(evidencePack.ixbrl || {})}
- PDF_EXTRACTS: ${safe(evidencePack.pdf || [])}
- DIRECTORY_MATCHES: ${safe(evidencePack.directories || [])}
- CSV_SUMMARY: ${safe(evidencePack.csv || inputs?.csvSummary || {})}
- INDUSTRY_SOURCES_GENERAL_MD: ${safe(loadIndustrySources(inputs?.campaign_industry || inputs?.buyer_industry || inputs?.industry).general)}
- INDUSTRY_SOURCES_SECTOR_MD: ${safe(loadIndustrySources(inputs?.campaign_industry || inputs?.buyer_industry || inputs?.industry).sector)}
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
      packs,
      useDocSpec: true
    });
    messages = built.messages;
  } else {
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
      `- SUPPLIER_USPS (comma-separated): ${input.supplier_usps || input.usps || ""}`,
      `- Personas (comma-separated): ${input.personas || ""}`,
      `- Extra context: ${input.context || ""}`,
      "",
      evidenceText,
      "",
    ].join("\n");

    messages = [
      { role: "system", content: system },
      { role: "user", content: userText }
    ];
  }
  // --- 4) Retry/jitter + behavior knobs
  const attempts = Math.max(1, Number(options.retry?.attempts ?? 2));
  const backoffMs = Math.max(0, Number(options.retry?.backoffMs ?? 600));
  try {
    console.log("[harness] limits", {
      maxTokens,
      evidenceCharsMax: MAX_STR,
      timeoutMs: Number(options.timeoutMs ?? LLM_TIMEOUT_MS_DEFAULT)
    });
  } catch { }
  const maxTokens = Number.isFinite(Number(options.maxTokens))
    ? Number(options.maxTokens)
    : (ENV_MAX_TOKENS || 8192);
  const temperature = (typeof options.temperature === "number") ? options.temperature : 0;

  const seedEnv = (process.env.LLM_SEED != null ? Number(process.env.LLM_SEED) : undefined);
  const seed = (options.seed != null ? Number(options.seed) : seedEnv);

  // --- 5) Endpoint URL
  const base = azure.endpoint.replace(/\/+$/, "");
  const dep = encodeURIComponent(azure.deployment);
  const ver = encodeURIComponent(azure.apiVersion);
  const url = `${base}/openai/deployments/${dep}/chat/completions?api-version=${ver}`;

  // --- 6) Fresh body per attempt (adds a tiny non-semantic attempt hint in system on retries)
  const mkReqBody = (format, attemptIndex) => {
    const bumped = (attemptIndex > 0)
      ? messages.map(m => (m.role === "system")
        ? { ...m, content: `${m.content}\n\n[attempt:${attemptIndex + 1}]` }
        : m)
      : messages;

    const body = {
      messages: bumped,
      temperature,
      max_tokens: maxTokens
    };

    if (format === "json_object") {
      body.response_format = { type: "json_object" };
    } else {
      body.response_format = {
        type: "json_schema",
        json_schema: {
          name: (effectiveSchema.title || "campaign_schema").replace(/[^\w\-]/g, "_"),
          schema: effectiveSchema,
          strict: true
        }
      };
    }

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
      // 1) First attempt: strict json_schema
      let res = await fetch(url, {
        method: "POST",
        signal: controller.signal,
        headers: { "Content-Type": "application/json", "api-key": azure.apiKey },
        body: JSON.stringify(mkReqBody("json_schema", i))
      });

      let raw = await res.text();
      if (!res.ok) {
        const msg = raw && raw.length > 2000 ? raw.slice(0, 2000) + "…(truncated)" : (raw || res.statusText);
        throw new Error(`OpenAI error ${res.status}: ${msg}`);
      }

      let wire; try { wire = raw ? JSON.parse(raw) : null; } catch { wire = null; }
      let content = wire?.choices?.[0]?.message?.content;
      if (!content) throw new Error("Empty model response");
      function stripLeadingSchemaEcho(s) {
        try {
          const firstOpen = s.indexOf("{");
          const lastClose = s.lastIndexOf("}");
          if (firstOpen === -1 || lastClose === -1) return s;

          // Take the first top-level object block
          const firstBlock = s.slice(firstOpen, s.indexOf("}", firstOpen) + 1);
          const firstObj = JSON.parse(firstBlock);

          // If it looks like a JSON-Schema (has $schema & properties), drop it and keep the next object
          if (firstObj && typeof firstObj === "object" && firstObj.$schema && firstObj.properties) {
            const rest = s.slice(firstOpen + firstBlock.length);
            const nextObjStart = rest.indexOf("{");
            if (nextObjStart !== -1) return rest.slice(nextObjStart);
          }
          return s;
        } catch { return s; }
      }

      try {
        const obj = parseModelJsonOrRepair(stripLeadingSchemaEcho(content));
        if (obj === null || typeof obj !== "object" || Array.isArray(obj)) {
          throw new Error("Model returned non-object JSON");
        }
        return obj;
      } catch (parseErr) {
        // Fallback: one lightweight re-ask in json_object mode (same messages)
        // Only do this when the parse truly failed
        if (!(parseErr && parseErr.code === "draft_json_parse_error")) throw parseErr;

        // Re-issue once with json_object (same timeout & headers)
        res = await fetch(url, {
          method: "POST",
          signal: controller.signal,
          headers: { "Content-Type": "application/json", "api-key": azure.apiKey },
          body: JSON.stringify(mkReqBody("json_object", i))
        });

        raw = await res.text();
        if (!res.ok) {
          const msg2 = raw && raw.length > 2000 ? raw.slice(0, 2000) + "…(truncated)" : (raw || res.statusText);
          throw new Error(`OpenAI error ${res.status}: ${msg2}`);
        }
        try { wire = raw ? JSON.parse(raw) : null; } catch { wire = null; }
        content = wire?.choices?.[0]?.message?.content || "";

        const obj2 = parseModelJsonOrRepair(stripLeadingSchemaEcho(content));
        if (obj2 === null || typeof obj2 !== "object" || Array.isArray(obj2)) {
          const e2 = new Error("draft_json_parse_error: unrecoverable JSON in fallback");
          e2.code = "draft_json_parse_error";
          e2.details = { head: String(content).slice(0, 1200), tail: String(content).slice(-1200), length: String(content).length };
          throw e2;
        }
        return obj2;
      }
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
