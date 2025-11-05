// /api/lib/prompt-harness.js 2025-11-05 v11
// Exports:
//   - buildDefaultMessages({ inputs, evidencePack, packs, useDocSpec? })
//   - buildCustomMessages({ customPromptText, evidencePack })
//   - schemaJson  (from loaded/default schema)
//   - generate({ schemaPath?, packs?, input?, evidencePack?, options? }) -> ALWAYS returns a JSON object
//
// Behaviour:
// - If schemaPath is provided, that schema is used for JSON Schema mode (strict).
// - Azure OpenAI Chat Completions (2024-08-01-preview) only.
// - Env can be overridden via options.azure { endpoint, apiKey, apiVersion, deployment }.
// - Node 18/20 global fetch; no extra deps.

const fs = require("fs");
const path = require("path");

// ---- Small util: load industry sources (optional packs) ----
function loadIndustrySources(industryRaw) {
  try {
    const industry = String(industryRaw || "").toLowerCase().replace(/\s+/g, "-");
    const baseDir = path.join(__dirname, "..", "packs", "industry-sources");
    const generalPath = path.join(baseDir, "sources.md");
    const sectorPath  = path.join(baseDir, `${industry}.md`);
    const general = fs.existsSync(generalPath) ? fs.readFileSync(generalPath, "utf8") : "";
    const sector  = fs.existsSync(sectorPath)  ? fs.readFileSync(sectorPath,  "utf8") : "";
    return { general, sector };
  } catch {
    return { general: "", sector: "" };
  }
}

// ---- Env defaults (override with options.azure below) ----
const ENV = {
  endpoint:   process.env.AZURE_OPENAI_ENDPOINT,
  deployment: process.env.AZURE_OPENAI_DEPLOYMENT,
  apiVersion: process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview",
  apiKey:     process.env.AZURE_OPENAI_API_KEY,
  timeoutMs:  Number(process.env.LLM_TIMEOUT_MS || "45000"),
  maxTokens:  Number(process.env.LLM_MAX_TOKENS || process.env.AZURE_OPENAI_MAX_TOKENS || "8192"),
  promptMax:  (() => {
    const raw = process.env.PROMPT_MAX_EVIDENCE_CHARS || process.env.MAX_STR;
    const n = Number(raw);
    if (!Number.isFinite(n)) return 380_000;
    return Math.min(Math.max(n, 50_000), 1_500_000);
  })()
};

// dev-only visibility
if (process.env.NODE_ENV !== "production" && !global.__LOGGED_PROMPT_MAX__) {
  try { console.log(`[prompt-harness] PROMPT_MAX_EVIDENCE_CHARS=${ENV.promptMax}`); } catch {}
  global.__LOGGED_PROMPT_MAX__ = true;
}

// ---- Default schema path ----
const DEFAULT_SCHEMA_PATH = path.join(__dirname, "..", "schemas", "campaign.schema.json");

// Robust schema path resolver (must exist)
function resolveSchemaPath(p) {
  if (!p) return DEFAULT_SCHEMA_PATH;
  if (path.isAbsolute(p)) return p;
  const candidates = [
    p,
    path.join(__dirname, "..", p),
    path.join(process.cwd(), p),
    path.join(process.cwd(), "api", p)
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  throw new Error(`Schema path not found: ${p}`);
}

function loadJsonFile(absOrRelPath) {
  const abs = resolveSchemaPath(absOrRelPath);
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

// ---- Safe clipping (head + tail) ----
const MAX_STR = ENV.promptMax;
function clipString(s, max = MAX_STR) {
  const str = typeof s === "string" ? s : JSON.stringify(s ?? "");
  if (str.length <= max) return str;
  const keep = Math.floor(max / 2);
  return str.slice(0, keep) + " …TRUNCATED… " + str.slice(-keep);
}
function safe(obj, max = MAX_STR) {
  try { return clipString(obj, max); } catch { return "null"; }
}

// ---- Robust JSON extraction / repair helpers ----
function extractJsonCandidate(s) {
  if (!s) return "";
  // Prefer ```json fenced blocks
  const fenced = s.match(/```json\s*([\s\S]*?)```/i);
  if (fenced && fenced[1]) return fenced[1].trim();

  // Any fenced code block
  const anyFence = s.match(/```\s*([\s\S]*?)```/);
  if (anyFence && anyFence[1]) {
    const inner = anyFence[1].trim();
    const start = inner.indexOf("{");
    const end   = inner.lastIndexOf("}");
    if (start !== -1 && end > start) return inner.slice(start, end + 1).trim();
  }

  // First plausible top-level object
  const start = s.indexOf("{");
  const end = s.lastIndexOf("}");
  if (start !== -1 && end !== -1 && end > start) return s.slice(start, end + 1).trim();

  return s.trim();
}

function tryParseJson(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function stripLeadingSchemaEcho(s) {
  // Some models echo the JSON Schema before the answer
  try {
    const firstOpen = s.indexOf("{");
    const lastClose = s.lastIndexOf("}");
    if (firstOpen === -1 || lastClose === -1) return s;

    // Try parse first block; if it looks like a JSON-Schema (has $schema & properties), remove it.
    let depth = 0, i = firstOpen;
    for (; i <= lastClose; i++) {
      if (s[i] === "{") depth++;
      else if (s[i] === "}") { depth--; if (depth === 0) break; }
    }
    const firstBlock = s.slice(firstOpen, i + 1);
    const firstObj = JSON.parse(firstBlock);
    if (firstObj && typeof firstObj === "object" && firstObj.$schema && firstObj.properties) {
      const rest = s.slice(i + 1);
      const nextStart = rest.indexOf("{");
      if (nextStart !== -1) return rest.slice(nextStart);
    }
    return s;
  } catch { return s; }
}

function parseModelJsonOrRepair(rawText) {
  let candidate = extractJsonCandidate(String(rawText || ""));
  candidate = stripLeadingSchemaEcho(candidate);

  // 1) Fast path
  let obj = tryParseJson(candidate);
  if (obj) return obj;

  // 2) Repairs for common issues
  let repaired = candidate
    .replace(/^\uFEFF/, "")                           // strip BOM
    .replace(/"([^"\\]*(\\.[^"\\]*)*)"/g, m =>       // collapse CR/LF in quoted strings
      m.replace(/\r?\n/g, "\\n"))
    .replace(/,\s*([}\]])/g, "$1");                   // trailing commas

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

// ---- Personas & rules (unchanged in spirit; tightened text) ----
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
- Map product_names to buyer needs and blockers (from csv_signals). Each section must show how at least one product addresses a specific buyer need or purchase driver.

STRICT NO-FABRICATION (CASE STUDIES)
- Only include case studies that appear in the evidence pack OR are direct pages on the same host as the company website.
- If none, return [] (do not invent customers or URLs).

SOURCE MIX
- Ensure ≥2 items from Company site (product pages) and ≥3 items from external regulators/government (Ofcom/ONS/DSIT) where applicable.

CSV & USPs
- Use inline (CSV) tag only in text; evidence_log.source_type remains the concrete source (Company site/regulator/etc.).
- Use SUPPLIER_USPS only when corroborated (tag with (Company site)); otherwise incorporate as input without a tag.

STYLE
- UK English. Currency & numerals: £20.756m, £1,421,646, 8.4%.
- No generic advice; be specific to the evidence.
`.trim();

const DOC_SPEC = `
DOCUMENT STRUCTURE & SIZE LIMITS
- evidence_log ≤ 14 items, case_study_library ≤ 3, competitor_set = 5, discovery_questions 5–7, objection_cards 3, emails exactly 4 (~90–120 words each).

EXECUTIVE SUMMARY
- executive_summary is an array of strings.
  • Item #1 (~110–140 words): name the company and explicitly reference website product names.
  • Items #2–#5: “Why now” bullets with concrete figures and regulator/company citations.

MANDATORY EVIDENCE CHECKLIST
- [ ] CSV addressable market (Directory)
- [ ] ≥1 regulator/government source (Ofcom/ONS/DSIT/NCSC)
- [ ] ≥2 Company site items (product/capability pages)
- [ ] All user-supplied competitors + ≥1 inferred alternative (deep links)
- [ ] ≥1 LinkedIn item (inline "(LinkedIn)" tag in summary)

SECTION RULES (short)
- Positioning: value_prop concise; differentiators ≥3 (include ≥2 SUPPLIER_USPS if available).
- Messaging: nonnegotiables ≥3; rows include persona, pain, value_statement (cited), proof (cited), cta.
- Offer: what_you_get reflects CSV + USPs; outcomes/proof cited.
- Channel: 4 emails, each with ≥1 citation; LinkedIn items specific and tied to CSV/site.
- Enablement/Measurement/Compliance/Risks: concrete, cited where external.

CSV MAPPING
- Use selected industry only (when specified). Map TopBlockers/TopNeedsSupplier/TopPurchases into pains, nonnegotiables, offer, objections.
`.trim();

const PARTNER_PERSONA = `
You are a top-performing UK B2B channel strategist (tech markets) and CMO.
You understand partner recruitment, enablement, and common channel constraints (M&A, higher interest rates, lead-gen pressure).
Your job is to produce an evidence-only campaign that adds value to partners by making them more competitive, improving their customer service, and differentiating them in the market.
`.trim();

const DIRECT_PERSONA = `
You are a top-performing UK B2B tech market strategist and CMO.
You understand the challenges that your customers face (need better productivity, invest in the right technologies and why).
Your job is to produce an evidence-only campaign that adds value to direct customers by making them more efficient and productive, improving customer service, and differentiating them in the market. (Key: cybersecurity, AI, IoT, mobile data connectivity)
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
- Name: ${inputs.prospect_company || inputs.supplier_company || ""}
- Website: ${inputs.prospect_website || inputs.supplier_website || ""}
- Buyer type: ${inputs.buyer_type || ""}
- Sales model (strict): ${inputs.sales_model || inputs.salesModel || inputs.call_type || ""}
- USER_NOTES (integrate explicitly): ${safe(inputs.notes || "")}
- Product/service focus: ${inputs.product_service || ""}
- SUPPLIER_USPS (comma-separated): ${Array.isArray(inputs.supplier_usps) ? inputs.supplier_usps.join(", ") : (inputs.supplier_usps || inputs.usps || "")}
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

// ---- Schema holder (lazy default) ----
let schemaJson = tryLoadDefaultSchema() || { title: "campaign", type: "object" };

// ---- Core generate() ----
async function generate({ schemaPath, packs = {}, input = {}, evidencePack = {}, options = {} } = {}) {
  // Param-name-agnostic: safe counts log
  try {
    const ep = evidencePack || {};
    console.log("[harness] evidencePack counts", {
      website: Array.isArray(ep.website) ? ep.website.length : 0,
      linkedin: Array.isArray(ep.linkedin) ? ep.linkedin.length : 0,
      pdf: Array.isArray(ep.pdf) ? ep.pdf.length : 0,
      directories: Array.isArray(ep.directories) ? ep.directories.length : 0,
      ixbrlKeys: ep.ixbrl ? Object.keys(ep.ixbrl).length : 0,
      csvKeys: ep.csv ? Object.keys(ep.csv).length : 0
    });
  } catch {}

  // 1) Load schema WITHOUT mutating the module global (thread-safe)
  let effectiveSchema = schemaJson;
  if (schemaPath) {
    const loaded = loadJsonFile(schemaPath);
    if (!loaded || typeof loaded !== "object") throw new Error("Invalid schema");
    effectiveSchema = loaded;
  }

  // 2) Azure config (env with per-call overrides)
  const azure = {
    endpoint:  options.azure?.endpoint  || ENV.endpoint,
    apiKey:    options.azure?.apiKey    || ENV.apiKey,
    apiVersion:options.azure?.apiVersion|| ENV.apiVersion,
    deployment:options.azure?.deployment|| ENV.deployment,
    api:       (options.azure?.api || "chat").toLowerCase()
  };
  if (!azure.endpoint)   throw new Error("AZURE_OPENAI_ENDPOINT not configured");
  if (!azure.deployment) throw new Error("AZURE_OPENAI_DEPLOYMENT not configured");
  if (!azure.apiKey)     throw new Error("AZURE_OPENAI_API_KEY not configured");

  // 3) Build messages (prefer external messages if provided)
  let messages;
  if (Array.isArray(options.messages) && options.messages.length) {
    messages = options.messages;
  } else {
    const built = buildDefaultMessages({
      inputs: input,
      evidencePack,
      packs,
      useDocSpec: true
    });
    messages = built.messages;
  }

  // 4) Behaviour knobs
  const attempts   = Math.max(1, Number(options.retry?.attempts ?? 2));
  const backoffMs  = Math.max(0, Number(options.retry?.backoffMs ?? 600));
  const maxTokens  = Number.isFinite(Number(options.maxTokens)) ? Number(options.maxTokens) : (ENV.maxTokens || 8192);
  const temperature= (typeof options.temperature === "number") ? options.temperature : 0;
  const seedEnv    = (process.env.LLM_SEED != null ? Number(process.env.LLM_SEED) : undefined);
  const seed       = (options.seed != null ? Number(options.seed) : seedEnv);
  const timeoutMsRaw = options.timeoutMs ?? ENV.timeoutMs;
  const timeoutMs  = (Number.isFinite(Number(timeoutMsRaw)) && Number(timeoutMsRaw) > 0) ? Number(timeoutMsRaw) : ENV.timeoutMs;

  try {
    console.log("[harness] limits", { maxTokens, evidenceCharsMax: MAX_STR, timeoutMs });
  } catch {}

  // 5) Endpoint URL
  const base = azure.endpoint.replace(/\/+$/, "");
  const dep  = encodeURIComponent(azure.deployment);
  const ver  = encodeURIComponent(azure.apiVersion);
  const url  = `${base}/openai/deployments/${dep}/chat/completions?api-version=${ver}`;

  // 6) Fresh body per attempt (adds tiny, non-semantic attempt hint in system on retries)
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

  // 7) Attempt loop with jittered linear backoff
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort("llm_timeout"), timeoutMs);

    try {
      // First: strict json_schema
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

      // Parse or fallback
      try {
        const obj = parseModelJsonOrRepair(content);
        if (obj === null || typeof obj !== "object" || Array.isArray(obj)) {
          throw new Error("Model returned non-object JSON");
        }
        return obj;
      } catch (parseErr) {
        if (!(parseErr && parseErr.code === "draft_json_parse_error")) throw parseErr;

        // One fallback attempt: json_object (same messages)
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

        const obj2 = parseModelJsonOrRepair(content);
        if (obj2 === null || typeof obj2 !== "object" || Array.isArray(obj2)) {
          const e2 = new Error("draft_json_parse_error: unrecoverable JSON in fallback");
          e2.code = "draft_json_parse_error";
          e2.details = { head: String(content).slice(0, 1200), tail: String(content).slice(-1200), length: String(content).length };
          throw e2;
        }
        return obj2;
      }
    } catch (e) {
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

// ---- Exports ----
Object.defineProperty(module.exports, "schemaJson", {
  enumerable: true,
  get() { return schemaJson; }
});
module.exports.buildDefaultMessages = buildDefaultMessages;
module.exports.buildCustomMessages  = buildCustomMessages;
module.exports.generate             = generate;
