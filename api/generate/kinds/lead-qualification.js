// generate/kinds/lead-qualification.js

const fs = require("fs");
const path = require("path");
const Ajv = require("ajv");

// ajv-formats is optional; if missing we still run.
let addFormats = null;
try { addFormats = require("ajv-formats"); } catch { /* optional */ }

const Busboy = require("busboy");
const pdfParse = require("pdf-parse");
const htmlDocx = require("html-docx-js");
const https = require("https");
const http = require("http");
const HTTPS_AGENT = new https.Agent({ keepAlive: true, maxSockets: 100 });
const HTTP_AGENT = new http.Agent({ keepAlive: true, maxSockets: 100 });

// ---------- Config (tweak via env) ----------
const VERSION = process.env.VERSION || "qual-1.0.0";
const FETCH_TIMEOUT_MS = Number(process.env.FETCH_TIMEOUT_MS || "8000");
const QUAL_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";
const QUAL_AZURE_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT || "";
const QUAL_AZURE_API_VERSION = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";
const PDF_PAGE_CAPS = (process.env.PDF_PAGE_CAPS || "10,25").split(",").map(n => Number(n.trim())).filter(Boolean);
const PDF_CHAR_CAP = Number(process.env.PDF_CHAR_CAP || "120000");
const FULL_TARGET_WORDS = Number(process.env.QUAL_FULL_TARGET_WORDS || "1750");
const DEFAULT_MIN_SUMMARY_CHARS = Number(process.env.QUAL_MIN_SUMMARY_CHARS || "600");
const DEFAULT_MIN_FULL_CHARS = Number(process.env.QUAL_MIN_FULL_CHARS || "1800");

// ---- Ajv setup & dynamic min-length for report.md ----
const ajv = new Ajv({ allErrors: true, allowUnionTypes: true, strict: false });
if (addFormats) addFormats(ajv);

// Load base schema once (api/schemas/qualification.v2.json)
const QUAL_SCHEMA_PATH = path.join(__dirname, "..", "..", "schemas", "qualification.v2.json");
let baseQualSchema;
try {
  baseQualSchema = JSON.parse(fs.readFileSync(QUAL_SCHEMA_PATH, "utf8"));
} catch {
  baseQualSchema = null;
}
// Fail fast at cold start if schema is missing (clear message for ops)
if (!baseQualSchema) {
  throw new Error(`qualification.v2.json not found or unreadable at ${QUAL_SCHEMA_PATH}`);
}

//feedback function for tool learning and improvement
const { trackGenerate, extractDomain, hashDomain } = require("../../utils/telemetry");

// -------------------- CORS (ALLOWED_ORIGIN aware) --------------------
function parseAllowedOrigins() {
  const raw = process.env.ALLOWED_ORIGIN || "";
  return raw.split(",").map(s => s.trim()).filter(Boolean);
}
function buildCorsHeaders(req) {
  const allowed = parseAllowedOrigins();
  const origin = (req.headers?.origin || req.headers?.Origin || "").trim();
  const headers = {
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-ms-client-principal",
  };
  if (allowed.length === 0) { headers["Access-Control-Allow-Origin"] = "*"; return headers; }
  if (origin && allowed.includes(origin)) { headers["Access-Control-Allow-Origin"] = origin; headers["Vary"] = "Origin"; }
  return headers;
}

// -------------------- Helpers --------------------
function deepClone(obj) { return obj ? JSON.parse(JSON.stringify(obj)) : obj; }

const _validatorCache = new Map();
function getQualValidator(minMdChars) {
  const key = String(minMdChars || 0);
  if (_validatorCache.has(key)) return _validatorCache.get(key);
  const schema = deepClone(baseQualSchema);

  try {
    let target = null;
    if (schema?.properties?.report?.properties?.md) {
      target = schema.properties.report.properties.md;
    } else if (schema?.definitions?.Report?.properties?.md) {
      target = schema.definitions.Report.properties.md;
    }
    if (target) {
      if (typeof target.minLength !== "number" || target.minLength < minMdChars) {
        target.minLength = Number(minMdChars);
      }
    }
  } catch { /* non-fatal */ }

  const validate = ajv.compile(schema);
  _validatorCache.set(key, validate);
  return validate;
}

const nowIso = () => new Date().toISOString();
const jparse = (x, fb) => { try { return typeof x === "string" ? JSON.parse(x) : (x ?? fb); } catch { return fb; } };
const stripJsonFences = s =>
  String(s || "").replace(/^```json\s*/i, "").replace(/^```/i, "").replace(/```$/i, "").trim();
const ensureArray = v => Array.isArray(v) ? v : (v ? [v] : []);

// -------------------- Multipart parsing (PDFs etc.) --------------------
function parseMultipart(req, { maxFiles = 2, maxFileBytes = 15 * 1024 * 1024 } = {}) {
  return new Promise((resolve, reject) => {
    try {
      const ct = (req.headers?.["content-type"] || req.headers?.["Content-Type"] || "");
      if (!/multipart\/form-data/i.test(ct)) return reject(new Error("Not multipart/form-data"));
      const bb = Busboy({ headers: req.headers || {} });
      const fields = {};
      const files = [];
      let total = 0;

      bb.on("file", (fieldname, file, info) => {
        const chunks = [];
        const filename = info?.filename || info?.fileName || "file";
        const contentType = info?.mimeType || info?.mimetype || "application/octet-stream";
        file.on("data", d => {
          total += d.length;
          if (total > maxFileBytes) {
            file.resume();
            bb.emit("error", new Error("File too large"));
            return;
          }
          chunks.push(d);
        });
        file.on("end", () => {
          if (files.length < maxFiles) files.push({ fieldname, filename, contentType, buffer: Buffer.concat(chunks) });
        });
      });
      bb.on("field", (name, val) => { fields[name] = val; });
      bb.on("error", err => reject(err));
      bb.on("finish", () => resolve({ fields, files }));

      const raw =
        Buffer.isBuffer(req.body) ? req.body :
          Buffer.isBuffer(req.rawBody) ? req.rawBody :
            (typeof req.body === "string" ? Buffer.from(req.body) :
              typeof req.rawBody === "string" ? Buffer.from(req.rawBody) :
                Buffer.alloc(0));
      bb.end(raw);
    } catch (e) { reject(e); }
  });
}

// -------------------- PDF extraction --------------------
function hasFinancialSignals(text) {
  if (!text) return false;
  const rx = /(turnover|revenue|gross\s+profit|operating\s+profit|profit\s+and\s+loss|comprehensive\s+income|balance\s+sheet|cash\s*(?:at\s*bank|and\s*in\s*hand)|current\s+assets|current\s+liabilities|net\s+assets|£\s?\d|\d{1,3}(?:,\d{3}){1,3})/i;
  return rx.test(text);
}

async function extractPdfTexts(fileObjs) {
  const out = [];
  for (const f of fileObjs) {
    let pickedText = "";
    let usedCap = 0;
    for (const cap of (PDF_PAGE_CAPS.length ? PDF_PAGE_CAPS : [10])) {
      try {
        const parsed = await pdfParse(f.buffer, {
          max: cap,
          pagerender: page => page.getTextContent().then(tc => tc.items.map(i => i.str).join(" "))
        });
        const raw = (parsed?.text || "").replace(/\r/g, "").trim();
        const sliced = raw.slice(0, PDF_CHAR_CAP);
        pickedText = sliced;
        usedCap = cap;
        if (hasFinancialSignals(sliced)) break;
      } catch {
        try {
          const parsedFull = await pdfParse(f.buffer);
          const rawFull = (parsedFull?.text || "").replace(/\r/g, "").trim();
          pickedText = rawFull.slice(0, PDF_CHAR_CAP);
          usedCap = 0;
        } catch { pickedText = ""; }
        break;
      }
    }
    out.push({ filename: f.filename || "report.pdf", text: pickedText, pagesTried: usedCap || undefined });
  }
  return out;
}

// -------------------- Fetch with timeout + site bundle --------------------
async function fetchText(url, { timeout = FETCH_TIMEOUT_MS, userAgent = `inside-track-tools/${VERSION}` } = {}) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeout);
  try {
    const r = await fetch(url, { headers: { "User-Agent": userAgent }, signal: ac.signal });
    if (!r.ok) return "";
    return await r.text();
  } catch { return ""; }
  finally { clearTimeout(t); }
}

function htmlToText(html, { cap = 150000 } = {}) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, cap);
}

async function expandWebsiteBundle(rootUrl, context) {
  const result = { text: "", pages: [] };
  if (!/^https?:\/\//i.test(rootUrl || "")) return result;

  const base = new URL(rootUrl);
  const SLUGS = [
    "", "/about", "/company", "/who-we-are", "/leadership", "/team", "/board", "/management", "/executive",
    "/partners", "/technology-partners", "/vendors", "/alliances",
    "/news", "/insights", "/media", "/press", "/blog",
    "/events", "/webinars", "/industries", "/sectors", "/solutions", "/services"
  ];

  const seen = new Set();
  const pages = [];
  const chunks = [];

  for (const slug of SLUGS) {
    let u;
    try { u = new URL(slug, base.origin + (base.pathname.endsWith("/") ? base.pathname : base.pathname + "/")); }
    catch { continue; }
    if (u.origin !== base.origin) continue;
    const href = u.toString().replace(/#.*$/, "");
    if (seen.has(href)) continue;
    seen.add(href);

    const html = await fetchText(href);
    if (!html) continue;

    const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
    const title = titleMatch ? titleMatch[1].replace(/\s+/g, " ").trim() : href;
    const text = htmlToText(html, { cap: 30000 });

    pages.push({ url: href, label: slug || "/" });
    chunks.push(`--- WEBSITE PAGE: ${title} (${href}) ---\n${text}`);
    if (pages.length >= 8) break; // small, safe limit
  }

  result.pages = pages;
  result.text = chunks.join("\n\n").slice(0, 180000);
  try { context.log?.(`[${VERSION}] website pages fetched: ${pages.length}`); } catch { }
  return result;
}

// -------------------- Prompt builder --------------------
function buildQualificationJsonPrompt(args) {
  const v = args.values || {};
  const callType = String(v.call_type || "").toLowerCase().startsWith("p") ? "Partner" : "Direct";
  const detailMode = args.detailMode === "summary" ? "summary" : "full";
  const targetWords = Number(args.targetWords || 0);
  const targetWordsLine = targetWords ? `TARGET LENGTH: ~${targetWords} words (±10%). HARD CAP ~${Math.round(targetWords * 1.06)}.` : "";

  const modeLine = detailMode === "summary"
    ? [
      "OUTPUT MODE: EXECUTIVE SUMMARY.",
      "- Keep each section to 1–2 crisp sentences maximum.",
      "- If a point lacks evidence in the provided sources, write “No public evidence found.”",
    ].join("\n")
    : [
      "OUTPUT MODE: FULL DETAIL.",
      "- Provide complete, evidenced detail across all sections.",
      "- Quote figures with year labels (e.g., “FY24: £20.76m”).",
      "- If unknown, say so explicitly; do not speculate.",
    ].join("\n");

  const banlist = [
    "well-positioned", "decades of experience", "cybersecurity landscape",
    "market differentiation", "client-centric", "cutting-edge",
    "robust posture", "holistic", "industry-leading", "best-in-class"
  ];
  const banlistLine = "BANNED WORDING: " + banlist.join(", ") + ".";

  const evidDensityRules = [
    "EVIDENCE DENSITY:",
    "- Company profile MUST include labelled figures where available (e.g., “FY24: £20.76m revenue; Loss before tax £824k; Average employees 92”).",
    "- Only list partners/technologies if explicitly present in WEBSITE TEXT or PDFs.",
    "- Trade shows: list only those explicitly evidenced in WEBSITE TEXT this calendar year; otherwise write ‘No public evidence found.’",
  ].join("\n");

  const ix = args.ixbrl || {};
  const ixBrief = JSON.stringify(ix?.years ? ix.years : (ix.summary?.years || ix));

  const pdfs = Array.isArray(args.pdfs) ? args.pdfs : [];
  const pdfBundle = pdfs.map(p => (`--- PDF: ${p.filename || "report.pdf"} ---\n${p.text || ""}`)).join("\n\n");
  const websiteText = args.websiteText || "";
  const seller = args.seller || { name: "", company: "", url: "" };
  const offer = args.ourOffer || { product: "", otherContext: "" };
  const websiteBlock = websiteText ? (`--- WEBSITE TEXT (multiple pages) ---\n${websiteText}`) : "";

  const role = [
    "You are a UK B2B/channel salesperson and GTM strategist.",
    "Write VALID JSON only. No markdown outside JSON.",
    "Use only the evidence provided here (PDF text, website text, iXBRL summary)."
  ].join("\n");

  const schema = `
JSON schema (conceptual):
{
  "report": {
    "md": string,
    "citations": [ { "label": string, "url": string } ]
  },
  "tips": [string, string, string]
}`.trim();

  const constraints = [
    "CONSTRAINTS:",
    "- UK business English; no generalisations; no assumptions.",
    "- If something is not evidenced, write exactly “No public evidence found.”",
    "",
    "FINANCIALS:",
    "- Search for: Revenue/Turnover, Gross profit, Operating profit/loss, Cash (bank and in hand),",
    "  Current assets, Current liabilities, Net assets/liabilities, Average monthly employees.",
    "- Quote figures with currency symbols and YEAR LABELS (e.g., “FY24: £20.76m”).",
    "",
    "TIE TO THE SELLER:",
    `- Salesperson: ${seller.name || "(unknown)"} · Company: ${seller.company || "(unknown)"} ${seller.url ? "· URL: " + seller.url : ""}`,
    `- Offer/product focus: ${offer.product || "(unspecified)"} · Other context: ${offer.otherContext || "(none)"}`
  ].join("\n");

  return [
    role, "",
    `CALL_TYPE: ${callType}`,
    `MODE: ${detailMode.toUpperCase()}`,
    modeLine,
    targetWordsLine,
    banlistLine,
    evidDensityRules,
    `Prospect website: ${v.prospect_website || "(not provided)"}`,
    `LinkedIn (URL only): ${v.company_linkedin || "(not provided)"}`,
    "",
    schema, "",
    constraints, "",
    "iXBRL summary (most recent first):",
    ixBrief, "",
    websiteBlock, "",
    pdfBundle
  ].join("\n");
}

// -------------------- OpenAI / Azure OpenAI --------------------
async function callModel({ system, prompt, temperature = 0.2, max_tokens = 2000, response_format }) {
  // Azure path
  if (process.env.AZURE_OPENAI_ENDPOINT && QUAL_AZURE_DEPLOYMENT) {
    const url = `${process.env.AZURE_OPENAI_ENDPOINT.replace(/\/+$/, "")}/openai/deployments/${encodeURIComponent(QUAL_AZURE_DEPLOYMENT)}/chat/completions?api-version=${encodeURIComponent(QUAL_AZURE_API_VERSION)}`;
    const body = {
      messages: [
        { role: "system", content: system || "You are a precise assistant." },
        { role: "user", content: prompt }
      ],
      temperature,
      max_tokens
    };
    if (response_format && typeof response_format === "object") {
      body.response_format = response_format;
    }
    const r = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "api-key": process.env.AZURE_OPENAI_API_KEY
      },
      body: JSON.stringify(body),
      agent: url.startsWith("https:") ? HTTPS_AGENT : HTTP_AGENT
    });
    if (!r.ok) throw new Error(`Azure OpenAI error: ${r.status} ${await r.text()}`);
    return await r.json();
  }

  // OpenAI path
  if (!process.env.OPENAI_API_KEY) throw new Error("OPENAI_API_KEY not set");
  const body = {
    model: QUAL_MODEL,
    messages: [
      { role: "system", content: system || "You are a precise assistant." },
      { role: "user", content: prompt }
    ],
    temperature,
    max_tokens
  };
  if (response_format && typeof response_format === "object") {
    body.response_format = response_format;
  }
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error(`OpenAI error: ${r.status} ${await r.text()}`);
  return await r.json();
}

function extractText(llmRes) {
  const c = llmRes?.choices?.[0];
  if (c?.message?.content) return c.message.content;
  if (typeof llmRes === "string") return llmRes;
  return "";
}

// -------------------- JSON normalisation --------------------
function sanitizeModelJson(obj) {
  const out = obj && typeof obj === "object" ? obj : {};
  if (!out.report || typeof out.report !== "object") out.report = {};
  if (typeof out.report.md !== "string") out.report.md = String(out.report.text || out.text || "");
  if (!Array.isArray(out.report.citations)) out.report.citations = [];
  if (!Array.isArray(out.tips)) out.tips = [];
  return out;
}

function normaliseTips(tips) {
  const t = ensureArray(tips).map(x => String(x || "").trim()).filter(Boolean);
  while (t.length < 3) t.push("No tip provided.");
  return t.slice(0, 3);
}

// ==================== Lead Qualification (generate) ====================
async function handleLeadQualification(context, req) {
  const cors = buildCorsHeaders(req);

  // timings + outcome (Observability E: steps 1/2/6/7)
  const __tStart = Date.now();
  let __tPdf = 0, __tWeb = 0, __tLlm = 0, __tValidate = 0;
  let __outcome = "success";
  let __consent = false;

  // Accept JSON or multipart
  const ct = String(req.headers?.["content-type"] || req.headers?.["Content-Type"] || "");
  const isMultipart = /multipart\/form-data/i.test(ct);

  let fields = {}, files = [];
  if (isMultipart) {
    const m = await parseMultipart(req);
    fields = m.fields || {};
    files = (m.files || []).filter(f => /^application\/pdf\b/i.test(f.contentType || "")).slice(0, 2);
  } else {
    fields = typeof req.body === "string" ? (jparse(req.body, {}) || {}) : (req.body || {});
  }

  const vars = jparse(fields.variables, fields.variables || {}) || {};
  const websiteUrl = String(vars.prospect_website || "").trim();
  const linkedinUrl = String(vars.company_linkedin || vars.linkedin_company || "").trim();

  // ---- Consent once (step 2) ----
  __consent =
    (typeof fields?.consent === "string" ? fields.consent === "true" : !!fields?.consent) ||
    (typeof req.body === "object" && !!req.body?.consent);

  if (!websiteUrl) {
    return {
      status: 400,
      headers: cors,
      body: { error: "prospect_website is required (e.g., https://example.com)", version: VERSION }
    };
  }

  const isSummary = /^(summary|short|exec|brief)$/.test(String(vars.detail || vars.detail_level || "").toLowerCase());
  const detailMode = isSummary ? "summary" : "full";
  const targetWords = isSummary ? 0 : FULL_TARGET_WORDS;

  // Extract PDFs (timed)
  let __t0 = Date.now();
  const pdfTexts = files.length ? await extractPdfTexts(files) : [];
  __tPdf = Date.now() - __t0;

  // Website bundle (timed)
  __t0 = Date.now();
  const websiteBundle = await expandWebsiteBundle(websiteUrl, context);
  __tWeb = Date.now() - __t0;
  const websiteText = websiteBundle.text;

  // iXBRL summary (optional)
  const ixbrl = jparse(fields.ixbrlSummary, fields.ixbrlSummary || {}) || {};

  // Build prompt
  const prompt = buildQualificationJsonPrompt({
    values: vars,
    ixbrl,
    pdfs: pdfTexts,
    websiteText,
    seller: {
      name: String(vars.seller_name || ""),
      company: String(vars.seller_company || ""),
      url: String(vars.seller_company_url || "")
    },
    ourOffer: {
      product: String(vars.product_service || ""),
      otherContext: String(vars.context || "")
    },
    detailMode,
    targetWords
  });

  // Call model — require JSON (timed + failure telemetry)
  let raw;

  // (F.7) Short-circuit quickly if provider just failed and the breaker is open
  if (circuitOpen()) {
    throw new Error("Service temporarily unavailable (circuit open)");
  }

  __t0 = Date.now();
  try {
    // (F.5) One jittered retry on 429/5xx via callWithRetry
    const llmRes = await callWithRetry(() => callModel({
      system: "You output valid JSON only for evidence-based B2B lead qualification.",
      prompt, temperature: 0.2, max_tokens: isSummary ? 2000 : Math.min(6000, Math.ceil((targetWords || 1750) * 1.8) + 600),
      response_format: { type: "json_object" }
    }), { tries: 2, baseMs: 400 });

    raw = extractText(llmRes) || "";
  } catch (err) {
    __tLlm = Date.now() - __t0;
    __outcome = "llm_fail";
    context.log?.error(`[${VERSION}] callModel error: ${err?.message || err}`);

    // (F.7) Trip the breaker on bursty provider errors
    if (/\b(429|500|502|503|504)\b/.test(String(err && err.message))) {
      tripCircuit(15000); // 15s pause; tune as needed
    }

    // keep your existing consent-gated failure telemetry block here...

    return {
      status: 502,
      headers: cors,
      body: { error: "LLM call failed", detail: String(err?.message || err), version: VERSION }
    };
  }
  __tLlm = Date.now() - __t0;

  // Parse + normalise
  const stripped = stripJsonFences(raw);
  let parsed = null;
  try { parsed = JSON.parse(stripped); } catch {
    const first = stripped.indexOf("{"), last = stripped.lastIndexOf("}");
    if (first >= 0 && last > first) { try { parsed = JSON.parse(stripped.slice(first, last + 1)); } catch { } }
  }
  if (!parsed) {
    return {
      status: 502,
      headers: cors,
      body: { error: "Model did not return valid JSON", version: VERSION, sample: stripped.slice(0, 300) }
    };
  }

  parsed = sanitizeModelJson(parsed);
  parsed.tips = normaliseTips(parsed.tips);

  // ---------- HARD SCHEMA VALIDATION (timed) ----------
  const minChars = isSummary ? DEFAULT_MIN_SUMMARY_CHARS : DEFAULT_MIN_FULL_CHARS;
  __t0 = Date.now();
  let validate = getQualValidator(minChars);
  let ok = validate(parsed);

  const allowRelax = String(process.env.QUAL_SCHEMA_RELAX || "1") !== "0";
  if (!ok && allowRelax) {
    const issues = (validate.errors || []).map(e => `${e.instancePath || "(root)"} ${e.message || ""}`).join("; ");
    const looksLikeLengthOnly = /minLength/.test(issues) && !/type|required|additionalProperties/.test(issues);
    if (looksLikeLengthOnly) {
      const relaxedMin = Math.max(200, Math.floor(minChars * 0.7));
      validate = getQualValidator(relaxedMin);
      ok = validate(parsed);
    }
  }
  __tValidate = Date.now() - __t0;

  if (!ok) {
    __outcome = "schema_fail";

    try {
      if (process.env.TELEMETRY_ENABLED === "1" && __consent) {
        const corr = (context?.invocationId || "") || (req?.headers?.["x-correlation-id"] || "");
        const domain = extractDomain(websiteUrl);
        const domainHash = domain ? hashDomain(domain, process.env.DOMAIN_HASH_SALT || "") : "";
        trackGenerate({
          correlationId: corr,
          outcome: __outcome,
          mode: isSummary ? "summary" : "full",
          buyer_type: String(vars.buyer_type || vars.call_type || "").trim() || "(unspecified)",
          product_category: String(vars.product_category || vars.product_service || "").trim() || "(unspecified)",
          responseMs: Date.now() - __tStart,
          timings: { pdf_parse_ms: __tPdf, website_fetch_ms: __tWeb, llm_ms: __tLlm, validation_ms: __tValidate },
          prospect_domain_hash: domainHash
        });
      }
    } catch { }

    const errors = (validate.errors || []).slice(0, 12).map(e => ({
      path: e.instancePath || "(root)", keyword: e.keyword, message: e.message, params: e.params || {}
    }));
    return {
      status: 422,
      headers: cors,
      body: { error: "Model JSON failed schema validation", version: VERSION, minCharsEnforced: minChars, errors }
    };
  }

  // Merge citations: model first, then server-added (website, LinkedIn, CH, PDFs)
  const citations = Array.isArray(parsed.report?.citations) ? parsed.report.citations.slice() : [];
  const normUrl = u => { try { const x = new URL(u); x.hash = ""; return x.href.replace(/\/+$/, "").toLowerCase(); } catch { return ""; } };
  const seen = new Set(citations.map(c => (c?.url ? `u:${normUrl(c.url)}` : `l:${String(c?.label || "").toLowerCase()}`)));

  // Website pages
  for (const p of (websiteBundle.pages || [])) {
    const key = `u:${normUrl(p.url)}`;
    if (!seen.has(key)) { citations.push({ label: `Website: ${p.label}`, url: p.url }); seen.add(key); }
  }
  // LinkedIn
  if (linkedinUrl) {
    const key = `u:${normUrl(linkedinUrl)}`;
    if (!seen.has(key)) { citations.push({ label: "Company LinkedIn", url: linkedinUrl }); seen.add(key); }
  }
  // Companies House
  const chNum = String(vars.ch_number || vars.company_number || vars.companies_house_number || "").trim();
  if (chNum) {
    const url = "https://find-and-update.company-information.service.gov.uk/company/" + encodeURIComponent(chNum);
    const key = `u:${normUrl(url)}`;
    if (!seen.has(key)) { citations.push({ label: "Companies House (filings)", url }); seen.add(key); }
  }
  // PDF filenames
  for (let i = 0; i < (pdfTexts.length || 0); i++) {
    const label = "Annual report: " + (pdfTexts[i].filename || ("report-" + (i + 1) + ".pdf"));
    const key = `l:${label.toLowerCase()}`;
    if (!seen.has(key)) { citations.push({ label }); seen.add(key); }
  }

  const md = String(parsed.report?.md || "");
  const corr = (context?.invocationId || "") || (req?.headers?.["x-correlation-id"] || "");

  // ---- Success telemetry (timings + baseline actions) ----
  try {
    if (process.env.TELEMETRY_ENABLED === "1" && __consent) {
      const domain = extractDomain(websiteUrl);
      const domainHash = domain ? hashDomain(domain, process.env.DOMAIN_HASH_SALT || "") : "";
      trackGenerate({
        correlationId: corr,
        outcome: __outcome,
        buyer_type: String(vars.buyer_type || vars.call_type || "").trim() || "(unspecified)",
        product_category: String(vars.product_category || vars.product_service || "").trim() || "(unspecified)",
        mode: isSummary ? "summary" : "full",
        prospect_domain_hash: domainHash,
        input_meta: {
          has_pdfs: pdfTexts.length > 0,
          website_pages: (websiteBundle.pages || []).length
        },
        report_meta: {
          md_length: md.length,
          citations_count: citations.length
        },
        responseMs: Date.now() - __tStart,
        timings: {
          pdf_parse_ms: __tPdf,
          website_fetch_ms: __tWeb,
          llm_ms: __tLlm,
          validation_ms: __tValidate
        },
        actions: { docx_exports: 0 }
      });
    }
  } catch { /* never fail user flow on telemetry */ }

  return {
    status: 200,
    headers: cors,
    body: {
      report: { md, citations },
      tips: parsed.tips,
      version: VERSION,
      mode: "qualification",
      timestamp: nowIso(),
      correlationId: corr
    }
  };
}

// ==================== qualification-email ====================
async function handleQualificationEmail(context, req) {
  const cors = buildCorsHeaders(req);

  const body = typeof req.body === "string" ? (jparse(req.body, {}) || {}) : (req.body || {});
  const v = body.variables || {};
  const co = (v.prospect_company || "Lead").trim();
  const notes = String(body.notes || "").trim();
  const report = String(body.reportMdText || "").trim();

  const subject = `Summary of opportunity with ${co} for sales management`;
  const prompt =
    `You are a UK B2B salesperson writing an internal executive summary for Sales Management.\n` +
    `Constraints:\n` +
    `- UK business English. Plain text only. No pleasantries.\n` +
    `- <= 350 words.\n` +
    `- Begin with: "Subject: ${subject}"\n` +
    `Sections:\n` +
    `• Headline assessment (fit, size, timing)\n` +
    `• Evidence-based summary from report/notes\n` +
    `• Key risks & mitigations\n` +
    `• Recommendation & explicit ask\n\n` +
    `--- REPORT (markdown) ---\n${report || "(none)"}\n\n--- NOTES (verbatim) ---\n${notes || "(none)"}\n`;

  let text = "";
  try {
    const llmRes = await callModel({
      system: "Write crisp internal executive summaries. No small talk. UK business English.",
      prompt, temperature: 0.3, max_tokens: 800
    });
    text = extractText(llmRes) || "";
  } catch {
    text =
      `Subject: ${subject}

Headline assessment
- Fit: (add)
- Size: (add)
- Timing: (add)

Evidence-based summary
${report ? report.slice(0, 800) : "(no report provided)"}

Key risks & mitigations
- (add)

Recommendation & ask
- (add)
${notes ? `\nNotes:\n${notes.slice(0, 600)}` : ""}`;
  }
  if (!/^Subject:/i.test(text)) text = `Subject: ${subject}\n\n` + text;

  return { status: 200, headers: cors, body: { email: text, version: VERSION } };
}

// ==================== qualification-docx (with export telemetry) ====================
async function handleQualificationDocx(context, req) {
  const cors = buildCorsHeaders(req);

  const body = typeof req.body === "string" ? (jparse(req.body, {}) || {}) : (req.body || {});
  const html = String(body.html || "<p>No content</p>");
  try {
    const buffer = htmlDocx.asBlob(html);

    // consent-gated, metadata-only telemetry (NO HTML captured)
    try {
      const consent =
        (typeof body.consent === "string" ? body.consent === "true" : !!body.consent) ||
        (typeof req.body === "object" && !!req.body?.consent);
      if (process.env.TELEMETRY_ENABLED === "1" && consent) {
        trackGenerate({
          name: "qual_docx_export",
          correlationId: (context?.invocationId || ""),
          report_meta: { html_len: typeof html === "string" ? Math.min(html.length, 500000) : 0 }
        });
      }
    } catch { }

    return {
      status: 200,
      headers: {
        ...cors,
        "Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "Content-Disposition": "attachment; filename=lead-qualification.docx"
      },
      body: buffer
    };
  } catch {
    return { status: 200, headers: { ...cors, "Content-Type": "text/html; charset=utf-8" }, body: html };
  }
}

// ==================== Dispatcher / Export ====================
module.exports = async function leadQualificationKind(context, req) {
  const cors = buildCorsHeaders(req);

  try {
    if (req.method === "OPTIONS") return { status: 204, headers: cors };
    if (req.method === "GET") {
      return {
        status: 200,
        headers: { ...cors, "x-debug-version": VERSION, "x-debug-pid": String(process.pid) },
        body: { ok: true, route: "generate/lead-qualification-kind", version: VERSION, node: process.version }
      };
    }
    if (req.method !== "POST") return { status: 405, headers: cors, body: { error: "Method Not Allowed", version: VERSION } };

    // Determine kind from JSON or multipart fields
    const ct = String(req.headers?.["content-type"] || req.headers?.["Content-Type"] || "");
    let kind = "";
    if (/multipart\/form-data/i.test(ct)) {
      const m = await parseMultipart(req, { maxFiles: 0 }); // parse fields only (safe)
      kind = String(m.fields?.kind || "").toLowerCase();
      // Re-attach parsed body so inner handlers can reuse fields if needed
      req.__multipartFields = m.fields;
    } else {
      const body = typeof req.body === "string" ? (jparse(req.body, {}) || {}) : (req.body || {});
      kind = String(body.kind || "").toLowerCase();
    }

    if (kind === "lead-qualification") return await handleLeadQualification(context, req);
    if (kind === "qualification-email") return await handleQualificationEmail(context, req);
    if (kind === "qualification-docx") return await handleQualificationDocx(context, req);

    return { status: 400, headers: cors, body: { error: "Unsupported kind for this handler", version: VERSION } };
  } catch (e) {
    // Catch-all: do NOT throw; respond and (optionally) send minimal telemetry
    try {
      const body = typeof req.body === "string" ? jparse(req.body, {}) : (req.body || {});
      const consent =
        (typeof body?.consent === "boolean" ? body.consent : false) ||
        (typeof req.__multipartFields?.consent === "string" ? req.__multipartFields.consent === "true" : false);
      if (process.env.TELEMETRY_ENABLED === "1" && consent) {
        trackGenerate({
          correlationId: (context?.invocationId || ""),
          outcome: "unhandled_error",
          responseMs: 0
        });
      }
    } catch { /* ignore */ }

    context.log?.error(`[${VERSION}] Unhandled error: ${e?.stack || e}`);
    return { status: 500, headers: cors, body: { error: "Internal error", detail: String(e?.message || e), version: VERSION } };
  }
};

// Optional named exports (if you map directly elsewhere)
module.exports.leadQualification = handleLeadQualification;
module.exports.qualificationEmail = handleQualificationEmail;
module.exports.qualificationDocx = handleQualificationDocx;
