// api/qualification-generate/index.js 17-10-2025 v11
// Azure Function: qualification-generate
// Goal: unify the recent (incomplete) generator with the archive's full feature set
// Notes:
// - Preserves current names/shape; augments with optional multipart + PDF + website bundling
// - Degrades gracefully if optional deps (busboy, pdf-parse, ajv) are not installed

// ------------------------------
// Utilities
// ------------------------------
const toInt = (v, d) => {
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n > 0 ? n : d;
};

const DEFAULT_TIMEOUT_MS = toInt(process.env.FETCH_TIMEOUT, 60_000);
const MODEL_TIMEOUT_MS = toInt(process.env.LLM_TIMEOUT_MS, 120_000);
const PER_PAGE_CAP = toInt(process.env.PDF_CHAR_CAP, 12_000);
const TEXT_CAP = toInt(process.env.QUAL_TEXT_CAP, PER_PAGE_CAP * 10);
const PDF_PAGE_CAPS = (process.env.PDF_PAGE_CAPS || "10,25")
  .split(",")
  .map(n => Number(String(n).trim()))
  .filter(Boolean); // e.g., [10,25]
const PDF_CHAR_CAP = toInt(process.env.PDF_CHAR_CAP, 120000);
const QUAL_MAX_PDFS = toInt(process.env.QUAL_MAX_PDFS, 2);
const QUAL_MAX_PDF_PER_FILE_BYTES = toInt(
  process.env.QUAL_MAX_PDF_PER_FILE_BYTES || process.env.MAX_UPLOAD_BYTES,
  10 * 1024 * 1024 // 10MB per file default
);
const QUAL_TOTAL_PDF_BYTES = toInt(
  process.env.QUAL_TOTAL_PDF_BYTES || process.env.QUAL_MAX_PDF_BYTES,
  QUAL_MAX_PDF_PER_FILE_BYTES * QUAL_MAX_PDFS // default combined cap
);
const DEDUP_NGRAM = toInt(process.env.QUAL_DEDUP_NGRAM, 12);
const MIN_PAGE_UNIQUE_PCT = Math.max(0, Math.min(1, Number(process.env.QUAL_MIN_PAGE_UNIQUE_PCT || '0.25')));


// Best-effort optional deps
let Ajv = null;
try { Ajv = require('ajv'); } catch (_) { /* optional */ }
let Busboy = null;
try { Busboy = require('busboy'); } catch (_) { /* optional */ }
let pdfParse = null;
try { pdfParse = require('pdf-parse'); } catch (_) { /* optional */ }
const fs = require('fs/promises');

const isLocal = () => process.env.WEBSITE_INSTANCE_ID == null;

function cors(context) {
  const h = context.res?.headers ?? {};
  const origin = context.req?.headers?.origin || '*';  // <- echo caller
  return {
    ...h,
    'Access-Control-Allow-Origin': origin,
    'Vary': 'Origin',
    'Access-Control-Allow-Credentials': 'true',
    'Access-Control-Allow-Methods': 'OPTIONS, GET, POST',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
  };
}

function ok(context, body, status = 200) {
  context.res = { status, headers: cors(context), body };
}
function err(context, message, status = 400, details = undefined) {
  context.res = { status, headers: cors(context), body: { error: message, ...(details ? { details } : {}) } };
}

// ---- Auth helpers (shared-secret + Easy Auth) ----
function parsePrincipal(req) {
  const raw = req.headers['x-ms-client-principal'];
  if (!raw) return null;
  try {
    const json = Buffer.from(raw, 'base64').toString('utf8');
    return JSON.parse(json);
  } catch {
    return null;
  }
}

// case-insensitive header getter
function getHeader(req, name) {
  if (!req || !req.headers) return '';
  const h = req.headers;
  return h[name] ?? h[name.toLowerCase()] ?? h[name.toUpperCase()] ?? '';
}

// extract Bearer token, case-insensitive
function getBearerToken(req) {
  const auth = String(getHeader(req, 'authorization') || '');
  const m = /^Bearer\s+(.+)$/i.exec(auth);
  return m ? m[1].trim() : '';
}

// constant-time equality when lengths match
function secretsEqual(a, b) {
  try {
    const crypto = require('crypto');
    const A = Buffer.from(String(a || ''), 'utf8');
    const B = Buffer.from(String(b || ''), 'utf8');
    if (A.length !== B.length || A.length === 0) return false;
    return crypto.timingSafeEqual(A, B);
  } catch {
    // fallback
    return a === b && a.length > 0;
  }
}

// shared-secret check (QUAL_API_KEY via Bearer token)
function hasSharedSecret(req) {
  const expected = String(process.env.QUAL_API_KEY || '').trim();
  if (!expected) return false;
  const token = getBearerToken(req);
  return secretsEqual(token, expected);
}

function hasAccess(principal, req) {
  // 1) local dev or explicit override
  const allowAnon = String(process.env.ALLOW_ANON || '').trim();
  if (isLocal() || allowAnon === '1' || /^true$/i.test(allowAnon)) return true;

  // 2) shared-secret via Authorization: Bearer <QUAL_API_KEY>
  if (hasSharedSecret(req)) return true;

  // 3) Easy Auth roles (if present)
  if (!principal) return false;
  const roles = new Set((principal.userRoles || []).map(r => String(r).toLowerCase()));
  return roles.has('authenticated') || roles.has('contributor') || roles.has('admin');
}


// ------------------------------
// Fetch helpers
// ------------------------------

async function timedFetch(url, init = {}, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort('timeout'), timeoutMs);
  try {
    const res = await fetch(url, { ...init, signal: ac.signal });
    return res;
  } finally {
    clearTimeout(t);
  }
}

function htmlToText(html) {
  if (!html) return '';
  // strip scripts/styles
  const withoutScripts = String(html).replace(/<script[\s\S]*?<\/script>/gi, '').replace(/<style[\s\S]*?<\/style>/gi, '');
  const stripped = withoutScripts.replace(/<[^>]+>/g, ' ');
  return stripped.replace(/\s+/g, ' ').trim();
}

function take(str, n) { return (str || '').slice(0, n); }
function tokenizeWords(s) {
  // Lowercase, keep letters/numbers, split on non-word
  return String(s || '')
    .toLowerCase()
    .replace(/[^a-z0-9£€$%\.\-\s]/g, ' ')
    .split(/\s+/)
    .filter(Boolean);
}

function djb2(str) {
  let h = 5381;
  for (let i = 0; i < str.length; i++) {
    h = ((h << 5) + h) + str.charCodeAt(i);
    h = h >>> 0; // unsigned 32
  }
  return h;
}

function shingles(words, n) {
  if (!Array.isArray(words) || words.length < n) return [];
  const out = [];
  for (let i = 0; i <= words.length - n; i++) {
    // join a short window; hashing avoids big memory
    const sh = djb2(words.slice(i, i + n).join(' '));
    out.push(sh);
  }
  return out;
}

/**
 * Compute unique contribution of `text` given a set of seen shingles.
 * Returns { uniquePct: number (0..1), acceptedShingles: number[], total: number }
 */
function measureUniqueContribution(text, seen, n = DEDUP_NGRAM) {
  const words = tokenizeWords(text);
  const grams = shingles(words, n);
  if (grams.length === 0) return { uniquePct: 0, acceptedShingles: [], total: 0 };

  let uniqueCount = 0;
  const newOnes = [];
  for (const g of grams) {
    if (!seen.has(g)) {
      uniqueCount++;
      newOnes.push(g);
    }
  }
  const uniquePct = uniqueCount / grams.length;
  return { uniquePct, acceptedShingles: newOnes, total: grams.length };
}

// ------------------------------
// Multipart (optional)
// ------------------------------

async function parseMultipart(req) {
  if (!Busboy) {
    throw new Error('Multipart parser unavailable: install "busboy" to accept file uploads.');
  }

  // defensively get a Buffer for the request body
  const bodyBuf = (() => {
    if (Buffer.isBuffer(req.body)) return req.body;
    if (typeof req.body === 'string') return Buffer.from(req.body, 'utf8');
    if (req.body && req.body.data && Array.isArray(req.body.data)) {
      return Buffer.from(req.body.data);
    }
    return null;
  })();

  if (!bodyBuf) {
    throw new Error('multipart body not available as Buffer');
  }

  const contentType = String(req.headers['content-type'] || '');
  if (!/^multipart\/form-data/i.test(contentType)) {
    return { fields: {}, files: [] };
  }

  const bb = Busboy({
    headers: req.headers,
    limits: {
      fileSize: QUAL_MAX_PDF_PER_FILE_BYTES, // per-file cap
      files: QUAL_MAX_PDFS                 // number of files processed
    }
  });

  const fields = {};
  /** @type {{ filename:string, mime:string, data:Buffer }[]} */
  const files = [];
  let totalBytes = 0;

  const done = new Promise((resolve, reject) => {
    bb.on('file', (name, file, info) => {
      const { filename, mimeType } = info;
      const isPdf = /pdf$/i.test(mimeType) || /application\/pdf/i.test(mimeType);
      const chunks = [];
      let truncated = false;

      file.on('data', d => {
        totalBytes += d.length;
        if (totalBytes > QUAL_TOTAL_PDF_BYTES) {
          return; // drain but don't buffer beyond the total budget
        }
        if (isPdf && !truncated) chunks.push(d);
      });

      file.on('limit', () => { truncated = true; });

      file.on('end', () => {
        if (isPdf && !truncated) {
          files.push({ filename, mime: mimeType, data: Buffer.concat(chunks) });
        }
      });
    });

    bb.on('field', (name, val) => { fields[name] = val; });
    bb.on('error', reject);
    bb.on('close', resolve);
  });

  // IMPORTANT: in Functions v4, use bb.end(buffer) — do NOT req.pipe(bb)
  bb.end(bodyBuf);

  await done;

  const pdfs = files
    .filter(f => /pdf$/i.test(f.mime) || /application\/pdf/i.test(f.mime))
    .slice(0, QUAL_MAX_PDFS);

  return { fields, files: pdfs };
}

// ------------------------------
// PDF extraction (optional)
// ------------------------------

function normalizeFinancialText(s) {
  if (!s) return '';
  return String(s)
    .replace(/\r/g, '')
    .replace(/Â£/g, '£')                       // mis-encoded pound
    .replace(/[‐-–—−]/g, '-')                  // various minus/dashes → hyphen
    .replace(/\((\s*\d[\d,\s]*\s*)\)/g, '-$1') // (123) → -123
    .replace(/(\d)\s+(?=\d)/g, '$1')           // 1 2 3 456 → 123456
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function hasFinancialSignals(text) {
  if (!text) return false;
  const t = String(text);
  const labelRx =
    /(turnover|revenue|gross\s+profit|operating\s+(?:profit|loss)|profit\s*(?:\/|\(?)\s*loss\s*before\s*tax|balance\s*sheet|cash\s*(?:at\s*bank|and\s*in\s*hand|equivalents)|current\s+assets|current\s+liabilities|net\s+(?:assets|liabilities)|average\s+(?:monthly\s+)?(?:number\s+of\s+)?employees)/i;
  const numberRx = /(?:£|gbp)\s*\d|\b\d{1,3}(?:,\d{3}){1,3}\b/i;
  return labelRx.test(t) && numberRx.test(t);
}

async function extractPdfTexts(files) {
  const out = [];
  if (!pdfParse || !Array.isArray(files)) return out;

  const caps = PDF_PAGE_CAPS.length ? PDF_PAGE_CAPS : [10, 25, 50, 200]; // widen if needed

  for (const f of files) {
    let pickedText = '';
    let usedCap = 0;

    // Pass 1: progressive caps with custom pagerender
    for (const cap of caps) {
      try {
        const parsed = await pdfParse(f.data, {
          max: cap,
          pagerender: page =>
            page.getTextContent().then(tc => tc.items.map(i => i.str).join(' '))
        });
        const norm = normalizeFinancialText(parsed?.text || '');
        pickedText = norm.slice(0, PDF_CHAR_CAP);
        usedCap = cap;
        if (hasFinancialSignals(pickedText)) break; // good enough
      } catch (_) {
        // ignore and try next strategy
      }
    }

    // Pass 2: full parse with default pagerender (some PDFs extract better this way)
    if (!hasFinancialSignals(pickedText)) {
      try {
        const parsedFull = await pdfParse(f.data); // no options
        const normFull = normalizeFinancialText(parsedFull?.text || '');
        const sliced = normFull.slice(0, PDF_CHAR_CAP);
        if (sliced.length > pickedText.length) {
          pickedText = sliced;
          usedCap = 0;
        }
      } catch (_) { /* ignore */ }
    }

    if (pickedText && pickedText.trim()) {
      out.push({
        filename: f.filename || 'report.pdf',
        text: pickedText,
        pagesTried: usedCap || undefined
      });
    }
  }

  return out;
}

// ------------------------------
// Website bundling
// ------------------------------

function normalizeBase(url) {
  try {
    const u = new URL(url);
    return `${u.protocol}//${u.host}`;
  } catch {
    return null;
  }
}

const DEFAULT_SLUGS = [
  '/', '/about', '/about-us', '/leadership', '/team', '/management', '/partners', '/alliances',
  '/solutions', '/services', '/products', '/industries', '/sectors', '/news', '/blog', '/customers', '/case-studies', '/security'
];

// Crawl-lite: sample a handful of useful URLs from /sitemap.xml on the same host
async function sampleFromSitemap(base, max = 12) {
  try {
    const res = await timedFetch(`${base}/sitemap.xml`, { method: 'GET' }, DEFAULT_TIMEOUT_MS);
    if (!res.ok) return [];
    const xml = await res.text();
    const locs = Array.from(xml.matchAll(/<loc>(.*?)<\/loc>/gi))
      .map(m => m[1])
      .filter(Boolean);

    const sameHost = [];
    for (const u of locs) {
      try {
        const uu = new URL(u);
        if (uu.protocol !== 'https:') continue;
        if (uu.host !== new URL(base).host) continue;
        if (/\.(xml|jpg|jpeg|png|gif|webp|svg)$/i.test(uu.pathname)) continue;
        sameHost.push(u);
      } catch { /* ignore bad URLs */ }
    }

    const priority = ['case', 'customer', 'leadership', 'team', 'board', 'about', 'partner',
      'solution', 'service', 'industry', 'sector', 'news', 'press', 'blog',
      'security', 'compliance', 'resource', 'insight'];
    sameHost.sort((a, b) => {
      const sa = priority.some(k => a.includes(k)) ? 1 : 0;
      const sb = priority.some(k => b.includes(k)) ? 1 : 0;
      return sb - sa;
    });

    return Array.from(new Set(sameHost)).slice(0, max);
  } catch {
    return [];
  }
}

async function bundleWebsite(website, extraPaths = []) {
  if (!website) return { pages: [], text: '' };
  const base = normalizeBase(website);
  if (!base) return { pages: [], text: '' };

  // Normalize extra paths to absolute URLs on same host
  const normalizedExtras = (extraPaths || [])
    .map(s => String(s).trim())
    .filter(Boolean)
    .map(s => (s.startsWith('http') ? s : (s.startsWith('/') ? base + s : `${base}/${s}`)));

  // Sample from sitemap + known slugs
  const fromSitemap = await sampleFromSitemap(base, 12);
  const fromSlugs = DEFAULT_SLUGS.map(s => (s.startsWith('http') ? s : base + s));

  // Candidate list (deduped)
  const candidates = Array.from(new Set([...fromSitemap, ...normalizedExtras, ...fromSlugs]));

  const pages = [];
  let text = '';
  const MAX_PAGES = 18;

  // Global shingle set to suppress repeats across pages
  const seen = new Set();

  for (const url of candidates) {
    try {
      const res = await timedFetch(url, { method: 'GET' }, DEFAULT_TIMEOUT_MS);
      if (!res.ok) continue;
      const html = await res.text();
      const chunkRaw = htmlToText(html);
      if (!chunkRaw) continue;

      // Cap per page first (cheap) then measure contribution
      const chunk = take(chunkRaw, PER_PAGE_CAP);

      const { uniquePct, acceptedShingles } = measureUniqueContribution(chunk, seen, DEDUP_NGRAM);

      // Only accept pages that add enough genuinely new content
      if (uniquePct >= MIN_PAGE_UNIQUE_PCT) {
        // Merge shingles into seen set
        for (const g of acceptedShingles) seen.add(g);

        pages.push(url);
        text += ' \n---\n' + chunk;

        if (text.length >= TEXT_CAP || pages.length >= MAX_PAGES) break;
      } else {
        // Skip largely-duplicate page silently
        continue;
      }
    } catch {
      // skip bad/slow pages
    }
  }

  return { pages, text: take(text, TEXT_CAP) };
}

// ------------------------------
// Schema loading and validation
// ------------------------------
async function loadSchema(req) {
  // Archive-compatible JSON shape
  return {
    type: "object",
    properties: {
      report: {
        type: "object",
        properties: {
          md: { type: "string", minLength: 1 },
          citations: {
            type: "array",
            items: {
              type: "object",
              properties: {
                label: { type: "string", minLength: 1 },
                url: { type: "string" }
              },
              required: ["label"],
              additionalProperties: false
            },
            default: []
          }
        },
        required: ["md"],
        additionalProperties: true
      },
      tips: {
        type: "array",
        items: { type: "string", minLength: 1 },
        minItems: 3,
        maxItems: 3
      }
    },
    required: ["report", "tips"],
    additionalProperties: true
  };
}

function getValidator(schema) {
  if (Ajv) {
    const ajv = new Ajv({ allErrors: true, strict: false });
    return ajv.compile(schema);
  }
  // minimal fallback
  return (data) => {
    if (!data || typeof data !== 'object') return false;
    if (!data.report || typeof data.report.md !== 'string') return false;
    if (!Array.isArray(data.tips) || data.tips.length < 1) return false;
    return true;
  };
}
// Require that the '### Financials (evidenced)' subsection exists and that
// at least three core financial lines with YEAR labels are present.
function hasFinancialsSection(md) {
  if (typeof md !== 'string') return false;
  return /^###\s*Financials\s*\(evidenced\)\s*$/mi.test(md);
}

function hasFinancialLines(md) {
  if (typeof md !== 'string') return false;
  // Look for at least three different labelled metrics with FYyy and a currency/number
  const tests = [
    /(revenue|turnover)[^\n]*FY\d{2}[^£\n]*£?\s?\d/i,
    /gross\s+profit[^\n]*FY\d{2}[^£\n]*£?\s?\d/i,
    /(operating\s+profit|operating\s+loss)[^\n]*FY\d{2}[^£\n]*£?\s?\d/i,
    /(profit|loss)\s+before\s+tax[^\n]*FY\d{2}[^£\n]*£?\s?\d/i,
    /cash[^\n]*\b(bank\b|in\s*hand)\b[^\n]*FY\d{2}[^£\n]*£?\s?\d/i,
    /current\s+assets[^\n]*FY\d{2}[^£\n]*£?\s?\d/i,
    /current\s+liabilities[^\n]*FY\d{2}[^£\n]*£?\s?\d/i,
    /net\s+assets?[^\n]*FY\d{2}[^£\n]*£?\s?\d/i,
    /(average|avg\.?)\s+(monthly\s+)?employees?[^\n]*FY\d{2}[^0-9\n]*\d+/i,
    /\bARR\b[^\n]*\bFY\d{2}\b/i,
    /(undrawn|revolving|rcf)[^\n]*\bFY\d{2}\b/i
  ];
  let hits = 0;
  for (const rx of tests) if (rx.test(md)) hits++;
  return hits >= 3;
}

// Ensure all required headings appear in order (using markdown '## ' anchors)
function headingsInOrder(md, headings) {
  if (typeof md !== 'string' || !md.trim()) return { ok: false, missing: headings.slice(0) };
  const idxs = [];
  const missing = [];
  let lastIdx = -1;

  for (const h of headings) {
    // escape regex special chars in heading
    const pat = new RegExp('^\\s*' + h.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s*$', 'mi');
    const m = md.match(pat);
    if (!m) {
      missing.push(h);
      idxs.push(-1);
      continue;
    }
    const pos = m.index;
    idxs.push(pos);
    if (pos <= lastIdx) {
      // order violation
      return { ok: false, missing: [], order: false };
    }
    lastIdx = pos;
  }

  return { ok: missing.length === 0, missing, order: true };
}

// Required section headings
const REQUIRED_HEADINGS_BASE = [
  '## Company profile (what can be evidenced)',
  '## Pain points',
  '## Relationship value',
  '## Decision-making process',
  '## Competition & differentiation',
  '## Bottom line for you',
  '## What we could not evidence (and why)'
];

/**
 * Validate that all required headings are present and appear in order.
 * If callType === "Partner", the extra section is required (position may be after the base list).
 * Returns: { ok:boolean, missing:string[], order:boolean, partnerRequired:boolean, partnerPresent:boolean }
 */
function validateReportStructure(md, { isSummary, callType } = {}) {
  const partnerRequired = /^partner$/i.test(String(callType || ''));
  const expected = REQUIRED_HEADINGS_BASE.slice();
  if (partnerRequired) expected.push('## Potential partnership risks and mitigations');

  const res = headingsInOrder(md, expected); // uses the object-returning helper above
  const partnerPresent = /##\s*Potential partnership risks and mitigations/i.test(md);
  const ok = !!res && res.ok === true && (!partnerRequired || partnerPresent);

  return {
    ok,
    missing: Array.isArray(res?.missing) ? res.missing : [],
    order: res?.order !== false,
    partnerRequired,
    partnerPresent
  };
}

// --- Section helpers to check Financials formatting/content ---
const FINANCIALS_HX = /^###\s*Financials\s*\(evidenced\)\s*$/mi;

function sectionSlice(md, headingRx) {
  if (typeof md !== 'string') return '';
  const m = md.match(headingRx);
  if (!m) return '';
  const start = m.index + m[0].length;
  const rest = md.slice(start);
  const next = rest.search(/^\s*##\s+/m);
  return (next === -1 ? rest : rest.slice(0, next)).trim();
}

function financialsIsParagraph(md) {
  const body = sectionSlice(md, FINANCIALS_HX);
  if (!body) return false;
  const hasBullets = /^\s*[-*•]/m.test(body);             // no list markers
  const sentences = (body.match(/[.!?]\s/g) || []).length; // at least a couple of sentences
  return !hasBullets && sentences >= 2 && body.length >= 140;
}

function financialsHasLabeledFigures(md) {
  const body = sectionSlice(md, FINANCIALS_HX);
  if (!body) return false;
  // require at least 3 different labeled metrics with FY and a number/currency
  const tests = [
    /(revenue|turnover)[^\n]*\bFY\d{2}\b[^\n]*?(?:£|GBP)?\s*\d/i,
    /gross\s+profit[^\n]*\bFY\d{2}\b[^\n]*?(?:£|GBP)?\s*\d/i,
    /operating\s+(?:profit|loss)[^\n]*\bFY\d{2}\b[^\n]*?(?:£|GBP)?\s*\d/i,
    /(profit|loss)\s+before\s+tax[^\n]*\bFY\d{2}\b[^\n]*?(?:£|GBP)?\s*\d/i,
    /cash[^\n]*(?:bank|in\s*hand|equivalents)[^\n]*\bFY\d{2}\b[^\n]*?(?:£|GBP)?\s*\d/i,
    /current\s+assets[^\n]*\bFY\d{2}\b[^\n]*?(?:£|GBP)?\s*\d/i,
    /current\s+liabilities[^\n]*\bFY\d{2}\b[^\n]*?(?:£|GBP)?\s*\d/i,
    /net\s+(?:assets|liabilities)[^\n]*\bFY\d{2}\b[^\n]*?(?:£|GBP)?\s*\d/i,
    /average\s+(?:monthly\s+)?(?:number\s+of\s+)?employees[^\n]*\bFY\d{2}\b[^\n]*?\d/i
  ];
  let hits = 0;
  for (const rx of tests) if (rx.test(body)) hits++;
  return hits >= 3;
}


function makeFinancialsDigest(pdfs) {
  const labels = [
    { key: 'Revenue/Turnover', rx: /(turnover|revenue)/i },
    { key: 'Gross profit', rx: /gross\s+profit/i },
    { key: 'Operating profit/(loss)', rx: /operating\s+(?:profit|loss)/i },
    { key: 'Profit/(loss) before tax', rx: /(?:profit|loss)\s+before\s+tax/i },
    { key: 'Cash (bank and in hand)', rx: /cash\s+(?:at\s*bank|and\s*in\s*hand|equivalents)/i },
    { key: 'Current assets', rx: /current\s+assets/i },
    { key: 'Current liabilities', rx: /current\s+liabilities/i },
    { key: 'Net assets/(liabilities)', rx: /net\s+(?:assets|liabilities)/i },
    { key: 'Average employees', rx: /average\s+(?:monthly\s+)?(?:number\s+of\s+)?employees/i }
  ];
  const numberRx = /(?:£|gbp)\s*\d|\b\d{1,3}(?:,\d{3}){1,3}\b/i;

  const blocks = [];
  for (const p of (pdfs || [])) {
    if (!p.text) continue;
    const lines = String(p.text).split(/\n+/);
    const picks = [];
    for (const lab of labels) {
      const hit = lines.find(line => lab.rx.test(line) && numberRx.test(line));
      if (hit) picks.push(`- ${lab.key}: ${hit.trim().slice(0, 180)}`);
    }
    if (picks.length) {
      blocks.push(`--- FINANCIAL CANDIDATES from ${p.filename} ---\n${picks.join('\n')}`);
    }
  }
  return blocks.join('\n\n');
}

// ------------------------------
// Prompt assembly
// ------------------------------
function buildQualificationJsonPrompt(args) {
  const v = args.values || {};
  const callType = String(v.call_type || "").toLowerCase().startsWith("p") ? "Partner" : "Direct";
  const sellerNotes = String(args.sellerNotes || '').trim();
  const detailMode = (args.detailMode === "summary") ? "summary" : "full";
  const targetWords = Number(args.targetWords || 0);
  const targetWordsLine = targetWords
    ? `TARGET LENGTH: Aim for about ${targetWords} words (±10%). HARD CAP: Do not exceed ${Math.round(targetWords * 1.06)} words.`
    : "";
  const modeLine =
    (detailMode === "summary")
      ? [
        "OUTPUT MODE: EXECUTIVE SUMMARY.",
        "- Keep each section to 1–2 crisp sentences maximum.",
        "- Include only the highest-signal facts with figures and year labels (e.g., “FY24: £20.76m”).",
        "- If a point lacks evidence in the provided sources, write “No public evidence found.” Do NOT speculate.",
        "- No softeners or generalisations; be direct and specific."
      ].join("\n")
      : [
        "OUTPUT MODE: FULL DETAIL.",
        "- Provide complete, evidenced detail across all sections.",
        "- Quote figures with year labels; include relevant operational context from sources.",
        "- Still avoid speculation; if unknown, state it explicitly."
      ].join("\n");
  const banlist = [
    "well-positioned", "decades of experience", "cybersecurity landscape",
    "market differentiation", "client-centric", "cutting-edge",
    "robust posture", "holistic", "industry-leading", "best-in-class"
  ];

  const banlistLine =
    "BANNED WORDING (do not use any of these): " + banlist.join(", ") + ".";

  const evidDensityRules = [
    "EVIDENCE DENSITY:",
    "- Company profile MUST include labelled figures where available (e.g., “FY24: £20.76m revenue; Loss before tax £824k; Average employees 92”).",
    "- If a required number is not present in the provided sources, write a one-line ‘No public evidence found’ under that section—do NOT generalise.",
    "- Only list partners/technologies if explicitly present in the provided WEBSITE TEXT or PDFs you’ve been given.",
    "- Trade shows: list only those explicitly evidenced in WEBSITE TEXT; otherwise write ‘No public evidence found this calendar year.’",
  ].join("\n");
  const ix = args.ixbrl || {};
  const ixBrief = JSON.stringify(ix && ix.years ? ix.years : (ix.summary && ix.summary.years) || ix);

  const pdfs = Array.isArray(args.pdfs) ? args.pdfs : []; // [{filename,text}]
  const pdfBundle = pdfs.map(p => (`--- PDF: ${p.filename || "report.pdf"} ---\n${p.text || ""}`)).join("\n\n");

  const websiteText = args.websiteText || "";
  const seller = args.seller || { name: "", company: "", url: "" };
  const offer = args.ourOffer || { product: "", otherContext: "" };

  const websiteBlock = websiteText ? (`--- WEBSITE TEXT (multiple pages) ---\n${websiteText}`) : "";
  const notesText = take(String(sellerNotes || '').replace(/\r/g, ''), 4000);
  const notesBlock = notesText
    ? "--- SELLER NOTES (verbatim; context only — not evidence) ---\n" + notesText
    : "";

  const role = [
    "You are a top-performing UK B2B/channel salesperson and GTM strategist.",
    "You are a CMO-level operator focused on partner recruitment and enablement.",
    "Write **valid JSON only** (no markdown outside JSON).",
    "All insights must be specific and evidenced from the provided sources only."
  ].join("\n");

  const schema = [
    "JSON schema (shape & content rules — follow exactly):",
    "{",
    '  "report": {',
    '    "md": string,              // Markdown with these EXACT section headings in this EXACT order:',
    '                               // "Here is your evidence-based qualification for your opportunity with {Company}..."',
    '                               // ## Company profile (what can be evidenced)',
    '                               // ## Pain points',
    '                               // ## Relationship value',
    '                               // ## Decision-making process',
    '                               // ## Competition & differentiation',
    '                               // ## Bottom line for you',
    '                               // ## What we could not evidence (and why)',
    '                               // If CALL_TYPE = Partner, ALSO include (anywhere after the above):',
    '                               // ## Potential partnership risks and mitigations',
    '                               // Inline evidence tags: use [S#] for website, [P#] for PDFs when claims rely on evidence.',
    '    "citations": [              // Provide any citations the model used (server will add more).',
    '      { "label": string, "url"?: string }',
    "    ]",
    "  },",
    '  "tips": [string, string, string]   // Exactly three concise, practical tips',
    "}"
  ].join("\n");

  const constraints = [
    "CONSTRAINTS:",
    "- UK business English; no generalisations; no assumptions.",
    "- Cite only from the PDFs, iXBRL summary and website text provided here.",
    "- If something is not evidenced, write a clear “No public evidence found” line.",
    "- Seller notes are context ONLY; do not assert them as facts unless corroborated by WEBSITE TEXT or PDFs.",
    "",
    "FINANCIALS (MANDATORY if present in sources):",
    "- Search PDFs/iXBRL for: Revenue/Turnover, Gross profit, Operating profit/loss, Cash (bank and in hand),",
    "  Current assets, Current liabilities, Net assets/liabilities, Average monthly employees.",
    "- Quote figures with currency symbols and YEAR LABELS (e.g., “FY24: £20.76m”).",
    "- If the income statement is not filed (small companies regime), state that explicitly and use balance-sheet items you DO have.",
    "- If no numbers are in the sources, you MUST say so in “What we could not evidence”.",
    "- Present the subsection “### Financials (evidenced)” in paragraph form (no bullets).",
    "",
    "DECISION MAKERS & PARTNERS (if in website text):",
    "- Extract named roles/titles (CEO, CFO, Directors, etc.) and partner/vendor logos/lists where visible.",
    "- If none are present in the scraped pages, say “No public evidence in provided sources.”",
    "",
    "TRADE SHOWS:",
    "- Only list attendance this calendar year if present in the provided website text; otherwise say none and DO NOT estimate budgets.",
    "",
    "TIE TO THE SALESPERSON’S COMPANY:",
    `- Salesperson: ${seller.name || "(unknown)"} · Company: ${seller.company || "(unknown)"} ${seller.url ? "· URL: " + seller.url : ""}`,
    `- Offer/product focus: ${offer.product || "(unspecified)"}`,
    `- Other context from seller: ${offer.otherContext || "(none)"}`,
    "- In “Relationship value” and/or “Competition & differentiation”, explicitly map how the seller’s company can add value to the prospect’s stack. If it cannot, say why."
  ].join("\n");

  // ---- Inline evidence tagging guidance and numbered source lists ----
  const sitePages = Array.isArray(args.sitePages) ? args.sitePages : [];
  const siteTagList = sitePages.length
    ? sitePages.map((u, i) => `[S${i + 1}] ${u}`).join("\n")
    : "None.";
  const pdfTagList = pdfs.length
    ? pdfs.map((p, i) => `[P${i + 1}] ${p.filename || "report.pdf"}`).join("\n")
    : "None.";

  const citationRules = [
    "EVIDENCE TAGGING (MANDATORY):",
    "- When a claim depends on evidence, add inline tags in report.md:",
    "  · [S#] for Website sources (see list below)",
    "  · [P#] for PDF sources (see list below)",
    "- Use the correct # that corresponds to the lists below.",
    "- If there are no relevant sources for a claim, do not invent a tag; write “No public evidence found.”"
  ].join("\n");

  const tradeShows = Array.isArray(args.tradeShows) ? args.tradeShows : [];
  const tradeShowScanBlock = [
    "TRADE SHOW SCAN (THIS CALENDAR YEAR):",
    tradeShows.length ? ("- Look specifically for: " + tradeShows.join(", ")) : "- No explicit trade show list provided.",
    "- Only list shows if attendance/sponsorship is evidenced in WEBSITE TEXT; otherwise write 'No public evidence found this calendar year.'"
  ].join("\n");

  const citationLists = [
    "Website sources (S#):",
    siteTagList,
    "",
    "PDF sources (P#):",
    pdfTagList
  ].join("\n");

  const websiteUseRules = [
    "WEBSITE USE (MANDATORY when pages are provided):",
    "- You MUST incorporate insights from the prospect WEBSITE TEXT.",
    "- Include at least two [S#] tags if two or more website pages are provided; otherwise include at least one [S#].",
    "- When a claim derives from the website (leadership, services, partner logos, events, news), tag it with the correct [S#]."
  ].join("\n");

  const sellerNotesRules = [
    "SELLER NOTES HANDLING (MANDATORY):",
    "- Treat the Seller Notes as questions to ANSWER in the report.",
    "- Do NOT treat Seller Notes as evidence unless corroborated by WEBSITE TEXT or PDFs.",
    "- In the section “## Bottom line for you”, include a bold line exactly like:",
    "  **Response to seller’s question:** <a one-to-two sentence answer, grounded in the provided evidence; if not evidenced, say “No public evidence found.”>",
  ].join("\n");

  const financeCandidates = makeFinancialsDigest(pdfs);

  return [
    role,
    "",
    `CALL_TYPE: ${callType}`,
    `MODE: ${detailMode.toUpperCase()}`,
    modeLine,
    targetWordsLine,
    banlistLine,
    evidDensityRules,
    `Prospect website (scraped pages included): ${v.prospect_website || "(not provided)"}`,
    `LinkedIn (URL only, content not scraped): ${v.company_linkedin || "(not provided)"}`,
    "",
    schema,
    "",
    constraints,
    "",
    citationRules,
    "",
    citationLists,
    "",
    sellerNotesRules,
    "",
    websiteUseRules,
    "",
    tradeShowScanBlock,
    "",
    (financeCandidates ? "FINANCIALS EXTRACTION CANDIDATES:\n" + financeCandidates + "\n" : ""),
    "iXBRL summary (most recent first):",
    ixBrief,
    "",
    notesBlock,
    "",
    websiteBlock,
    "",
    pdfBundle
  ].join("\n");
}

// ------------------------------
// LLM calls
// ------------------------------

function pickModel() {
  // Prefer Azure if configured
  if (process.env.AZURE_OPENAI_API_KEY && process.env.AZURE_OPENAI_ENDPOINT && process.env.AZURE_OPENAI_DEPLOYMENT) {
    return { provider: 'azure' };
  }
  if (process.env.OPENAI_API_KEY) return { provider: 'openai' };
  return { provider: 'none' };
}

async function callAzureChatJSON({ messages, max_tokens = 1200 }) {
  const endpoint = process.env.AZURE_OPENAI_ENDPOINT; // e.g., https://xxx.openai.azure.com
  const deployment = process.env.AZURE_OPENAI_DEPLOYMENT;
  const apiVersion = process.env.AZURE_OPENAI_API_VERSION || '2024-08-01-preview';
  const url = `${endpoint}/openai/deployments/${deployment}/chat/completions?api-version=${apiVersion}`;

  const body = {
    messages,
    temperature: 0.2,
    response_format: { type: 'json_object' },
    max_tokens,
  };

  const ac = new AbortController();
  const t = setTimeout(() => ac.abort('timeout'), MODEL_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'api-key': process.env.AZURE_OPENAI_API_KEY
      },
      body: JSON.stringify(body),
      signal: ac.signal,
    });
    if (!res.ok) throw new Error(`Azure OpenAI error ${res.status}`);
    const json = await res.json();
    const content = json?.choices?.[0]?.message?.content || '';
    return { content, provider: 'azure' };
  } finally {
    clearTimeout(t);
  }
}

async function callOpenAIChatJSON({ messages, max_tokens = 1200 }) {
  const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';
  const apiKey = process.env.OPENAI_API_KEY;
  const baseUrl = (process.env.OPENAI_BASE_URL || 'https://api.openai.com').replace(/\/+$/, '');
  const url = `${baseUrl}/v1/chat/completions`;

  if (!apiKey) {
    throw new Error('OpenAI API key missing (set OPENAI_API_KEY).');
  }

  const body = {
    model,
    messages,
    temperature: 0.2,
    response_format: { type: 'json_object' },
    max_tokens,
  };

  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${apiKey}`,
  };
  if (process.env.OPENAI_ORGANIZATION) {
    headers['OpenAI-Organization'] = process.env.OPENAI_ORGANIZATION;
  }
  if (process.env.OPENAI_PROJECT) {
    headers['OpenAI-Project'] = process.env.OPENAI_PROJECT;
  }

  const ac = new AbortController();
  const t = setTimeout(() => ac.abort('timeout'), MODEL_TIMEOUT_MS);

  try {
    const res = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
      signal: ac.signal,
    });

    if (!res.ok) {
      // Try to surface the API’s error JSON if available
      let detail = '';
      try {
        const errJson = await res.json();
        detail = errJson?.error?.message || JSON.stringify(errJson);
      } catch {
        try { detail = await res.text(); } catch { /* ignore */ }
      }
      throw new Error(`OpenAI error ${res.status}${detail ? `: ${detail}` : ''}`);
    }

    const json = await res.json();
    const content = json?.choices?.[0]?.message?.content || '';
    const usage = json?.usage || null;
    return { content, provider: 'openai', usage };
  } finally {
    clearTimeout(t);
  }
}

// ------------------------------
// JSON recovery & normalization
// ------------------------------

function stripFences(s) {
  if (!s) return '';
  return String(s)
    .replace(/^```[a-z]*\n?/i, '')
    .replace(/```\s*$/i, '')
    .trim();
}

function salvageJson(s) {
  const x = stripFences(s);
  const first = x.indexOf('{');
  const last = x.lastIndexOf('}');
  if (first >= 0 && last > first) return x.slice(first, last + 1);
  return x;
}

function ensureArray(a) { return Array.isArray(a) ? a : (a == null ? [] : [a]); }

// Archive-shape normalisation: tips=3 and citations nested under report.citations
function normalizeQualification(q) {
  const out = q && typeof q === 'object' ? { ...q } : {};
  out.report = out.report || { md: 'Unknown' };
  if (typeof out.report.md !== 'string' || !out.report.md.trim()) out.report.md = 'Unknown';

  const tips = ensureArray(out.tips).map(s => String(s)).filter(Boolean);
  while (tips.length < 3) tips.push('Unknown');
  out.tips = tips.slice(0, 3);

  // Ensure citations nested under report.citations as [{label,url?}]
  const rc = Array.isArray(out.report?.citations) ? out.report.citations : [];
  out.report.citations = rc
    .map(c => (typeof c === 'string' ? { label: c } : c))
    .filter(c => c && typeof c.label === 'string' && c.label.trim())
    .map(c => ({ label: c.label.trim(), ...(c.url ? { url: String(c.url) } : {}) }));

  // Remove any stray top-level citations
  if (Array.isArray(out.citations)) delete out.citations;

  out.meta = Object.assign({ generatedAt: new Date().toISOString() }, out.meta || {});
  return out;
}
async function loadTradeShows() {
  // Try env path first (plaintext or JSON), else fall back to default list
  const defaultList = [
    "Connected Britain",
    "Channel Live",
    "Managed Services Summit Manchester",
    "Managed Services Summit London",
    "Infosecurity Europe"
  ];
  const p = process.env.QUAL_TRADE_SHOWS_FILE;
  if (!p) return defaultList;
  try {
    const raw = await fs.readFile(p, 'utf8');
    try {
      const asJson = JSON.parse(raw);
      if (Array.isArray(asJson) && asJson.every(s => typeof s === 'string')) return asJson;
    } catch { /* not JSON */ }
    // plaintext: one per line
    return raw.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
  } catch {
    return defaultList;
  }
}

// ------------------------------
// Main handler
// ------------------------------

module.exports = async function (context, req) {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    return ok(context, '');
  }

  try {
    // --------------------------
    // Auth
    // --------------------------
    const principal = parsePrincipal(req);

    // BYPASS: set QUAL_DISABLE_AUTH=1 to skip all app-level auth
    if (String(process.env.QUAL_DISABLE_AUTH) !== '1') {
      if (!hasAccess(principal, req)) {
        try { context.log('[auth] denied; roles=%j', (principal && principal.userRoles) || []); } catch { }
        return err(context, 'Unauthorized', 401, { reason: 'app-auth' });
      }
    } else {
      try { context.log('[auth] bypass active'); } catch { }
    }

    // --------------------------
    // GET = diagnostics
    // --------------------------
    if (req.method === 'GET') {
      return ok(context, {
        status: 'ok',
        version: 'qualification-generate/merged-1',
        provider: pickModel().provider,
        local: isLocal(),
        time: new Date().toISOString(),
      });
    }

    // --------------------------
    // Only POST supported beyond this point
    // --------------------------
    if (req.method !== 'POST') {
      return err(context, 'Method not allowed', 405);
    }

    // --------------------------
    // Input parsing: JSON or multipart
    // --------------------------
    let variables = {};
    let notes = '';
    let website = '';
    let extraPaths = [];
    let pdfFiles = [];

    const ct = (req.headers['content-type'] || '').toLowerCase();
    context.log('[qual] POST start; content-type=%s', ct);

    if (ct.startsWith('multipart/form-data')) {
      try {
        const { fields, files } = await parseMultipart(req); // will throw helpful error if Busboy missing
        pdfFiles = files || [];
        if (fields.variables) {
          try { variables = JSON.parse(fields.variables); } catch { variables = {}; }
        }
        notes = fields.notes || fields.free_text || '';
        website = fields.website || fields.prospect_website || '';
        extraPaths = (fields.extra_paths ? fields.extra_paths.split(',') : [])
          .map(s => s.trim()).filter(Boolean)
          .map(s => s.startsWith('/') || s.startsWith('http') ? s : '/' + s);
        context.log('[qual] multipart parsed: files=%d', (pdfFiles || []).length);
      } catch (e) {
        return err(context, 'Failed to parse multipart input', 400, { message: String(e?.message || e) });
      }
    } else {
      // JSON body
      const body = req.body || {};
      variables = body.variables || {};
      notes = body.notes || body.free_text || '';
      website = body.website || body.prospect_website || '';
      extraPaths = (Array.isArray(body.extraPaths) ? body.extraPaths : [])
        .map(s => String(s).trim())
        .filter(Boolean)
        .map(s => s.startsWith('/') || s.startsWith('http') ? s : '/' + s);
    }

    // --------------------------
    // Evidence collection
    // --------------------------
    const [siteBundle, pdfExtracts] = await Promise.all([
      bundleWebsite(website, extraPaths).catch(() => ({ pages: [], text: '' })),
      extractPdfTexts(pdfFiles).catch(() => [])
    ]);

    const pages = Array.isArray(siteBundle?.pages) ? siteBundle.pages : [];
    const websiteText = typeof siteBundle?.text === 'string' ? siteBundle.text : '';
    const pdfs = Array.isArray(pdfExtracts) ? pdfExtracts : [];

    context.log('[qual] website pages=%d, websiteTextChars=%d, pdfs=%d',
      (pages || []).length, (websiteText || '').length, (pdfs || []).length);

    // --------------------------
    // Schema & validator
    // --------------------------
    const schema = await loadSchema(req);
    const validator = getValidator(schema);
    const tradeShowsList = await loadTradeShows();

    // --------------------------
    // Prompt build (archive)
    // --------------------------
    const detailRaw = String((variables.detail || variables.detail_level || '')).toLowerCase();
    const isSummary = /^(summary|short|exec|brief)$/.test(detailRaw);
    const FULL_TARGET_WORDS = Number(process.env.QUAL_FULL_TARGET_WORDS || '1750');
    const targetWords = isSummary ? 0 : FULL_TARGET_WORDS;

    const user = buildQualificationJsonPrompt({
      values: variables,
      ixbrl: variables.ixbrlSummary || {},
      pdfs,
      websiteText,
      sitePages: pages,
      seller: {
        name: String(variables.seller_name || variables.your_name || ''),
        company: String(variables.seller_company || variables.your_company || ''),
        url: String(variables.seller_company_url || variables.your_website || '')
      },
      ourOffer: {
        product: String(variables.product_service || variables['Product / service offered'] || ''),
        otherContext: String(notes || variables.context || '')
      },
      detailMode: isSummary ? 'summary' : 'full',
      targetWords,
      tradeShows: tradeShowsList,
      sellerNotes: String(notes || variables.context || variables.other_context || '')
    });

    const system = 'You are a precise assistant that outputs valid JSON only for evidence-based B2B partner qualification.';

    const messages = [
      { role: 'system', content: system },
      { role: 'user', content: user }
    ];

    // Token budget
    const max_tokens = isSummary
      ? 2000
      : Math.min(6000, Math.ceil((targetWords || 1750) * 1.8) + 600);

    // --------------------------
    // Model call
    // --------------------------
    const { provider } = pickModel();
    if (provider === 'none') {
      return err(context, 'No LLM configured: set Azure (AZURE_OPENAI_*) or OPENAI_API_KEY.', 500);
    }
    context.log('[qual] model provider=%s', provider);

    async function onemodel(msgs, maxTok) {
      if (provider === 'azure') return callAzureChatJSON({ messages: msgs, max_tokens: maxTok });
      return callOpenAIChatJSON({ messages: msgs, max_tokens: maxTok });
    }

    const first = await onemodel(messages, max_tokens);
    context.log('[qual] model returned chars=%d', (first && first.content ? first.content.length : 0));

    function tryParse(content) {
      try {
        const raw = salvageJson(content);
        return JSON.parse(raw);
      } catch { return null; }
    }

    // --------------------------
    // Parse & validate
    // --------------------------
    let parsed = tryParse(first.content);
    let normalized = normalizeQualification(parsed || {});
    let valid = !!validator(normalized);

    const callType = String(variables.call_type || '').toLowerCase().startsWith('p') ? 'Partner' : 'Direct';
    const hasSellerNotes = !!String(notes || variables.context || '').trim();
    function answeredSellerNotes(md) {
      if (!hasSellerNotes) return true; // nothing to enforce
      // Look for the required callout anywhere in the md
      return /\*\*Response to seller[’']?s question:\*\s*/i.test(md || '');
    }

    // structure & length rules
    const struct1 = validateReportStructure(normalized.report?.md || '', { isSummary, callType });
    valid = valid && struct1.ok;

    // quality gate helpers
    function looksGeneric(md) {
      const bannedRx = /\b(well-positioned|decades of experience|cybersecurity landscape|market differentiation|client-centric|cutting-edge|holistic|industry-leading|best-in-class)\b/i;
      return bannedRx.test(md || "");
    }
    function hasEvidenceMarks(md) {
      const kw = /(turnover|revenue|gross profit|operating (?:profit|loss)|profit before tax|net assets|current assets|current liabilities|employees)/i.test(md || '');
      const bigNum = /\b\d{1,3}(?:,\d{3}){1,3}\b/.test(md || ''); // e.g., 20,760,000
      const fy = /FY\d{2}/i.test(md || '');
      const poundLike = /(?:£|GBP)\s*\d/.test(md || '');
      return fy && kw && (bigNum || poundLike);
    }
    function hasInlineCitations(md) {
      if (typeof md !== 'string') return false;
      return /\[S\d+\]/.test(md) || /\[P\d+\]/.test(md);
    }
    function requiredHeadingsFor(callType) {
      const base = [
        '## Company profile (what can be evidenced)',
        '## Pain points',
        '## Relationship value',
        '## Decision-making process',
        '## Competition & differentiation',
        '## Bottom line for you',
        '## What we could not evidence (and why)'
      ];
      if (/^partner$/i.test(String(callType || ''))) {
        base.push('## Potential partnership risks and mitigations');
      }
      return base;
    }

    let redoApplied = false;
    const tooShortFull = !isSummary && ((normalized.report?.md || '').length < 900);
    const structureFailed = !struct1.ok;
    const evidenceExists = (!isSummary) && ((pages && pages.length) || (pdfs && pdfs.length));

    function countMatches(md, rx) { return (String(md).match(rx) || []).length; }

    const citationsMissing = evidenceExists && !hasInlineCitations(normalized.report?.md || '');
    const notesUnanswered = !answeredSellerNotes(normalized.report?.md || '');

    // Website tag minimum (compute BEFORE using)
    const sTags = countMatches(normalized.report?.md || '', /\[S\d+\]/g);
    const minSiteTags = (pages && pages.length >= 2) ? 2 : ((pages && pages.length >= 1) ? 1 : 0);
    const websiteCitationsTooFew = minSiteTags > 0 && sTags < minSiteTags;

    // Financials checks
    const financialsSectionMissing = evidenceExists && !hasFinancialsSection(normalized.report?.md || '');
    const financialsParagraphBad = evidenceExists && !financialsIsParagraph(normalized.report?.md || '');
    const financialsFiguresMissing = evidenceExists && !financialsHasLabeledFigures(normalized.report?.md || '');

    if (!valid
      || looksGeneric(normalized.report?.md)
      || !hasEvidenceMarks(normalized.report?.md)
      || tooShortFull
      || structureFailed
      || financialsSectionMissing
      || financialLinesMissing
      || citationsMissing
      || notesUnanswered
      || websiteCitationsTooFew) {

      const requiredHeadingsList = requiredHeadingsFor(callType).map(h => '- ' + h).join('\n');
      const addendum = [
        "=== STRICT REWRITE INSTRUCTIONS ===",
        "The previous draft failed structural and/or evidence rules.",
        "Rewrite the ENTIRE report now and fix ALL of the following:",
        "- Use these EXACT headings in this EXACT order (verbatim):",
        requiredHeadingsList,
        "- INSIDE “## Company profile (what can be evidenced)”, add the subsection exactly named:",
        "  ### Financials (evidenced)",
        "- Under that subsection, include YEAR-labelled figures for at least:",
        "  • Revenue/Turnover   • Gross profit   • Operating profit/loss   • Profit/Loss before tax",
        "  • Cash at bank and in hand   • Current assets   • Current liabilities   • Net assets/(liabilities)   • Average employees",
        "- Add inline [P#] tags next to each financial figure you cite.",
        "- If website pages were provided, include at least " + minSiteTags + " website tags ([S#]) tied to concrete facts.",
        "- **MANDATORY:** In “## Bottom line for you”, add the exact line: **Response to seller’s question:** <a one–two sentence answer grounded in the provided evidence; if not evidenced, write “No public evidence found.”>.",
        "- Add inline [S#] or [P#] tags for every other claim that depends on evidence.",
        "- If a data point cannot be found in the provided sources, write exactly: “No public evidence found.”",
        "- Keep the schema and JSON shape exactly as instructed (no markdown fences)."
      ].join("\n");

      const messages2 = [
        { role: 'system', content: system },
        { role: 'user', content: user + "\n\n" + addendum }
      ];

      const second = await onemodel(messages2, max_tokens);
      const parsed2 = tryParse(second.content);
      const normalized2 = normalizeQualification(parsed2 || {});
      const valid2 = !!validator(normalized2);
      const struct2 = validateReportStructure(normalized2.report?.md || '', { isSummary, callType });
      const longEnough = isSummary || (normalized2.report?.md || '').length >= 900;
      const citesOk = (!evidenceExists) || hasInlineCitations(normalized2.report?.md || '');
      const notesAnswered2 = answeredSellerNotes(normalized2.report?.md || '');

      if (valid2 && struct2.ok && longEnough && citesOk && !looksGeneric(normalized2.report?.md) && hasEvidenceMarks(normalized2.report?.md) && notesAnswered2) {
        normalized = normalized2;
        valid = true;
        redoApplied = true;
      }
    }


    // --------------------------
    // Merge server citations
    // --------------------------
    const final = { ...normalized };

    const serverCitations = [];
    for (let i = 0; i < (pages || []).length; i++) {
      serverCitations.push({ label: `Website: [S${i + 1}]`, url: pages[i] });
    }
    for (const p of (pdfs || [])) {
      serverCitations.push({ label: `Annual report: ${p.filename || 'report.pdf'}` });
    }
    const chNum = String(variables.ch_number || variables.company_number || variables.companies_house_number || '').trim();
    if (chNum) {
      serverCitations.push({
        label: 'Companies House (filings)',
        url: 'https://find-and-update.company-information.service.gov.uk/company/' + encodeURIComponent(chNum)
      });
    }
    const linkedin = String(variables.company_linkedin || variables.linkedin_company || '').trim();
    if (linkedin) {
      serverCitations.push({ label: 'Company LinkedIn', url: linkedin });
    }

    const seen = new Set();
    function normUrl(u) { try { const x = new URL(u); x.hash = ''; return x.href.replace(/\/+$/, '').toLowerCase(); } catch { return ''; } }
    final.report.citations = [...(final.report.citations || []), ...serverCitations].filter(c => {
      const key = c.url ? 'u:' + normUrl(c.url) : 'l:' + (c.label || '').trim().toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    final.meta = Object.assign({}, final.meta || {}, {
      provider,
      redoApplied,
      website: website || null,
      pageCount: (pages || []).length,
      pdfCount: (pdfs || []).length,
    });

    if (!valid) {
      return err(context, 'Output failed schema validation', 422, {
        sample: take(final?.report?.md, 400),
        tips: final?.tips
      });
    }
    // --- Internal summary email for sales leadership (plain text) ---
    try {
      const tone = String(variables.tone || variables.email_tone || "Professional").trim() || "Professional";
      const seller = {
        name: String(variables.seller_name || variables.your_name || ""),
        company: String(variables.seller_company || variables.your_company || ""),
      };
      const prospect = {
        name: String(variables.prospect_name || ""),
        role: String(variables.prospect_role || ""),
        company: String(variables.prospect_company || variables.company_name || ""),
      };

      // Inputs for the email: use the generated report.md plus any seller notes the user provided
      const scriptMdText = String(final?.report?.md || "").trim();
      const callNotes = String(notes || variables.context || variables.other_context || "").trim();

      // Build a compact prompt for a JSON-wrapped plain-text email
      const emailSystem = [
        "You are an expert sales manager assistant.",
        "Write a concise internal email to a sales leader summarising an opportunity.",
        "Audience: sales leadership (internal).",
        "Requirements:",
        "- Be factual, crisp, and action-oriented.",
        "- Start with a one-line opportunity summary (who/what/why-now).",
        "- Then 3–6 bullets: buyer pains, current state, value hypothesis, proof/cred, risk/gaps.",
        "- Close with 1 clear next step + owner + timeframe.",
        "- No salutations or signatures. Plain text only.",
        `Tone: ${tone}.`
      ].join("\n");

      const emailUser = [
        "== SELLER ==",
        `Name: ${seller.name}`,
        `Company: ${seller.company}`,
        "",
        "== PROSPECT ==",
        `Name: ${prospect.name}`,
        `Role: ${prospect.role}`,
        `Company: ${prospect.company}`,
        "",
        "== QUALIFICATION REPORT (Markdown) ==",
        scriptMdText || "(none)",
        "",
        "== SELLER NOTES (verbatim) ==",
        callNotes || "(none)"
      ].join("\n");

      // Ask for JSON: { "text": "<plain text email>" }
      const emailSchema = {
        type: "object",
        properties: { text: { type: "string", description: "The internal email in plain text, no greeting/signoff" } },
        required: ["text"],
        additionalProperties: false
      };

      const emailMsgs = [
        { role: "system", content: emailSystem },
        {
          role: "user", content: [
            "Return valid JSON only. Shape:",
            JSON.stringify(emailSchema),
            "",
            emailUser
          ].join("\n")
        }
      ];

      // Reuse the same provider and JSON-calling helpers defined above
      const emailMaxTok = 700;
      const emailResp = await (provider === 'azure'
        ? callAzureChatJSON({ messages: emailMsgs, max_tokens: emailMaxTok })
        : callOpenAIChatJSON({ messages: emailMsgs, max_tokens: emailMaxTok })
      );

      const emailParsed = (() => {
        try { return JSON.parse(salvageJson(emailResp?.content || "")); } catch { return null; }
      })();

      const emailText = (emailParsed && typeof emailParsed.text === 'string')
        ? emailParsed.text.trim()
        : "";

      // Attach to response (even if empty string — client can decide)
      final.email = { text: emailText };

    } catch (e) {
      // Non-fatal: don’t block the main report; surface a hint for diagnostics
      final.email = { text: "" };
      try { context.log('[qual] email generation failed: %s', String(e && e.message || e)); } catch { }
    }

    return ok(context, final);

  } catch (e) {
    // Never let the function fall through with a blank 500
    try { context.log.error('[qualification-generate] Unhandled error', e && e.stack || e); } catch { }
    return err(context, 'Internal error', 500, { message: String(e && e.message || e) });
  }
};

