// Azure Function: qualification-generate
// Goal: unify the recent (incomplete) generator with the archive's full feature set
// Notes:
// - Preserves current names/shape; augments with optional multipart + PDF + website bundling
// - Degrades gracefully if optional deps (busboy, pdf-parse, ajv) are not installed
// - No repo renames; single-file implementation

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
const MAX_PDF_PER_FILE = toInt(process.env.MAX_UPLOAD_BYTES, 10 * 1024 * 1024);
const MAX_PDFS = toInt(process.env.QUAL_MAX_PDFS, 2);
const TEXT_CAP = toInt(process.env.QUAL_TEXT_CAP, PER_PAGE_CAP * 10);
const MAX_PDF_BYTES = toInt(process.env.QUAL_MAX_PDF_BYTES, MAX_PDF_PER_FILE * MAX_PDFS);

// Best-effort optional deps
let Ajv = null;
try { Ajv = require('ajv'); } catch (_) { /* optional */ }
let Busboy = null;
try { Busboy = require('busboy'); } catch (_) { /* optional */ }
let pdfParse = null;
try { pdfParse = require('pdf-parse'); } catch (_) { /* optional */ }

const isLocal = () => process.env.WEBSITE_INSTANCE_ID == null;

function cors(context) {
  const h = context.res?.headers ?? {};
  return {
    ...h,
    'Access-Control-Allow-Origin': '*',
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

function hasAccess(principal) {
  if (isLocal()) return true; // relax locally
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

// ------------------------------
// Multipart (optional)
// ------------------------------

async function parseMultipart(req) {
  if (!Busboy) return { fields: {}, files: [] };

  const bb = Busboy({
    headers: req.headers,
    limits: { fileSize: MAX_PDF_PER_FILE, files: MAX_PDFS }
  });

  const fields = {};
  /** @type {{ filename:string, mime:string, data:Buffer }[]} */
  const files = [];
  let totalBytes = 0;

  const done = new Promise((resolve, reject) => {
    bb.on('file', (name, file, info) => {
      const { filename, mimeType } = info;

      // Optional early MIME filter to avoid counting non-PDF bytes:
      const isPdf = /pdf$/i.test(mimeType) || /application\/pdf/i.test(mimeType);
      const chunks = [];
      let fileBytes = 0;
      let truncated = false;

      file.on('data', d => {
        fileBytes += d.length;
        // Stop buffering this file if total cap exceeded
        if (totalBytes + d.length > MAX_PDF_BYTES) {
          // still need to drain the stream to the end:
          return; // simply don't push; Busboy keeps reading
        }
        // Only buffer if it's a PDF and we haven't hit per-file cap (Busboy enforces per-file)
        if (isPdf && !truncated) {
          chunks.push(d);
        }
        totalBytes += d.length;
      });

      file.on('limit', () => {
        truncated = true; // per-file cap reached (Busboy cut it)
      });

      file.on('end', () => {
        // Only keep if it's a PDF and it wasn't truncated
        if (isPdf && !truncated) {
          const data = Buffer.concat(chunks);
          files.push({ filename, mime: mimeType, data });
        }
        // else: oversize or non-PDF -> drop silently
      });
    });

    bb.on('field', (name, val) => { fields[name] = val; });
    bb.on('error', reject);
    bb.on('close', resolve);
  });

  req.pipe(bb);
  await done;

  // Safety: still filter to PDFs and cap count
  const pdfs = files.filter(f => /pdf$/i.test(f.mime) || /application\/pdf/i.test(f.mime)).slice(0, MAX_PDFS);
  return { fields, files: pdfs };
}

// ------------------------------
// PDF extraction (optional)
// ------------------------------

async function extractPdfTexts(files) {
  const out = [];
  if (!pdfParse || !Array.isArray(files)) return out;
  for (const f of files) {
    try {
      const res = await pdfParse(f.data);
      const text = take(res.text || '', PER_PAGE_CAP * 5); // pragmatic cap
      if (text?.trim()) out.push({ filename: f.filename, text });
    } catch (e) {
      // ignore this file; continue
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

async function bundleWebsite(website, extraPaths = []) {
  if (!website) return { pages: [], text: '' };
  const base = normalizeBase(website);
  if (!base) return { pages: [], text: '' };
  const slugs = Array.from(new Set([...(extraPaths || []), ...DEFAULT_SLUGS]));
  const pages = [];
  let text = '';
  for (const slug of slugs) {
    const url = slug.startsWith('http') ? slug : base + slug;
    try {
      const res = await timedFetch(url, { method: 'GET' }, DEFAULT_TIMEOUT_MS);
      if (!res.ok) continue;
      const html = await res.text();
      const chunk = htmlToText(html);
      if (!chunk) continue;
      pages.push(url);
      text += ' \n---\n' + take(chunk, PER_PAGE_CAP);
      if (text.length >= TEXT_CAP) break;
    } catch { /* skip */ }
  }
  return { pages, text: take(text, TEXT_CAP) };
}

// ------------------------------
// Schema loading and validation
// ------------------------------

async function loadSchema(req) {
  // Try to fetch from public route (works in SWA -> Functions proxy)
  const host = req.headers.host;
  const proto = req.headers['x-forwarded-proto'] || 'https';
  const url = `${proto}://${host}/api/schemas/qualification.v2.json`;
  try {
    const res = await timedFetch(url, { method: 'GET' }, DEFAULT_TIMEOUT_MS);
    if (res.ok) {
      const json = await res.json();
      return json;
    }
  } catch { /* fall through */ }
  // Minimal fallback (kept intentionally tiny)
  return {
    type: 'object',
    properties: {
      report: { type: 'object', properties: { md: { type: 'string' } }, required: ['md'] },
      tips: { type: 'array', items: { type: 'string' }, minItems: 3, maxItems: 3 },
      citations: { type: 'array', items: { type: 'string' } },
      meta: { type: 'object' }
    },
    required: ['report', 'tips']
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

// ------------------------------
// Prompt assembly
// ------------------------------

function buildPrompt({ variables = {}, notes = '', websiteText = '', pdfs = [], schemaText = '' }) {
  const v = variables || {};
  const pdfNote = pdfs.length ? `PDF extracts (filename: first chars):\n${pdfs.map(p => `- ${p.filename}: ${take(p.text, 800)}`).join('\n')}` : 'No PDFs provided.';
  const siteNote = websiteText ? `Website bundle (first chars):\n${take(websiteText, 4000)}` : 'No website pages fetched or site text empty.';

  const sys = [
    'You are an expert B2B qualification analyst for UK technology MSP/channel deals.',
    'Write UK business English. Be precise, neutral, and verifiable. Avoid fluff.',
    'Use only the evidence provided below (website bundle + PDFs + variables + notes).',
    'Never invent facts; use "Unknown" where evidence is absent.',
    'Output must be valid JSON that conforms exactly to the schema provided.'
  ].join(' ');

  const user = [
    'Task: produce a buyer-ready qualification report with crisp bullets and a decisive bottom line.',
    '',
    'Evidence:',
    `Variables: ${JSON.stringify(v)}`,
    `Notes: ${notes || ''}`,
    siteNote,
    pdfNote,
    '',
    'Schema (verbatim, follow strictly):',
    schemaText,
    '',
    'Important JSON rules:',
    '- Respond with JSON ONLY. No code fences, no backticks, no prose before/after.',
    '- Use "Unknown" for any missing values.',
    '- Keep report.md tightly written and evidence-anchored; avoid generic statements.',
  ].join('\n');

  return { system: sys, user };
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
        'api-key': process.env.AZURE_OPENAI_API_KEY,
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
  const url = 'https://api.openai.com/v1/chat/completions';
  const body = {
    model,
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
        'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify(body),
      signal: ac.signal,
    });
    if (!res.ok) throw new Error(`OpenAI error ${res.status}`);
    const json = await res.json();
    const content = json?.choices?.[0]?.message?.content || '';
    return { content, provider: 'openai' };
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
  // find outermost braces
  const first = x.indexOf('{');
  const last = x.lastIndexOf('}');
  if (first >= 0 && last > first) {
    return x.slice(first, last + 1);
  }
  return x; // best effort
}

function ensureArray(a) { return Array.isArray(a) ? a : (a == null ? [] : [a]); }

function normalizeQualification(q) {
  const out = q && typeof q === 'object' ? { ...q } : {};
  out.report = out.report || { md: 'Unknown' };
  if (typeof out.report.md !== 'string' || !out.report.md.trim()) out.report.md = 'Unknown';
  out.tips = ensureArray(out.tips).map(s => String(s)).filter(Boolean);
  // enforce exactly 3 tips if schema expects that; pad with 'Unknown'
  while (out.tips.length < 3) out.tips.push('Unknown');
  if (out.tips.length > 3) out.tips = out.tips.slice(0, 3);
  out.citations = ensureArray(out.citations).map(s => String(s)).filter(Boolean);
  out.meta = Object.assign({ generatedAt: new Date().toISOString() }, out.meta || {});
  return out;
}

// ------------------------------
// Main handler
// ------------------------------

module.exports = async function (context, req) {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    return ok(context, '');
  }

  const principal = parsePrincipal(req);
  if (!hasAccess(principal)) {
    return err(context, 'Unauthorized', 401);
  }

  if (req.method === 'GET') {
    // Diagnostics (lightweight)
    return ok(context, {
      status: 'ok',
      version: 'qualification-generate/merged-1',
      provider: pickModel().provider,
      local: isLocal(),
      time: new Date().toISOString(),
    });
  }

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

  if (ct.startsWith('multipart/form-data') && Busboy) {
    try {
      const { fields, files } = await parseMultipart(req);
      pdfFiles = files || [];
      // Fields: variables may arrive as JSON string
      if (fields.variables) {
        try { variables = JSON.parse(fields.variables); } catch { variables = {}; }
      }
      notes = fields.notes || fields.free_text || '';
      website = fields.website || fields.prospect_website || '';
      extraPaths = (fields.extra_paths ? fields.extra_paths.split(',') : [])
        .map(s => s.trim())
        .filter(Boolean)
        .map(s => s.startsWith('/') || s.startsWith('http') ? s : '/' + s);
    } catch (e) {
      return err(context, 'Failed to parse multipart input', 400, { message: String(e?.message || e) });
    }
  } else {
    // JSON body
    const body = req.body || {};
    variables = body.variables || {};
    notes = body.notes || body.free_text || '';
    website = body.website || body.prospect_website || '';
    extraPaths = Array.isArray(body.extraPaths) ? body.extraPaths : [];
    // No PDFs in JSON mode
  }

  // --------------------------
  // Evidence collection
  // --------------------------
  const [{ pages, text: websiteText }, pdfs] = await Promise.all([
    bundleWebsite(website, extraPaths),
    extractPdfTexts(pdfFiles)
  ]);

  // --------------------------
  // Schema & validator
  // --------------------------
  const schema = await loadSchema(req);
  const validator = getValidator(schema);
  const schemaText = JSON.stringify(schema);

  // --------------------------
  // Prompt build
  // --------------------------
  const { system, user } = buildPrompt({ variables, notes, websiteText, pdfs, schemaText });

  const messages = [
    { role: 'system', content: system },
    { role: 'user', content: user }
  ];

  const { provider } = pickModel();
  if (provider === 'none') {
    return err(context, 'No LLM configured: set Azure (AZURE_OPENAI_*) or OPENAI_API_KEY.', 500);
  }

  async function onemodel(msgs) {
    if (provider === 'azure') return callAzureChatJSON({ messages: msgs, max_tokens: 1400 });
    return callOpenAIChatJSON({ messages: msgs, max_tokens: 1400 });
  }

  let first;
  try {
    first = await onemodel(messages);
  } catch (e) {
    return err(context, 'Model call failed', 502, { message: String(e?.message || e) });
  }

  function tryParse(content) {
    try {
      const raw = salvageJson(content);
      return JSON.parse(raw);
    } catch (e) { return null; }
  }

  let parsed = tryParse(first.content);
  let normalized = normalizeQualification(parsed || {});
  let valid = !!validator(normalized);

  // Quality/redo gate: if invalid or looks generic (no citations and report is short), try one redo
  let redoApplied = false;
  if (!valid || (!normalized.citations?.length && normalized.report?.md?.length < 400)) {
    const addendum = 'ADDENDUM: The previous output failed validation or was too generic. Strengthen evidence ties (cite website page themes and PDF filenames), and ensure the JSON strictly matches the schema.';
    const messages2 = [...messages, { role: 'user', content: addendum }];
    let second;
    try {
      second = await onemodel(messages2);
    } catch (_) { /* ignore */ }
    if (second?.content) {
      const parsed2 = tryParse(second.content);
      const normalized2 = normalizeQualification(parsed2 || {});
      const valid2 = !!validator(normalized2);
      if (valid2) {
        normalized = normalized2;
        valid = true;
        redoApplied = true;
      }
    }
  }

  // Merge server-known citations
  const serverCitations = [
    ...pages,
    ...(pdfs || []).map(p => `pdf:${p.filename}`)
  ];
  const final = { ...normalized };
  final.citations = Array.from(new Set([...(final.citations || []), ...serverCitations]));
  final.meta = Object.assign({}, final.meta || {}, {
    provider,
    durationMs: undefined, // not measured precisely here
    redoApplied,
    website: website || null,
    pageCount: pages.length,
    pdfCount: (pdfs || []).length,
  });

  if (!valid) {
    return err(context, 'Output failed schema validation', 422, { sample: take(final?.report?.md, 400), tips: final?.tips });
  }

  return ok(context, final);
};
