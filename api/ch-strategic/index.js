/** api/ch-strategic/index.js single Azure handler 22/09/2025 */
'use strict';

/**
 * CH Strategic — Azure Functions HTTP trigger (no Express/Multer)
 * - Small run:    POST  /api/ch-strategic            (multipart; no storage; ≤ SMALL_MAX_ROWS)
 * - Start (big):  POST  /api/ch-strategic/start      (multipart; requires storage)
 * - Status:       GET   /api/ch-strategic/status/:id (reads blob JSON)
 * - Download:     GET   /api/ch-strategic/download/:id (streams CSV)
 * - Feedback:     POST  /api/ch-strategic/feedback   (small JSON body)
 * - Health:       GET   /api/ch-strategic/healthz
 *
 * Ports unchanged: Node 7072 (Functions), SWA 4280 (proxy), Python 7071
 */

const Busboy = require('busboy'); // v1.6.0 (already in your deps)
const { parse: parseSync } = require('csv-parse/sync');
const { BlobServiceClient } = require('@azure/storage-blob');
const { randomUUID } = require('crypto');

// ------------------------------- Config / Constants -------------------------------
const CORS = Object.freeze({
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, content-type, x-ms-client-principal, x-correlation-id'
});

const SMALL_MAX_ROWS = Number(process.env.CH_STRATEGIC_SMALL_MAX_ROWS || 50);
const UPLOAD_LIMIT_BYTES = Number(process.env.CH_STRATEGIC_UPLOAD_LIMIT_BYTES || 20 * 1024 * 1024); // 20MB default
const AZ_CONN = process.env.AzureWebJobsStorage || null;

// ------------------------------- Utils -------------------------------
function uuid() {
  try { return randomUUID(); } catch {
    // Should not happen on Node 20+, but keep a fallback
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
      const r = (Math.random() * 16) | 0, v = c === 'x' ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }
}

function getCid(req) {
  const hdr = req?.headers?.['x-correlation-id'];
  return (typeof hdr === 'string' && hdr.trim()) || uuid();
}

function ok(code, body, cid) {
  return {
    status: code,
    headers: { ...CORS, 'Content-Type': 'application/json; charset=utf-8', 'X-Correlation-Id': cid },
    body
  };
}

function err(code, message, cid) {
  const msg = typeof message === 'string' ? message : (message?.message || 'Internal error');
  return {
    status: code,
    headers: { ...CORS, 'Content-Type': 'application/json; charset=utf-8', 'X-Correlation-Id': cid },
    body: { ok: false, code, message: msg }
  };
}

function csvEscape(s) {
  const str = String(s);
  return /["\n,\r]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
}

// --- Header normalisation + lookup ---
function normHeader(s) {
  return String(s || '')
    .normalize('NFKC')               // unify Unicode forms
    .replace(/^\uFEFF/, '')          // strip BOM if present
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');      // drop spaces, underscores, punctuation
}

function ciIndexOf(haystack, needle, from = 0) {
  if (!haystack || !needle) return -1;
  const h = String(haystack).toLowerCase();
  const n = String(needle).toLowerCase();
  return h.indexOf(n, from);
}
function snippetAround(text, needle, radius = 80) {
  if (!text || !needle) return '';
  const idx = ciIndexOf(text, needle);
  if (idx < 0) return '';
  const start = Math.max(0, idx - radius);
  const end = Math.min(text.length, idx + String(needle).length + radius);
  let s = text.slice(start, end).replace(/\s+/g, ' ').trim();
  if (start > 0) s = '… ' + s;
  if (end < text.length) s = s + ' …';
  return s;
}

// Get context snippet from the company report
async function tryGetReportText(companyNumber, { timeoutMs = 8000, maxLen = 200000 } = {}) {
  const base = process.env.CH_STRATEGIC_TEXT_URL || process.env.CH_READER_ENDPOINT || '';
  if (!companyNumber || !base) return null;

  // Build candidate URLs: prefer /:id, then ?id=, then ?companyNumber=
  const cleanBase = String(base).replace(/\/+$/, '');
  const enc = encodeURIComponent(companyNumber);
  const candidates = [
    `${cleanBase}/${enc}`,
    `${cleanBase}?id=${enc}`,
    `${cleanBase}?companyNumber=${enc}`,
  ];

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  // Local helper to normalize various JSON shapes to a string
  const pickText = (j) => {
    if (!j || typeof j !== 'object') return '';
    if (typeof j.text === 'string') return j.text;
    if (typeof j.body === 'string') return j.body;
    if (typeof j.content === 'string') return j.content;
    if (j.data) {
      if (typeof j.data.text === 'string') return j.data.text;
      if (typeof j.data.body === 'string') return j.data.body;
      if (typeof j.data.content === 'string') return j.data.content;
    }
    return '';
  };

  try {
    for (const url of candidates) {
      try {
        const res = await fetch(url, {
          method: 'GET',
          headers: { 'Accept': 'application/json, text/plain, text/html' },
          signal: controller.signal,
        });
        if (!res.ok) {
          // Try next candidate on 404/400; bail on hard server errors
          if (res.status >= 500) return null;
          continue;
        }

        const ct = (res.headers.get('content-type') || '').toLowerCase();

        let text = '';
        if (ct.includes('application/json')) {
          const j = await res.json();
          text = pickText(j);
        } else {
          // Accept text/plain and text/html as plain text
          text = await res.text();
        }

        if (typeof text !== 'string') continue;
        text = text.trim();
        if (!text) continue;

        // Cap length to protect memory / CSV size
        if (text.length > maxLen) text = text.slice(0, maxLen);
        return text;
      } catch {
        // Network or abort — try next candidate
        continue;
      }
    }
    return null;
  } finally {
    clearTimeout(timer);
  }
}

// ADD: Multi-term parsing that remains backward-compatible with single term
function parseEvidenceList(input) {
  const s = (input ?? '').trim();
  if (!s) return [];
  // split on commas, keep phrases intact, trim/normalize, drop empties, dedupe
  const terms = s.split(',').map(t => t.trim()).filter(Boolean);
  return Array.from(new Set(terms));
}

// ADD: OR-match across terms and return WHICH term matched
function evidenceAnyWithTerm(text, tagOrList, singleMatch) {
  const terms = Array.isArray(tagOrList) ? tagOrList : parseEvidenceList(tagOrList);
  if (terms.length <= 1) {
    const t = terms[0] ?? (typeof tagOrList === 'string' ? tagOrList : '');
    return singleMatch(text, t) ? { hit: true, term: t } : { hit: false, term: '' };
  }
  for (const t of terms) {
    if (singleMatch(text, t)) return { hit: true, term: t };
  }
  return { hit: false, term: '' };
}

// ADD: Wrap existing single-term matcher with an ANY-of-terms check
// IMPORTANT: 'singleTermMatch' must be your current matching function or inline check.
// If you don't have a named function, pass a callback that implements your current logic.
function evidenceAny(text, tagOrList, singleTermMatch) {
  const terms = Array.isArray(tagOrList) ? tagOrList : parseEvidenceList(tagOrList);
  // If no commas (i.e., 0 or 1 term), fall through to the original single-term matcher
  if (terms.length <= 1) {
    const t = terms[0] ?? (typeof tagOrList === 'string' ? tagOrList : '');
    return singleTermMatch(text, t);
  }
  for (const t of terms) {
    if (singleTermMatch(text, t)) return true;
  }
  return false;
}

// ADD: simple single-term matcher (case-insensitive, collapses whitespace)
function textMatchesEvidence(text, term) {
  if (!text || !term) return false;
  const norm = (s) => String(s).toLowerCase().replace(/\s+/g, ' ').trim();
  return norm(text).includes(norm(term));
}

// ADD: parse comma-separated list and OR-match, returning WHICH term hit
function parseEvidenceList(input) {
  const s = (input ?? '').trim();
  if (!s) return [];
  return Array.from(new Set(s.split(',').map(t => t.trim()).filter(Boolean)));
}
function evidenceAnyWithTerm(text, tagOrList) {
  const terms = Array.isArray(tagOrList) ? tagOrList : parseEvidenceList(tagOrList);
  if (terms.length <= 1) {
    const t = terms[0] ?? (typeof tagOrList === 'string' ? tagOrList : '');
    return textMatchesEvidence(text, t) ? { hit: true, term: t } : { hit: false, term: '' };
  }
  for (const t of terms) {
    if (textMatchesEvidence(text, t)) return { hit: true, term: t };
  }
  return { hit: false, term: '' };
}

function findHeader(headers, targetLabel) {
  const want = normHeader(targetLabel);
  return headers.find(h => normHeader(h) === want) || null;
}

function validateRequired(headers) {
  const missing = [];
  if (!findHeader(headers, 'Company Name')) missing.push('Company Name');
  if (!findHeader(headers, 'Company Number')) missing.push('Company Number');
  return missing;
}

function preflight(req) {
  return (req?.method || '').toUpperCase() === 'OPTIONS';
}

// ------------------------------- Storage -------------------------------
const BLOB = AZ_CONN ? BlobServiceClient.fromConnectionString(AZ_CONN) : null;
const CONTAINERS = Object.freeze({
  out: 'ch-strategic-out',
  status: 'ch-strategic-status',
  feedback: 'ch-strategic-feedback'
});

async function ensureContainers() {
  if (!BLOB) throw new Error('AzureWebJobsStorage is not configured');
  await Promise.all(Object.values(CONTAINERS).map(async (name) => {
    const c = BLOB.getContainerClient(name);
    await c.createIfNotExists();
  }));
}

async function putJson(containerName, blobName, obj) {
  const c = BLOB.getContainerClient(containerName);
  const b = c.getBlockBlobClient(blobName);
  const data = Buffer.from(JSON.stringify(obj, null, 2), 'utf8');
  await b.uploadData(data, { blobHTTPHeaders: { blobContentType: 'application/json; charset=utf-8' } });
}

async function getJson(containerName, blobName) {
  const c = BLOB.getContainerClient(containerName);
  const b = c.getBlockBlobClient(blobName);
  if (!(await b.exists())) return null;
  const dl = await b.download();
  const chunks = [];
  for await (const d of dl.readableStreamBody) chunks.push(d);
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

async function putCsv(containerName, blobName, buffer) {
  const c = BLOB.getContainerClient(containerName);
  const b = c.getBlockBlobClient(blobName);
  await b.uploadData(buffer, { blobHTTPHeaders: { blobContentType: 'text/csv; charset=utf-8' } });
}

async function getCsvStream(containerName, blobName) {
  const c = BLOB.getContainerClient(containerName);
  const b = c.getBlockBlobClient(blobName);
  if (!(await b.exists())) return null;
  const dl = await b.download();
  return { stream: dl.readableStreamBody, size: dl.contentLength || undefined };
}

// ADD: header resolver with common aliases (works with your normHeader/findHeader)
function findHeaderWithAliases(headers, target) {
  const key = findHeader(headers, target);
  if (key) return key;
  const aliases = {
    'Company Name': [
      'Company Name', 'CompanyName', 'Name', 'Company', 'Organisation', 'Organization'
    ],
    'Company Number': [
      'Company Number', 'CompanyNumber', 'Number', 'Company No', 'Company No.', 'Registration Number',
      'Reg Number', 'Companies House Number', 'Co Number', 'Co. Number'
    ]
  };
  const wanted = aliases[target] || [target];
  const norm = (s) => normHeader(String(s || ''));
  const set = new Map(headers.map(h => [norm(h), h]));
  for (const a of wanted) {
    const hit = set.get(norm(a));
    if (hit) return hit;
  }
  return null;
}

// ADD: ensure json400 exists (structured 400 response with CORS)
function json400(context, body) {
  context.res = {
    status: 400,
    headers: CORS,
    body
  };
  return;
}

// ------------------------------- CSV helpers (sync; no Node streams) -------------------------------

// ADD: robust header normalisation + single-source resolver for required keys
function stripBOMTrim(s) {
  return String(s || '').replace(/^\uFEFF/, '').trim();
}
function normalizeHeaderList(headers) {
  return Array.isArray(headers) ? headers.map(stripBOMTrim) : [];
}
/**
 * Resolve required CSV keys from a Buffer using the SAME logic everywhere.
 * Returns { headers, nameKey, numKey } or throws on parse error.
 */
function resolveCompanyKeysFromBuffer(buffer) {
  const { headers } = parseCsvFlexible(buffer);
  const norm = normalizeHeaderList(headers);
  // Prefer aliases against normalised headers, then raw as fallback
  const nameKey = findHeaderWithAliases(norm, 'Company Name') || findHeaderWithAliases(headers, 'Company Name');
  const numKey = findHeaderWithAliases(norm, 'Company Number') || findHeaderWithAliases(headers, 'Company Number');
  return { headers: norm, nameKey, numKey };
}


// Try multiple CSV dialects and pick the best (prefer the one that contains required headers)
function parseCsvFlexible(buffer) {
  const delims = [',', ';', '\t', '|'];
  let best = null;

  for (const d of delims) {
    try {
      const recs = parseSync(buffer, {
        columns: true,
        bom: true,
        relax_column_count: true,
        skip_empty_lines: true,
        trim: true,
        delimiter: d
      });

      const headersSet = new Set();
      for (const r of recs) Object.keys(r).forEach(h => headersSet.add(String(h)));
      const headers = Array.from(headersSet);

      // scoring: prefer dialects that contain both required columns (normalized), then more columns
      const hasBoth =
        headers.some(h => normHeader(h) === normHeader('Company Name')) &&
        headers.some(h => normHeader(h) === normHeader('Company Number'));

      const score = (hasBoth ? 1000 : 0) + headers.length; // tie-breaker: wider header set wins

      if (!best || score > best.score) {
        best = { recs, headers, delimiter: d, score };
      }
    } catch { /* ignore this delimiter */ }
  }

  if (!best) {
    // fallback to default comma
    const recs = parseSync(buffer, {
      columns: true, bom: true, relax_column_count: true, skip_empty_lines: true, trim: true
    });
    const headers = Array.from(new Set(recs.flatMap(r => Object.keys(r).map(String))));
    return { recs, headers, delimiter: ',' };
  }
  return best;
}

// ADD: parse comma-separated evidence list (backward-compatible with single term)
function parseEvidenceList(input) {
  const s = (input ?? '').trim();
  if (!s) return [];
  return Array.from(new Set(s.split(',').map(t => t.trim()).filter(Boolean)));
}

async function summarizeCsv(buffer, opts = {}) {
  const evidenceTag = (opts?.evidenceTag ?? '').trim();
  const evidenceTerms = Array.isArray(opts?.evidenceTerms) ? opts.evidenceTerms : parseEvidenceList(evidenceTag);

  const { recs } = parseCsvFlexible(buffer);
  const resolved = resolveCompanyKeysFromBuffer(buffer);
  const headers = resolved.headers;
  const nameKey = resolved.nameKey;
  const numKey = resolved.numKey;
  
  let rows = 0, matched = 0, skipped = 0;
  const errorsByReason = {};
  const itemsSample = [];
  const matches = [];

  for (const r of recs) {
    rows += 1;
    const name = String(nameKey ? r[nameKey] : '').trim();
    const num = String(numKey ? r[numKey] : '').trim();

    if (!name || !num) {
      skipped += 1;
      const reason = !name && !num ? 'missing_both'
        : (!num ? 'missing_company_number' : 'missing_company_name');
      errorsByReason[reason] = (errorsByReason[reason] || 0) + 1;
      continue;
    }

    // Prefer Azure reader text if available, otherwise fall back to Company Name
    const reportText = (await tryGetReportText(num)) || name;

    const { hit, term } = evidenceAnyWithTerm(
      reportText,
      (evidenceTerms.length ? evidenceTerms : evidenceTag)
    );

    if (hit) {
      matched += 1;
      const snippet = snippetAround(reportText, term || evidenceTag);
      const m = { companyNumber: num, companyName: name, evidence: term || evidenceTag, snippet };
      matches.push(m);
      if (itemsSample.length < 10) itemsSample.push(m);
    } else {
      skipped += 1;
      errorsByReason['no_evidence_match'] = (errorsByReason['no_evidence_match'] || 0) + 1;
    }
  }

  return { rows, matched, skipped, errorsByReason, headers, itemsSample, matches };
}

async function buildOutputCsv(buffer, evidenceTag) {
  const { recs, headers } = parseCsvFlexible(buffer);
  const nameKey = findHeaderWithAliases(headers, 'Company Name');
  const numKey = findHeaderWithAliases(headers, 'Company Number');

  const rows = [];
  for (const r of recs) {
    const name = String(nameKey ? r[nameKey] : '').trim();
    const num = String(numKey ? r[numKey] : '').trim();
    if (name && num) {
      rows.push(`${num},${csvEscape(name)},${csvEscape(evidenceTag || '')}`);
    }
  }
  const header = 'Company Number,Company Name,Evidence';
  return Buffer.from(`${header}\n${rows.join('\n')}\n`, 'utf8');
}

// ADD: Build CSV from the server-side matches array so each row uses its matched term
async function buildOutputCsvFromMatches(matches, fallbackEvidence = '') {
  const header = 'Company Number,Company Name,Evidence,Snippet';
  const rows = (matches || []).map(m => {
    const num = String(m.companyNumber ?? '').trim();
    const name = String(m.companyName ?? '').trim();
    const ev = String(m.evidence ?? fallbackEvidence ?? '').trim();
    const snippet = String(m.snippet ?? '').trim();
    if (!num || !name) return '';
    return `${num},${csvEscape(name)},${csvEscape(ev)},${csvEscape(snippet)}`;
  }).filter(Boolean);
  return Buffer.from(`${header}\n${rows.join('\n')}\n`, 'utf8');
}

// ------------------------------- Multipart (Busboy v1.6.0, Azure Functions classic req) -------------------------------
async function readMultipart(req) {
  // Azure Functions v4 (classic model): use req.headers + req.rawBody (Buffer|string)
  const ct = req.headers?.['content-type'] || '';
  if (!/^multipart\/form-data/i.test(ct)) return { error: 'Expected multipart/form-data' };
  if (req.rawBody == null) return { error: 'Missing rawBody for multipart parsing' };

  const bodyBuf = Buffer.isBuffer(req.rawBody) ? req.rawBody : Buffer.from(req.rawBody);

  // Busboy v1: called as a function (no `new`)
  const bb = require('busboy')({
    headers: { 'content-type': ct }, // must include boundary
    limits: {
      fileSize: Number(process.env.CH_STRATEGIC_UPLOAD_LIMIT_BYTES || 20 * 1024 * 1024), // 20MB default
      files: 1,
      fields: 10,
    },
  });

  const fields = {};
  let file = null;

  const p = new Promise((resolve, reject) => {
    // Capture simple fields (trimmed)
    bb.on('field', (name, val) => {
      fields[name] = (typeof val === 'string') ? val.trim() : val;
    });

    // v1 signature: (fieldname, stream, filename, encoding, mimetype)
    bb.on('file', (name, stream, filename, encoding, mimetype) => {
      // Only buffer the agreed file field; drain everything else
      if (name !== 'csv_file') { stream.resume(); return; }
      const chunks = [];
      stream.on('data', d => chunks.push(d));
      stream.on('end', () => {
        file = { buffer: Buffer.concat(chunks), filename, encoding, mimetype };
      });
      stream.on('error', reject);
    });

    // Convert limit events to friendly errors
    bb.on('partsLimit', () => reject(new Error('Too many parts')));
    bb.on('filesLimit', () => reject(new Error('Too many files')));
    bb.on('fieldsLimit', () => reject(new Error('Too many fields')));

    bb.on('error', reject);
    bb.on('finish', () => resolve({ fields, file })); // v1 emits 'finish'
  });

  // Push the whole raw body into Busboy
  bb.end(bodyBuf);
  return p;
}

// ------------------------------- Route helpers -------------------------------
function normalizePath(context) {
  const rel = String(context.bindingData?.path || '').replace(/^\/+/, '');
  return '/' + rel;
}

function isHealth(path) {
  return path === '/healthz' || path === '/api/ch-strategic/healthz' || path === '/ch-strategic/healthz';
}

function isSmallRun(method, path) {
  return method === 'POST' && (path === '/' || path === '/api/ch-strategic' || path === '/ch-strategic');
}

function isStart(method, path) {
  return method === 'POST' && (path === '/start' || path === '/api/ch-strategic/start' || path === '/ch-strategic/start');
}

function isStatus(method, path) {
  return method === 'GET' && /^\/(api\/ch-strategic\/|ch-strategic\/)?status\/[a-f0-9-]{16,}$/i.test(path);
}

function isDownload(method, path) {
  return method === 'GET' && /^\/(api\/ch-strategic\/|ch-strategic\/)?download\/[a-f0-9-]{16,}$/i.test(path);
}

function isFeedback(method, path) {
  return method === 'POST' && (path === '/feedback' || path === '/api/ch-strategic/feedback' || path === '/ch-strategic/feedback');
}

// ------------------------------- Azure Function entry -------------------------------
module.exports = async function (context, req) {
  try {
    const cid = getCid(req);
    const method = (req.method || 'GET').toUpperCase();
    const path = normalizePath(context);

    // CORS preflight
    if (preflight(req)) { context.res = { status: 204, headers: CORS }; return; }

    // Health
    if (method === 'GET' && isHealth(path)) {
      context.res = ok(200, {
        ok: true,
        name: 'ch-strategic',
        version: process.env.CH_STRATEGIC_VERSION || 'dev',
        node: process.version,
        storageConfigured: !!AZ_CONN,
        time: new Date().toISOString(),
        cid
      }, cid);
      return;
    }

    // Small run (no storage) → POST /api/ch-strategic
    if (isSmallRun(method, path)) {
      const { file, fields, error } = await readMultipart(req);
      if (error) { context.res = err(415, error, cid); return; }
      if (!file?.buffer?.length) { context.res = err(400, 'Missing file', cid); return; }

      // Evidence: single or comma-separated terms (deduped)
      const evidenceTag = (fields?.evidenceTag ?? '').trim();
      const termPattern = /^[A-Za-z0-9 _-]{1,50}$/;
      const evidenceTerms = evidenceTag
        ? Array.from(new Set(evidenceTag.split(',').map(t => t.trim()).filter(Boolean)))
        : [];

      // Validate each term (or the single tag if no commas)
      for (const t of (evidenceTerms.length ? evidenceTerms : [evidenceTag])) {
        if (t && !termPattern.test(t)) {
          context.res = err(400, `Invalid evidence term: "${t}"`, cid);
          return;
        }
      }
      // Optional safety cap
      if (evidenceTerms.length > 10) {
        context.res = err(400, 'Too many evidence terms (max 10).', cid);
        return;
      }

      // Flexible CSV header validation
      let parsed;
      try {
        parsed = resolveCompanyKeysFromBuffer(file.buffer);
      } catch (e) {
        return json400(context, {
          ok: false,
          code: 400,
          message: "Could not parse CSV",
          detail: String((e && e.message) || e)
        });
      }
      const { headers, nameKey, numKey } = parsed;
      if (!nameKey || !numKey) {
        return json400(context, {
          ok: false,
          code: 400,
          message: "Missing required column(s): Company Name, Company Number",
          headers
        });
      }

      // Run analysis (pass evidence to enable multi-term matching)
      const summary = await summarizeCsv(file.buffer, { evidenceTag, evidenceTerms });

      // Required headers
      const required = ['Company Name', 'Company Number'];
      const missing = validateRequired(summary.headers);
      if (missing.length) { context.res = err(400, `Missing required column(s): ${missing.join(', ')}`, cid); return; }

      // Row cap
      if (summary.rows > SMALL_MAX_ROWS) {
        context.res = err(413, `Too many rows for small run: ${summary.rows} > ${SMALL_MAX_ROWS}. Use /start.`, cid);
        return;
      }

      context.log?.info?.({ cid, method, path, bytes: file.buffer.length, rows: summary.rows }, 'ch-strategic small-run');
      context.res = ok(200, {
        ok: true,
        correlationId: cid,
        evidenceTag: evidenceTag || null,
        rows: summary.rows,
        matched: summary.matched,
        skipped: summary.skipped,
        errorsByReason: summary.errorsByReason,
        headers: summary.headers,
        itemsSample: summary.itemsSample
      }, cid);
      return;
    }

    // Start (large run; writes to Azure) → POST /api/ch-strategic/start
    if (isStart(method, path)) {
      if (!AZ_CONN) { context.res = err(500, 'Storage not configured', cid); return; }
      await ensureContainers();

      const { file, fields, error } = await readMultipart(req);
      if (error) { context.res = err(415, error, cid); return; }
      if (!file?.buffer?.length) { context.res = err(400, 'Missing file', cid); return; }

      // Evidence: single or comma-separated terms (deduped)
      const evidenceTag = (fields?.evidenceTag ?? '').trim();
      const termPattern = /^[A-Za-z0-9 _-]{1,50}$/;
      const evidenceTerms = evidenceTag
        ? Array.from(new Set(evidenceTag.split(',').map(t => t.trim()).filter(Boolean)))
        : [];

      // Validate each term (or the single tag if no commas)
      for (const t of (evidenceTerms.length ? evidenceTerms : [evidenceTag])) {
        if (t && !termPattern.test(t)) {
          context.res = err(400, `Invalid evidence term: "${t}"`, cid);
          return;
        }
      }
      // Optional safety cap
      if (evidenceTerms.length > 10) {
        context.res = err(400, 'Too many evidence terms (max 10).', cid);
        return;
      }

      // Flexible CSV header validation
      let parsed;
      try {
        parsed = resolveCompanyKeysFromBuffer(file.buffer);
      } catch (e) {
        return json400(context, {
          ok: false,
          code: 400,
          message: "Could not parse CSV",
          detail: String((e && e.message) || e)
        });
      }
      const { headers, nameKey, numKey } = parsed;
      if (!nameKey || !numKey) {
        return json400(context, {
          ok: false,
          code: 400,
          message: "Missing required column(s): Company Name, Company Number",
          headers
        });
      }

      // Create job
      const jobId = uuid();
      const statusName = `${jobId}.json`;
      const outName = `${jobId}.csv`;

      // Initial status
      await putJson(CONTAINERS.status, statusName, {
        state: 'running',
        jobId,
        startedAt: new Date().toISOString(),
        evidenceTag: evidenceTag || null,
        totalRows: null,
        matched: 0,
        skipped: 0,
        errorsByReason: {},
        downloadUrl: `/api/ch-strategic/download/${jobId}`
      });

      // Process (pass evidence to enable multi-term matching)
      const analysis = await summarizeCsv(file.buffer, { evidenceTag, evidenceTerms });

      // Required headers
      const missing = validateRequired(analysis.headers);
      if (missing.length) {
        await putJson(CONTAINERS.status, statusName, {
          state: 'error',
          jobId,
          finishedAt: new Date().toISOString(),
          error: `Missing required column(s): ${missing.join(', ')}`
        });
        context.res = err(400, `Missing required column(s): ${missing.join(', ')}`, cid);
        return;
      }

      // Build CSV from matches (writes only the term that matched per row)
      const outBuffer = await buildOutputCsvFromMatches(analysis.matches, evidenceTag);
      await putCsv(CONTAINERS.out, outName, outBuffer);

      // Final status
      await putJson(CONTAINERS.status, statusName, {
        state: 'done',
        jobId,
        finishedAt: new Date().toISOString(),
        evidenceTag: evidenceTag || null,
        totalRows: analysis.rows,
        matched: analysis.matched,
        skipped: analysis.skipped,
        errorsByReason: analysis.errorsByReason,
        downloadUrl: `/api/ch-strategic/download/${jobId}`
      });

      context.log?.info?.({ cid, method, path, jobId, rows: analysis.rows, outBytes: outBuffer.length }, 'ch-strategic start done');

      context.res = ok(200, {
        ok: true,
        jobId,
        statusUrl: `/api/ch-strategic/status/${jobId}`,
        downloadUrl: `/api/ch-strategic/download/${jobId}`,
        correlationId: cid
      }, cid);
      return;
    }

    // Status → GET /api/ch-strategic/status/:id
    if (isStatus(method, path)) {
      const id = path.split('/').pop();
      if (!AZ_CONN) { context.res = err(500, 'Storage not configured', cid); return; }
      const status = await getJson(CONTAINERS.status, `${id}.json`);
      if (!status) { context.res = err(404, 'Not found', cid); return; }
      context.res = ok(200, status, cid);
      return;
    }

    // Download → GET /api/ch-strategic/download/:id
    if (isDownload(method, path)) {
      const id = path.split('/').pop();
      if (!AZ_CONN) { context.res = err(500, 'Storage not configured', cid); return; }
      const info = await getCsvStream(CONTAINERS.out, `${id}.csv`);
      if (!info) { context.res = err(404, 'Not found', cid); return; }
      context.res = {
        status: 200,
        headers: {
          ...CORS,
          'Content-Type': 'text/csv; charset=utf-8',
          'Content-Disposition': `attachment; filename="ch-strategic-${id}.csv"`,
          'X-Correlation-Id': cid
        },
        body: info.stream
      };
      return;
    }

    // Feedback (JSON body; small)
    if (isFeedback(method, path)) {
      if (!AZ_CONN) { context.res = err(500, 'Storage not configured', cid); return; }
      await ensureContainers();
      const body = typeof req.body === 'object' && req.body ? req.body : {};
      const payload = {
        receivedAt: new Date().toISOString(),
        correlationId: cid,
        up: !!body.up,
        details: (body.details || '').toString().slice(0, 2000)
      };
      await putJson(CONTAINERS.feedback, `${uuid()}.json`, payload);
      context.res = { status: 204, headers: CORS };
      return;
    }

    // Fallback
    context.res = err(404, `Not found: ${method} ${path}`, cid);
  } catch (e) {
    const cid = getCid(req);
    context.log?.error?.('ch-strategic fatal', e);
    context.res = err(500, e?.message || 'Internal error', cid);
  }
};
