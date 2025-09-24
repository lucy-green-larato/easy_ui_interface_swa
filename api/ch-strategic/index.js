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
// (URL OCR → CH URL OCR → CH buffer OCR)
async function tryGetReportText(companyNumber, { timeoutMs = 8000, maxLen = 200000 } = {}) {
  aiTrack('ch-strategic.reader.start', { num: companyNumber, ...envFingerprint() });
  logProbe('start', {
    companyNumber,
    base: !!(process.env.CH_STRATEGIC_TEXT_URL || process.env.CH_READER_ENDPOINT),
    di: !!(process.env.AZ_DI_ENDPOINT && process.env.AZ_DI_KEY),
    ch: !!(process.env.CH_API_KEY)
  });
  if (!companyNumber) return null;

  const base = process.env.CH_STRATEGIC_TEXT_URL || process.env.CH_READER_ENDPOINT || '';
  const cleanBase = String(base || '').replace(/\/+$/, '');
  const encNum = encodeURIComponent(normalizeCompanyNumber(companyNumber));
  const candidates = cleanBase
    ? [`${cleanBase}/${encNum}`, `${cleanBase}?id=${encNum}`, `${cleanBase}?companyNumber=${encNum}`]
    : [];

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

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
    aiTrack('ch-strategic.reader.end', { num: companyNumber, returned: false });
    return '';
  };

  const pagesFirst = Number(process.env.CH_SR_PAGES_FIRST || 0) || null;

  try {
    // (1) Try your reader service first (if configured)
    for (const url of candidates) {
      try {
        const res = await fetch(url, {
          method: 'GET',
          headers: { 'Accept': 'application/json, text/plain, text/html, application/pdf, text/markdown' },
          signal: controller.signal,
        });

        const ctype = (res.headers.get('content-type') || '').toLowerCase();
        logProbe('reader.fetch', { url, ok: res.ok, status: res.status, contentType: ctype });

        if (!res.ok) {
          if (res.status >= 500) break; // hard server error: stop trying reader
          continue;                     // try next candidate on 4xx
        }

        if (ctype.includes('application/json')) {
          const j = await res.json();
          let text = pickText(j);
          if (typeof text === 'string') text = text.replace(/^\uFEFF/, '').trim();
          if (text && !/<!doctype html>|<html[\s>]/i.test(text)) {
            return text.length > maxLen ? text.slice(0, maxLen) : text;
          }
          // JSON may reference a PDF URL → OCR via DI (URL first, then authenticated bytes)
          const pdfUrl = pickPdfUrl(j, encNum);
          logProbe('reader.json.pdfUrl', { hasPdfUrl: !!pdfUrl, pdfUrl });
          if (pdfUrl) {
            const byUrl = await azureReadTextFromUrl(pdfUrl, { pagesFirst });
            logProbe('reader.json.ocr.url.done', { got: !!byUrl, len: (byUrl || '').length });
            if (byUrl) return byUrl.length > maxLen ? byUrl.slice(0, maxLen) : byUrl;

            const pdfBuf = await chDownloadPdfBuffer(pdfUrl);
            logProbe('reader.json.pdf.download', { ok: !!pdfBuf, len: pdfBuf ? pdfBuf.length : 0 });
            if (pdfBuf && pdfBuf.length) {
              const byBuf = await azureReadTextFromBuffer(pdfBuf);
              logProbe('reader.json.ocr.buf.done', { got: !!byBuf, len: (byBuf || '').length });
              if (byBuf) return byBuf.length > maxLen ? byBuf.slice(0, maxLen) : byBuf;
            }
          }
          continue;
        }

        if (ctype.includes('application/pdf')) {
          const byUrl = await azureReadTextFromUrl(url, { pagesFirst });
          logProbe('reader.pdf.ocr.url.done', { got: !!byUrl, len: (byUrl || '').length });
          if (byUrl) return byUrl.length > maxLen ? byUrl.slice(0, maxLen) : byUrl;

          const pdfBuf = await chDownloadPdfBuffer(url);
          logProbe('reader.pdf.download', { ok: !!pdfBuf, len: pdfBuf ? pdfBuf.length : 0 });
          if (pdfBuf && pdfBuf.length) {
            const byBuf = await azureReadTextFromBuffer(pdfBuf);
            logProbe('reader.pdf.ocr.buf.done', { got: !!byBuf, len: (byBuf || '').length });
            if (byBuf) return byBuf.length > maxLen ? byBuf.slice(0, maxLen) : byBuf;
          }
          continue;
        }

        // Treat as text (text/plain, text/markdown, etc.)
        let text = await res.text();
        if (typeof text !== 'string') continue;
        text = text.replace(/^\uFEFF/, '').trim();
        if (!text) continue;
        if (/<!doctype html>|<html[\s>]/i.test(text)) continue;
        return text.length > maxLen ? text.slice(0, maxLen) : text;

      } catch {
        continue; // network/parse error → next candidate
      }
    }

    // (2) Reader gave nothing → Companies House fallback (filing history → PDF → OCR)
    logProbe('ch.fh.lookup', { haveKey: !!process.env.CH_API_KEY, num: companyNumber });
    const pdfUrl = await chFindAccountsPdfUrl(normalizeCompanyNumber(companyNumber));
    logProbe('ch.fh.pdfUrl', { ok: !!pdfUrl, pdfUrl });
    if (pdfUrl) {
      const byUrl = await azureReadTextFromUrl(pdfUrl, { pagesFirst });
      logProbe('ch.ocr.url', { got: !!byUrl, len: (byUrl || '').length });
      if (byUrl) return byUrl.length > maxLen ? byUrl.slice(0, maxLen) : byUrl;

      const pdfBuf = await chDownloadPdfBuffer(pdfUrl);
      logProbe('ch.pdf.download', { ok: !!pdfBuf, len: pdfBuf ? pdfBuf.length : 0 });
      if (pdfBuf && pdfBuf.length) {
        const byBuf = await azureReadTextFromBuffer(pdfBuf);
        logProbe('ch.ocr.buf', { got: !!byBuf, len: (byBuf || '').length });
        if (byBuf) return byBuf.length > maxLen ? byBuf.slice(0, maxLen) : byBuf;
      }
    }

    logProbe('end', { returned: false, len: 0 });
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

function textMatchesEvidence(text, term) {
  if (!text || !term) return false;
  const norm = (s) => String(s)
    .normalize('NFKD')                // split accents
    .replace(/[\u0300-\u036f]/g, '')  // drop diacritics
    .replace(/[\u00A0\u2007\u202F]/g, ' ') // NBSPs -> space
    .replace(/[‐-‒–—]/g, '-')         // dash variants -> '-'
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
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

// REPLACE the whole validateRequired() with alias-aware version
function validateRequired(headers) {
  // Accept common aliases like "Name" / "Number" etc.
  const missing = [];
  const nameKey = findHeaderWithAliases(headers, 'Company Name');
  const numKey = findHeaderWithAliases(headers, 'Company Number');
  if (!nameKey) missing.push('Company Name');
  if (!numKey) missing.push('Company Number');
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
  return String(s || '')
    .replace(/^\uFEFF/, '')
    .replace(/[\u00A0\u2007\u202F]/g, ' ') // NBSPs to space
    .trim();
}

function normalizeHeaderList(headers) {
  return Array.isArray(headers) ? headers.map(stripBOMTrim) : [];
}

// REPLACE resolveCompanyKeysFromBuffer to use parseCsvFlexibleAuto
function resolveCompanyKeysFromBuffer(buffer) {
  const { headers } = parseCsvFlexibleAuto(buffer);
  const cleanHeaders = Array.isArray(headers)
    ? headers.map(h => String(h).replace(/^\uFEFF/, '').trim())
    : [];
  const nameKey = findHeaderWithAliases(cleanHeaders, 'Company Name');
  const numKey = findHeaderWithAliases(cleanHeaders, 'Company Number');
  return { headers: cleanHeaders, nameKey, numKey };
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

// REPLACE summarizeCsv with SR-aware matching + safe fallbacks (SR → full filing → company name)
// REPLACE summarizeCsv with SR-aware matching, robust fallbacks, and debug logs
async function summarizeCsv(buffer, opts = {}) {
  const evidenceTag = (opts?.evidenceTag ?? '').trim();
  const evidenceTerms = Array.isArray(opts?.evidenceTerms) ? opts.evidenceTerms : parseEvidenceList(evidenceTag);
  const DEBUG = process.env.CH_DEBUG === '1';

  // Parse CSV (auto delimiter) and resolve keys once
  const { recs } = parseCsvFlexibleAuto(buffer);
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
    const numRaw = String(numKey ? r[numKey] : '').trim();
    const num = normalizeCompanyNumber(numRaw);

    if (!name || !num) {
      skipped += 1;
      const reason = !name && !num ? 'missing_both'
        : (!num ? 'missing_company_number' : 'missing_company_name');
      errorsByReason[reason] = (errorsByReason[reason] || 0) + 1;
      continue;
    }

    // Fetch full text from reader
    let fullText = '';
    fullText = await tryGetReportText(num) || '';
    if (process.env.CH_DEBUG === '1') {
      // eslint-disable-next-line no-console
      console.log('reader', { numRaw: numRaw, numNorm: num, got: !!fullText, len: fullText ? fullText.length : 0 });
    }
    const srOnly = extractStrategicReport(fullText);
    const searchText = (srOnly && srOnly.trim())
      ? srOnly
      : (fullText && fullText.trim())
        ? fullText
        : name;

    if (DEBUG) {
      const meta = {
        num,
        haveFull: !!fullText,
        fullLen: fullText ? fullText.length : 0,
        haveSR: !!srOnly,
        srLen: srOnly ? srOnly.length : 0,
        searchLen: searchText.length
      };
      // eslint-disable-next-line no-console
      console.log('summarizeCsv:body', meta);
    }

    const { hit, term } = evidenceAnyWithTerm(
      searchText,
      (evidenceTerms.length ? evidenceTerms : evidenceTag)
    );

    if (hit) {
      matched += 1;
      const snippetSource = (srOnly && srOnly.trim()) ? srOnly : (fullText && fullText.trim()) ? fullText : name;
      const snippet = sentenceSnippet(snippetSource, term || evidenceTag);
      const m = { companyNumber: num, companyName: name, evidence: term || evidenceTag, snippet };
      matches.push(m);
      if (itemsSample.length < 10) itemsSample.push(m);
    } else {
      skipped += 1;
      const reason = (fullText || srOnly) ? 'no_evidence_match_in_text' : 'no_text_available';
      errorsByReason[reason] = (errorsByReason[reason] || 0) + 1;

      if (DEBUG) {
        // eslint-disable-next-line no-console
        console.log('summarizeCsv:nohit', { num, name, terms: evidenceTerms.length ? evidenceTerms : [evidenceTag] });
      }
    }
  }

  return { rows, matched, skipped, errorsByReason, headers, itemsSample, matches };
}

async function buildOutputCsv(buffer, evidenceTag) {
  const { recs } = parseCsvFlexible(buffer);
  const parsed = resolveCompanyKeysFromBuffer(buffer);
  const headers = parsed.headers;
  const nameKey = parsed.nameKey;
  const numKey = parsed.numKey;
  const rows = [];
  for (const r of recs) {
    const name = String(nameKey ? r[nameKey] : '').trim();
    const numRaw = String(numKey ? r[numKey] : '').trim();
    const num = normalizeCompanyNumber(numRaw);
    if (name && num) {
      rows.push(`${num},${csvEscape(name)},${csvEscape(evidenceTag || '')}`);
    }
  }
  const header = 'Company Number,Company Name,Evidence';
  return Buffer.from(`${header}\n${rows.join('\n')}\n`, 'utf8');
}


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
// ROUTE + CORS HELPERS (drop-in)
function normalizePath(context) {
  // Works in Azure Functions (Node 20). Strips origin + query.
  const url = context?.req?.url || '';
  const p = url.replace(/^https?:\/\/[^/]+/i, '').split('?')[0];
  // Ensure no trailing slash duplication issues
  return p.replace(/\/{2,}/g, '/').replace(/\/+$/, (m) => (m.length > 1 ? '/' : ''));
}

function preflight(req) {
  return (req.method || '').toUpperCase() === 'OPTIONS';
}

// Exact routes this single function handles:
function isSmallRun(method, path) {
  return method === 'POST' && /^\/api\/ch-strategic\/?$/i.test(path);
}
function isStart(method, path) {
  return method === 'POST' && /^\/api\/ch-strategic\/start\/?$/i.test(path);
}
function isStatus(method, path) {
  return method === 'GET' && /^\/api\/ch-strategic\/status\/[A-Za-z0-9_-]+\/?$/i.test(path);
}
function isDownload(method, path) {
  return method === 'GET' && /^\/api\/ch-strategic\/download\/[A-Za-z0-9_-]+\/?$/i.test(path);
}
function isHealth(path) {
  // keep whatever your health path is; this matches /api/ch-strategic/health if you have it,
  // or let your existing check stay (this keeps compatibility with your earlier code)
  return /health$/i.test(path);
}
function isFeedback(method, path) {
  return method === 'POST' && /^\/api\/ch-strategic\/feedback\/?$/i.test(path);
}

// DEBUG? ADD: Ensure CSV buffer is UTF-8; auto-convert UTF-16LE if detected
function ensureUtf8Buffer(buf) {
  if (!Buffer.isBuffer(buf) || buf.length < 2) return buf;
  const b0 = buf[0], b1 = buf[1];
  // UTF-16LE BOM 0xFFFE or heuristic (lots of 0x00 in header area)
  const looksUtf16 = (b0 === 0xFF && b1 === 0xFE) || (buf.slice(0, 64).some((x, i) => i % 2 === 1 && x === 0x00));
  if (!looksUtf16) return buf;
  const dec = new TextDecoder('utf-16le');
  const s = dec.decode(buf);
  return Buffer.from(s, 'utf8');
}

// ADD: tiny delimiter autodetect for first non-empty line
function detectDelimiter(sampleLine) {
  if (!sampleLine) return ',';
  const counts = [
    [',', (sampleLine.match(/,/g) || []).length],
    [';', (sampleLine.match(/;/g) || []).length],
    ['\t', (sampleLine.match(/\t/g) || []).length],
  ].sort((a, b) => b[1] - a[1]);
  return counts[0][1] > 0 ? counts[0][0] : ',';
}

// ADD: parse CSV flexibly with auto delimiter (returns { recs, headers })
function parseCsvFlexibleAuto(buffer) {
  const s = Buffer.isBuffer(buffer) ? buffer.toString('utf8') : String(buffer || '');
  const lines = s.split(/\r?\n/).filter(l => l.trim().length > 0);
  const delim = detectDelimiter(lines[0] || '');
  const { parse } = require('csv-parse/sync');
  const recs = parse(s, {
    columns: true,
    delimiter: delim,
    bom: true,
    skip_empty_lines: true,
    trim: true
  });
  const headers = recs.length
    ? Object.keys(recs[0])
    : (lines[0] ? lines[0].split(delim).map(h => h.replace(/^\uFEFF/, '').trim()) : []);
  return { recs, headers };
}

// ADD: normalize whitespace + heading-friendly lowercasing
function _canon(s) { return String(s || '').replace(/\s+/g, ' ').trim(); }
function _lower(s) { return _canon(s).toLowerCase(); }

// ADD: extract only the "Strategic Report" section from a full filing text
// REPLACE: robust Strategic Report extractor
function extractStrategicReport(fullText) {
  if (!fullText) return '';
  const text = String(fullText).replace(/\r/g, '\n'); // normalize newlines
  const lc = text.toLowerCase();

  // Headings: allow hyphen/en-dash variations and optional "the"
  const startPatterns = [
    /\bthe?\s*strategic[\s\-–—]*report\b/i,
    /\bstrategic[\s\-–—]*report\b/i
  ];
  // Common section starts that typically FOLLOW Strategic Report
  const endPatterns = [
    /\bdirectors[\s’']?\s*report\b/i,
    /\bindependent\s+auditor/i,
    /\bcorporate\s+governance/i,
    /\bgovernance\s+statement\b/i,
    /\bremuneration\s+report\b/i,
    /\bstatement\s+of\s+directors/i,
    /\bprofit\s+and\s+loss\b/i,
    /\bbalance\s+sheet\b/i,
    /\bconsolidated\s+statement\b/i
  ];

  // Find first start
  let start = -1;
  for (const re of startPatterns) {
    const m = lc.match(re);
    if (m && (start === -1 || m.index < start)) start = m.index;
  }
  if (start === -1) return '';

  // Find earliest valid end AFTER start
  let end = text.length;
  for (const re of endPatterns) {
    const m = lc.slice(start + 16).match(re);
    if (m) {
      const idx = (start + 16) + m.index;
      if (idx < end) end = idx;
    }
  }

  let section = text.slice(start, end);

  // Remove the heading line to keep snippets clean
  section = section.replace(/^\s*the?\s*strategic[\s\-–—]*report[^\n]*\n?/i, '');

  // Collapse multi-spaces / page artifacts
  return section.replace(/\s+/g, ' ').trim();
}

// ADD: sentence-friendly snippet around the matched term (not just raw slice)
function sentenceSnippet(text, term, maxChars = 250) {
  if (!text || !term) return '';
  const hay = text;
  const idx = hay.toLowerCase().indexOf(String(term).toLowerCase());
  if (idx < 0) return '';

  // Expand to sentence boundaries (., !, ?, newline) within reasonable window
  const win = 600; // guardrail for pathological files
  const from = Math.max(0, idx - win);
  const to = Math.min(hay.length, idx + term.length + win);
  const windowText = hay.slice(from, to);

  // Find nearest sentence bounds inside the window
  const rel = idx - from;
  const left = windowText.slice(0, rel).lastIndexOf('. ');
  const right = windowText.slice(rel).indexOf('. ');
  let snippet = windowText.slice(left >= 0 ? left + 2 : 0, right >= 0 ? rel + right + 2 : windowText.length);

  snippet = _canon(snippet);
  if (snippet.length > maxChars) {
    // Center the term within maxChars
    const half = Math.floor(maxChars / 2);
    const cutFrom = Math.max(0, rel - half);
    const cutTo = Math.min(windowText.length, rel + (maxChars - (rel - cutFrom)));
    snippet = _canon(windowText.slice(cutFrom, cutTo));
    if (cutFrom > 0) snippet = '… ' + snippet;
    if (cutTo < windowText.length) snippet = snippet + ' …';
  }
  return snippet;
}

// ADD: convenience wrapper to fetch full text and return just the Strategic Report body
async function getStrategicReportText(companyNumber) {
  const full = await tryGetReportText(companyNumber); // your reader endpoint
  const sr = extractStrategicReport(full);
  return sr || full || ''; // prefer SR; fall back to whole filing if SR missing
}

// ADD: Azure Document Intelligence (Read) OCR — URL source only
// Env: AZ_DI_ENDPOINT (e.g. https://<resourcename>.cognitiveservices.azure.com)
//      AZ_DI_KEY
// Notes: Uses Read (prebuilt-read) to OCR image-only PDFs.
async function azureReadTextFromUrl(
  pdfUrl,
  { timeoutMs = 45000, pollMs = 1200, maxLen = 2_000_000, pagesFirst = null } = {}
) {
  const endpoint = (process.env.AZ_DI_ENDPOINT || '').replace(/\/+$/, '');
  const key = process.env.AZ_DI_KEY || '';
  if (!endpoint || !key || !pdfUrl) return '';

  const analyzeUrl = `${endpoint}/documentintelligence/documentModels/prebuilt-read:analyze?api-version=2024-07-31`;

  // If you only want the first N pages, pass ["1","2",...]
  const body = { urlSource: pdfUrl };
  if (pagesFirst && Number(pagesFirst) > 0) {
    const n = Math.max(1, Math.min(1000, Number(pagesFirst)));
    body.pages = Array.from({ length: n }, (_, i) => String(i + 1));
  }

  const res = await fetch(analyzeUrl, {
    method: 'POST',
    headers: {
      'Ocp-Apim-Subscription-Key': key,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });
  if (!res.ok) return '';

  const opLoc = res.headers.get('operation-location');
  if (!opLoc) return '';

  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    await new Promise(r => setTimeout(r, pollMs));
    const s = await fetch(opLoc, { headers: { 'Ocp-Apim-Subscription-Key': key } });
    if (!s.ok) return '';
    const j = await s.json();
    const status = (j.status || '').toLowerCase();
    if (status === 'succeeded') {
      let txt = (j.analyzeResult && j.analyzeResult.content) || '';
      if (!txt) {
        const pages = (j.analyzeResult && j.analyzeResult.pages) || [];
        txt = pages.map(p => p.content || '').join('\n');
      }
      txt = (txt || '').replace(/^\uFEFF/, '').trim();
      if (txt.length > maxLen) txt = txt.slice(0, maxLen);
      return txt;
    }
    if (status === 'failed' || status === 'error') return '';
  }
  return '';
}

// ADD: extract a PDF URL from various reader payload shapes or env template
// Env optional: CH_PDF_URL_TEMPLATE e.g. "https://example/{companyNumber}.pdf"
function pickPdfUrl(j, companyNumber) {
  const cand = [
    j && j.pdfUrl,
    j && j.url,
    j && j.pdf && j.pdf.url,
    process.env.CH_PDF_URL_TEMPLATE ? String(process.env.CH_PDF_URL_TEMPLATE).replace('{companyNumber}', companyNumber) : ''
  ].filter(Boolean);
  // Basic sanity: ends with .pdf or has content-type hint elsewhere
  const u = cand.find(u => /\.pdf(\?|#|$)/i.test(String(u)));
  return u || '';
}

// ADD: Normalise Companies House numbers (fix lost leading zeros etc.)
function normalizeCompanyNumber(raw) {
  let s = String(raw || '').toUpperCase().trim().replace(/\s+/g, '');
  if (!s) return '';
  // Pure digits → left-pad to 8 (CH common)
  if (/^\d+$/.test(s)) {
    if (s.length < 8) s = s.padStart(8, '0');
    return s;
  }
  // Alpha + digits (e.g., SC, NI, OC, LP…) → keep prefix, pad numeric tail to 6 where applicable
  const m = s.match(/^([A-Z]{1,3})(\d{1,7})$/);
  if (m) {
    const prefix = m[1];
    let tail = m[2];
    // Most prefixed series use 6 digits; keep a safe pad without truncation
    if (tail.length < 6) tail = tail.padStart(6, '0');
    return prefix + tail;
  }
  return s;
}

// ADD: Companies House API fallback (if your reader returns nothing)
// Env:
//   CH_API_KEY                 → Companies House API key
//   CH_API_BASE (optional)     → default "https://api.company-information.service.gov.uk"
//   CH_DOC_API_BASE (optional) → default "https://document-api.company-information.service.gov.uk"
function chAuthHeader() {
  const key = process.env.CH_API_KEY || '';
  if (!key) return null;
  // CH uses HTTP Basic with API key as username, blank password
  const token = Buffer.from(`${key}:`).toString('base64');
  return { Authorization: `Basic ${token}` };
}

async function chFetchJson(url) {
  const hdr = chAuthHeader();
  if (!hdr) return null;
  const res = await fetch(url, { headers: { ...hdr, 'Accept': 'application/json' } });
  if (!res.ok) return null;
  return res.json();
}

// Find a likely Accounts filing and resolve to a PDF download URL via the Document API
async function chFindAccountsPdfUrl(companyNumber) {
  const base = (process.env.CH_API_BASE || 'https://api.company-information.service.gov.uk').replace(/\/+$/, '');
  const docBase = (process.env.CH_DOC_API_BASE || 'https://document-api.company-information.service.gov.uk').replace(/\/+$/, '');
  const num = normalizeCompanyNumber(companyNumber);
  // Get recent filing history; increase items_per_page if needed
  const fh = await chFetchJson(`${base}/company/${encodeURIComponent(num)}/filing-history?items_per_page=100`);
  if (!fh || !Array.isArray(fh.items)) return null;

  // Heuristic: pick the newest item in category "accounts" that has a document link
  const candidate = fh.items.find(it =>
    it &&
    (it.category === 'accounts' || /accounts/i.test(it.category || '')) &&
    it.links && it.links.document_metadata
  );
  if (!candidate) return null;

  // Fetch document metadata → find PDF link
  const metaUrl = `${docBase}${candidate.links.document_metadata}`;
  const hdr = chAuthHeader();
  const mRes = await fetch(metaUrl, { headers: { ...hdr, 'Accept': 'application/json' } });
  if (!mRes.ok) return null;
  const meta = await mRes.json();

  // Newer doc API payloads expose "links.document" (direct), or "resources.application/pdf.href"
  const direct = meta?.links?.document || meta?.links?.self; // sometimes "document"
  const resPdf = meta?.resources && (meta.resources['application/pdf'] || meta.resources['application/pdf; charset=utf-8']);
  const href = (resPdf && (resPdf.url || resPdf.href)) || direct;
  if (!href) return null;

  // If the href is relative (starts with /document/...), prefix with docBase
  const pdfUrl = /^https?:\/\//i.test(href) ? href : `${docBase}${href}`;
  return pdfUrl;
}
// ADD: Download a Companies House PDF (authenticated) and return a Buffer
async function chDownloadPdfBuffer(pdfUrl) {
  if (!pdfUrl) return null;
  const hdr = chAuthHeader();
  if (!hdr) return null;
  const res = await fetch(pdfUrl, {
    method: 'GET',
    headers: { ...hdr, 'Accept': 'application/pdf' }
  });
  if (!res.ok) return null;
  const ab = await res.arrayBuffer();
  return Buffer.from(ab);
}

// ADD: Azure Document Intelligence (Read) OCR — BUFFER source (no public URL required)
async function azureReadTextFromBuffer(
  pdfBuffer,
  { timeoutMs = 45000, pollMs = 1200, maxLen = 2_000_000 } = {}
) {
  const endpoint = (process.env.AZ_DI_ENDPOINT || '').replace(/\/+$/, '');
  const key = process.env.AZ_DI_KEY || '';
  if (!endpoint || !key || !pdfBuffer || !pdfBuffer.length) return '';

  const analyzeUrl = `${endpoint}/documentintelligence/documentModels/prebuilt-read:analyze?api-version=2024-07-31`;

  // Start analyze with binary body
  const res = await fetch(analyzeUrl, {
    method: 'POST',
    headers: {
      'Ocp-Apim-Subscription-Key': key,
      'Content-Type': 'application/pdf'
    },
    body: pdfBuffer
  });
  if (!res.ok) return '';

  const opLoc = res.headers.get('operation-location');
  if (!opLoc) return '';

  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    await new Promise(r => setTimeout(r, pollMs));
    const s = await fetch(opLoc, { headers: { 'Ocp-Apim-Subscription-Key': key } });
    if (!s.ok) return '';
    const j = await s.json();
    const status = (j.status || '').toLowerCase();
    if (status === 'succeeded') {
      let txt = (j.analyzeResult && j.analyzeResult.content) || '';
      if (!txt) {
        const pages = (j.analyzeResult && j.analyzeResult.pages) || [];
        txt = pages.map(p => p.content || '').join('\n');
      }
      txt = (txt || '').replace(/^\uFEFF/, '').trim();
      if (txt.length > maxLen) txt = txt.slice(0, maxLen);
      return txt;
    }
    if (status === 'failed' || status === 'error') return '';
  }
  return '';
}
// ADD: one-shot probe logger so we can see exactly which path returned (or failed)
function logProbe(stage, data) {
  if (process.env.CH_DEBUG !== '1') return;
  try {
    // eslint-disable-next-line no-console
    console.log(`CH_PROBE:${stage}`, JSON.stringify(data));
  } catch { }
}
// ADD: minimal App Insights boot (safe: only starts if a key/conn string exists)
let _ai = null;
function initAI() {
  if (_ai) return _ai;
  const cs = process.env.APPLICATIONINSIGHTS_CONNECTION_STRING;
  const ik = process.env.APPINSIGHTS_INSTRUMENTATIONKEY;
  try {
    if (cs || ik) {
      const appInsights = require('applicationinsights');
      if (!_ai) {
        if (cs) process.env.APPLICATIONINSIGHTS_CONNECTION_STRING = cs;
        if (ik && !process.env.APPINSIGHTS_INSTRUMENTATIONKEY) process.env.APPINSIGHTS_INSTRUMENTATIONKEY = ik;
        appInsights.setup()
          .setAutoCollectRequests(true)
          .setAutoCollectDependencies(true)
          .setAutoCollectExceptions(true)
          .setSendLiveMetrics(false)
          .start();
        _ai = appInsights.defaultClient;
      }
    }
  } catch (_) { /* ignore */ }
  return _ai;
}
function aiTrack(name, props) {
  const c = initAI();
  if (!c) return;
  try { c.trackEvent({ name, properties: props || {} }); } catch (_) { }
}
function logProbe(stage, data) {
  if (process.env.CH_DEBUG !== '1') return;
  const payload = { stage, ...(data || {}) };
  try { console.log(`CH_PROBE ${JSON.stringify(payload)}`); } catch (_) { }
  aiTrack(`CH_PROBE.${stage}`, payload);
}

// ADD: quick env fingerprint to confirm which app/slot handled the request
function envFingerprint() {
  return {
    WEBSITE_SITE_NAME: process.env.WEBSITE_SITE_NAME || null,
    WEBSITE_HOSTNAME: process.env.WEBSITE_HOSTNAME || null,
    FUNCTIONS_EXTENSION_VERSION: process.env.FUNCTIONS_EXTENSION_VERSION || null,
    REGION_NAME: process.env.REGION_NAME || null,
    AI_conn: !!process.env.APPLICATIONINSIGHTS_CONNECTION_STRING,
    AI_key: !!process.env.APPINSIGHTS_INSTRUMENTATIONKEY,
    CH_DEBUG: process.env.CH_DEBUG || null
  };
}

// ADD: diag route helper
function isDiag(method, path) {
  return method === 'GET' && /^\/api\/ch-strategic\/diag\/[A-Za-z0-9]+\/?$/i.test(path);
}

// ------------------------------- Azure Function entry -------------------------------
module.exports = async function (context, req) {
  try {
    const cid = getCid(req);
    const method = (req.method || 'GET').toUpperCase();
    const path = normalizePath(context);
    aiTrack('ch-strategic.entry', { cid, method, path, ...envFingerprint() });
    // DEBUG ADD: routing/build fingerprints
    context.log?.info?.(
      { cid, method, path, build: process.env.CH_STRATEGIC_VERSION || 'dev', fp: 'router-hit-v1' },
      'router-hit'
    );
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

    // ADD: GET /api/ch-strategic/diag/:num  → shows which app handled it + sample text
    if (isDiag(method, path)) {
      const m = path.match(/\/diag\/([A-Za-z0-9]+)\/?$/i);
      const numRaw = m ? m[1] : '';
      const num = normalizeCompanyNumber(numRaw);
      const t0 = Date.now();
      const full = await tryGetReportText(num);
      const sr = extractStrategicReport(full || '');
      aiTrack('ch-strategic.diag', {
        cid, numRaw, numNorm: num,
        haveFull: !!full, fullLen: (full || '').length,
        haveSR: !!sr, srLen: (sr || '').length
      });
      context.res = ok(200, {
        ok: true,
        cid,
        env: envFingerprint(),
        numRaw, numNorm: num,
        haveFull: !!full, fullLen: (full || '').length,
        haveSR: !!sr, srLen: (sr || '').length,
        sample: (sr && sr.trim() ? sr : (full || '')).slice(0, 300),
        ms: Date.now() - t0
      }, cid);
      return;
    }

    // Small run (no storage) → POST /api/ch-strategic
    if (isSmallRun(method, path)) {
      const { file, fields, error } = await readMultipart(req);
      if (error) { context.res = err(415, error, cid); return; }
      if (!file?.buffer?.length) { context.res = err(400, 'Missing file', cid); return; }
      const bufUtf8 = ensureUtf8Buffer(file.buffer);

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
        parsed = resolveCompanyKeysFromBuffer(bufUtf8);
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
        // 9) Deep header diagnostics before returning
        const headerCodes = Array.isArray(headers)
          ? headers.map(h => Array.from(String(h)).map(ch => ch.charCodeAt(0)))
          : [];
        context.log?.info?.({ cid, headers, headerCodes }, 'start-headers-deep');

        return json400(context, {
          ok: false,
          code: 400,
          message: "Missing required column(s): Company Name, Company Number",
          headers
        });
      }

      // Run analysis (pass evidence to enable multi-term matching)
      const summary = await summarizeCsv(bufUtf8, { evidenceTag, evidenceTerms });

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
      const bufUtf8 = ensureUtf8Buffer(file.buffer);

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

      // START (large run) header validation
      let parsed;
      try {
        parsed = resolveCompanyKeysFromBuffer(bufUtf8);
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
        // 9) Deep header diagnostics before returning
        const headerCodes = Array.isArray(headers)
          ? headers.map(h => Array.from(String(h)).map(ch => ch.charCodeAt(0)))
          : [];
        context.log?.info?.({ cid, headers, headerCodes }, 'start-headers-deep');

        return json400(context, {
          ok: false,
          code: 400,
          message: "Missing required column(s): Company Name, Company Number",
          headers
        });
      }

      context.log?.info?.({ cid, headers, nameKey, numKey }, 'start-key-resolution');

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
      const analysis = await summarizeCsv(bufUtf8, { evidenceTag, evidenceTerms });

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

    // STATUS
    if (isStatus(method, path)) {
      if (!AZ_CONN) { context.res = err(500, 'Storage not configured', cid); return; }
      const m = path.match(/\/status\/([A-Za-z0-9_-]+)\/?$/i);
      const id = m ? m[1] : '';
      if (!id) { context.res = err(400, 'Bad status path', cid); return; }
      const status = await getJson(CONTAINERS.status, `${id}.json`);
      if (!status) { context.res = err(404, 'Not found', cid); return; }
      context.res = ok(200, status, cid);
      return;
    }

    // DOWNLOAD
    if (isDownload(method, path)) {
      if (!AZ_CONN) { context.res = err(500, 'Storage not configured', cid); return; }
      const m = path.match(/\/download\/([A-Za-z0-9_-]+)\/?$/i);
      const id = m ? m[1] : '';
      if (!id) { context.res = err(400, 'Bad download path', cid); return; }
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
