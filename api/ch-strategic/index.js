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

// ------------------------------- CSV helpers (sync; no Node streams) -------------------------------
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

async function summarizeCsv(buffer) {
  const { recs, headers } = parseCsvFlexible(buffer);

  // Resolve actual column keys robustly
  const nameKey = findHeader(headers, 'Company Name');
  const numKey = findHeader(headers, 'Company Number');

  let rows = 0, matched = 0, skipped = 0;
  const errorsByReason = {};
  const itemsSample = [];

  for (const r of recs) {
    rows += 1;
    const name = String(nameKey ? r[nameKey] : '').trim();
    const num = String(numKey ? r[numKey] : '').trim();

    if (name && num) {
      matched += 1;
      if (itemsSample.length < 10) itemsSample.push({ companyNumber: num, companyName: name });
    } else {
      skipped += 1;
      const reason = !name && !num ? 'missing_both'
        : (!num ? 'missing_company_number' : 'missing_company_name');
      errorsByReason[reason] = (errorsByReason[reason] || 0) + 1;
    }
  }

  return { rows, matched, skipped, errorsByReason, headers, itemsSample };
}

async function buildOutputCsv(buffer, evidenceTag) {
  const { recs, headers } = parseCsvFlexible(buffer);
  const nameKey = findHeader(headers, 'Company Name');
  const numKey = findHeader(headers, 'Company Number');

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

// ------------------------------- Multipart (Busboy v1.6.0, Azure Functions classic req) -------------------------------
async function readMultipart(req) {
  // Azure Functions (v4, classic programming model) gives:
  //   - req.headers (must include Content-Type with boundary)
  //   - req.rawBody   (Buffer | string)  <-- we use this instead of req.pipe(...)
  // There is NO req.pipe here.
  const ct = req.headers?.['content-type'] || '';
  if (!/^multipart\/form-data/i.test(ct)) return { error: 'Expected multipart/form-data' };

  // Ensure we have bytes to parse
  const hasRaw = req.rawBody != null;
  if (!hasRaw) return { error: 'Missing rawBody for multipart parsing' };

  const bodyBuf = Buffer.isBuffer(req.rawBody) ? req.rawBody : Buffer.from(req.rawBody);

  // NOTE: Busboy v1.x usage — call as a function (no `new`)
  const bb = require('busboy')({
    headers: { 'content-type': ct },             // make sure boundary is passed
    limits: {
      fileSize: Number(process.env.CH_STRATEGIC_UPLOAD_LIMIT_BYTES || 20 * 1024 * 1024), // 20MB default
      files: 1,
      fields: 10
    }
  });

  const fields = {};
  let file = null;
  let fileCount = 0;

  const p = new Promise((resolve, reject) => {
    bb.on('field', (name, val) => { fields[name] = val; });

    // v1 signature: (fieldname, fileStream, filename, encoding, mimetype)
    bb.on('file', (name, stream, filename, encoding, mimetype) => {
      if (++fileCount > 1) { stream.resume(); return; } // ignore extras defensively
      const chunks = [];
      stream.on('data', d => chunks.push(d));
      stream.on('end', () => {
        if (name === 'csv_file') {
          file = { buffer: Buffer.concat(chunks), filename, encoding, mimetype };
        }
      });
      stream.on('error', reject);
    });

    // Limits → convert to user-facing errors
    bb.on('partsLimit', () => reject(new Error('Too many parts')));
    bb.on('filesLimit', () => reject(new Error('Too many files')));
    bb.on('fieldsLimit', () => reject(new Error('Too many fields')));

    bb.on('error', reject);
    bb.on('finish', () => resolve({ fields, file }));   // v1 uses 'finish'
  });

  // IMPORTANT: In Functions classic, push the whole raw body into Busboy
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

    // Small run (no storage)
    if (isSmallRun(method, path)) {
      const { file, fields, error } = await readMultipart(req);
      if (error) { context.res = err(415, error, cid); return; }
      if (!file?.buffer?.length) { context.res = err(400, 'Missing file', cid); return; }

      const evidenceTag = (fields.evidenceTag || '').trim();
      if (evidenceTag && !/^[A-Za-z0-9 _-]{1,50}$/.test(evidenceTag)) { context.res = err(400, 'Invalid evidenceTag', cid); return; }

      const summary = await summarizeCsv(file.buffer);

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

    // Start (requires storage)
    if (isStart(method, path)) {
      if (!AZ_CONN) { context.res = err(500, 'Storage not configured', cid); return; }
      await ensureContainers();

      const { file, fields, error } = await readMultipart(req);
      if (error) { context.res = err(415, error, cid); return; }
      if (!file?.buffer?.length) { context.res = err(400, 'Missing file', cid); return; }

      const evidenceTag = (fields.evidenceTag || '').trim();
      if (evidenceTag && !/^[A-Za-z0-9 _-]{1,50}$/.test(evidenceTag)) { context.res = err(400, 'Invalid evidenceTag', cid); return; }

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

      // Process
      const analysis = await summarizeCsv(file.buffer);

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

      const outBuffer = await buildOutputCsv(file.buffer, evidenceTag);
      await putCsv(CONTAINERS.out, outName, outBuffer);

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

    // Status
    if (isStatus(method, path)) {
      const id = path.split('/').pop();
      if (!AZ_CONN) { context.res = err(500, 'Storage not configured', cid); return; }
      const status = await getJson(CONTAINERS.status, `${id}.json`);
      if (!status) { context.res = err(404, 'Not found', cid); return; }
      context.res = ok(200, status, cid);
      return;
    }

    // Download
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
    // Surface in Functions logs / App Insights if configured
    context.log?.error?.('ch-strategic fatal', e);
    context.res = err(500, e?.message || 'Internal error', cid);
  }
};
