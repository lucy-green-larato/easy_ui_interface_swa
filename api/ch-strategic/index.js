// api/ch-strategic/index.js
'use strict';

/**
 * CH Strategic – fully functional HTTP API for Azure Static Web Apps + Functions
 * -----------------------------------------------------------------------------
 * Endpoints (mounted under route "ch-strategic/{*path}" → /api/ch-strategic/*):
 *  - POST /ch-strategic                        : Small run (multipart: file + evidenceTag)
 *  - POST /ch-strategic/start                  : "Large" run (synchronous processing here)
 *  - GET  /ch-strategic/status/:id             : Read status JSON from blob
 *  - GET  /ch-strategic/download/:id           : Stream generated CSV
 *  - POST /ch-strategic/feedback               : Store feedback JSON
 *  - POST /ch-strategic/pbi-export             : Role-gated CSV export (optional)
 *
 * Storage containers (auto-created):
 *  - ch-strategic-input     (optional, not persisted here)
 *  - ch-strategic-out
 *  - ch-strategic-status
 *  - ch-strategic-feedback
 *
 * Env (optional):
 *  - AzureWebJobsStorage                : Storage connection (Functions default)
 *  - CH_STRATEGIC_UPLOAD_LIMIT_BYTES    : default 20MB
 *  - CH_STRATEGIC_JSON_LIMIT            : default "2mb"
 *  - CH_STRATEGIC_URLENC_LIMIT          : default "64kb"
 *  - CH_STRATEGIC_SMALL_MAX_ROWS        : default 5000
 *  - CH_STRATEGIC_PBI_ROLE              : default "pbi-exporter"
 *  - CH_STRATEGIC_VERSION               : arbitrary version string
 */

const { createHandler } = require('azure-function-express');
const express = require('express');
const multer = require('multer');
const { BlobServiceClient } = require('@azure/storage-blob');
const { parse } = require('csv-parse');
const { PassThrough } = require('stream');
const { webcrypto } = require('crypto');

const crypto = globalThis.crypto ?? webcrypto;

// ----------------------------------------------------------------------------
// Minimal HTTP helpers (self-contained; no external lib/http module)
// ----------------------------------------------------------------------------
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, content-type, x-ms-client-principal, x-correlation-id'
};

function uuid() {
  return crypto.randomUUID ? crypto.randomUUID()
    : ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, c =>
        (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16));
}

function preflight(req, res) {
  if (req.method !== 'OPTIONS') return false;
  res.set(CORS).status(204).end();
  return true;
}

function ok(res, body = {}, code = 200, cid) {
  const headers = { ...CORS, 'Content-Type': 'application/json' };
  if (cid) headers['X-Correlation-Id'] = cid;
  res.status(code).set(headers).end(JSON.stringify(body));
}

function err(res, e, code = 500, cid) {
  const headers = { ...CORS, 'Content-Type': 'application/json' };
  if (cid) headers['X-Correlation-Id'] = cid;
  const message = typeof e === 'string' ? e : e?.message || 'Internal error';
  res.status(code).set(headers).end(JSON.stringify({ ok: false, code, message }));
}

// ----------------------------------------------------------------------------
// Express app setup
// ----------------------------------------------------------------------------
const app = express();
app.disable('x-powered-by');
app.set('trust proxy', true);

app.use((req, res, next) => { if (preflight(req, res)) return; next(); });
app.use((req, res, next) => { Object.entries(CORS).forEach(([k, v]) => res.setHeader(k, v)); next(); });

app.use(express.json({ limit: process.env.CH_STRATEGIC_JSON_LIMIT || '2mb' }));
app.use(express.urlencoded({ extended: true, limit: process.env.CH_STRATEGIC_URLENC_LIMIT || '64kb' }));

app.use((req, res, next) => {
  const hdr = req.headers['x-correlation-id'];
  req.correlationId = (typeof hdr === 'string' && hdr.trim()) || uuid();
  res.setHeader('X-Correlation-Id', req.correlationId);
  next();
});

// ----------------------------------------------------------------------------
// SWA principal & role helpers
// ----------------------------------------------------------------------------
function getPrincipal(req) {
  try {
    const raw = req.headers['x-ms-client-principal'];
    if (!raw) return null;
    const json = Buffer.from(raw, 'base64').toString('utf8');
    return JSON.parse(json);
  } catch { return null; }
}

function requireRole(...roles) {
  const required = new Set(roles.filter(Boolean));
  return (req, res, next) => {
    if (required.size === 0) return next();
    const p = getPrincipal(req);
    const userRoles = Array.isArray(p?.userRoles) ? p.userRoles : [];
    const allowed = userRoles.some(r => required.has(r));
    if (!allowed) return err(res, 'Forbidden', 403, req.correlationId);
    next();
  };
}

// ----------------------------------------------------------------------------
// Multer (in-memory) for multipart uploads
// ----------------------------------------------------------------------------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: Number(process.env.CH_STRATEGIC_UPLOAD_LIMIT_BYTES || 20 * 1024 * 1024) }
});

// ----------------------------------------------------------------------------
// Azure Blob Storage helpers
// ----------------------------------------------------------------------------
const AZ_CONN = process.env.AzureWebJobsStorage;
if (!AZ_CONN) {
  // We do NOT throw at module load to allow local dev without storage for healthz.
  // Endpoints that need storage will error gracefully if missing.
}

const BLOB = AZ_CONN ? BlobServiceClient.fromConnectionString(AZ_CONN) : null;
const CONTAINERS = {
  input: 'ch-strategic-input',
  out: 'ch-strategic-out',
  status: 'ch-strategic-status',
  feedback: 'ch-strategic-feedback'
};

async function ensureContainers() {
  if (!BLOB) throw new Error('AzureWebJobsStorage is not configured');
  await Promise.all(Object.values(CONTAINERS).map(async name => {
    const c = BLOB.getContainerClient(name);
    await c.createIfNotExists();
  }));
}

function container(name) {
  if (!BLOB) throw new Error('AzureWebJobsStorage is not configured');
  return BLOB.getContainerClient(name);
}

async function putJson(containerName, blobName, obj) {
  const c = container(containerName);
  const block = c.getBlockBlobClient(blobName);
  const data = Buffer.from(JSON.stringify(obj, null, 2), 'utf8');
  await block.uploadData(data, {
    blobHTTPHeaders: { blobContentType: 'application/json; charset=utf-8' }
  });
}

async function getJson(containerName, blobName) {
  const c = container(containerName);
  const block = c.getBlockBlobClient(blobName);
  if (!(await block.exists())) return null;
  const dl = await block.download();
  const buf = await streamToBuffer(dl.readableStreamBody);
  return JSON.parse(buf.toString('utf8'));
}

async function putCsv(containerName, blobName, buffer) {
  const c = container(containerName);
  const block = c.getBlockBlobClient(blobName);
  await block.uploadData(buffer, {
    blobHTTPHeaders: { blobContentType: 'text/csv; charset=utf-8' }
  });
}

async function getCsvStream(containerName, blobName) {
  const c = container(containerName);
  const block = c.getBlockBlobClient(blobName);
  if (!(await block.exists())) return null;
  const dl = await block.download();
  return { stream: dl.readableStreamBody, size: dl.contentLength || undefined };
}

async function streamToBuffer(readable) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readable.on('data', (d) => chunks.push(d));
    readable.on('end', () => resolve(Buffer.concat(chunks)));
    readable.on('error', reject);
  });
}

// ----------------------------------------------------------------------------
// CSV parsing / processing helpers
// ----------------------------------------------------------------------------
const SMALL_MAX_ROWS = Number(process.env.CH_STRATEGIC_SMALL_MAX_ROWS || 5000);

// Count rows and check headers quickly (streaming)
async function analyzeCsv(buffer) {
  return new Promise((resolve, reject) => {
    let rows = 0;
    let headers = null;
    const errorsByReason = {};
    const parser = parse({
      columns: (hdr) => { headers = hdr.map(h => String(h || '').trim()); return true; },
      bom: true,
      relax_column_count: true,
      skip_empty_lines: true,
      trim: true
    });
    parser.on('readable', () => {
      let record;
      while ((record = parser.read()) !== null) {
        rows += 1;
      }
    });
    parser.on('error', reject);
    parser.on('end', () => {
      resolve({ rows, headers: headers || [], errorsByReason });
    });
    parser.write(buffer);
    parser.end();
  });
}

// Basic “business logic” for demo: a row is "matched" if it has both Company Number and Company Name
async function summarizeCsv(buffer) {
  return new Promise((resolve, reject) => {
    const headerSeen = new Set();
    let headers = null;
    let rows = 0, matched = 0, skipped = 0;
    const errorsByReason = {};
    const itemsSample = [];

    const parser = parse({
      columns: (hdr) => { headers = hdr.map(h => String(h || '').trim()); return true; },
      bom: true,
      relax_column_count: true,
      skip_empty_lines: true,
      trim: true
    });

    const colIndex = (name) => headers ? headers.findIndex(h => h.toLowerCase() === name.toLowerCase()) : -1;

    parser.on('readable', () => {
      let rec;
      while ((rec = parser.read()) !== null) {
        rows += 1;
        if (headers) headers.forEach(h => headerSeen.add(h));
        const nameIdx = colIndex('Company Name');
        const numIdx  = colIndex('Company Number');
        const hasName = nameIdx >= 0 && String(rec[nameIdx] ?? '').trim().length > 0;
        const hasNum  = numIdx  >= 0 && String(rec[numIdx]  ?? '').trim().length > 0;

        if (hasName && hasNum) {
          matched += 1;
          if (itemsSample.length < 10) {
            itemsSample.push({ companyNumber: String(rec[numIdx]).trim(), companyName: String(rec[nameIdx]).trim() });
          }
        } else {
          skipped += 1;
          const reason = !hasNum && !hasName ? 'missing_both' : (!hasNum ? 'missing_company_number' : 'missing_company_name');
          errorsByReason[reason] = (errorsByReason[reason] || 0) + 1;
        }
      }
    });

    parser.on('error', reject);
    parser.on('end', () => {
      resolve({
        rows, matched, skipped, errorsByReason,
        headers: [...headerSeen],
        itemsSample
      });
    });

    parser.write(buffer);
    parser.end();
  });
}

// Create an output CSV (for download) – here we output only matched rows with the 2 key columns
async function buildOutputCsv(buffer) {
  return new Promise((resolve, reject) => {
    let headers = null;
    const out = [];
    const parser = parse({
      columns: (hdr) => { headers = hdr.map(h => String(h || '').trim()); return true; },
      bom: true, relax_column_count: true, skip_empty_lines: true, trim: true
    });
    const colIndex = (name) => headers ? headers.findIndex(h => h.toLowerCase() === name.toLowerCase()) : -1;

    parser.on('readable', () => {
      let rec;
      while ((rec = parser.read()) !== null) {
        const nameIdx = colIndex('Company Name');
        const numIdx  = colIndex('Company Number');
        const hasName = nameIdx >= 0 && String(rec[nameIdx] ?? '').trim().length > 0;
        const hasNum  = numIdx  >= 0 && String(rec[numIdx]  ?? '').trim().length > 0;
        if (hasName && hasNum) {
          out.push(`${String(rec[numIdx]).trim()},${csvEscape(String(rec[nameIdx]).trim())}`);
        }
      }
    });

    parser.on('error', reject);
    parser.on('end', () => {
      const header = 'Company Number,Company Name';
      const body = out.join('\n');
      resolve(Buffer.from(`${header}\n${body}\n`, 'utf8'));
    });

    parser.write(buffer);
    parser.end();
  });
}

function csvEscape(s) {
  if (s.includes('"') || s.includes(',') || s.includes('\n') || s.includes('\r')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

// ----------------------------------------------------------------------------
// Health
// ----------------------------------------------------------------------------
app.get(['/healthz', '/ch-strategic/healthz'], async (req, res) => {
  const meta = {
    ok: true,
    name: 'ch-strategic',
    version: process.env.CH_STRATEGIC_VERSION || 'dev',
    node: process.version,
    site: process.env.WEBSITE_SITE_NAME || null,
    slot: process.env.WEBSITE_SLOT_NAME || null,
    commit: process.env.SCM_COMMIT_ID || process.env.COMMIT_SHA || null,
    storageConfigured: !!AZ_CONN,
    time: new Date().toISOString(),
    cid: req.correlationId
  };
  return ok(res, meta, 200, req.correlationId);
});

// ----------------------------------------------------------------------------
// Small run (multipart)
// ----------------------------------------------------------------------------
app.post(['/', '/ch-strategic', '/ch-strategic/'], upload.single('csv_file'), async (req, res) => {
  try {
    if (!req.file?.buffer) return err(res, 'Missing file', 400, req.correlationId);
    const evidenceTag = (req.body?.evidenceTag || '').trim();
    if (evidenceTag && !/^[A-Za-z0-9 _-]{1,50}$/.test(evidenceTag)) {
      return err(res, 'Invalid evidenceTag', 400, req.correlationId);
    }

    const summary = await summarizeCsv(req.file.buffer);
    const result = {
      ok: true,
      correlationId: req.correlationId,
      evidenceTag: evidenceTag || null,
      rows: summary.rows,
      matched: summary.matched,
      skipped: summary.skipped,
      errorsByReason: summary.errorsByReason,
      headers: summary.headers,
      itemsSample: summary.itemsSample
    };
    return ok(res, result, 200, req.correlationId);
  } catch (e) {
    return err(res, e, 400, req.correlationId);
  }
});

// ----------------------------------------------------------------------------
// "Large" run (processed synchronously here, with blob status/output)
// ----------------------------------------------------------------------------
app.post(['/start', '/ch-strategic/start'], upload.single('csv_file'), async (req, res) => {
  try {
    if (!req.file?.buffer) return err(res, 'Missing file', 400, req.correlationId);
    if (!AZ_CONN) return err(res, 'Storage not configured', 500, req.correlationId);
    await ensureContainers();

    const evidenceTag = (req.body?.evidenceTag || '').trim();
    if (evidenceTag && !/^[A-Za-z0-9 _-]{1,50}$/.test(evidenceTag)) {
      return err(res, 'Invalid evidenceTag', 400, req.correlationId);
    }

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

    // Analyze + build output
    const analysis = await summarizeCsv(req.file.buffer);
    const outputCsv = await buildOutputCsv(req.file.buffer);

    // Save CSV
    await putCsv(CONTAINERS.out, outName, outputCsv);

    // Final status
    await putJson(CONTAINERS.status, statusName, {
      state: 'done',
      jobId,
      startedAt: undefined,
      finishedAt: new Date().toISOString(),
      evidenceTag: evidenceTag || null,
      totalRows: analysis.rows,
      matched: analysis.matched,
      skipped: analysis.skipped,
      errorsByReason: analysis.errorsByReason,
      downloadUrl: `/api/ch-strategic/download/${jobId}`
    });

    return ok(res, {
      ok: true,
      jobId,
      statusUrl: `/api/ch-strategic/status/${jobId}`,
      downloadUrl: `/api/ch-strategic/download/${jobId}`,
      correlationId: req.correlationId
    }, 200, req.correlationId);
  } catch (e) {
    return err(res, e, 400, req.correlationId);
  }
});

// ----------------------------------------------------------------------------
// Status (reads from blob)
// ----------------------------------------------------------------------------
app.get(['/status/:id', '/ch-strategic/status/:id'], async (req, res) => {
  try {
    if (!AZ_CONN) return err(res, 'Storage not configured', 500, req.correlationId);
    const id = String(req.params.id || '').trim();
    if (!/^[a-f0-9-]{16,}$/.test(id)) return err(res, 'Bad id', 400, req.correlationId);
    const status = await getJson(CONTAINERS.status, `${id}.json`);
    if (!status) return err(res, 'Not found', 404, req.correlationId);
    return ok(res, status, 200, req.correlationId);
  } catch (e) {
    return err(res, e, 400, req.correlationId);
  }
});

// ----------------------------------------------------------------------------
// Download (streams CSV)
// ----------------------------------------------------------------------------
app.get(['/download/:id', '/ch-strategic/download/:id'], async (req, res) => {
  try {
    if (!AZ_CONN) return err(res, 'Storage not configured', 500, req.correlationId);
    const id = String(req.params.id || '').trim();
    if (!/^[a-f0-9-]{16,}$/.test(id)) return err(res, 'Bad id', 400, req.correlationId);
    const name = `${id}.csv`;
    const streamInfo = await getCsvStream(CONTAINERS.out, name);
    if (!streamInfo) return err(res, 'Not found', 404, req.correlationId);

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="ch-strategic-${id}.csv"`);
    res.setHeader('X-Correlation-Id', req.correlationId);
    if (streamInfo.size) res.setHeader('Content-Length', String(streamInfo.size));
    streamInfo.stream.pipe(res);
  } catch (e) {
    return err(res, e, 400, req.correlationId);
  }
});

// ----------------------------------------------------------------------------
// Feedback
// ----------------------------------------------------------------------------
app.post(['/feedback', '/ch-strategic/feedback'], async (req, res) => {
  try {
    if (!AZ_CONN) return err(res, 'Storage not configured', 500, req.correlationId);
    await ensureContainers();
    const p = getPrincipal(req);
    const body = Object(req.body || {});
    const payload = {
      receivedAt: new Date().toISOString(),
      correlationId: req.correlationId,
      principal: p ? { userId: p.userId, identityProvider: p.identityProvider, userDetails: p.userDetails, userRoles: p.userRoles } : null,
      up: !!body.up,
      details: (body.details || '').toString().slice(0, 2000)
    };
    await putJson(CONTAINERS.feedback, `${uuid()}.json`, payload);
    res.set(CORS).status(204).end();
  } catch (e) {
    return err(res, e, 400, req.correlationId);
  }
});

// ----------------------------------------------------------------------------
// Optional: PBI Export (role gated). Here we emit a trivial CSV from the payload.
// ----------------------------------------------------------------------------
app.post(['/pbi-export', '/ch-strategic/pbi-export'],
  requireRole(process.env.CH_STRATEGIC_PBI_ROLE || 'pbi-exporter'),
  async (req, res) => {
    try {
      const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
      const csv = toCsv(rows);
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('X-Correlation-Id', req.correlationId);
      res.status(200).end(csv);
    } catch (e) {
      return err(res, e, 400, req.correlationId);
    }
  }
);

// CSV serializer for PBI export
function toCsv(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return '';
  const headers = Array.from(new Set(rows.flatMap(r => Object.keys(r)))); // union of keys
  const headerLine = headers.map(csvEscape).join(',');
  const lines = rows.map(r => headers.map(h => csvEscape(r[h] == null ? '' : String(r[h]))).join(','));
  return `${headerLine}\n${lines.join('\n')}\n`;
}

// ----------------------------------------------------------------------------
// 404 and error guard
// ----------------------------------------------------------------------------
app.all('*', (req, res) => err(res, `Not found: ${req.method} ${req.path}`, 404, req.correlationId));

app.use((e, req, res, _next) => {
  const code = Number(e?.statusCode || e?.status || 500);
  return err(res, e, code, req?.correlationId || undefined);
});

// ----------------------------------------------------------------------------
// Azure Function handler export
// ----------------------------------------------------------------------------
module.exports = createHandler(app);
