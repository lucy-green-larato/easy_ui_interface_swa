// api/generate/kinds/ch-strategic.js
'use strict';

/**
 * CH Strategic – Feature Module
 * -----------------------------
 * Exports:
 *  - mount(app, { upload, requireRole, getPrincipal })
 *  - smallRun({ csvBuffer, evidence, cid })
 *  - startLarge({ csvBuffer, evidence, cid })
 *  - getStatus(jobId)
 *  - download(jobId) -> { stream, filename }
 *  - feedback(payload, principal, cid)
 *  - pbiExport(payload, { cid, principal })  // optional helper used by /pbi-export
 *
 * Storage containers (auto-created):
 *  - ch-strategic-input     (not used for persistence in this module, but reserved)
 *  - ch-strategic-out
 *  - ch-strategic-status
 *  - ch-strategic-feedback
 *
 * Env (optional):
 *  - AzureWebJobsStorage                : Storage connection (Functions default)
 *  - CH_STRATEGIC_SMALL_MAX_ROWS        : default 5000 (only applied by smallRun validator if you use it directly)
 */

const { BlobServiceClient } = require('@azure/storage-blob');
const { parse } = require('csv-parse');
const { webcrypto } = require('crypto');

const crypto = globalThis.crypto ?? webcrypto;

// ---------------------------
// Utilities
// ---------------------------
function uuid() {
  return crypto.randomUUID ? crypto.randomUUID()
    : ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, c =>
        (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16));
}

function csvEscape(s) {
  if (s.includes('"') || s.includes(',') || s.includes('\n') || s.includes('\r')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

const SMALL_MAX_ROWS = Number(process.env.CH_STRATEGIC_SMALL_MAX_ROWS || 5000);

// ---------------------------
// Azure Blob helpers
// ---------------------------
const AZ_CONN = process.env.AzureWebJobsStorage || '';
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

// ---------------------------
/** CSV analysis/processing
 *
 * “Business logic” assumptions (safe baseline you can extend):
 *  - A row is considered "matched" iff both "Company Name" and "Company Number" are present (non-empty)
 *  - Output CSV contains only matched rows, with columns: Company Number,Company Name
 *  - We stream-parse the CSV to control memory use
 */
// ---------------------------

function analyzeCsv(buffer) {
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

function summarizeCsv(buffer) {
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
          const reason = !hasNum && !hasName ? 'missing_both'
                        : (!hasNum ? 'missing_company_number' : 'missing_company_name');
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

function buildOutputCsv(buffer) {
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

// ---------------------------
// Public functions
// ---------------------------

/**
 * Small run: analyze in-memory and return summary JSON (no blob I/O).
 */
async function smallRun({ csvBuffer, evidence, cid }) {
  const ev = (evidence || '').trim();
  if (ev && !/^[A-Za-z0-9 _-]{1,50}$/.test(ev)) {
    const e = new Error('Invalid evidence tag'); e.statusCode = 400; throw e;
  }
  const analysis = await summarizeCsv(csvBuffer);
  return {
    ok: true,
    correlationId: cid || uuid(),
    evidenceTag: ev || null,
    rows: analysis.rows,
    matched: analysis.matched,
    skipped: analysis.skipped,
    errorsByReason: analysis.errorsByReason,
    headers: analysis.headers,
    itemsSample: analysis.itemsSample
  };
}

/**
 * “Large” run: synchronously produce output CSV and persist status + CSV to blob storage.
 */
async function startLarge({ csvBuffer, evidence, cid }) {
  if (!BLOB) throw new Error('Storage not configured');
  await ensureContainers();

  const ev = (evidence || '').trim();
  if (ev && !/^[A-Za-z0-9 _-]{1,50}$/.test(ev)) {
    const e = new Error('Invalid evidence tag'); e.statusCode = 400; throw e;
  }

  const jobId = uuid();
  const statusName = `${jobId}.json`;
  const outName = `${jobId}.csv`;

  // Initial status: running
  await putJson(CONTAINERS.status, statusName, {
    state: 'running',
    jobId,
    startedAt: new Date().toISOString(),
    evidenceTag: ev || null,
    totalRows: null,
    matched: 0,
    skipped: 0,
    errorsByReason: {},
    downloadUrl: `/api/ch-strategic/download/${jobId}`
  });

  // Analyze & build output
  const summary = await summarizeCsv(csvBuffer);
  const outputCsv = await buildOutputCsv(csvBuffer);
  await putCsv(CONTAINERS.out, outName, outputCsv);

  // Final status: done
  await putJson(CONTAINERS.status, statusName, {
    state: 'done',
    jobId,
    finishedAt: new Date().toISOString(),
    evidenceTag: ev || null,
    totalRows: summary.rows,
    matched: summary.matched,
    skipped: summary.skipped,
    errorsByReason: summary.errorsByReason,
    downloadUrl: `/api/ch-strategic/download/${jobId}`
  });

  return {
    jobId,
    statusUrl: `/api/ch-strategic/status/${jobId}`,
    downloadUrl: `/api/ch-strategic/download/${jobId}`
  };
}

async function getStatus(jobId) {
  if (!BLOB) throw new Error('Storage not configured');
  const id = String(jobId || '').trim();
  if (!/^[a-f0-9-]{16,}$/.test(id)) {
    const e = new Error('Bad id'); e.statusCode = 400; throw e;
  }
  const status = await getJson(CONTAINERS.status, `${id}.json`);
  if (!status) {
    const e = new Error('Not found'); e.statusCode = 404; throw e;
  }
  return status;
}

async function download(jobId) {
  if (!BLOB) throw new Error('Storage not configured');
  const id = String(jobId || '').trim();
  if (!/^[a-f0-9-]{16,}$/.test(id)) {
    const e = new Error('Bad id'); e.statusCode = 400; throw e;
  }
  const name = `${id}.csv`;
  const s = await getCsvStream(CONTAINERS.out, name);
  if (!s) {
    const e = new Error('Not found'); e.statusCode = 404; throw e;
  }
  return { stream: s.stream, filename: `ch-strategic-${id}.csv` };
}

async function feedback(payload, principal, cid) {
  if (!BLOB) throw new Error('Storage not configured');
  await ensureContainers();
  const body = Object(payload || {});
  const doc = {
    receivedAt: new Date().toISOString(),
    correlationId: cid || uuid(),
    principal: capturePrincipal(principal),
    up: !!body.up,
    details: (body.details || '').toString().slice(0, 2000)
  };
  await putJson(CONTAINERS.feedback, `${uuid()}.json`, doc);
}

function capturePrincipal(pEncodedOrObj) {
  try {
    if (!pEncodedOrObj) return null;
    if (typeof pEncodedOrObj === 'string') {
      const json = Buffer.from(pEncodedOrObj, 'base64').toString('utf8');
      const p = JSON.parse(json);
      return { userId: p.userId, identityProvider: p.identityProvider, userDetails: p.userDetails, userRoles: p.userRoles };
    }
    const p = pEncodedOrObj;
    return { userId: p.userId, identityProvider: p.identityProvider, userDetails: p.userDetails, userRoles: p.userRoles };
  } catch {
    return null;
  }
}

/**
 * Optional helper used by /pbi-export route (role-gated).
 * Accepts an array of objects in payload.rows and returns CSV text.
 */
async function pbiExport(payload /* { rows: [] } */, { cid, principal } = {}) {
  const rows = Array.isArray(payload?.rows) ? payload.rows : [];
  if (rows.length === 0) return '';
  const headers = Array.from(new Set(rows.flatMap(r => Object.keys(r))));
  const headerLine = headers.map(csvEscape).join(',');
  const lines = rows.map(r => headers.map(h => csvEscape(r[h] == null ? '' : String(r[h]))).join(','));
  return `${headerLine}\n${lines.join('\n')}\n`;
}

// ---------------------------
// Express router mount
// ---------------------------

/**
 * mount(app, { upload, requireRole, getPrincipal })
 * Wires all endpoints under /ch-strategic/*
 * Expects:
 *  - upload: multer instance (in-memory) with .single('file')
 *  - requireRole: middleware factory for role checks
 *  - getPrincipal: function(req) -> SWA principal object
 */
function mount(app, { upload, requireRole, getPrincipal }) {
  if (!app) throw new Error('mount(app, ...) requires an Express app');

  // Small run (multipart; returns analysis JSON)
  app.post('/ch-strategic', upload.single('file'), async (req, res) => {
    try {
      if (!req.file?.buffer) return res.status(400).json({ ok: false, message: 'Missing file' });
      const evidence = (req.body?.evidenceTag || '').trim();
      if (evidence && !/^[A-Za-z0-9 _-]{1,50}$/.test(evidence)) {
        return res.status(400).json({ ok: false, message: 'Invalid evidenceTag' });
      }
      const result = await smallRun({ csvBuffer: req.file.buffer, evidence, cid: req.correlationId });
      return res.status(200).json(result);
    } catch (e) {
      return res.status(Number(e?.statusCode || 400)).json({ ok: false, message: e?.message || 'Error' });
    }
  });

  // Start "large" run (synchronous processing with persisted status + CSV)
  app.post('/ch-strategic/start', upload.single('file'), async (req, res) => {
    try {
      if (!req.file?.buffer) return res.status(400).json({ ok: false, message: 'Missing file' });
      if (!BLOB) return res.status(500).json({ ok: false, message: 'Storage not configured' });

      const evidence = (req.body?.evidenceTag || '').trim();
      if (evidence && !/^[A-Za-z0-9 _-]{1,50}$/.test(evidence)) {
        return res.status(400).json({ ok: false, message: 'Invalid evidenceTag' });
      }

      const out = await startLarge({ csvBuffer: req.file.buffer, evidence, cid: req.correlationId });
      return res.status(200).json({ ok: true, correlationId: req.correlationId, ...out });
    } catch (e) {
      return res.status(Number(e?.statusCode || 400)).json({ ok: false, message: e?.message || 'Error' });
    }
  });

  // Status
  app.get('/ch-strategic/status/:id', async (req, res) => {
    try {
      const status = await getStatus(String(req.params.id || '').trim());
      return res.status(200).json(status);
    } catch (e) {
      return res.status(Number(e?.statusCode || 400)).json({ ok: false, message: e?.message || 'Error' });
    }
  });

  // Download (streams CSV)
  app.get('/ch-strategic/download/:id', async (req, res) => {
    try {
      const { stream, filename } = await download(String(req.params.id || '').trim());
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      if (req.correlationId) res.setHeader('X-Correlation-Id', req.correlationId);
      stream.pipe(res);
    } catch (e) {
      return res.status(Number(e?.statusCode || 400)).json({ ok: false, message: e?.message || 'Error' });
    }
  });

  // Feedback
  app.post('/ch-strategic/feedback', async (req, res) => {
    try {
      const principal = typeof getPrincipal === 'function' ? getPrincipal(req) : null;
      await feedback(req.body || {}, principal, req.correlationId);
      return res.status(204).end();
    } catch (e) {
      return res.status(Number(e?.statusCode || 400)).json({ ok: false, message: e?.message || 'Error' });
    }
  });

  // Optional: PBI Export (require a specific role; default 'pbi-exporter')
  const pbiRole = process.env.CH_STRATEGIC_PBI_ROLE || 'pbi-exporter';
  app.post('/ch-strategic/pbi-export',
    typeof requireRole === 'function' ? requireRole(pbiRole) : (_req, _res, next) => next(),
    async (req, res) => {
      try {
        const principal = typeof getPrincipal === 'function' ? getPrincipal(req) : null;
        const csv = await pbiExport(req.body || {}, { cid: req.correlationId, principal });
        if (!csv) return res.status(204).end();
        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        if (req.correlationId) res.setHeader('X-Correlation-Id', req.correlationId);
        return res.status(200).end(csv);
      } catch (e) {
        return res.status(Number(e?.statusCode || 400)).json({ ok: false, message: e?.message || 'Error' });
      }
    }
  );
}

// ---------------------------
// Module exports
// ---------------------------
module.exports = {
  mount,
  smallRun,
  startLarge,
  getStatus,
  download,
  feedback,
  pbiExport
};
