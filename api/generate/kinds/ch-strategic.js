'use strict';

const path = require('path');
const crypto = require('crypto');
const { Readable } = require('stream');
const { BlobServiceClient } = require('@azure/storage-blob');
const { QueueClient } = require('@azure/storage-queue');
const parse = require('csv-parse');

const STATUS_CONTAINER = 'ch-strategic-status';
const CACHE_CONTAINER  = 'ch-strategic-cache';
const OUT_CONTAINER    = 'ch-strategic-out';
const QUEUE_NAME       = 'ch-strategic-jobs';

const CHUNK_SIZE = parseInt(process.env.CH_STRATEGIC_CHUNK_SIZE || '5000', 10); // rows per chunk
const MAX_ROWS   = parseInt(process.env.CH_STRATEGIC_MAX_ROWS || '200000', 10);
const MAX_SIZE   = parseInt(process.env.CH_STRATEGIC_MAX_UPLOAD_BYTES || String(20 * 1024 * 1024), 10); // 20MB
const TTL_DAYS   = parseInt(process.env.CH_STRATEGIC_TTL_DAYS || '7', 10);

const ALLOW_PBI  = { workspaces: [], reports: [], visuals: [], ...(global.__CH_STRAT_ALLOW_PBI || {}) };

function newId(prefix = 'job') {
  return `${prefix}_${crypto.randomBytes(10).toString('hex')}`;
}

function toCsvRow(values) {
  const esc = (v) => {
    const s = (v == null) ? '' : String(v);
    return (/["\n,]/.test(s)) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return values.map(esc).join(',') + '\n';
}

async function streamToBuffer(readable) {
  const chunks = [];
  for await (const ch of readable) chunks.push(ch);
  return Buffer.concat(chunks);
}

function validateEvidenceTag(tag) {
  if (typeof tag !== 'string') return false;
  if (tag.length > 50) return false;
  return /^[A-Za-z0-9 _-]*$/.test(tag);
}

function createChunkWriter({ blob, jobId }) {
  const container = blob.getContainerClient(CACHE_CONTAINER);
  return async function writeChunk(index, header, rows) {
    await container.createIfNotExists();
    const name = `jobs/${jobId}/chunks/chunk-${index}.csv`;
    const client = container.getBlockBlobClient(name);
    const body = Buffer.from(toCsvRow(header) + rows.map(toCsvRow).join(''), 'utf8');
    await client.upload(body, body.length, { blobHTTPHeaders: { blobContentType: 'text/csv; charset=utf-8' }});
    return name; // relative path inside CACHE_CONTAINER
  };
}

async function chunkAndEnqueue({ blob, queue, jobId, inputBlobPath, headerExpect }) {
  // Download input
  const cache = blob.getContainerClient(CACHE_CONTAINER);
  const inputBlob = cache.getBlobClient(inputBlobPath);
  const download = await inputBlob.download();
  const parser = parse({ bom: true, columns: true, relax_column_count: true, skip_empty_lines: true });
  download.readableStreamBody.pipe(parser);

  const headerOut = ['Company Name', 'Domain']; // minimal trusted columns
  const writer = createChunkWriter({ blob, jobId });

  let rows = [];
  let chunks = [];
  let rowCount = 0;
  for await (const rec of parser) {
    // pick safe columns
    const name = (rec['Company Name'] || rec['company_name'] || rec['name'] || '').toString().trim();
    const domain = (rec['Domain'] || rec['domain'] || '').toString().trim();
    if (!name && !domain) continue;

    rows.push([name, domain]);
    rowCount++;
    if (rowCount > MAX_ROWS) throw new Error(`Row limit exceeded (${MAX_ROWS})`);

    if (rows.length >= CHUNK_SIZE) {
      const chunkPath = await writer(chunks.length, headerOut, rows);
      chunks.push(chunkPath);
      rows = [];
    }
  }
  if (rows.length) {
    const chunkPath = await writer(chunks.length, headerOut, rows);
    chunks.push(chunkPath);
  }

  const totalChunks = chunks.length;
  const manifestName = `jobs/${jobId}/manifest.json`;
  await cache.getBlockBlobClient(manifestName).upload(
    Buffer.from(JSON.stringify({ totalChunks, chunks }), 'utf8'),
    Buffer.byteLength(JSON.stringify({ totalChunks, chunks }, null, 0)),
    { blobHTTPHeaders: { blobContentType: 'application/json' } }
  );

  // Enqueue one message per chunk
  for (let i = 0; i < totalChunks; i++) {
    const payload = { jobId, chunkIndex: i, chunkBlobPath: chunks[i], totalChunks };
    const text = Buffer.from(JSON.stringify(payload)).toString('base64');
    await queue.sendMessage(text);
  }

  return { totalChunks };
}

async function putInitialStatus(blob, jobId, { totalChunks }) {
  const c = blob.getContainerClient(STATUS_CONTAINER);
  await c.createIfNotExists();
  const statusName = `${jobId}.json`;
  const now = new Date().toISOString();
  const status = {
    jobId, state: 'queued', createdAt: now, updatedAt: now,
    totalChunks, completedChunks: 0, outputBlob: null, ttlDays: TTL_DAYS
  };
  await c.getBlockBlobClient(statusName).upload(
    Buffer.from(JSON.stringify(status)), Buffer.byteLength(JSON.stringify(status)),
    { blobHTTPHeaders: { blobContentType: 'application/json' } }
  );
  return status;
}

function assertAllowListOrFail(allowPbi, isProd) {
  if (!isProd) return;
  const lists = allowPbi || ALLOW_PBI;
  if (!lists || !Array.isArray(lists.workspaces) || !lists.workspaces.length) {
    const err = new Error('PBI allow-list is required in production'); err.status = 403; throw err;
  }
}

function createAzureClients() {
  const conn = process.env.AzureWebJobsStorage;
  if (!conn) throw new Error('AzureWebJobsStorage is not configured');
  return {
    blob: BlobServiceClient.fromConnectionString(conn),
    queue: new QueueClient(conn, QUEUE_NAME)
  };
}

function safeJson(res, code, payload) {
  res.status(code).json(payload);
}

exports.mount = function mount(app, { multer, allowPbi = {} }) {
  const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_SIZE } });

  // POST /api/ch-strategic/start
  app.post('/ch-strategic/start', upload.single('file'), async (req, res) => {
    try {
      const isProd = (process.env.NODE_ENV === 'production');
      assertAllowListOrFail(allowPbi, isProd);

      if (!req.file || !req.file.buffer) {
        return safeJson(res, 400, { error: 'No file uploaded' });
      }
      const evidenceTag = (req.body?.evidenceTag || '').trim();
      if (evidenceTag && !validateEvidenceTag(evidenceTag)) {
        return safeJson(res, 400, { error: 'Invalid evidenceTag' });
      }

      const jobId = newId('chstrat');
      const { blob, queue } = createAzureClients();

      // 1) Save input to cache
      const cache = blob.getContainerClient(CACHE_CONTAINER);
      await cache.createIfNotExists();
      const inputName = `jobs/${jobId}/input.csv`;
      const input = cache.getBlockBlobClient(inputName);
      await input.upload(req.file.buffer, req.file.buffer.length, {
        blobHTTPHeaders: { blobContentType: 'text/csv; charset=utf-8' },
        metadata: evidenceTag ? { evidenceTag } : undefined
      });

      // 2) Chunk & enqueue
      await queue.createIfNotExists();
      const { totalChunks } = await chunkAndEnqueue({
        blob, queue, jobId, inputBlobPath: inputName
      });

      // 3) Initial status
      await putInitialStatus(blob, jobId, { totalChunks });

      const base = '/api/ch-strategic';
      return safeJson(res, 202, {
        jobId,
        statusUrl: `${base}/status/${jobId}`,
        downloadUrl: `${base}/download/${jobId}`
      });
    } catch (err) {
      const code = err.status || 500;
      return safeJson(res, code, { error: err.message || 'Internal error' });
    }
  });

  // GET /api/ch-strategic/status/:jobId
  app.get('/ch-strategic/status/:jobId', async (req, res) => {
    try {
      const { jobId } = req.params;
      const { blob } = createAzureClients();
      const status = await (async () => {
        const c = blob.getContainerClient(STATUS_CONTAINER);
        const b = c.getBlobClient(`${jobId}.json`);
        if (!(await b.exists())) return null;
        const d = await b.download();
        return JSON.parse(await streamToBuffer(d.readableStreamBody));
      })();
      if (!status) return safeJson(res, 404, { error: 'Unknown jobId' });
      return safeJson(res, 200, status);
    } catch (err) {
      return safeJson(res, 500, { error: 'Internal error' });
    }
  });

  // GET /api/ch-strategic/download/:jobId
  app.get('/ch-strategic/download/:jobId', async (req, res) => {
    try {
      const { jobId } = req.params;
      const { blob } = createAzureClients();
      const statusC = blob.getContainerClient(STATUS_CONTAINER);
      const statusB = statusC.getBlobClient(`${jobId}.json`);
      if (!(await statusB.exists())) return safeJson(res, 404, { error: 'Unknown jobId' });
      const status = JSON.parse((await streamToBuffer((await statusB.download()).readableStreamBody)).toString('utf8'));
      if (status.state !== 'done') return safeJson(res, 409, { error: `Job not completed (state=${status.state})` });

      const outC = blob.getContainerClient(OUT_CONTAINER);
      const outName = `${jobId}.csv`;
      const outB = outC.getBlobClient(outName);
      if (!(await outB.exists())) return safeJson(res, 404, { error: 'Output not found' });

      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="${jobId}.csv"`);
      const d = await outB.download();
      d.readableStreamBody.pipe(res);
    } catch (err) {
      return safeJson(res, 500, { error: 'Internal error' });
    }
  });

  // POST /api/ch-strategic/cancel/:jobId  (optional)
  app.post('/ch-strategic/cancel/:jobId', async (req, res) => {
    try {
      const { jobId } = req.params;
      const { blob } = createAzureClients();
      const c = blob.getContainerClient(STATUS_CONTAINER);
      await c.createIfNotExists();
      // Create a cancel flag blob
      const cancel = c.getBlockBlobClient(`${jobId}.cancel`);
      await cancel.uploadData(Buffer.from('1'));
      // Update status to cancelling
      const sBlob = c.getBlobClient(`${jobId}.json`);
      if (await sBlob.exists()) {
        const status = JSON.parse((await streamToBuffer((await sBlob.download()).readableStreamBody)).toString('utf8'));
        status.state = 'cancelling';
        status.updatedAt = new Date().toISOString();
        await sBlob.upload(Buffer.from(JSON.stringify(status)), Buffer.byteLength(JSON.stringify(status)), { overwrite: true });
      }
      return safeJson(res, 202, { ok: true });
    } catch (err) {
      return safeJson(res, 500, { error: 'Internal error' });
    }
  });
};
