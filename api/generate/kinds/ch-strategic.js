'use strict';

const crypto = require('crypto');
const { BlobServiceClient } = require('@azure/storage-blob');
const { QueueClient } = require('@azure/storage-queue');
const parse = require('csv-parse');
const { error, ok, uuid } = require('../../lib/http');

const STATUS_CONTAINER = 'ch-strategic-status';
const CACHE_CONTAINER = 'ch-strategic-cache';
const OUT_CONTAINER = 'ch-strategic-out';
const QUEUE_NAME = 'ch-strategic-jobs';

const CHUNK_SIZE = parseInt(process.env.CH_STRATEGIC_CHUNK_SIZE || '5000', 10);
const MAX_ROWS = parseInt(process.env.CH_STRATEGIC_MAX_ROWS || '200000', 10);
const MAX_SIZE = parseInt(process.env.CH_STRATEGIC_MAX_UPLOAD_BYTES || String(20 * 1024 * 1024), 10); // 20MB
const TTL_DAYS = parseInt(process.env.CH_STRATEGIC_TTL_DAYS || '7', 10);

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

function assertAllowListOrFail(allowPbi, isProd) {
  if (!isProd) return;
  const a = allowPbi || {};
  const ok = (Array.isArray(a.workspaces) && a.workspaces.length) ||
    (Array.isArray(a.reports) && a.reports.length) ||
    (Array.isArray(a.visuals) && a.visuals.length);
  if (!ok) {
    const err = new Error('PBI allow-list is required in production'); err.status = 403; throw err;
  }
}

// --- chunk writer with retention metadata ---
function createChunkWriter({ blob, jobId }) {
  const container = blob.getContainerClient(CACHE_CONTAINER);
  return async function writeChunk(index, header, rows) {
    await container.createIfNotExists();
    const name = `jobs/${jobId}/chunks/chunk-${index}.csv`;
    const client = container.getBlockBlobClient(name);
    const body = Buffer.from(toCsvRow(header) + rows.map(toCsvRow).join(''), 'utf8');
    await client.upload(body, body.length, { blobHTTPHeaders: { blobContentType: 'text/csv; charset=utf-8' } });
    await client.setMetadata({ 'retention-days': String(TTL_DAYS) });
    console.info(JSON.stringify({ cid: jobId, event: 'chunk-written', chunk: index }));
    return name;
  };
}

async function chunkAndEnqueue({ blob, queue, jobId, inputBlobPath }) {
  const cache = blob.getContainerClient(CACHE_CONTAINER);
  const inputBlob = cache.getBlobClient(inputBlobPath);
  const download = await inputBlob.download();
  const parser = parse({ bom: true, columns: true, relax_column_count: true, skip_empty_lines: true });
  download.readableStreamBody.pipe(parser);

  const headerOut = ['Company Name', 'Domain'];
  const writer = createChunkWriter({ blob, jobId });

  if (CHUNK_SIZE <= 0 || CHUNK_SIZE > 25000) {
    throw Object.assign(new Error('Invalid CHUNK_SIZE; must be 1..25000'), { status: 500 });
  }
  let rows = [];
  let chunks = [];
  let rowCount = 0;
  for await (const rec of parser) {
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
  const manifestClient = cache.getBlockBlobClient(manifestName);
  await manifestClient.upload(
    Buffer.from(JSON.stringify({ totalChunks, chunks }), 'utf8'),
    Buffer.byteLength(JSON.stringify({ totalChunks, chunks })),
    { blobHTTPHeaders: { blobContentType: 'application/json' } }
  );
  await manifestClient.setMetadata({ 'retention-days': String(TTL_DAYS) });

  for (let i = 0; i < totalChunks; i++) {
    const payload = { jobId, chunkIndex: i, chunkBlobPath: chunks[i], totalChunks };
    const text = Buffer.from(JSON.stringify(payload)).toString('base64');
    await queue.sendMessage(text);
  }

  console.info(JSON.stringify({ cid: jobId, event: 'enqueued', totalChunks }));
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
  const statusClient = c.getBlockBlobClient(statusName);
  await statusClient.upload(
    Buffer.from(JSON.stringify(status)),
    Buffer.byteLength(JSON.stringify(status)),
    { blobHTTPHeaders: { blobContentType: 'application/json' } }
  );
  await statusClient.setMetadata({ 'retention-days': String(TTL_DAYS) });
  console.info(JSON.stringify({ cid: jobId, event: 'status-init' }));
  return status;
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

// --- Main route mount ---
exports.mount = function mount(app, { multer, allowPbi = {} }) {
  const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_SIZE } });

  // --- /start ---
  app.post('/ch-strategic/start', upload.single('file'), async (req, res) => {
    const cid = req.correlationId || uuid(); // correlationId from middleware
    try {
      if (!req.file || !req.file.buffer) {
        return error(res, 400, 'No file uploaded', undefined, cid);
      }
      const ct = (req.file.mimetype || '').toLowerCase();
      if (!(ct.includes('csv') || ct.includes('text'))) {
        return error(res, 415, 'Unsupported Media Type', undefined, cid);
      }
      const evidenceTag = (req.body?.evidenceTag || '').trim();
      if (evidenceTag && !validateEvidenceTag(evidenceTag)) {
        return error(res, 400, 'Invalid evidenceTag', undefined, cid);
      }
      const firstBytes = req.file.buffer.subarray(0, 256).toString('utf8');
      if (!/[,;\t]/.test(firstBytes)) {
        return error(res, 400, 'CSV parse error: no delimiter detected', undefined, cid);
      }

      const jobId = newId('chstrat');
      const { blob, queue } = createAzureClients();

      // --- /feedback (capture user feedback for learning loop) ---
      app.post('/ch-strategic/feedback', express.json({ limit: '8kb' }), async (req, res) => {
        const cid = req.correlationId || uuid();
        try {
          // Basic authz: must be signed in (index middleware ensures req.principal)
          const p = req.principal;
          if (!p) return error(res, 401, 'Unauthenticated', undefined, cid);

          // Validate payload (strict, minimal)
          const {
            jobId,
            useful,               // "up" | "down"
            tags = [],            // ["false-positive","false-negative","other"]
            comment = "",         // <= 500 chars
            includeSample = false,
            evidenceTag = "",     // optional echo
            totals = {}           // { total, matched, skipped }
          } = req.body || {};

          if (!jobId || typeof jobId !== 'string' || jobId.length > 100) {
            return error(res, 400, 'Invalid jobId', undefined, cid);
          }
          if (useful !== 'up' && useful !== 'down') {
            return error(res, 400, 'Invalid useful flag', undefined, cid);
          }
          const ALLOWED_TAGS = new Set(['false-positive', 'false-negative', 'other']);
          const safeTags = Array.isArray(tags) ? tags.filter(t => ALLOWED_TAGS.has(String(t))) : [];
          const safeComment = String(comment || '').slice(0, 500); // hard cap
          const safeEvidence = String(evidenceTag || '').slice(0, 50);

          // Optionally verify job exists (status blob) but do not load full details
          const { blob } = createAzureClients();
          const statusC = blob.getContainerClient(STATUS_CONTAINER);
          const statusB = statusC.getBlobClient(`${jobId}.json`);
          if (!(await statusB.exists())) {
            return error(res, 404, 'Unknown jobId', undefined, cid);
          }

          // Build record
          const now = new Date();
          const yyyy = String(now.getUTCFullYear());
          const mm = String(now.getUTCMonth() + 1).padStart(2, '0');
          const dd = String(now.getUTCDate()).padStart(2, '0');
          const ts = now.toISOString().replace(/[:.]/g, '-');
          const rid = newId('fbk'); // unique row id

          const record = {
            id: rid,
            jobId,
            evidenceTag: safeEvidence,
            useful,                // "up" | "down"
            tags: safeTags,
            comment: safeComment,
            includeSample: !!includeSample,
            totals: {
              total: Number(totals?.total) || 0,
              matched: Number(totals?.matched) || 0,
              skipped: Number(totals?.skipped) || 0
            },
            user: {
              id: p.userId || null,
              name: p.userDetails || null,
              roles: p.userRoles || []
            },
            createdAt: now.toISOString(),
            cid
          };

          // Persist to feedback container
          const FB_CONTAINER = 'ch-strategic-feedback';
          const fbC = blob.getContainerClient(FB_CONTAINER);
          await fbC.createIfNotExists();
          const fbPath = `${yyyy}/${mm}/${dd}/${jobId}/${ts}_${rid}.json`;
          const fbB = fbC.getBlockBlobClient(fbPath);
          const body = Buffer.from(JSON.stringify(record), 'utf8');
          await fbB.upload(body, body.length, {
            blobHTTPHeaders: { blobContentType: 'application/json' }
          });
          const TTL_FEEDBACK = parseInt(process.env.CH_STRATEGIC_FEEDBACK_TTL_DAYS || '90', 10);
          try { await fbB.setMetadata({ 'retention-days': String(TTL_FEEDBACK) }); } catch { }

          // Structured log (no PII beyond role+hashed user if you prefer)
          console.info(JSON.stringify({
            cid, event: 'feedback', jobId, useful,
            tags: safeTags, totals: record.totals,
            principal: { roles: p.userRoles || [], userId: !!p.userId ? 'present' : 'missing' }
          }));

          return ok(res, 201, { ok: true, id: rid, correlationId: cid }, cid);
        } catch (err) {
          console.error(JSON.stringify({ cid, error: err.message || err }));
          return error(res, err.status || 500, err.message || 'Internal error', undefined, cid);
        }
      });

      // Input blob
      const cache = blob.getContainerClient(CACHE_CONTAINER);
      await cache.createIfNotExists();
      const inputName = `jobs/${jobId}/input.csv`;
      const input = cache.getBlockBlobClient(inputName);
      await input.upload(req.file.buffer, req.file.buffer.length, {
        blobHTTPHeaders: { blobContentType: 'text/csv; charset=utf-8' },
        metadata: evidenceTag ? { evidenceTag } : undefined
      });
      await input.setMetadata({ 'retention-days': String(TTL_DAYS) });

      const { totalChunks } = await chunkAndEnqueue({ blob, queue, jobId, inputBlobPath: inputName });
      await putInitialStatus(blob, jobId, { totalChunks });

      const base = '/api/ch-strategic';
      return ok(res, 202, {
        jobId,
        statusUrl: `${base}/status/${jobId}`,
        downloadUrl: `${base}/download/${jobId}`,
        correlationId: cid
      }, cid);
    } catch (err) {
      console.error(JSON.stringify({ cid, error: err.message || err }));
      return error(res, err.status || 500, err.message || 'Internal error', undefined, cid);
    }
  });

  // --- /status/:jobId ---
  app.get('/ch-strategic/status/:jobId', async (req, res) => {
    const cid = req.correlationId || uuid();
    try {
      const { jobId } = req.params;
      const { blob } = createAzureClients();
      const c = blob.getContainerClient(STATUS_CONTAINER);
      const b = c.getBlobClient(`${jobId}.json`);
      if (!(await b.exists())) return error(res, 404, 'Unknown jobId', undefined, cid);
      const d = await b.download();
      const status = JSON.parse((await streamToBuffer(d.readableStreamBody)).toString('utf8'));
      return safeJson(res, 200, { ...status, correlationId: cid });
    } catch (err) {
      console.error(JSON.stringify({ cid, error: err.message || err }));
      return error(res, err.status || 500, err.message || 'Internal error', undefined, cid);
    }
  });

  // --- /download/:jobId ---
  app.get('/ch-strategic/download/:jobId', async (req, res) => {
    const cid = req.correlationId || uuid();
    try {
      const { jobId } = req.params;
      const { blob } = createAzureClients();
      const statusC = blob.getContainerClient(STATUS_CONTAINER);
      const statusB = statusC.getBlobClient(`${jobId}.json`);
      if (!(await statusB.exists())) return error(res, 404, 'Unknown jobId', undefined, cid);
      const statusBuf = await streamToBuffer((await statusB.download()).readableStreamBody);
      const status = JSON.parse(statusBuf.toString('utf8'));
      if (status.state !== 'done') return error(res, 409, `Job not completed (state=${status.state})`, undefined, cid);

      const outC = blob.getContainerClient(OUT_CONTAINER);
      const fileName = `${jobId}.csv`;
      const outB = outC.getBlobClient(fileName);
      if (!(await outB.exists())) return error(res, 404, 'Output not found', undefined, cid);
      try { await outB.setMetadata({ 'retention-days': String(TTL_DAYS) }); } catch { }

      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="${fileName}"; filename*=UTF-8''${encodeURIComponent(fileName)}`);
      res.setHeader('X-Correlation-Id', cid);

      const d = await outB.download();
      d.readableStreamBody.on('error', (err) => {
        console.error(JSON.stringify({ cid, error: err.message || err }));
        error(res, 500, err.message || 'Stream error', undefined, cid);
      });
      d.readableStreamBody.pipe(res);
    } catch (err) {
      console.error(JSON.stringify({ cid, error: err.message || err }));
      return error(res, err.status || 500, err.message || 'Internal error', undefined, cid);
    }
  });

  // --- /cancel/:jobId ---
  app.post('/ch-strategic/cancel/:jobId', async (req, res) => {
    const cid = req.correlationId || uuid();
    try {
      const { jobId } = req.params;
      const { blob } = createAzureClients();
      const c = blob.getContainerClient(STATUS_CONTAINER);
      await c.createIfNotExists();
      const cancel = c.getBlockBlobClient(`${jobId}.cancel`);
      await cancel.uploadData(Buffer.from('1'));
      await cancel.setMetadata({ 'retention-days': String(TTL_DAYS) });

      const sBlob = c.getBlobClient(`${jobId}.json`);
      if (await sBlob.exists()) {
        const status = JSON.parse((await streamToBuffer((await sBlob.download()).readableStreamBody)).toString('utf8'));
        status.state = 'cancelling';
        status.updatedAt = new Date().toISOString();
        await sBlob.upload(Buffer.from(JSON.stringify(status)), Buffer.byteLength(JSON.stringify(status)), { overwrite: true });
        await sBlob.setMetadata({ 'retention-days': String(TTL_DAYS) });
      }
      return safeJson(res, 202, { ok: true, correlationId: cid });
    } catch (err) {
      console.error(JSON.stringify({ cid, error: err.message || err }));
      return error(res, err.status || 500, err.message || 'Internal error', undefined, cid);
    }
  });
};
