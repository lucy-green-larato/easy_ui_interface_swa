/** api-worker/ch-strategic-worker/index.js Sept 22 2025 v2 */
'use strict';

/**
 * Queue-triggered worker (large-run):
 * - Message shape: { jobId, chunkIndex, chunkBlobPath, totalChunks }
 * - Reads chunk CSV from CACHE container (line-oriented; header on first line)
 * - Appends output rows to OUT container as Append Blob: "<jobId>.csv"
 * - Updates STATUS container "<jobId>.json"
 * - Cancellation: presence of "<jobId>.cancel" blob in STATUS container
 */

const { BlobServiceClient } = require('@azure/storage-blob');
const readline = require('readline');

// ---- Containers (align with your /start chunker) -----------------------------
const STATUS_CONTAINER = process.env.CH_STATUS_CONTAINER   || 'ch-strategic-status';
const CACHE_CONTAINER  = process.env.CH_CACHE_CONTAINER    || 'ch-strategic-cache';
const OUT_CONTAINER    = process.env.CH_OUT_CONTAINER      || 'ch-strategic-out';

// ---- Output CSV header (adjust as needed) ------------------------------------
const OUTPUT_HEADER = (process.env.CH_OUTPUT_HEADER || 'Company Name,Domain,Match,Evidence,Confidence')
  .split(',').map(s => s.trim());

// ---- Blob Service singleton ---------------------------------------------------
let _blobSvc;
function getBlobSvc() {
  if (_blobSvc) return _blobSvc;
  const conn = process.env.AzureWebJobsStorage;
  if (!conn) throw new Error('AzureWebJobsStorage is not configured');
  _blobSvc = BlobServiceClient.fromConnectionString(conn);
  return _blobSvc;
}

// ---- Small helpers ------------------------------------------------------------
async function downloadToBuffer(blobClient) {
  const resp = await blobClient.download();
  const chunks = [];
  for await (const ch of resp.readableStreamBody) chunks.push(ch);
  return Buffer.concat(chunks);
}

async function streamExists(containerClient, blobName) {
  return await containerClient.getBlobClient(blobName).exists();
}

async function readJson(container, name) {
  const svc = getBlobSvc();
  const c = svc.getContainerClient(container);
  const b = c.getBlockBlobClient(name);
  if (!(await b.exists())) return null;
  const resp = await b.download();
  const buf = await (async () => {
    const chunks = [];
    for await (const ch of resp.readableStreamBody) chunks.push(ch);
    return Buffer.concat(chunks);
  })();
  return JSON.parse(buf.toString('utf8'));
}

async function writeJson(container, name, data) {
  const svc = getBlobSvc();
  const c = svc.getContainerClient(container);
  await c.createIfNotExists();
  const b = c.getBlockBlobClient(name);
  const json = Buffer.from(JSON.stringify(data));
  await b.upload(json, json.length, {
    blobHTTPHeaders: { blobContentType: 'application/json; charset=utf-8' }
  });
}

async function ensureAppendBlobWithHeader(container, name, headerArray) {
  const svc = getBlobSvc();
  const c = svc.getContainerClient(container);
  await c.createIfNotExists();
  const a = c.getAppendBlobClient(name);
  const exists = await a.exists();
  if (!exists) {
    // Create and write header line atomically-ish; tolerate race on create
    try {
      await a.create({
        blobHTTPHeaders: { blobContentType: 'text/csv; charset=utf-8' }
      });
    } catch (e) {
      // If another worker just created it, continue
      // eslint-disable-next-line no-empty
    }
    const headerLine = toCsvRow(headerArray) + '\r\n';
    // If another worker already wrote header, this just appends another header.
    // To avoid duplicate headers, check size; if size==0, write header.
    try {
      const props = await a.getProperties();
      if ((props.contentLength || 0) === 0) {
        await a.appendBlock(Buffer.from(headerLine, 'utf8'), Buffer.byteLength(headerLine));
      }
    } catch {
      // Best-effort; if props fail, append header anyway
      await a.appendBlock(Buffer.from(headerLine, 'utf8'), Buffer.byteLength(headerLine));
    }
  }
  return a;
}

// Robust-enough CSV line splitter for simple fields (handles quoted commas & "")
function splitCsvLine(line) {
  const out = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQ) {
      if (ch === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; } // escaped quote
        else { inQ = false; }
      } else {
        cur += ch;
      }
    } else {
      if (ch === ',') { out.push(cur); cur = ''; }
      else if (ch === '"') { inQ = true; }
      else { cur += ch; }
    }
  }
  out.push(cur);
  return out;
}

function toCsvRow(values) {
  const esc = (v) => {
    const s = (v == null) ? '' : String(v);
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return values.map(esc).join(',');
}

// ---- Core worker --------------------------------------------------------------
async function processChunk(msg, log) {
  const { jobId, chunkIndex, chunkBlobPath, totalChunks } = msg;

  const svc = getBlobSvc();
  const cacheC  = svc.getContainerClient(CACHE_CONTAINER);
  const statusC = svc.getContainerClient(STATUS_CONTAINER);

  const statusName = `${jobId}.json`;
  const cancelName = `${jobId}.cancel`;
  const outName    = `${jobId}.csv`;

  // Fast cancel check
  const isCancelled = async () => await streamExists(statusC, cancelName);
  if (await isCancelled()) {
    const now = new Date().toISOString();
    const cur = (await readJson(STATUS_CONTAINER, statusName)) || {};
    await writeJson(STATUS_CONTAINER, statusName, {
      ...cur,
      state: 'cancelled',
      updatedAt: now,
      cancelledAt: now
    });
    log(`job ${jobId} chunk ${chunkIndex}: cancelled before start`);
    return;
  }

  // Ensure chunk exists
  const chunkBlob = cacheC.getBlobClient(chunkBlobPath);
  if (!(await chunkBlob.exists())) {
    // Count as error but keep progress moving
    const now = new Date().toISOString();
    const cur = (await readJson(STATUS_CONTAINER, statusName)) || {};
    await writeJson(STATUS_CONTAINER, statusName, {
      ...cur,
      state: (cur.completedChunks + 1 >= totalChunks) ? 'done' : 'running',
      errors: (cur.errors || 0) + 1,
      completedChunks: Math.min((cur.completedChunks || 0) + 1, totalChunks),
      totalChunks,
      outputBlob: `${OUT_CONTAINER}/${outName}`,
      updatedAt: now,
      finishedAt: (cur.completedChunks + 1 >= totalChunks) ? now : undefined
    });
    throw new Error(`Chunk not found: ${chunkBlobPath}`);
  }

  // Stream the chunk
  const resp = await chunkBlob.download();
  const rl = readline.createInterface({ input: resp.readableStreamBody, crlfDelay: Infinity });

  // Ensure output blob + header exists
  const outAppend = await ensureAppendBlobWithHeader(OUT_CONTAINER, outName, OUTPUT_HEADER);

  // Iterate lines
  let rowIndex = -1;
  let processedSinceCheck = 0;

  for await (const line of rl) {
    rowIndex++;
    if (rowIndex === 0) continue;           // skip header in chunk
    if (!line || !line.trim()) continue;

    // Periodic cancel check
    if (++processedSinceCheck >= 200) {
      processedSinceCheck = 0;
      if (await isCancelled()) {
        const now = new Date().toISOString();
        const cur = (await readJson(STATUS_CONTAINER, statusName)) || {};
        await writeJson(STATUS_CONTAINER, statusName, {
          ...cur,
          state: 'cancelled',
          updatedAt: now,
          cancelledAt: now
        });
        log(`job ${jobId} chunk ${chunkIndex}: cancelled during processing`);
        return;
      }
    }

    // Parse columns (the chunker should avoid embedded newlines; commas in quotes are handled)
    const cols = splitCsvLine(line);
    const name    = (cols[0] || '').trim();
    const domain  = (cols[1] || '').trim();

    // TODO: replace with your real matcher
    const match = false;
    const evidence = '';
    const confidence = 0;

    const row = [name, domain, match ? 'TRUE' : 'FALSE', evidence, confidence];
    const text = toCsvRow(row) + '\r\n';
    await outAppend.appendBlock(Buffer.from(text, 'utf8'), Buffer.byteLength(text));
  }

  // Update status
  const now = new Date().toISOString();
  const fresh = (await readJson(STATUS_CONTAINER, statusName)) || {};
  const completed = Math.min((fresh.completedChunks || 0) + 1, totalChunks);
  const done = completed >= totalChunks;
  const updated = {
    ...fresh,
    state: done ? 'done' : 'running',
    completedChunks: completed,
    totalChunks,
    outputBlob: `${OUT_CONTAINER}/${outName}`,
    updatedAt: now,
    finishedAt: done ? now : undefined
  };
  await writeJson(STATUS_CONTAINER, statusName, updated);
  log(`job ${jobId} chunk ${chunkIndex}: completed ${completed}/${totalChunks}`);
}

// ---- Azure Functions entrypoint ----------------------------------------------
module.exports = async function (context, queueItem) {
  try {
    const msg = (typeof queueItem === 'string')
      ? JSON.parse(queueItem)
      : (Buffer.isBuffer(queueItem) ? JSON.parse(queueItem.toString('utf8')) : queueItem);

    // Basic validation
    if (!msg || !msg.jobId || typeof msg.chunkIndex !== 'number' || !msg.chunkBlobPath || typeof msg.totalChunks !== 'number') {
      throw new Error('Invalid queue message shape. Expect { jobId, chunkIndex:number, chunkBlobPath, totalChunks:number }');
    }

    await processChunk(msg, context.log);
  } catch (err) {
    context.log.error('ch-strategic-worker error:', err?.message || err, err?.stack);
    // Re-throw so Functions can retry and/or send to poison queue
    throw err;
  }
};
