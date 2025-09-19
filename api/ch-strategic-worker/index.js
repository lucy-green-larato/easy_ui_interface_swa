'use strict';

/**
 * Queue-triggered worker:
 * - Message shape: { jobId, chunkIndex, chunkBlobPath, totalChunks }
 * - Reads chunk from "ch-strategic-cache"
 * - Runs scanOneCompany(row) via shared module
 * - Appends CSV lines to "ch-strategic-out/{jobId}.csv" (Append Blob)
 * - Updates status in "ch-strategic-status/{jobId}.json"
 */

const { BlobServiceClient } = require('@azure/storage-blob');

const AZURE_STORAGE = process.env.AzureWebJobsStorage;
const STATUS_CONTAINER = 'ch-strategic-status';
const CACHE_CONTAINER  = 'ch-strategic-cache';
const OUT_CONTAINER    = 'ch-strategic-out';

const blob = BlobServiceClient.fromConnectionString(AZURE_STORAGE);

async function readJson(container, name) {
  const c = blob.getContainerClient(container);
  const b = c.getBlobClient(name);
  if (!(await b.exists())) return null;
  const downloaded = await (await b.download()).readableStreamBody;
  const chunks = [];
  for await (const ch of downloaded) chunks.push(ch);
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

async function writeJson(container, name, data) {
  const c = blob.getContainerClient(container);
  await c.createIfNotExists();
  const b = c.getBlockBlobClient(name);
  const body = Buffer.from(JSON.stringify(data));
  await b.upload(body, body.length, { blobHTTPHeaders: { blobContentType: 'application/json' }});
}

async function appendCsvLine(container, name, text) {
  const c = blob.getContainerClient(container);
  await c.createIfNotExists();
  const a = c.getAppendBlobClient(name);
  if (!(await a.exists())) {
    await a.create({ blobHTTPHeaders: { blobContentType: 'text/csv; charset=utf-8' }});
  }
  await a.appendBlock(Buffer.from(text, 'utf8'), Buffer.byteLength(text));
}

function toCsvRow(values) {
  // very small safe CSV joiner; quote if needed
  const esc = (v) => {
    const s = (v == null) ? '' : String(v);
    return (/["\n,]/.test(s)) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return values.map(esc).join(',') + '\n';
}

async function processChunk({ jobId, chunkIndex, chunkBlobPath, totalChunks }, log) {
  // 1) Read status; bail if cancelled
  const statusName = `${jobId}.json`;
  const cancelName = `${jobId}.cancel`;
  const status = (await readJson(STATUS_CONTAINER, statusName)) || {};
  const cancelFlag = await blob.getContainerClient(STATUS_CONTAINER).getBlobClient(cancelName).exists();
  if (cancelFlag || status.state === 'cancelled') {
    // mark cancelled (idempotent)
    await writeJson(STATUS_CONTAINER, statusName, { ...status, state: 'cancelled', cancelledAt: new Date().toISOString() });
    return;
  }

  // 2) Stream & parse chunk
  const chunkBlob = blob.getContainerClient(CACHE_CONTAINER).getBlobClient(chunkBlobPath);
  if (!(await chunkBlob.exists())) {
    throw new Error(`Chunk not found: ${chunkBlobPath}`);
  }
  const stream = (await chunkBlob.download()).readableStreamBody;

  // The row format we expect was written by the /start chunker: CSV with header.
  // We'll implement the scan inline here to avoid importing server-only deps.
  // If you already have a "scanOneCompany" function, you can factor it out and require it here.
  const readline = require('readline');
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let header = null;
  const headerOut = ['Company Name', 'Domain', 'Match', 'Evidence', 'Confidence'];
  // If this is the first chunk, create the output and write header.
  const outName = `${jobId}.csv`;
  const outClient = blob.getContainerClient(OUT_CONTAINER);
  if (chunkIndex === 0) {
    await appendCsvLine(OUT_CONTAINER, outName, toCsvRow(headerOut));
  }

  let rowIndex = -1;
  for await (const line of rl) {
    rowIndex++;
    if (rowIndex === 0) { header = line.split(','); continue; } // skip header from chunk
    if (!line.trim()) continue;
    // naive CSV split for the common case (the chunker avoids embedded newlines)
    const cols = line.split(',');
    const name  = (cols[0] || '').trim();
    const domain= (cols[1] || '').trim();

    // Very simple placeholder matcher; replace with your real one when ready
    const match = false;
    const evidence = '';
    const confidence = 0;

    const outRow = [name, domain, match ? 'TRUE' : 'FALSE', evidence, confidence];
    await appendCsvLine(OUT_CONTAINER, outName, toCsvRow(outRow));
  }

  // 3) Update status
  const now = new Date().toISOString();
  const fresh = (await readJson(STATUS_CONTAINER, statusName)) || {};
  const completed = Math.min((fresh.completedChunks || 0) + 1, totalChunks);
  const nextState = (completed >= totalChunks) ? 'done' : 'running';
  const updated = {
    ...fresh,
    state: nextState,
    completedChunks: completed,
    totalChunks,
    outputBlob: `${OUT_CONTAINER}/${outName}`,
    updatedAt: now,
    finishedAt: nextState === 'done' ? now : undefined
  };
  await writeJson(STATUS_CONTAINER, statusName, updated);
}

module.exports = async function (context, queueItem) {
  const log = context.log;
  try {
    const msg = (typeof queueItem === 'string') ? JSON.parse(queueItem) : queueItem;
    if (!msg || !msg.jobId || typeof msg.chunkIndex !== 'number' || !msg.chunkBlobPath || !msg.totalChunks) {
      throw new Error('Invalid queue message shape');
    }
    await processChunk(msg, log);
  } catch (err) {
    context.log.error('Worker error', err && err.stack || err);
    throw err; // let Azure Functions handle retry for transient errors
  }
};
