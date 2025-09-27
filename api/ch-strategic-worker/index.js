/** api/ch-strategic-worker/index.js 26-09-2025 v1 */
"use strict";

/**
 * Queue worker for ch-strategic
 * Message schema (base64 JSON): { type:"ch-strategic", runId, cacheKey, submittedAt }
 */

const { BlobServiceClient } = require("@azure/storage-blob");
const crypto = require("crypto");

// Env & settings (same names as HTTP function)
const AZURE_STORAGE = process.env.AzureWebJobsStorage;
const CHS_OUT_CONTAINER      = process.env.CH_STRATEGIC_OUT_CONTAINER      || 'ch-strategic-out';
const CHS_STATUS_CONTAINER   = process.env.CH_STRATEGIC_STATUS_CONTAINER   || 'ch-strategic-status';
const CHS_CACHE_CONTAINER    = process.env.CH_STRATEGIC_CACHE_CONTAINER    || 'ch-strategic-cache';
const CH_STRATEGIC_CHUNK_SIZE = Number(process.env.CH_STRATEGIC_CHUNK_SIZE || 100);
const CH_STRATEGIC_MAX_ROWS   = Number(process.env.CH_STRATEGIC_MAX_ROWS || 5000);

const blobSvc = BlobServiceClient.fromConnectionString(AZURE_STORAGE);

// Helpers (duplicated here to keep function self-contained)
function nowIso() { return new Date().toISOString(); }

async function streamToBuffer(readable) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readable.on("data", d => chunks.push(Buffer.from(d)));
    readable.on("end", () => resolve(Buffer.concat(chunks)));
    readable.on("error", reject);
  });
}

async function writeStatus(runId, patch) {
  const c = blobSvc.getContainerClient(CHS_STATUS_CONTAINER);
  await c.createIfNotExists();
  const b = c.getBlockBlobClient(`${runId}.json`);
  let current = {};
  try {
    const dl = await b.download();
    const buf = await streamToBuffer(dl.readableStreamBody);
    current = JSON.parse(buf.toString("utf8"));
  } catch { /* first write */ }
  const merged = { ...current, ...patch, runId, updatedAt: nowIso() };
  const body = JSON.stringify(merged);
  await b.upload(body, Buffer.byteLength(body), { overwrite: true });
  return merged;
}

async function putJson(containerName, blobPath, obj) {
  const c = blobSvc.getContainerClient(containerName);
  await c.createIfNotExists();
  const b = c.getBlockBlobClient(blobPath);
  const body = JSON.stringify(obj);
  await b.upload(body, Buffer.byteLength(body), { overwrite: true });
  return `/${containerName}/${blobPath}`;
}

function csvToRows(buffer) {
  // basic CSV splitter (no quoted-field handling; OK for deterministic pipeline test datasets)
  const text = buffer.toString("utf8").replace(/^\uFEFF/, "");
  const lines = text.split(/\r?\n/).filter(l => l.trim() !== "");
  return lines;
}

module.exports = async function (context, msg) {
  try {
    const payload = typeof msg === "string" ? JSON.parse(msg) : msg;
    if (!payload || payload.type !== "ch-strategic") {
      context.log.warn("Ignoring message without type ch-strategic");
      return;
    }
    const { runId, cacheKey } = payload;
    if (!runId || !cacheKey) {
      context.log.error("Invalid message", payload);
      return;
    }

    await writeStatus(runId, { state: "Parsing", message: "Fetching cached CSV" });

    // Load CSV from cache
    const cache = blobSvc.getContainerClient(CHS_CACHE_CONTAINER);
    const blob = cache.getBlockBlobClient(`${cacheKey}.csv`);
    const dl = await blob.download();
    const buf = await streamToBuffer(dl.readableStreamBody);

    // Parse rows
    const lines = csvToRows(buf);
    if (lines.length === 0) {
      await writeStatus(runId, { state: "Failed", message: "Empty CSV in cache", processedRows: 0 });
      return;
    }
    if (lines.length > CH_STRATEGIC_MAX_ROWS) {
      await writeStatus(runId, { state: "Failed", message: `Too many rows (max ${CH_STRATEGIC_MAX_ROWS})`, processedRows: 0 });
      return;
    }

    const headers = lines[0].split(",");
    const dataLines = lines.slice(1);
    await writeStatus(runId, { state: "Analyzing", message: "Chunked processing started", totalRows: lines.length, processedRows: 0 });

    const items = [];
    let processed = 0;
    const chunkSize = Math.max(1, CH_STRATEGIC_CHUNK_SIZE);

    for (let i = 0; i < dataLines.length; i += chunkSize) {
      const chunk = dataLines.slice(i, i + chunkSize);
      for (const line of chunk) {
        const cells = line.split(",");
        const obj = {};
        headers.forEach((h, idx) => { obj[h.trim()] = (cells[idx] ?? "").trim(); });
        obj.__rowLength = cells.length;
        items.push(obj);
      }
      processed += chunk.length;
      await writeStatus(runId, { processedRows: processed, message: `Processing… ${processed}/${dataLines.length}` });
    }

    // Finalize artifacts
    const results = {
      runId,
      generatedAt: nowIso(),
      summary: { rows: items.length },
      items
    };
    const logObj = {
      runId,
      version: "1.0.0",
      info: [`Analyzed ${items.length} rows`, `Headers: ${headers.join('|')}`, `Chunk size: ${chunkSize}`],
      warnings: [],
      errors: []
    };

    const resultsPath = `${runId}/results.json`;
    const logPath = `${runId}/log.json`;
    await putJson(CHS_OUT_CONTAINER, resultsPath, results);
    await putJson(CHS_OUT_CONTAINER, logPath, logObj);

    await writeStatus(runId, {
      state: "Completed",
      message: "Completed",
      resultUrl: `/${CHS_OUT_CONTAINER}/${resultsPath}`,
      logUrl: `/${CHS_OUT_CONTAINER}/${logPath}`
    });

  } catch (e) {
    context.log.error("Worker failure:", e?.message || e);
    // Best effort: we can’t read runId if JSON parse failed; guard it.
    try {
      const maybe = typeof msg === "string" ? JSON.parse(msg) : msg;
      if (maybe?.runId) {
        await writeStatus(maybe.runId, { state: "Failed", message: e?.message || "Unexpected error" });
      }
    } catch { /* no-op */ }
  }
};
