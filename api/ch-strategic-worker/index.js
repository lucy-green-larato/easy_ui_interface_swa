// 27/09/2025  ch-strategic worker (queue-trigger)
// Consumes jobs from CH_STRATEGIC_JOBS_QUEUE, updates status, and writes results/log JSON.
// Artifacts:
//   - Status:  ch-strategic-status/<runId>.json
//   - Results: ch-strategic-out/<runId>.json
//   - Log:     ch-strategic-out/<runId>.log.json

'use strict';

// Shared config (same source of truth as the router)
const {
  // storage + clients
  blobSvc,
  AZURE_STORAGE,
  CHS_STATUS_CONTAINER,
  CHS_OUT_CONTAINER,
  CHS_CACHE_CONTAINER,
  // business limits (optional diagnostics)
  CH_STRATEGIC_MAX_ROWS,
  CH_STRATEGIC_CHUNK_SIZE,
  // optional: queue name if you want to log it
  CHS_JOBS_QUEUE,
} = require('../ch-strategic/config');

// ---------- small helpers ----------
function nowIso() { return new Date().toISOString(); }

function streamToBuffer(readable) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readable.on('data', (d) => chunks.push(d));
    readable.on('end', () => resolve(Buffer.concat(chunks)));
    readable.on('error', reject);
  });
}

async function getJsonIfExists(containerClient, name) {
  try {
    const blob = containerClient.getBlockBlobClient(name);
    const dl = await blob.download();
    const buf = await streamToBuffer(dl.readableStreamBody);
    return JSON.parse(buf.toString('utf8'));
  } catch {
    return {};
  }
}

async function putJson(containerClient, name, obj) {
  const body = Buffer.from(typeof obj === 'string' ? obj : JSON.stringify(obj));
  const blob = containerClient.getBlockBlobClient(name);
  await blob.upload(body, body.length, {
    overwrite: true,
    blobHTTPHeaders: { blobContentType: 'application/json' },
  });
}

async function writeStatus(statusC, runId, patch) {
  const name = `${runId}.json`;
  const current = await getJsonIfExists(statusC, name);
  const merged = {
    ...current,
    ...patch,
    runId,
    updatedAt: nowIso(),
  };
  await putJson(statusC, name, merged);
  return merged;
}

// ---------- worker entry ----------
module.exports = async function (context, queueItem) {
  const t0 = Date.now();

  // Guard: storage must be configured (host will also require this)
  if (!AZURE_STORAGE || !blobSvc) {
    context.log.error('ch-strategic-worker: AzureWebJobsStorage not configured');
    return;
  }

  // Normalise queue message (expected from /start)
  let msg = queueItem;
  if (typeof msg === 'string') {
    try { msg = JSON.parse(msg); } catch { /* leave as string if not JSON */ }
  }
  const {
    runId,
    cacheKey,              // optional hint for cached upload
    evidenceTag,           // passthrough from /start
    totalRows,             // optional (router may have set this)
    submittedAt,           // optional
    correlationId,         // optional
  } = msg || {};

  if (!runId) {
    context.log.error('ch-strategic-worker: missing runId', { msg });
    return;
  }

  // Containers
  const statusC = blobSvc.getContainerClient(CHS_STATUS_CONTAINER);
  const outC    = blobSvc.getContainerClient(CHS_OUT_CONTAINER);
  const cacheC  = blobSvc.getContainerClient(CHS_CACHE_CONTAINER);

  // Dev-friendly: ensure containers exist (noop if already there)
  await Promise.allSettled([
    statusC.createIfNotExists(),
    outC.createIfNotExists(),
    cacheC.createIfNotExists(),
  ]);

  // Idempotency: if results already exist, mark Completed & exit
  const resultsBlob = outC.getBlockBlobClient(`${runId}.json`);
  try {
    await resultsBlob.getProperties();
    await writeStatus(statusC, runId, {
      state: 'Completed',
      message: 'Results already present; skipping reprocess',
      completedAt: nowIso(),
      evidenceTag, totalRows, submittedAt, correlationId,
      limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE },
    });
    context.log('ch-strategic-worker: idempotent complete', { runId });
    return;
  } catch { /* not found → proceed */ }

  // Mark Processing
  await writeStatus(statusC, runId, {
    state: 'Processing',
    startedAt: nowIso(),
    evidenceTag, totalRows, submittedAt, correlationId,
    limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE },
    queue: CHS_JOBS_QUEUE || null,
  });

  try {
    // ----- Example processing skeleton -----
    // If you persist uploaded CSV into cache, attempt to read it here.
    // We don't assume a specific naming; try cacheKey first, then runId as fallback.
    let inputSummary = { rows: totalRows ?? null, source: null };
    if (cacheKey) {
      try {
        const blob = cacheC.getBlockBlobClient(`${cacheKey}`);
        const props = await blob.getProperties();
        inputSummary.source = `cache:${cacheKey}`;
        inputSummary.bytes = Number(props.contentLength || 0);
      } catch { /* ignore if not found */ }
    }
    if (!inputSummary.source) {
      try {
        const blob = cacheC.getBlockBlobClient(`${runId}`);
        const props = await blob.getProperties();
        inputSummary.source = `cache:${runId}`;
        inputSummary.bytes = Number(props.contentLength || 0);
      } catch { /* ignore */ }
    }

    // Do your domain logic here (placeholder: echo the message)
    const results = {
      ok: true,
      runId,
      processedAt: nowIso(),
      evidenceTag: evidenceTag || null,
      input: inputSummary,
    };

    // Write results + a lightweight log
    await putJson(outC, `${runId}.json`, results);
    await putJson(outC, `${runId}.log.json`, {
      runId,
      correlationId: correlationId || null,
      events: [
        { at: nowIso(), event: 'worker_started', queue: CHS_JOBS_QUEUE || null },
        { at: nowIso(), event: 'worker_completed', durationMs: Date.now() - t0 },
      ],
    });

    // Final status
    await writeStatus(statusC, runId, {
      state: 'Completed',
      message: 'Completed',
      resultUrl: `blob://${CHS_OUT_CONTAINER}/${runId}.json`,
      logUrl:    `blob://${CHS_OUT_CONTAINER}/${runId}.log.json`,
      completedAt: nowIso(),
    });

    context.log('ch-strategic-worker: completed', { runId, ms: Date.now() - t0 });
  } catch (err) {
    context.log.error('ch-strategic-worker: failed', { runId, error: String(err?.message || err) });
    await writeStatus(statusC, runId, {
      state: 'Failed',
      error: { code: 'worker_error', message: String(err?.message || err) },
      failedAt: nowIso(),
    });
  }
};
