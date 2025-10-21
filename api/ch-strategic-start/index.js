//--- api/ch-strategic/start/index.js 20-10-2025 v7 (add more variables, size guard + preflight + output binding)
"use strict";
const Busboy = require("busboy");
const crypto = require("crypto");
const {
  blobSvc,
  CHS_STATUS_CONTAINER,
  CHS_OUT_CONTAINER,
  CHS_CACHE_CONTAINER,
  CH_STRATEGIC_JOBS_QUEUE,
  STANDARD_MAX_ITEMS,
  RECORD_SOFT_TIMEOUT_MS,
  RECORD_MAX_ATTEMPTS,
  MAX_UPLOAD_BYTES,
  CH_STRATEGIC_MAX_ROWS,
  CH_STRATEGIC_CHUNK_SIZE,
  ensureInfrastructure,
} = require("../ch-strategic/config");

function bad(status, code, message, extra) {
  return {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
    body: extra ? { error: code, message, ...extra } : { error: code, message }
  };
}
const nowIso = () => new Date().toISOString();

async function putJson(containerName, blobPath, obj) {
  const c = blobSvc.getContainerClient(containerName);
  await c.createIfNotExists();
  const b = c.getBlockBlobClient(blobPath);
  const body = JSON.stringify(obj);
  await b.upload(body, Buffer.byteLength(body), {
    overwrite: true,
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }
  });
}

// ✅ FIX: define a proper helper instead of the invalid `module.exports` function
async function writeStatus(runId, patch) {
  await putJson(CHS_STATUS_CONTAINER, `${runId}.json`, {
    runId, updatedAt: nowIso(), ...(patch || {})
  });
}

module.exports = async function (context, req) {
  try {
    // CORS preflight
    if ((req.method || "").toUpperCase() === "OPTIONS") {
      context.res = {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "POST,OPTIONS",
          "Access-Control-Allow-Headers": "authorization, content-type, x-ms-client-principal, x-correlation-id"
        }
      };
      return;
    }

    if (!blobSvc) { context.res = bad(500, "internal", "AzureWebJobsStorage not configured"); return; }
    await ensureInfrastructure();

    if (!/^multipart\/form-data/i.test(String(req.headers?.["content-type"] || ""))) {
      context.res = bad(400, "bad_request", "Content-Type must be multipart/form-data"); return;
    }

    // HEADERS size guard (works even if body already buffered by Functions runtime)
    const contentLen = Number(req.headers["content-length"] || 0);
    if (contentLen && MAX_UPLOAD_BYTES && contentLen > MAX_UPLOAD_BYTES) {
      context.res = bad(413, "payload_too_large", `File exceeds limit of ${MAX_UPLOAD_BYTES} bytes`,
        { maxUploadBytes: MAX_UPLOAD_BYTES, contentLength: contentLen });
      return;
    }

    // Parse multipart, enforce file size while reading
    const bb = Busboy({
      headers: req.headers,
      limits: { fileSize: MAX_UPLOAD_BYTES || undefined }
    });

    let fileBuf = null, evidenceTag = null;
    let fileTooLarge = false;

    const parsed = new Promise((resolve, reject) => {
      bb.on("file", (_field, file /*, info = {} */) => {
        const chunks = [];
        file.on("limit", () => { fileTooLarge = true; file.resume(); });
        file.on("data", d => chunks.push(d));
        file.on("end", () => { fileBuf = Buffer.concat(chunks); });
      });
      bb.on("field", (name, val) => {
        if (name === "evidenceTag") evidenceTag = String(val || "").slice(0, 128);
      });
      bb.on("error", reject);
      bb.on("finish", resolve);
    });

    // Feed busboy with raw body (Functions exposes rawBody; if not, fall back to body)
    const raw = Buffer.isBuffer(req.rawBody)
      ? req.rawBody
      : Buffer.isBuffer(req.body)
        ? req.body
        : typeof req.body === "string"
          ? Buffer.from(req.body)
          : typeof req.rawBody === "string"
            ? Buffer.from(req.rawBody)
            : Buffer.alloc(0);
    bb.end(raw);
    await parsed;

    if (fileTooLarge) {
      context.res = bad(413, "payload_too_large", `File exceeds limit of ${MAX_UPLOAD_BYTES} bytes`,
        { maxUploadBytes: MAX_UPLOAD_BYTES }); return;
    }
    if (!fileBuf?.length) { context.res = bad(400, "bad_request", "CSV file is required"); return; }

    // Count rows defensively (don’t trust client)
    const text = fileBuf.toString("utf8").replace(/^\uFEFF/, "");
    const totalRows = Math.max(0, (text.match(/\r?\n/g)?.length || 1) - 1);
    const clippedRows = Math.min(totalRows, CH_STRATEGIC_MAX_ROWS);

    // Create run
    const runId = crypto.randomUUID();

    // Cache upload
    const cacheC = blobSvc.getContainerClient(CHS_CACHE_CONTAINER);
    await cacheC.createIfNotExists();
    await cacheC.getBlockBlobClient(runId).upload(fileBuf, fileBuf.length, { overwrite: true });

    // Initial status
    await writeStatus(runId, {
      state: "Received",
      submittedAt: nowIso(),
      message: `Received ${clippedRows} rows via upload`,
      totalRows: clippedRows,
      evidenceTag,
      limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE }
    });

    // --- Standard Track guardrail + single-run enqueue (no children) ---

    // Enforce Standard Track cap AFTER we know how many rows were uploaded
    if (Number.isFinite(STANDARD_MAX_ITEMS) && clippedRows > STANDARD_MAX_ITEMS) {
      context.res = bad(
        400,
        "TooManyItems",
        `This run has ${clippedRows} items which exceeds the Standard Track limit (${STANDARD_MAX_ITEMS}). Please split your file and try again.`,
        { standardMaxItems: STANDARD_MAX_ITEMS }
      );
      return;
    }

    // One job = the entire input set, processed by the worker as the parent run.
    const parentRunId = runId;
    const message = JSON.stringify({
      runId: parentRunId,          // parent runId (canonical artifact names)
      parentRunId: null,           // single-run path
      cacheKey: parentRunId,       // worker reads the cached upload by this key
      evidenceTag,
      totalRows: clippedRows,
      rowOffset: 0,
      rowLimit: null,              // worker processes all rows
      submittedAt: nowIso(),
      correlationId: context.invocationId
    });

    // Use the existing output binding (array) with a single message
    context.bindings.outJobs = [message];

    // Initialize/refresh status for the parent run (no chunkPlan on Standard Track)
    await writeStatus(parentRunId, {
      state: "Queued",
      submittedAt: nowIso(),
      message: `Queued ${clippedRows} rows for single-run processing`,
      totalRows: clippedRows,
      evidenceTag,
      partial: false,
      limits: {
        maxRows: CH_STRATEGIC_MAX_ROWS,
        chunkSize: CH_STRATEGIC_CHUNK_SIZE,
        standardMaxItems: STANDARD_MAX_ITEMS,
        recordSoftTimeoutMs: RECORD_SOFT_TIMEOUT_MS,
        recordMaxAttempts: RECORD_MAX_ATTEMPTS
      }
    });
    // --- end Standard Track single-run enqueue ---

    context.res = {
      status: 202,
      headers: {
        "content-type": "application/json; charset=utf-8",
        "cache-control": "no-store"
      },
      body: {
        ok: true,
        runId,
        statusUrl: `/api/ch-strategic/status?runId=${runId}`,
        downloads: {
          results: `/api/ch-strategic/download?runId=${runId}&file=results`,
          log: `/api/ch-strategic/download?runId=${runId}&file=log`,
          csv: `/api/ch-strategic/download?runId=${runId}&file=csv`
        }
      }
    };
  } catch (err) {
    context.log.error("start: failed", String(err?.message || err));
    context.res = bad(500, "internal", "internal");
  }
};
