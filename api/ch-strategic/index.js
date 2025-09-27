/** api/ch-strategic/index.js 27-09-2025 v3
 * ch-strategic — multi-route HTTP trigger (Node 20)
 * Endpoints:
 *   POST /api/ch-strategic/start           (multipart/form-data)
 *   GET  /api/ch-strategic/status?runId=…
 *   GET  /api/ch-strategic/download?runId=…&file=results|log
 *   POST /api/ch-strategic/feedback        (JSON { runId, note })
 *   GET  /api/ch-strategic/health
 *
 * Security:
 * - authLevel "anonymous" (SWA EasyAuth in front)
 * - Role enforcement against x-ms-client-principal: ["campaign","campaign-admin","sales-admin"]
 * - Always echo x-correlation-id in responses and include in logs.
 *
 * Storage layout (containers from env, with safe defaults for local dev):
 * - OUT:      CH_STRATEGIC_OUT_CONTAINER       →  <runId>/results.json  (and <runId>/log.json)
 * - STATUS:   CH_STRATEGIC_STATUS_CONTAINER    →  <runId>.json
 * - CACHE:    CH_STRATEGIC_CACHE_CONTAINER     →  <cacheKey>.csv
 * - FEEDBACK: CH_STRATEGIC_FEEDBACK_CONTAINER  →  <runId>/feedback-<ts>.json
 * - JOBS:     CH_STRATEGIC_JOBS_QUEUE          →  queue message per run (required; worker processes chunks)
 *
 * Status blob schema (<STATUS>/<runId>.json):
 *   {
 *     "runId": string,
 *     "state": "Received"|"Parsing"|"Analyzing"|"Completed"|"Failed",
 *     "submittedAt": iso8601,
 *     "updatedAt": iso8601,
 *     "totalRows": number|null,
 *     "processedRows": number|null,
 *     "message": string|null,
 *     "resultUrl": string|null,  // blob path (not SAS)
 *     "logUrl": string|null,     // blob path (not SAS)
 *     "cacheKey": string|null,
 *     "evidenceTag": string|null
 *   }
 *
 * Results blob schema (<OUT>/<runId>/results.json):
 *   {
 *     "runId": string,
 *     "generatedAt": iso8601,
 *     "summary": { "rows": number, "evidenceTag"?: string },
 *     "items": [ { ...rowProjection } ]
 *   }
 */

'use strict';

const Busboy = require('busboy');
const { BlobServiceClient } = require('@azure/storage-blob');
const { QueueClient } = require('@azure/storage-queue');
const crypto = require('crypto');
const { ensureCorrelationId } = require("../lib/auth");
const { requireAuth, ensureCorrelationId } = require('../lib/auth.js');
const { requireRole } = require('../lib/auth');

// Use centralised config (router + worker share this)
const {
  ALLOWED_ROLES,
  MAX_UPLOAD_BYTES,
  DEFAULT_ALLOWED_UPLOAD_MIME,
  CH_STRATEGIC_MAX_ROWS,
  CH_STRATEGIC_CHUNK_SIZE,
  CHS_OUT_CONTAINER,
  CHS_STATUS_CONTAINER,
  CHS_CACHE_CONTAINER,
  CHS_FEEDBACK_CONTAINER,
  blobSvc,
  queueClient,
} = require("./config");

const CORS = Object.freeze({
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, content-type, x-ms-client-principal, x-correlation-id'
});

const MIME_JSON = 'application/json; charset=utf-8';

// ---------- Small helpers ----------
function jsonRes(status, body, cid, headers = {}) {
  return {
    status,
    headers: { 'content-type': MIME_JSON, 'x-correlation-id': cid, ...CORS, ...headers },
    body: JSON.stringify(body)
  };
}
function nowIso() { return new Date().toISOString(); }
function err(status, code, message, cid, extra = {}) {
  return {
    status,
    headers: { "Content-Type": "application/json", "x-correlation-id": cid },
    body: JSON.stringify({ error: code, message, ...extra })
  };
}
function getBodyBuffer(req) {
  if (Buffer.isBuffer(req.body)) return req.body;
  if (typeof req.body === "string") return Buffer.from(req.body);
  if (Buffer.isBuffer(req.rawBody)) return req.rawBody;       // fallback
  if (typeof req.rawBody === "string") return Buffer.from(req.rawBody);
  return null;
}
function newId() { return crypto.randomUUID(); }
function maxUploadLabel() {
  const mb = Math.max(1, Math.floor(Number(MAX_UPLOAD_BYTES || 0) / 1048576));
  return `${mb}MB`;
}

// Auth shim: works with both throwing and non-throwing requireAuth implementations
function authGate(req, cid) {
  try {
    const result = requireAuth(req, ALLOWED_ROLES);
    // If helper returns a gate object
    if (result && typeof result === 'object' && ('ok' in result)) {
      if (result.ok) return { ok: true, principal: result.principal || null };
      return { ok: false, res: err(result.code || 403, result.error || 'forbidden', result.message || 'Forbidden', cid) };
    }
    // Otherwise: assume it returned a principal
    return { ok: true, principal: result || null };
  } catch (e) {
    if (e && typeof e === 'object' && 'status' in e && 'body' in e) {
      return { ok: false, res: { status: e.status, headers: { 'x-correlation-id': cid, ...CORS, 'content-type': MIME_JSON }, body: JSON.stringify(e.body) } };
    }
    return { ok: false, res: err(403, 'forbidden', 'Forbidden', cid) };
  }
}

async function ensureContainers() {
  const names = [CHS_OUT_CONTAINER, CHS_STATUS_CONTAINER, CHS_CACHE_CONTAINER, CHS_FEEDBACK_CONTAINER];
  await Promise.all(names.map(async name => {
    const c = blobSvc.getContainerClient(name);
    await c.createIfNotExists();
  }));
  if (queueClient) {
    try { await queueClient.createIfNotExists(); } catch { /* ignore */ }
  }
}

// ---------- CSV utilities ----------
function estimateCsvRows(buffer) {
  const text = buffer.toString('utf8');
  if (!text) return 0;
  const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
  return lines.length;
}

async function readMultipartCsv(context, req, cid) {
  return new Promise((resolve, reject) => {
    const bb = Busboy({ headers: req.headers, limits: { fileSize: MAX_UPLOAD_BYTES } });

    const chunks = [];
    let total = 0;
    let gotFile = false;
    let aborted = false;

    let filename = "upload.csv";
    let mimeType = "";
    const fields = {};

    function failOnce(payload) {
      if (aborted) return;
      aborted = true;
      try { bb.removeAllListeners(); } catch { }
      reject(payload);
    }

    bb.on("file", (_name, stream, info) => {
      gotFile = true;
      filename = info?.filename || filename;
      mimeType = String(info?.mimeType || "").toLowerCase();

      stream.on("data", (chunk) => {
        if (aborted) return;
        total += chunk.length;
        if (total > MAX_UPLOAD_BYTES) {
          try { stream.resume(); } catch { }
          return failOnce({ code: 413, error: "payload_too_large", message: `Max ${maxUploadLabel()}` });
        }
        chunks.push(chunk);
      });

      stream.on("limit", () => failOnce({ code: 413, error: "payload_too_large", message: `Max ${maxUploadLabel()}` }));
      stream.on("error", (e) => failOnce({ code: 500, error: "internal", message: e?.message || "Stream error" }));
    });

    bb.on("field", (name, val) => {
      if (aborted) return;
      fields[name] = typeof val === "string" ? val.trim() : val;
    });

    bb.on("error", (e) => failOnce({ code: 500, error: "internal", message: e?.message || "Parse error" }));

    bb.on("finish", () => {
      if (aborted) return;
      if (!gotFile) return failOnce({ code: 400, error: "bad_request", message: "No file found (multipart/form-data required)" });

      const allowed = DEFAULT_ALLOWED_UPLOAD_MIME;
      const looksCsvByExt = /\.csv$/i.test(filename || "");
      const isAllowedMime = !!mimeType && allowed.includes(mimeType);

      if (!isAllowedMime && !looksCsvByExt) {
        return failOnce({ code: 415, error: "unsupported_media_type", message: "CSV required" });
      }

      const buffer = chunks.length ? Buffer.concat(chunks, total) : Buffer.alloc(0);
      resolve({ buffer, filename, mimeType: mimeType || (looksCsvByExt ? "text/csv" : ""), fields });
    });

    // Feed Busboy with buffer (Functions req is not a stream)
    const bodyBuf = Buffer.isBuffer(req.body)
      ? req.body
      : (req.rawBody
        ? (Buffer.isBuffer(req.rawBody) ? req.rawBody : Buffer.from(req.rawBody))
        : Buffer.alloc(0));
    bb.end(bodyBuf);
  }).catch((e) => {
    if (e && e.code) throw e;
    throw { code: 500, error: "internal", message: e?.message || "Unexpected error" };
  });
}

// ---------- Blob helpers ----------
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
  const body = Buffer.from(JSON.stringify(merged));
  await b.upload(body, body.length, {
    overwrite: true,
    blobHTTPHeaders: { blobContentType: "application/json" }
  });
  return merged;
}

async function putJson(containerName, blobPath, obj) {
  const c = blobSvc.getContainerClient(containerName);
  const b = c.getBlockBlobClient(blobPath);
  const body = JSON.stringify(obj);
  await b.upload(body, Buffer.byteLength(body), { overwrite: true });
  return `/${containerName}/${blobPath}`;
}

async function getJson(containerName, blobPath) {
  const c = blobSvc.getContainerClient(containerName);
  const b = c.getBlockBlobClient(blobPath);
  const dl = await b.download();
  const buf = await streamToBuffer(dl.readableStreamBody);
  return JSON.parse(buf.toString('utf8'));
}

function streamToBuffer(readable) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readable.on("data", d => chunks.push(d));
    readable.on("end", () => resolve(Buffer.concat(chunks)));
    readable.on("error", reject);
  });
}

// ---------- Handlers ----------
async function handleStart(context, req, cid) {
  if (!blobSvc)   return err(500, "internal", "AzureWebJobsStorage not configured", cid);
  if (!queueClient) return err(500, "internal", "Jobs queue not configured", cid);

  // must be multipart/form-data
  const contentType = String(req.headers?.["content-type"] || "");
  if (!contentType.toLowerCase().startsWith("multipart/form-data")) {
    return err(400, "bad_request", "Content-Type must be multipart/form-data", cid, { contentType });
  }

  const bodyBuf = getBodyBuffer(req);
  if (!bodyBuf) return err(400, "bad_request", "Request body required", cid);

  // parse with Busboy
  const bb = Busboy({ headers: { "content-type": contentType }, limits: { fileSize: MAX_UPLOAD_BYTES } });
  const allowed = new Set(DEFAULT_ALLOWED_UPLOAD_MIME);
  let fileBuf = null, fileMime = null, evidenceTag = null;

  const parsed = new Promise((resolve, reject) => {
    bb.on("file", (field, file, info = {}) => {
      if (field !== "csv_file") { file.resume(); return; }
      fileMime = info.mimeType || info.mime || "application/octet-stream";
      if (fileMime && !allowed.has(String(fileMime).toLowerCase())) {
        file.resume(); reject(err(415, "unsupported_media_type", "Only CSV uploads are allowed", cid, { mime: fileMime })); return;
      }
      streamToBuffer(file).then(buf => { fileBuf = buf; }).catch(reject);
    });
    bb.on("field", (name, val) => {
      if (name === "evidenceTag") evidenceTag = String(val || "").slice(0,128);
    });
    bb.on("error", reject);
    bb.on("finish", resolve);
  });

  bb.end(bodyBuf);
  try { await parsed; } catch (e) { return e; }

  if (!fileBuf) return err(400, "bad_request", "csv_file is required", cid);
  if (fileBuf.length > MAX_UPLOAD_BYTES) return err(413, "payload_too_large", "CSV exceeds MAX_UPLOAD_BYTES", cid);

  // count rows quickly and clamp
  const text = fileBuf.toString("utf8");
  const totalRows = Math.max(0, (text.match(/\r?\n/g)?.length || 1) - 1);
  const clippedRows = Math.min(totalRows, CH_STRATEGIC_MAX_ROWS);

  // persist raw upload into cache (keyed by runId)
  const runId = crypto.randomUUID();
  const cacheC = blobSvc.getContainerClient(CHS_CACHE_CONTAINER);
  await cacheC.createIfNotExists();
  await cacheC.getBlockBlobClient(runId).upload(fileBuf, fileBuf.length, { overwrite: true });

  // initial status
  await writeStatus(runId, {
    state: "Received",
    submittedAt: nowIso(),
    message: `Received ${clippedRows} rows via upload`,
    totalRows: clippedRows,
    evidenceTag,
    limits: { maxRows: CH_STRATEGIC_MAX_ROWS, chunkSize: CH_STRATEGIC_CHUNK_SIZE }
  });

  // enqueue job
  await queueClient.createIfNotExists(); // no-op in prod; handy in dev
  const msg = { runId, cacheKey: runId, evidenceTag, totalRows: clippedRows, submittedAt: nowIso(), correlationId: cid };
  await queueClient.sendMessage(Buffer.from(JSON.stringify(msg)).toString("base64"));

  // return 202 with runId
  return {
    status: 202,
    headers: { "Content-Type": "application/json", "x-correlation-id": cid },
    body: JSON.stringify({
      ok: true, runId,
      statusUrl: `/api/ch-strategic/status?runId=${runId}`,
      downloads: {
        results: `/api/ch-strategic/download?runId=${runId}&file=results`,
        log:     `/api/ch-strategic/download?runId=${runId}&file=log`
      }
    })
  };
}

async function handleStatus(_context, req, cid) {
  const runId = (req.query?.runId || '').trim();
  if (!runId) return err(400, 'bad_request', 'Missing runId', cid);
  try {
    const status = await getJson(CHS_STATUS_CONTAINER, `${runId}.json`);
    return jsonRes(200, status, cid);
  } catch {
    return err(404, 'not_found', 'Status not found', cid);
  }
}

async function handleDownload(_context, req, cid) {
  const runId = (req.query?.runId || '').trim();
  const file = (req.query?.file || '').toLowerCase();
  if (!runId) return err(400, 'bad_request', 'Missing runId', cid);
  if (!['results', 'log'].includes(file)) return err(400, 'bad_request', 'Missing or invalid file parameter', cid);

  const path = `${runId}/${file}.json`;
  try {
    const container = blobSvc.getContainerClient(CHS_OUT_CONTAINER);
    const blob = container.getBlockBlobClient(path);
    const dl = await blob.download();

    return {
      status: 200,
      headers: {
        'content-type': 'application/json; charset=utf-8',
        'content-disposition': `attachment; filename="${file}-${runId}.json"`,
        'x-correlation-id': cid,
        ...CORS
      },
      body: dl.readableStreamBody
    };
  } catch {
    return err(404, 'not_found', 'Artifact not found', cid);
  }
}

async function handleFeedback(_context, req, cid) {
  const gate = authGate(req, cid);
  if (!gate.ok) return gate.res;

  const body = (req.body && typeof req.body === 'object') ? req.body : {};
  const runId = (body.runId || '').toString().trim();
  const note = (body.note || '').toString().trim().slice(0, 4000);
  if (!runId || !note) {
    return err(400, 'bad_request', 'runId and note required', cid, {
      runIdPresent: Boolean(runId), notePresent: Boolean(note)
    });
  }

  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const principal = gate.principal || {};
  const payload = {
    runId,
    note,
    correlationId: cid,
    submittedAt: nowIso(),
    user: {
      id: principal.userId || 'unknown',
      name: principal.name || 'unknown',
      roles: principal.userRoles || []
    },
    client: {
      ip: req.headers['x-forwarded-for'] || req.headers['x-client-ip'] || undefined,
      ua: req.headers['user-agent'] || undefined
    }
  };

  try {
    await putJson(CHS_FEEDBACK_CONTAINER, `${runId}/feedback-${ts}.json`, payload);
  } catch {
    return err(500, 'internal', 'Failed to persist feedback', cid);
  }
  return { status: 204, headers: { 'x-correlation-id': cid, ...CORS } };
}

async function handleHealth() {
  return {
    status: 200,
    headers: { 'content-type': MIME_JSON, ...CORS },
    body: JSON.stringify({
      ok: true,
      version: '1.0.0',
      time: nowIso(),
      limits: {
        maxUploadBytes: MAX_UPLOAD_BYTES,
        maxRows: CH_STRATEGIC_MAX_ROWS,
        chunkSize: CH_STRATEGIC_CHUNK_SIZE
      }
    })
  };
}

function isPreflight(req) {
  return (req.method || '').toUpperCase() === 'OPTIONS';
}

// ----------- Azure Function entry ----------- //
module.exports = async function (context, req) {
  console.log("CH-STRATEGIC ROUTER", {
    method: req?.method,
    url: req?.url,
    params: req?.params,
    query: req?.query,
    path: req?.params && req.params.path
  });

  const cid = ensureCorrelationId(req);


  try {
    // 1) CORS preflight (no auth)
    if (isPreflight(req)) {
      context.res = { status: 204, headers: { ...CORS, "x-correlation-id": cid } };
      return;
    }

    // 2) AUTH (returns 401/403 when not allowed)
    const authErr = requireRole(req, cid, CORS);
    if (authErr) {
      context.res = authErr;
      return;
    }

    // 3) Storage check
    if (!blobSvc) {
      context.res = err(500, "internal", "AzureWebJobsStorage not configured", cid);
      return;
    }

    // 4) Routing
    const rawPath = (context.bindingData && context.bindingData.path) ? `/${context.bindingData.path}` : "/";
    const path = rawPath.toLowerCase();
    const method = (req.method || "GET").toUpperCase();

    if (method === "POST" && path === "/start") { context.res = await handleStart(context, req, cid); return; }
    if (method === "GET" && path === "/status") { context.res = await handleStatus(context, req, cid); return; }
    if (method === "GET" && path === "/download") { context.res = await handleDownload(context, req, cid); return; }
    if (method === "POST" && path === "/feedback") { context.res = await handleFeedback(context, req, cid); return; }
    if (method === "GET" && path === "/health") { context.res = await handleHealth(); return; }

    context.res = err(404, "not_found", `Unknown route: ${method} ${path}`, cid);
  } catch (e) {
    context.log.error("ch-strategic unhandled", { correlationId: cid, error: e?.message });
    context.res = err(500, "internal", "Unexpected error", cid);
  }
};
