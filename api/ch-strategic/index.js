/** api/ch-strategic/index.js 20-10-2025 v9
 * ch-strategic — multi-route HTTP trigger (Node 20)
 * Endpoints:
 *   POST /api/ch-strategic/start           (multipart/form-data)
 *   GET  /api/ch-strategic/status?runId=…
 *   GET  /api/ch-strategic/download?runId=…&file=results|log|csv
 *   POST /api/ch-strategic/feedback        (JSON { runId, note })
 *   GET  /api/ch-strategic/health
 *
 * Security:
 * - authLevel "anonymous" (SWA EasyAuth in front)
 * - Role enforcement against x-ms-client-principal: ["campaign","campaign-admin","sales-admin"]
 * - Always echo x-correlation-id in responses and include in logs.
 *
 * Storage layout (container names from env; safe defaults in ./config):
 * - OUT:      CH_STRATEGIC_OUT_CONTAINER       →  <runId>.json, <runId>.log.json, <runId>.csv
 * - STATUS:   CH_STRATEGIC_STATUS_CONTAINER    →  <runId>.json
 * - CACHE:    CH_STRATEGIC_CACHE_CONTAINER     →  <runId>   (raw upload)
 * - FEEDBACK: CH_STRATEGIC_FEEDBACK_CONTAINER  →  <runId>/feedback-<ts>.json
 * - JOBS:     CH_STRATEGIC_JOBS_QUEUE          →  queue message per run
 */

'use strict';
const { requireAuth, ensureCorrelationId } = require('../lib/auth');

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
  STANDARD_MAX_ITEMS,
  RECORD_SOFT_TIMEOUT_MS,
  RECORD_MAX_ATTEMPTS,
  blobSvc,
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
    headers: { 'Content-Type': MIME_JSON, 'x-correlation-id': cid, ...CORS, ...headers },
    body: JSON.stringify(body)
  };
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function blobExists(containerClient, name) {
  try {
    return await containerClient.getBlockBlobClient(name).exists();
  } catch {
    return false;
  }
}
function buildResultsCsv(items = []) {
  const headers = ["Company Name", "Company Number", "Matched", "Details"];
  const esc = (v) => {
    const s = (v ?? "").toString();
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const rows = (items || []).map((it) => ({
    "Company Name": it.companyName || it["Company Name"] || "",
    "Company Number": it.companyNumber || it["Company Number"] || "",
    Matched: typeof it.matched === "boolean" ? (it.matched ? "yes" : "no") : (it.matched ? String(it.matched) : ""),
    Details: it.details || it.message || it.snippet || "",
  }));
  return [headers.join(","), ...rows.map(r => headers.map(h => esc(r[h])).join(","))].join("\r\n");
}
function nowIso() { return new Date().toISOString(); }
function err(status, code, message, cid, details = undefined) {
  const payload = details ? { error: code, message, details } : { error: code, message };
  return {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'x-correlation-id': cid, ...CORS },
    body: JSON.stringify(payload)
  };
}
function isPreflight(req) {
  return (req.method || '').toUpperCase() === 'OPTIONS';
}
// ---------- Blob helpers ----------
function streamToBuffer(readable) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readable.on("data", d => chunks.push(d));
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
  const body = Buffer.from(JSON.stringify(merged));
  await b.upload(body, body.length, {
    overwrite: true,
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }
  });
  return merged;
}

async function putJson(containerName, blobPath, obj) {
  const c = blobSvc.getContainerClient(containerName);
  await c.createIfNotExists();
  const b = c.getBlockBlobClient(blobPath);
  const body = JSON.stringify(obj);
  await b.upload(body, Buffer.byteLength(body), {
    overwrite: true,
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }
  });
  return `/${containerName}/${blobPath}`;
}

async function getJson(containerName, blobPath) {
  const c = blobSvc.getContainerClient(containerName);
  const b = c.getBlockBlobClient(blobPath);
  const dl = await b.download();
  const buf = await streamToBuffer(dl.readableStreamBody);
  return JSON.parse(buf.toString('utf8'));
}

async function handleStatus(_context, req, cid) {
  const runId = (req.query?.runId || '').trim();
  if (!runId) return err(400, 'bad_request', 'Missing runId', cid);
  const outC = blobSvc.getContainerClient(CHS_OUT_CONTAINER);
  const statusName = `${runId}.json`;
  const resultsName = `${runId}.json`;
  const logName = `${runId}.log.json`;
  const csvName = `${runId}.csv`;

  async function existsAll() {
    const [hasResults, hasCsv, hasLog] = await Promise.all([
      blobExists(outC, resultsName),
      blobExists(outC, csvName),
      blobExists(outC, logName),
    ]);
    return { hasResults, hasCsv, hasLog, ready: hasResults && hasCsv && hasLog };
  }

  try {
    const status = await getJson(CHS_STATUS_CONTAINER, statusName);
    const state = String(status?.state || "").toLowerCase();
    const { ready } = await existsAll();

    if (state !== "completed" && state !== "failed") {
      if (ready) {
        const completed = {
          ...status,
          state: "Completed",
          resultUrl: `blob://${CHS_OUT_CONTAINER}/${resultsName}`,
          logUrl: `blob://${CHS_OUT_CONTAINER}/${logName}`,
          csvUrl: `blob://${CHS_OUT_CONTAINER}/${csvName}`,
          updatedAt: nowIso(),
          message: status?.message || "Completed",
        };
        await writeStatus(runId, completed);
        return jsonRes(200, completed, cid);
      }
      return jsonRes(200, status, cid); // includes preview during Processing
    }

    if (state === "completed") {
      if (ready) return jsonRes(200, status, cid);
      // don't downgrade file on disk; just advertise Processing until blobs catch up
      return jsonRes(200, { ...status, state: "Processing", message: "Finalizing outputs…", updatedAt: nowIso() }, cid);
    }

    return jsonRes(200, status, cid);

  } catch {
    const { ready } = await existsAll().catch(() => ({ ready: false }));
    if (ready) {
      const completed = {
        runId,
        state: "Completed",
        synthesized: true,
        resultUrl: `blob://${CHS_OUT_CONTAINER}/${resultsName}`,
        logUrl: `blob://${CHS_OUT_CONTAINER}/${logName}`,
        csvUrl: `blob://${CHS_OUT_CONTAINER}/${csvName}`,
        updatedAt: nowIso(),
      };
      await writeStatus(runId, completed);
      return jsonRes(200, completed, cid);
    }
    return err(404, 'not_found', 'Status not found', cid);
  }
}

async function handleDownload(context, req, cid) {
  const runId = (req.query?.runId || "").trim();
  const file = (req.query?.file || "").trim().toLowerCase();

  if (!runId) return err(400, "bad_request", "Missing runId", cid);
  if (!["results", "log", "csv"].includes(file)) {
    return err(400, "bad_request", "file must be 'results', 'log', or 'csv'", cid);
  }
  if (!blobSvc) return err(500, "internal", "Storage not available", cid);

  const c = blobSvc.getContainerClient(CHS_OUT_CONTAINER);
  const resultsName = `${runId}.json`;
  const logName = `${runId}.log.json`;
  const csvName = `${runId}.csv`;

  const MAX_TRIES = 10;   // ~2.5s total
  const PAUSE_MS = 250;

  // Helper: download blob to string
  async function downloadText(containerClient, blobName) {
    const dl = await containerClient.getBlockBlobClient(blobName).download();
    const chunks = [];
    for await (const ch of dl.readableStreamBody) chunks.push(ch);
    return Buffer.concat(chunks).toString("utf8");
  }

  try {
    if (file === "csv") {
      // Try the CSV directly; if not present but results.json is, synthesize once.
      for (let i = 0; i < MAX_TRIES; i++) {
        if (await blobExists(c, csvName)) {
          const dl = await c.getBlockBlobClient(csvName).download();
          context.res = {
            status: 200,
            headers: {
              ...CORS,
              "x-correlation-id": cid,
              "Content-Type": "text/csv; charset=utf-8",
              "Content-Disposition": `attachment; filename="ch-strategic-${runId}.csv"`
            },
            body: dl.readableStreamBody
          };
          return;
        }
        if (await blobExists(c, resultsName)) {
          const text = await downloadText(c, resultsName);
          const results = JSON.parse(text);
          const csv = buildResultsCsv(results.items || []);
          await c.getBlockBlobClient(csvName).upload(csv, Buffer.byteLength(csv), {
            overwrite: true,
            blobHTTPHeaders: { blobContentType: "text/csv; charset=utf-8" }
          });
          context.res = {
            status: 200,
            headers: {
              ...CORS,
              "x-correlation-id": cid,
              "Content-Type": "text/csv; charset=utf-8",
              "Content-Disposition": `attachment; filename="ch-strategic-${runId}.csv"`
            },
            body: csv
          };
          return;
        }
        await sleep(PAUSE_MS);
      }
      return err(404, "not_found", "Artifact not found", cid);
    }

    // JSON artifacts (results/log) with the same brief wait + JSON validation
    const name = file === "results" ? resultsName : logName;

    for (let i = 0; i < MAX_TRIES; i++) {
      if (!(await blobExists(c, name))) {
        await sleep(PAUSE_MS);
        continue;
      }
      let text = "";
      try {
        text = await downloadText(c, name);
      } catch {
        await sleep(PAUSE_MS);
        continue;
      }
      const trimmed = (text || "").trim();
      if (trimmed && /^[{\[]/.test(trimmed)) {
        try {
          JSON.parse(trimmed); // validate only
          context.res = {
            status: 200,
            headers: {
              ...CORS,
              "x-correlation-id": cid,
              "Content-Type": "application/json; charset=utf-8",
              "Cache-Control": "no-store"
            },
            body: trimmed
          };
          return;
        } catch {
          // not valid yet; wait and retry
        }
      }
      await sleep(PAUSE_MS);
    }
    return err(404, "not_found", "Artifact not found", cid);

  } catch (e) {
    if (e?.statusCode === 404 || e?.code === "BlobNotFound") {
      context.res = err(404, "not_found", "Artifact not found", cid);
      return;
    }
    context.log.error("download failed", { correlationId: cid, error: e?.message });
    context.res = err(500, "internal", "Download failed", cid);
  }
}

async function handleFeedback(_context, req, cid) {
  // Strict auth gate — return standard envelope on failure
  try {
    requireAuth(_context, req, ALLOWED_ROLES);
  } catch (resp) {
    return {
      status: resp.status || 401,
      headers: { ...CORS, "x-correlation-id": cid, "Content-Type": "application/json; charset=utf-8" },
      body: JSON.stringify(resp.body || { error: "unauthenticated", message: "Auth required" })
    };
  }

  const body = (req.body && typeof req.body === 'object') ? req.body : {};
  const runId = (body.runId || '').toString().trim();
  const note = (body.note || '').toString().trim().slice(0, 4000);
  if (!runId || !note) {
    return err(400, 'bad_request', 'runId and note required', cid, {
      runIdPresent: Boolean(runId), notePresent: Boolean(note)
    });
  }

  const ts = new Date().toISOString().replace(/[:.]/g, '-');

  // Optional principal extraction (don’t fail if missing)
  let principal = null;
  try {
    // Use the already-imported requireAuth only to parse principal metadata
    const gate = requireAuth(_context, req, ALLOWED_ROLES);
    principal = gate?.principal || gate || null;
  } catch { /* ignore — proceed without enriched principal */ }

  const payload = {
    runId,
    note,
    correlationId: cid,
    submittedAt: nowIso(),
    user: {
      id: principal?.userId || 'unknown',
      name: principal?.name || 'unknown',
      roles: principal?.userRoles || []
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
    headers: { 'Content-Type': MIME_JSON, ...CORS },
    body: JSON.stringify({
      ok: true,
      version: '1.0.0',
      time: nowIso(),
      limits: {
        maxUploadBytes: MAX_UPLOAD_BYTES,
        maxRows: CH_STRATEGIC_MAX_ROWS,
        chunkSize: CH_STRATEGIC_CHUNK_SIZE,
        standardMaxItems: STANDARD_MAX_ITEMS,
        recordSoftTimeoutMs: RECORD_SOFT_TIMEOUT_MS,
        recordMaxAttempts: RECORD_MAX_ATTEMPTS
      }
    })
  };
}

// ----------- Azure Function entry ----------- //
module.exports = async function (context, req) {
  // Trace
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

    // 2) Routing target (compute before auth to allow /health unauthenticated)
    const rawPath = (context.bindingData && context.bindingData.path) ? `/${context.bindingData.path}` : "/";
    let path = rawPath.toLowerCase();
    if (path.length > 1 && path.endsWith("/")) path = path.replace(/\/+$/, "");
    const method = (req.method || "GET").toUpperCase();

    // Health is intentionally open
    if ((method === "GET" || method === "HEAD") && path === "/health") { context.res = await handleHealth(); return; }

    // 3) AUTH (only for /feedback; /health, /status, /download are open)
    if (method === "POST" && path === "/feedback") {
      try {
        requireAuth(context, req, ALLOWED_ROLES);
      } catch (resp) {
        context.res = {
          status: resp.status || 401,
          headers: { ...CORS, "x-correlation-id": cid, "Content-Type": "application/json; charset=utf-8" },
          body: JSON.stringify(resp.body || { error: "unauthenticated", message: "Auth required" })
        };
        return;
      }
    }

    // 4) Storage check
    if (!blobSvc) {
      context.res = err(500, "internal", "AzureWebJobsStorage not configured", cid);
      return;
    }

    // 5) Routes
    if (method === "GET" && path === "/status") { context.res = await handleStatus(context, req, cid); return; }
    if (method === "GET" && path === "/download") { await handleDownload(context, req, cid); return; }
    if (method === "POST" && path === "/feedback") { context.res = await handleFeedback(context, req, cid); return; }

    context.res = err(404, "not_found", `Unknown route: ${method} ${path}`, cid);
  } catch (e) {
    context.log.error("ch-strategic unhandled", { correlationId: cid, error: e?.message });
    context.res = err(500, "internal", "Unexpected error", cid, String(e && e.message || e));
  }
};
