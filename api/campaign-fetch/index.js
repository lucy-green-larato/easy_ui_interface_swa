// /api/campaign-fetch/index.js  · v6 (prefix-aware + micro-retry + clean success path)
// GET /api/campaign-fetch?runId=<id>&file=<campaign|evidence_log|csv|status|outline>

const { BlobServiceClient } = require("@azure/storage-blob");

// --- small utils -------------------------------------------------------------
function genId() {
  return `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
}
async function streamToString(readable) {
  const chunks = [];
  for await (const ch of readable) chunks.push(ch);
  return Buffer.concat(chunks).toString("utf8");
}
async function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

// bounded existence retry (100ms, 250ms) plus an immediate check
async function existsWithTinyRetry(blobClient, retry) {
  const tries = retry ? [0, 100, 250] : [0];
  for (const ms of tries) {
    if (ms) await wait(ms);
    try { if (await blobClient.exists()) return true; } catch { /* transient */ }
  }
  return false;
}
async function jsonOrNull(blobClient) {
  try {
    const dl = await blobClient.download();
    const text = await streamToString(dl.readableStreamBody);
    try { return text ? JSON.parse(text) : null; } catch { return null; }
  } catch { return null; }
}

// Optional auth; fall back to noop that still returns a correlation id
let requireAuth;
try {
  ({ requireAuth } = require("../lib/auth"));
} catch {
  requireAuth = async () => ({ correlationId: genId() });
}

// --- config ------------------------------------------------------------------
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";

// Allowed external-facing artifacts only (whitelist)
const VALID_MAP = Object.freeze({
  campaign: "campaign.json",
  evidence_log: "evidence_log.json",   // UI fetches evidence directly
  csv: "csv_normalized.json",          // useful for debugging
  status: "status.json",
  outline: "outline.json"              // helpful for troubleshooting
});

// --- azure helpers -----------------------------------------------------------
function blobSvc() {
  if (!STORAGE_CONN) {
    const e = new Error("AzureWebJobsStorage is not configured");
    e.code = "config";
    throw e;
  }
  return BlobServiceClient.fromConnectionString(STORAGE_CONN);
}

/**
 * Resolve the container-relative base prefix to read artifacts from.
 * Strategy:
 *  1) Try `runs/<runId>/status.json`
 *  2) Try `<runId>/status.json` (legacy/bare)
 *  3) Lightweight scan for any */status.json containing the same runId
 * Returns a prefix that ends with '/'.
 */
async function resolveBasePrefix(containerClient, runId) {
  // 1) Default path
  let candidate = `runs/${runId}/`;
  let statusBlob = containerClient.getBlockBlobClient(`${candidate}status.json`);
  if (await statusBlob.exists()) return candidate;

  // 2) Bare path fallback
  candidate = `${runId}/`;
  statusBlob = containerClient.getBlockBlobClient(`${candidate}status.json`);
  if (await statusBlob.exists()) return candidate;

  // 3) Lightweight scan (bounded)
  let seen = 0;
  for await (const item of containerClient.listBlobsFlat({ prefix: "" })) {
    if (!item.name.toLowerCase().endsWith("status.json")) continue;
    if (++seen > 200) break; // bound cost
    try {
      const bb = containerClient.getBlockBlobClient(item.name);
      const dl = await bb.download();
      const txt = await streamToString(dl.readableStreamBody);
      if (txt && txt.includes(`"runId":"${runId}"`)) {
        const dir = item.name.slice(0, item.name.length - "status.json".length);
        return dir; // includes trailing '/'
      }
    } catch { /* ignore */ }
  }

  // best-effort default
  return `runs/${runId}/`;
}

// --- handler -----------------------------------------------------------------
module.exports = async function (context, req) {
  // prefer auth(context, req); fall back to a fresh correlation id
  let correlationId = genId();
  try {
    const auth = await requireAuth(context, req);
    if (auth?.correlationId) correlationId = auth.correlationId;
  } catch { /* noop */ }

  const extraHeaders = { "x-correlation-id": correlationId, "cache-control": "no-store" };

  const method = (req?.method || "GET").toUpperCase();
  if (method !== "GET") {
    context.res = {
      status: 405,
      headers: { ...extraHeaders, "content-type": "application/json", "Allow": "GET" },
      body: { error: "method_not_allowed", message: "Only GET is supported" }
    };
    return;
  }

  try {
    const runId = (req.query?.runId || "").trim();
    const fileKey = (req.query?.file || "campaign").trim().toLowerCase();

    if (!runId) {
      context.res = {
        status: 400,
        headers: { ...extraHeaders, "content-type": "application/json" },
        body: { error: "bad_request", message: "Missing runId" }
      };
      return;
    }

    const relName = VALID_MAP[fileKey];
    if (!relName) {
      context.res = {
        status: 400,
        headers: { ...extraHeaders, "content-type": "application/json" },
        body: { error: "bad_request", message: `Unknown file '${fileKey}'` }
      };
      return;
    }

    const svc = blobSvc();
    const container = svc.getContainerClient(RESULTS_CONTAINER);

    // Resolve base prefix (don’t assume runs/<runId>/)
    const basePrefix = await resolveBasePrefix(container, runId);

    // Main target blob
    const blobPath = `${basePrefix}${relName}`;
    const blob = container.getBlobClient(blobPath);

    // Tiny retry for freshly-written artifacts (esp. campaign.json / evidence_log.json)
    const shouldRetry = (fileKey === "campaign" || fileKey === "evidence_log");
    const exists = await existsWithTinyRetry(blob, shouldRetry);

    // --- Fallbacks when the target blob isn't there yet -----------------------
    if (!exists) {
      // evidence_log → unwrap array from campaign.json
      if (fileKey === "evidence_log") {
        const campaignBlob = container.getBlobClient(`${basePrefix}${VALID_MAP.campaign}`);
        const campaignOk = await existsWithTinyRetry(campaignBlob, true);
        if (campaignOk) {
          const campaignObj = await jsonOrNull(campaignBlob);
          const arr = Array.isArray(campaignObj?.evidence_log) ? campaignObj.evidence_log : [];
          context.res = {
            status: 200,
            headers: { ...extraHeaders, "content-type": "application/json; charset=utf-8" },
            body: arr
          };
          return;
        }
      }

      // campaign → fall back to outline.json if present
      if (fileKey === "campaign") {
        const outlineBlob = container.getBlobClient(`${basePrefix}${VALID_MAP.outline}`);
        const outlineOk = await existsWithTinyRetry(outlineBlob, true);
        if (outlineOk) {
          const outlineObj = await jsonOrNull(outlineBlob);
          context.res = {
            status: 200,
            headers: { ...extraHeaders, "content-type": "application/json; charset=utf-8" },
            body: outlineObj ?? {}
          };
          return;
        }
      }

      // Nothing to return
      context.res = {
        status: 404,
        headers: { ...extraHeaders, "content-type": "application/json" },
        body: { error: "not_found", message: "File not found" }
      };
      return;
    }

    // --- Success path ---------------------------------------------------------
    if (relName.endsWith(".json")) {
      const obj = await jsonOrNull(blob);

      // Unwrap evidence_log.json if it was stored as { evidence_log: [...] }
      const bodyOut = (fileKey === "evidence_log")
        ? (Array.isArray(obj) ? obj
           : (obj && Array.isArray(obj.evidence_log)) ? obj.evidence_log
           : [])
        : (obj ?? {});

      context.res = {
        status: 200,
        headers: { ...extraHeaders, "content-type": "application/json; charset=utf-8" },
        body: bodyOut
      };
      return;
    }

    // Non-JSON artifacts (not used today, but kept correct)
    const dl = await blob.download();
    const text = await streamToString(dl.readableStreamBody);
    context.res = {
      status: 200,
      headers: { ...extraHeaders, "content-type": dl?.contentType || "application/octet-stream" },
      body: text
    };
  } catch (err) {
    context.log.error("[campaign-fetch] failed", err?.stack || String(err));
    context.res = {
      status: 500,
      headers: { "content-type": "application/json", "x-correlation-id": genId() },
      body: { error: "server_error", message: String(err?.message || err) }
    };
  }
};
