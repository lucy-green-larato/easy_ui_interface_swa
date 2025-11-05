// /api/campaign-fetch/index.js  · v5 (prefix-aware + micro-retry)
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
async function jsonOrNull(blob) {
  if (!(await blob.exists())) return null;
  const dl = await blob.download();
  const text = await streamToString(dl.readableStreamBody);
  try { return text ? JSON.parse(text) : null; } catch { return null; }
}
async function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

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
 *  1) Try `runs/<runId>/status.json` and use that folder if it exists.
 *  2) If status.json has a `prefix` field, prefer it (defensive if you add it later).
 *  3) Fallback to `runs/<runId>/`.
 */
async function resolveBasePrefix(container, runId) {
  const defaultPrefix = `runs/${runId}/`;
  const statusPath = `${defaultPrefix}${VALID_MAP.status}`;
  const statusBlob = container.getBlobClient(statusPath);

  let prefix = defaultPrefix;

  // If the default status.json exists, keep that folder as canonical
  if (await statusBlob.exists()) {
    try {
      const st = await jsonOrNull(statusBlob);
      // If a future worker writes an explicit prefix, honour it
      const hinted =
        (typeof st?.prefix === "string" && st.prefix.trim()) ||
        (typeof st?.artifacts_prefix === "string" && st.artifacts_prefix.trim()) ||
        null;
      if (hinted) {
        prefix = hinted.endsWith("/") ? hinted : (hinted + "/");
      }
    } catch { /* keep default */ }
    return prefix;
  }

  // If there is no status at the default place, just fall back.
  return prefix;
}

/** bounded existence retry (250ms, 500ms, 750ms) */
async function existsWithTinyRetry(blob, doRetry) {
  let ok = await blob.exists();
  if (!ok && doRetry) {
    for (let i = 0; i < 3 && !ok; i++) {
      await wait(250 * (i + 1));
      ok = await blob.exists();
    }
  }
  return ok;
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

    // tiny retry for freshly-written artifacts (esp. campaign.json at completion)
    const shouldRetry = (fileKey === "campaign" || fileKey === "evidence_log");
    const exists = await existsWithTinyRetry(blob, shouldRetry);

    if (!exists) {
      // --- Fallbacks ------------------------------------------------------------

      // 1) evidence_log → unwrap from campaign.json if present
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

      // 2) campaign → serve outline.json if campaign.json not yet written
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

      // Nothing to return → 404
      context.res = {
        status: 404,
        headers: { ...extraHeaders, "content-type": "application/json" },
        body: { error: "not_found", message: "File not found" }
      };
      return;
    }

    // --- Success path: blob exists ----------------------------------------------
    if (relName.endsWith(".json")) {
      const obj = await jsonOrNull(blob);

      // Special unwrap for evidence_log.json files that may contain { evidence_log: [...] }
      const bodyOut = (fileKey === "evidence_log")
        ? (Array.isArray(obj) ? obj : (Array.isArray(obj?.evidence_log) ? obj.evidence_log : []))
        : (obj ?? {});

      context.res = {
        status: 200,
        headers: { ...extraHeaders, "content-type": "application/json; charset=utf-8" },
        body: bodyOut
      };
    } else {
      const dl = await blob.download();
      const text = await streamToString(dl.readableStreamBody);
      context.res = {
        status: 200,
        headers: { ...extraHeaders, "content-type": dl?.contentType || "application/octet-stream" },
        body: text
      };
    }

    // Download + content-type
    const dl = await blob.download();
    const text = await streamToString(dl.readableStreamBody);
    const contentType = relName.endsWith(".json")
      ? "application/json; charset=utf-8"
      : (dl?.contentType || "application/octet-stream");

    // Parse JSON once; special-case evidence_log to return an array directly
    let bodyOut = text;
    if (relName.endsWith(".json")) {
      let parsed = null;
      try { parsed = text ? JSON.parse(text) : null; } catch { parsed = null; }

      if (fileKey === "evidence_log") {
        if (Array.isArray(parsed)) {
          bodyOut = parsed;
        } else if (parsed && Array.isArray(parsed.evidence_log)) {
          bodyOut = parsed.evidence_log; // unwrap for UI
        } else {
          bodyOut = []; // graceful empty
        }
      } else {
        bodyOut = parsed;
      }
    }

    context.res = {
      status: 200,
      headers: { ...extraHeaders, "content-type": contentType },
      body: bodyOut
    };
  } catch (err) {
    context.log.error("[campaign-fetch] failed", err?.stack || String(err));
    context.res = {
      status: 500,
      headers: { "content-type": "application/json", "x-correlation-id": correlationId },
      body: { error: "server_error", message: String(err?.message || err) }
    };
  }
};
