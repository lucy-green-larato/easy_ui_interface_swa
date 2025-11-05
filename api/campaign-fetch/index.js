// /api/campaign-fetch/index.js  · 05-11-2025 v5
// GET /api/campaign-fetch?runId=<id>&file=<campaign|evidence_log|csv|status|outline>[&prefix=<container-relative/>]
// Strict whitelist so only known artifacts are retrievable.

const { BlobServiceClient } = require("@azure/storage-blob");

// --- small utils -------------------------------------------------------------
function genId() {
  return `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
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

async function streamToString(readable) {
  const chunks = [];
  for await (const ch of readable) chunks.push(ch);
  return Buffer.concat(chunks).toString("utf8");
}

function cleanPrefix(pfx) {
  if (!pfx) return "";
  let s = String(pfx).trim();
  // container-relative only
  s = s.replace(/^\/+/, "");
  // ensure trailing slash exactly once
  if (!s.endsWith("/")) s += "/";
  return s;
}

// --- handler -----------------------------------------------------------------
module.exports = async function (context, req) {
  // prefer auth(context, req); fall back to a fresh correlation id
  let correlationId = genId();
  try {
    const auth = await requireAuth(context, req);
    if (auth?.correlationId) correlationId = auth.correlationId;
  } catch { /* noop */ }

  // OPTIONAL CORS (enable if your UI is cross-origin)
  // const corsHeaders = {
  //   "access-control-allow-origin": "*",
  //   "access-control-allow-methods": "GET, OPTIONS",
  //   "access-control-allow-headers": "content-type, x-requested-with"
  // };
  const extraHeaders = {}; // swap to corsHeaders if enabling CORS

  const method = (req?.method || "GET").toUpperCase();
  if (method === "OPTIONS") {
    context.res = { status: 204, headers: { ...extraHeaders, "x-correlation-id": correlationId } };
    return;
  }
  if (method !== "GET") {
    context.res = {
      status: 405,
      headers: {
        ...extraHeaders,
        "content-type": "application/json",
        "x-correlation-id": correlationId,
        "Allow": "GET, OPTIONS"
      },
      body: { error: "method_not_allowed", message: "Only GET is supported" }
    };
    return;
  }

  try {
    const runId = (req.query?.runId || "").trim();
    const fileKey = (req.query?.file || "campaign").trim().toLowerCase();
    const prefixParam = cleanPrefix(req.query?.prefix || "");

    if (!runId && !prefixParam) {
      context.res = {
        status: 400,
        headers: { ...extraHeaders, "content-type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: "Missing runId (or provide 'prefix' instead)" }
      };
      return;
    }

    const relName = VALID_MAP[fileKey];
    if (!relName) {
      context.res = {
        status: 400,
        headers: { ...extraHeaders, "content-type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: `Unknown file '${fileKey}'` }
      };
      return;
    }

    const container = blobSvc().getContainerClient(RESULTS_CONTAINER);

    // Prefer explicit prefix if supplied; otherwise default to runs/<runId>/
    const basePrefix = prefixParam || (runId ? `runs/${runId}/` : "");
    const blobPath = `${basePrefix}${relName}`;

    const blob = container.getBlobClient(blobPath);
    if (!(await blob.exists())) {
      // Fallback: if the caller asked for evidence_log, try to read it from campaign.json
      if (fileKey === "evidence_log") {
        const campaignBlob = container.getBlobClient(`${basePrefix}${VALID_MAP.campaign}`);
        if (await campaignBlob.exists()) {
          const dl2 = await campaignBlob.download();
          const text2 = await streamToString(dl2.readableStreamBody);
          try {
            const campaignObj = text2 ? JSON.parse(text2) : {};
            const arr = Array.isArray(campaignObj?.evidence_log) ? campaignObj.evidence_log : [];
            context.res = {
              status: 200,
              headers: {
                ...extraHeaders,
                "content-type": "application/json; charset=utf-8",
                "cache-control": "no-store",
                "x-correlation-id": correlationId
              },
              body: arr
            };
            return;
          } catch {
            // fall through to 404 below
          }
        }
      }
      context.res = {
        status: 404,
        headers: { ...extraHeaders, "content-type": "application/json", "x-correlation-id": correlationId },
        body: { error: "not_found", message: "File not found" }
      };
      return;
    }

    const dl = await blob.download();
    const text = await streamToString(dl.readableStreamBody);

    // Prefer JSON for .json artifacts
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
        // Keep backward-compatibility: return parsed (may be null on parse failure)
        bodyOut = parsed ?? null;
      }
    }

    context.res = {
      status: 200,
      headers: {
        ...extraHeaders,
        "content-type": contentType,
        "cache-control": "no-store",
        "x-correlation-id": correlationId
      },
      body: bodyOut
    };
  } catch (err) {
    const code = err?.code === "config" ? "config" : "server_error";
    const status = code === "config" ? 500 : 500;
    const message = String(err?.message || err);
    context.log.error("[campaign-fetch] failed", err?.stack || message);
    context.res = {
      status,
      headers: { "content-type": "application/json", "x-correlation-id": correlationId || genId() },
      body: { error: code, message }
    };
  }
};
