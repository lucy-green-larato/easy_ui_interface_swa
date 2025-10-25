// /api/campaign-status/index.js 22-10-2025 v1 
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// GET /api/campaign/status?runId=... -> reads status.json under the discovered prefix.
// Finds the blob by scanning for ".../<runId>/status.json" to avoid date/page ambiguity.

const { BlobServiceClient } = require("@azure/storage-blob");
let requireAuth;
try {
  ({ requireAuth } = require("../lib/auth")); // your existing helper (awaited below)
} catch {
  // Minimal fallback; in production your real helper should be present.
  requireAuth = async () => ({ correlationId: genId() });
}

const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";

// ---- utils ----
function readHeader(req, name) {
  if (!req || !req.headers) return undefined;
  const h = req.headers;
  return h[name] ?? h[name.toLowerCase()] ?? h[name.toUpperCase()];
}

function genId() {
  const s = () =>
    Math.floor((1 + Math.random()) * 0x10000)
      .toString(16)
      .slice(1);
  return `${s()}${s()}-${s()}-${s()}-${s()}-${s()}${s()}${s()}`;
}

function correlationIdFrom(req) {
  return String(readHeader(req, "x-correlation-id") || genId());
}

async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

// Search for any blob ending with `/<runId>/status.json` under campaign/
async function findStatusBlob(containerClient, runId) {
  const suffix = `/${runId}/status.json`;
  const prefixRoot = `campaign/`; // container-relative (RESULTS_CONTAINER is the container)
  for await (const item of containerClient.listBlobsFlat({ prefix: prefixRoot })) {
    if (item.name.endsWith(suffix)) return item.name;
  }
  return null;
}

module.exports = async function (context, req) {
  // Enforce GET only
  if (String(req?.method || "").toUpperCase() !== "GET") {
    const cid = correlationIdFrom(req);
    context.res = {
      status: 405,
      headers: {
        "Content-Type": "application/json",
        "x-correlation-id": cid,
        "allow": "GET"
      },
      body: { error: "method_not_allowed", message: "Only GET is supported" },
    };
    return;
  }

  // Auth (awaited). If your requireAuth denies, it will set context.res and we return.
  const auth = await requireAuth(context, req, ["campaign", "campaign-admin"]);
  if (!auth) return;
  const correlationId = auth.correlationId || correlationIdFrom(req);

  try {
    if (!process.env.AzureWebJobsStorage) {
      context.res = {
        status: 500,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "config", message: "AzureWebJobsStorage app setting is missing" },
      };
      return;
    }
    const runId = (req.query?.runId || "").trim();
    if (!runId) {
      context.res = {
        status: 400,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: "Missing runId" },
      };
      return;
    }
    // Accept hex/uuid-like IDs with dashes; adjust if you later switch ID shape.
    const RUNID_RE = /^[A-Za-z0-9-]{8,64}$/;
    if (!RUNID_RE.test(runId)) {
      context.res = {
        status: 400,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: "Invalid runId format" },
      };
      return;
    }

    const blobService = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
    const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);

    const blobName = await findStatusBlob(containerClient, runId);
    if (!blobName) {
      context.res = {
        status: 404,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { state: "Unknown", runId },
      };
      return;
    }

    const block = containerClient.getBlockBlobClient(blobName);

    // Get ETag / Last-Modified first
    const props = await block.getProperties();
    const etag = props.etag;
    const lastModified = props.lastModified ? props.lastModified.toUTCString() : undefined;

    // Conditional GET support
    const ifNoneMatch = readHeader(req, "if-none-match");
    if (ifNoneMatch && etag && ifNoneMatch === etag) {
      context.res = {
        status: 304,
        headers: {
          "ETag": etag,
          ...(lastModified ? { "Last-Modified": lastModified } : {}),
          "Cache-Control": "no-cache",
          "x-correlation-id": correlationId
        }
      };
      return;
    }

    // Download body (small JSON)
    const dl = await block.download();
    const body = await streamToString(dl.readableStreamBody);

    context.log(
      JSON.stringify({
        event: "campaign_status_read",
        runId,
        correlationId,
        outcome: "OK",
      })
    );

    context.res = {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "ETag": etag,
        ...(lastModified ? { "Last-Modified": lastModified } : {}),
        "Cache-Control": "no-cache",
        "x-correlation-id": correlationId
      },
      body, // already JSON text
    };
  } catch (err) {
    context.log.error(
      JSON.stringify({
        event: "campaign_status_error",
        correlationId,
        error: String(err?.message || err),
      })
    );
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
      body: { error: "internal", message: "Unexpected error" },
    };
  }
};
