// /api/campaign-status/index.js 22-10-2025 v1 
// /api/campaign-status/index.js — canonical status reader (CommonJS, Node 20/Azure Functions v4)
// Route: GET /api/campaign-status/{runId}  (also accepts ?runId=… as fallback)
// Reads: runs/<runId>/status.json in the CAMPAIGN_RESULTS_CONTAINER container.
// Returns: JSON text with strong validators (ETag, Last-Modified). No scanning, no date/page drift.

const { BlobServiceClient } = require("@azure/storage-blob");

let requireAuth;
try {
  ({ requireAuth } = require("../lib/auth")); // optional; if not present we fall back below
} catch {
  requireAuth = async () => ({ correlationId: genId() });
}

const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";

// ---- utils ----
function readHeader(req, name) {
  if (!req || !req.headers) return undefined;
  const h = req.headers;
  // Node lower-cases header keys; include defensives for safety.
  return h[name] ?? h[name.toLowerCase()] ?? h[name.toUpperCase()];
}

function genId() {
  const s = () => Math.floor((1 + Math.random()) * 0x10000).toString(16).slice(1);
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

module.exports = async function (context, req) {
  // Enforce GET only
  if (String(req?.method || "").toUpperCase() !== "GET") {
    const cid = correlationIdFrom(req);
    context.res = {
      status: 405,
      headers: {
        "Content-Type": "application/json",
        "x-correlation-id": cid,
        "Allow": "GET"
      },
      body: { error: "method_not_allowed", message: "Only GET is supported" }
    };
    return;
  }

  // Auth (optional helper). If it denies, it sets context.res and we return.
  const auth = await requireAuth(context, req, ["campaign", "campaign-admin"]);
  if (!auth) return;
  const correlationId = auth.correlationId || correlationIdFrom(req);

  try {
    if (!process.env.AzureWebJobsStorage) {
      context.res = {
        status: 500,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "config", message: "AzureWebJobsStorage app setting is missing" }
      };
      return;
    }

    // Prefer route param, fallback to query
    const routeRunId = (req.params && req.params.runId) ? String(req.params.runId).trim() : "";
    const queryRunId = (req.query && req.query.runId) ? String(req.query.runId).trim() : "";
    const runId = routeRunId || queryRunId;

    if (!runId) {
      context.res = {
        status: 400,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: "Missing runId" }
      };
      return;
    }

    // Accept alphanumeric + dash IDs; adjust if you later change ID shape.
    const RUNID_RE = /^[A-Za-z0-9-]{8,64}$/;
    if (!RUNID_RE.test(runId)) {
      context.res = {
        status: 400,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: "Invalid runId format" }
      };
      return;
    }

    const blobService = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
    const container = blobService.getContainerClient(RESULTS_CONTAINER);
    const blobName = `runs/${runId}/status.json`;
    const client = container.getBlockBlobClient(blobName);

    context.log({ event: "campaign_status_target", blob: `${RESULTS_CONTAINER}/${blobName}` });

    // Fast 404 if missing
    if (!(await client.exists())) {
      context.res = {
        status: 404,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { state: "Unknown", runId }
      };
      return;
    }

    // Read properties for validators
    const props = await client.getProperties();
    const etag = props.etag;
    const lastModified = props.lastModified ? props.lastModified.toUTCString() : undefined;

    // Conditional GET via If-None-Match
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

    // Download JSON text
    const dl = await client.download();
    const body = await streamToString(dl.readableStreamBody);

    // Validate it is JSON; if invalid, treat as server error to protect clients
    try { JSON.parse(body); } catch (e) {
      context.log.warn("[campaign-status] status.json is not valid JSON", { runId, error: String(e?.message || e) });
      context.res = {
        status: 502,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_status_payload", message: "status.json is not valid JSON" }
      };
      return;
    }

    context.log(JSON.stringify({ event: "campaign_status_read", runId, correlationId, outcome: "OK" }));

    context.res = {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "ETag": etag,
        ...(lastModified ? { "Last-Modified": lastModified } : {}),
        "Cache-Control": "no-cache",
        "x-correlation-id": correlationId
      },
      body // already JSON text
    };
  } catch (err) {
    context.log.error(JSON.stringify({
      event: "campaign_status_error",
      correlationId,
      error: String(err?.message || err)
    }));
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
      body: { error: "internal", message: "Unexpected error" }
    };
  }
};
