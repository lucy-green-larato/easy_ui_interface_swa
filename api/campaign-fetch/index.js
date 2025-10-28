// /api/campaign-fetch/index.js 2025-10-28 v4 (aligned to runs/<runId>/ layout)
// Classic Azure Functions (function.json + scriptFile), CommonJS.
//
// - Auth kept (requireAuth)
// - NO scanning: prefix is deterministic -> runs/<runId>/
// - Optional ?prefix=runs/<runId>/ accepted if valid
// - Strict file map kept
// - Clear 404s for run vs file (checks status.json first)
// - Materialise blob stream to UTF-8 JSON text by default (fixes front-end parsing)
// - Optional ?raw=1 to stream if a caller needs it
// - ETag / Last-Modified passthrough; 502 on corrupt JSON

const { BlobServiceClient } = require("@azure/storage-blob");
let requireAuth;
try {
  ({ requireAuth } = require("../lib/auth"));
} catch {
  requireAuth = async () => ({ correlationId: genId() });
}

const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const VALID_MAP = Object.freeze({
  campaign: "campaign.json",
  evidence_log: "evidence_log.json",
  status: "status.json"
});

// --- util: headers & correlation id ---
function readHeader(req, name) {
  if (!req || !req.headers) return undefined;
  const h = req.headers;
  return h[name] ?? h[name.toLowerCase()] ?? h[name.toUpperCase()];
}
function genId() {
  const s = () => Math.floor((1 + Math.random()) * 0x10000).toString(16).slice(1);
  return `${s()}${s()}-${s()}-${s()}-${s()}-${s()}${s()}${s()}`;
}
function correlationIdFrom(req) {
  return String(readHeader(req, "x-correlation-id") || genId());
}
function badRequest(cid, msg = "Invalid input") {
  return { status: 400, headers: { "Content-Type": "application/json", "x-correlation-id": cid }, body: { error: "bad_request", message: msg } };
}
function notFound(cid, msg = "Not found") {
  return { status: 404, headers: { "Content-Type": "application/json", "x-correlation-id": cid }, body: { error: "not_found", message: msg } };
}
function internalError(cid, msg = "Unexpected error") {
  return { status: 500, headers: { "Content-Type": "application/json", "x-correlation-id": cid }, body: { error: "internal", message: msg } };
}
async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

// --- prefix helpers (canonical) ---
const RUNID_RE = /^[A-Za-z0-9-]{8,64}$/;

// Accept only canonical `runs/<runId>/` when provided
function coercePrefix(optionalPrefix, runId) {
  if (typeof optionalPrefix === "string" && optionalPrefix.trim()) {
    let p = optionalPrefix.trim().replace(/\/{2,}/g, "/");
    if (p.startsWith("/")) p = p.slice(1);
    // Strip container name if caller accidentally included it
    if (p.startsWith(`${RESULTS_CONTAINER}/`)) p = p.slice(`${RESULTS_CONTAINER}/`.length);
    // Accept exactly runs/<runId> or runs/<runId>/ (container-relative)
    if (p === `runs/${runId}` || p === `runs/${runId}/`) {
      return p.endsWith("/") ? p : `${p}/`;
    }
  }
  // Fallback to deterministic canonical
  return `runs/${runId}/`;
}

module.exports = async function (context, req) {
  const fallbackCid = correlationIdFrom(req);

  if (String(req?.method || "").toUpperCase() !== "GET") {
    context.res = {
      status: 405,
      headers: { "Content-Type": "application/json", "x-correlation-id": fallbackCid, "Allow": "GET" },
      body: { error: "method_not_allowed", message: "Only GET is supported" }
    };
    return;
  }

  const auth = await requireAuth(context, req, ["campaign", "campaign-admin"]);
  if (!auth) return;
  const correlationId = auth.correlationId || fallbackCid;

  try {
    const runId = String(req.query?.runId || "").trim();
    const fileKey = String(req.query?.file || "").trim() || "campaign"; // default to campaign.json for convenience
    const optionalPrefix = String(req.query?.prefix || "").trim();
    const wantRaw = String(req.query?.raw || "").trim() === "1";

    if (!RUNID_RE.test(runId)) {
      context.res = badRequest(correlationId, "Invalid or missing runId");
      return;
    }
    if (!Object.prototype.hasOwnProperty.call(VALID_MAP, fileKey)) {
      context.res = badRequest(correlationId, "Invalid file key");
      return;
    }
    if (!process.env.AzureWebJobsStorage) {
      context.res = internalError(correlationId, "AzureWebJobsStorage app setting is missing");
      return;
    }

    const blobService = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
    const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);

    // Resolve canonical prefix (no scanning)
    const prefix = coercePrefix(optionalPrefix, runId);

    // 1) Prove run exists (status.json presence)
    const statusClient = containerClient.getBlockBlobClient(`${prefix}status.json`);
    if (!(await statusClient.exists())) {
      context.res = notFound(correlationId, "Run not found");
      return;
    }

    // 2) Fetch the requested file
    const blobName = `${prefix}${VALID_MAP[fileKey]}`;
    const client = containerClient.getBlockBlobClient(blobName);

    if (!(await client.exists())) {
      context.res = notFound(correlationId, "File not found");
      return;
    }

    // Properties for caching headers
    let etag, lastModified;
    try {
      const props = await client.getProperties();
      etag = props.etag;
      lastModified = props.lastModified ? props.lastModified.toUTCString() : undefined;
    } catch { /* non-fatal */ }

    // Conditional GET
    const inm = readHeader(req, "if-none-match");
    const ims = readHeader(req, "if-modified-since");
    if (etag && inm && inm === etag) {
      context.res = {
        status: 304,
        headers: {
          "Cache-Control": "no-cache",
          ETag: etag,
          ...(lastModified ? { "Last-Modified": lastModified } : {}),
          "x-correlation-id": correlationId
        }
      };
      return;
    }
    if (lastModified && ims) {
      const since = Date.parse(ims);
      const lm = Date.parse(lastModified);
      if (Number.isFinite(since) && Number.isFinite(lm) && lm <= since) {
        context.res = {
          status: 304,
          headers: {
            "Cache-Control": "no-cache",
            ETag: etag,
            "Last-Modified": lastModified,
            "x-correlation-id": correlationId
          }
        };
        return;
      }
    }

    // Legacy opt-in: raw stream
    if (wantRaw) {
      const dl = await client.download();
      const bodyStream = dl.readableStreamBody || Buffer.from("{}", "utf8");
      context.log(JSON.stringify({ event: "campaign_fetch_stream_raw", runId, file: fileKey, correlationId, outcome: "OK" }));
      context.res = {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Cache-Control": "no-cache",
          ...(etag ? { ETag: etag } : {}),
          ...(lastModified ? { "Last-Modified": lastModified } : {}),
          "x-correlation-id": correlationId
        },
        body: bodyStream,
        isRaw: true
      };
      return;
    }

    // Default: materialise → JSON text
    const dl = await client.download();
    const jsonText = await streamToString(dl.readableStreamBody);

    // Validate JSON (object or array)
    try {
      const parsed = JSON.parse(jsonText);
      const okType = parsed !== null && (Array.isArray(parsed) || typeof parsed === "object");
      if (!okType) throw new Error("JSON is not an object or array");
    } catch (e) {
      context.res = {
        status: 502,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-cache", "x-correlation-id": correlationId },
        body: { error: "invalid_json", message: "Stored JSON is invalid or wrong shape" }
      };
      return;
    }

    const bufLen = Buffer.byteLength(jsonText, "utf8");

    context.log(JSON.stringify({ event: "campaign_fetch_json_ok", runId, file: fileKey, size: bufLen, correlationId, outcome: "OK" }));

    context.res = {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "Content-Length": String(bufLen),
        ...(etag ? { ETag: etag } : {}),
        ...(lastModified ? { "Last-Modified": lastModified } : {}),
        "x-correlation-id": correlationId
      },
      body: jsonText
    };
  } catch (err) {
    context.log.error(JSON.stringify({ event: "campaign_fetch_error", correlationId, error: String(err?.message || err) }));
    context.res = internalError(correlationId);
  }
};
