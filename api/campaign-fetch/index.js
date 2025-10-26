// /api/campaign-fetch/index.js 26-10-2025 v3
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// - Auth kept (requireAuth)
// - Prefix discovery kept; ?prefix= fast-path kept
// - Strict file map kept
// - Correlation id on every response kept
// - Clear 404s for run vs file kept
// - NEW: materialise blob stream to UTF-8 JSON text by default (fixes front-end parsing)
// - NEW: optional ?raw=1 to stream (legacy) if a caller needs it
// - NEW: ETag / Last-Modified passthrough; 502 on corrupt JSON

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
  status: "status.json",
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
function internalError(cid, msg = "Unexpected error") {
  return { status: 500, headers: { "Content-Type": "application/json", "x-correlation-id": cid }, body: { error: "internal", message: msg } };
}
async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

// --- prefix helpers ---
function isLikelyValidPrefix(prefix, runId) {
  if (typeof prefix !== "string") return false;
  let p = prefix.trim().replace(/\/{2,}/g, "/");
  if (!p || p.includes("..") || p.includes("\\")) return false;
  if (p.startsWith(`${RESULTS_CONTAINER}/`)) p = p.slice(RESULTS_CONTAINER.length + 1);
  if (!p.startsWith("campaign/")) return false;
  p = p.replace(/\/+$/, "");
  const segs = p.split("/"); // campaign/<page>/<yyyy>/<MM>/<dd>/<runId>
  if (segs.length < 6) return false;
  return segs[segs.length - 1] === String(runId);
}
/** Returns 'campaign/<page>/<yyyy>/<MM>/<dd>/<runId>/' (container-relative). */
async function locateRunPrefix(containerClient, runId) {
  const suffix = `/${runId}/status.json`;
  const prefixRoot = `campaign/`;
  for await (const item of containerClient.listBlobsFlat({ prefix: prefixRoot })) {
    if (item.name.endsWith(suffix)) {
      return item.name.slice(0, item.name.length - "status.json".length);
    }
  }
  return null;
}

module.exports = async function (context, req) {
  const fallbackCid = correlationIdFrom(req);

  if (String(req?.method || "").toUpperCase() !== "GET") {
    context.res = {
      status: 405,
      headers: { "Content-Type": "application/json", "x-correlation-id": fallbackCid, "allow": "GET" },
      body: { error: "method_not_allowed", message: "Only GET is supported" },
    };
    return;
  }

  const auth = await requireAuth(context, req, ["campaign", "campaign-admin"]);
  if (!auth) return;
  const correlationId = auth.correlationId || fallbackCid;

  try {
    const runId = (req.query?.runId || "").trim();
    const fileKey = (req.query?.file || "").trim();
    const optionalPrefix = (req.query?.prefix || "").trim();
    const wantRaw = String(req.query?.raw || "").trim() === "1"; // opt-in legacy streaming

    if (!runId || !fileKey || !Object.prototype.hasOwnProperty.call(VALID_MAP, fileKey)) {
      context.res = badRequest(correlationId, "Invalid input");
      return;
    }
    if (!process.env.AzureWebJobsStorage) {
      context.res = internalError(correlationId, "AzureWebJobsStorage app setting is missing");
      return;
    }

    const blobService = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
    const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);

    // Resolve prefix
    let prefix;
    if (isLikelyValidPrefix(optionalPrefix, runId)) {
      let p = optionalPrefix.trim().replace(/\/{2,}/g, "/");
      if (p.startsWith("/")) p = p.slice(1);
      if (p.startsWith(`${RESULTS_CONTAINER}/`)) p = p.slice(`${RESULTS_CONTAINER}/`.length);
      prefix = p.endsWith("/") ? p : `${p}/`;
    } else {
      prefix = await locateRunPrefix(containerClient, runId);
    }

    if (!prefix) {
      context.res = { status: 404, headers: { "Content-Type": "application/json", "x-correlation-id": correlationId }, body: { error: "not_found", message: "Run not found" } };
      return;
    }

    const blobName = `${prefix}${VALID_MAP[fileKey]}`;
    const client = containerClient.getBlockBlobClient(blobName);

    if (!(await client.exists())) {
      context.res = { status: 404, headers: { "Content-Type": "application/json", "x-correlation-id": correlationId }, body: { error: "not_found", message: "File not found" } };
      return;
    }

    // Properties for caching headers
    let etag = undefined, lastModified = undefined;
    try {
      const props = await client.getProperties();
      etag = props.etag;
      lastModified = props.lastModified ? props.lastModified.toUTCString() : undefined;
    } catch { /* non-fatal */ }

    // Conditional GET (optional but reduces downloads)
    const inm = readHeader(req, "if-none-match");
    const ims = readHeader(req, "if-modified-since");
    if (etag && inm && inm === etag) {
      context.res = {
        status: 304,
        headers: {
          "Cache-Control": "no-cache",
          ETag: etag,
          ...(lastModified ? { "Last-Modified": lastModified } : {}),
          "x-correlation-id": correlationId,
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
            "x-correlation-id": correlationId,
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
          "x-correlation-id": correlationId,
        },
        body: bodyStream,
        isRaw: true,
      };
      return;
    }

    // Default: MATERIALISE → JSON text (fixes client parsing)
    const dl = await client.download();
    const jsonText = await streamToString(dl.readableStreamBody);

    // Validate JSON; allow object OR array (evidence_log may be an array)
    let parsed;
    try {
      parsed = JSON.parse(jsonText);
      const okType = parsed !== null && (Array.isArray(parsed) || typeof parsed === "object");
      if (!okType) throw new Error("JSON is not an object or array");
    } catch (e) {
      context.res = {
        status: 502,
        headers: { "Content-Type": "application/json", "Cache-Control": "no-cache", "x-correlation-id": correlationId },
        body: { error: "invalid_json", message: "Stored JSON is invalid or wrong shape" },
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
        "x-correlation-id": correlationId,
      },
      // IMPORTANT: return JSON TEXT, not a stream object and not a JS object
      body: jsonText,
    };
  } catch (err) {
    context.log.error(JSON.stringify({ event: "campaign_fetch_error", correlationId, error: String(err?.message || err) }));
    context.res = internalError(correlationId);
  }
};
