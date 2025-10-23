// /api/campaign-fetch/index.js
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// Complete drop-in replacement that restores ALL 26/09 behavior and fixes all issues:
//
// ✅ Prefix discovery by runId (date/page agnostic), with optional ?prefix= fast-path
// ✅ Streams blob body (memory efficient) and sets isRaw:true
// ✅ Route is owned by function.json ("campaign/fetch"); no v4 app.http usage
// ✅ Strict file mapping via VALID_MAP (prevents drift)
// ✅ Distinguishes "Run not found" vs "File not found" (clear 404 semantics)
// ✅ Robust correlation-id on every response (header or generated), even pre-auth
// ✅ Awaits requireAuth and keeps your suite behavior (it may set context.res on deny)
//
// Notes:
// - Expects AzureWebJobsStorage, CAMPAIGN_RESULTS_CONTAINER (default "results").
// - Optionally accepts ?prefix=<campaign/.../<runId>/> or container-qualified
//   (results/campaign/.../<runId>/ or <RESULTS_CONTAINER>/campaign/.../<runId>/) to avoid listing.

const { BlobServiceClient } = require("@azure/storage-blob");
let requireAuth;
try {
  ({ requireAuth } = require("../lib/auth")); // your existing helper
} catch {
  // Fallback (should not be used in prod)
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
  const s = () =>
    Math.floor((1 + Math.random()) * 0x10000)
      .toString(16)
      .slice(1);
  return `${s()}${s()}-${s()}-${s()}-${s()}-${s()}${s()}${s()}`;
}

function correlationIdFrom(req) {
  return String(readHeader(req, "x-correlation-id") || genId());
}

function badRequest(cid, msg = "Invalid input") {
  return {
    status: 400,
    headers: { "Content-Type": "application/json", "x-correlation-id": cid },
    body: { error: "bad_request", message: msg },
  };
}

function internalError(cid, msg = "Unexpected error") {
  return {
    status: 500,
    headers: { "Content-Type": "application/json", "x-correlation-id": cid },
    body: { error: "internal", message: msg },
  };
}

// --- prefix resolution ---
// Accepts optional ?prefix=...; boolean validator (container-relative or container-qualified).
function isLikelyValidPrefix(prefix, runId) {
  // Accepts container-relative ("campaign/...") or container-qualified
  // ("results/campaign/..." or "<RESULTS_CONTAINER>/campaign/...").
  if (typeof prefix !== "string") return false;

  let p = prefix.trim();
  p = p.replace(/\/{2,}/g, "/");
  if (!p) return false;

  // Basic hardening
  if (p.includes("..") || p.includes("\\")) return false;

  // Normalize for checks: strip container if present
  if (p.startsWith(`${RESULTS_CONTAINER}/`)) {
    p = p.slice(RESULTS_CONTAINER.length + 1); // remove "<container>/"
  }

  if (!p.startsWith("campaign/")) return false;

  // Ensure last path segment matches runId
  p = p.replace(/\/+$/, ""); // drop trailing slashes
  const segs = p.split("/"); // campaign/<page>/<yyyy>/<MM>/<dd>/<runId>
  if (segs.length < 6) return false;

  const last = segs[segs.length - 1];
  if (last !== String(runId)) return false;

  return true;
}

/** Returns 'campaign/<page>/<yyyy>/<MM>/<dd>/<runId>/' (container-relative). */
async function locateRunPrefix(containerClient, runId) {
  const suffix = `/${runId}/status.json`;
  const prefixRoot = `campaign/`; // container-relative (RESULTS_CONTAINER is the container)
  for await (const item of containerClient.listBlobsFlat({ prefix: prefixRoot })) {
    // item.name is container-relative
    if (item.name.endsWith(suffix)) {
      // strip "status.json"
      return item.name.slice(0, item.name.length - "status.json".length);
    }
  }
  return null;
}

module.exports = async function (context, req) {
  // Precompute CID so every path returns it
  const fallbackCid = correlationIdFrom(req);

  // Enforce GET only (route is controlled by function.json: "campaign/fetch")
  if (String(req?.method || "").toUpperCase() !== "GET") {
    context.res = {
      status: 405,
      headers: {
        "Content-Type": "application/json",
        "x-correlation-id": fallbackCid,
        "allow": "GET"
      },
      body: { error: "method_not_allowed", message: "Only GET is supported" },
    };
    return;
  }

  // Auth (awaited). If your requireAuth denies, it will set context.res and we return.
  const auth = await requireAuth(context, req, ["campaign", "campaign-admin"]);
  if (!auth) return;
  const correlationId = auth.correlationId || fallbackCid;

  try {
    const runId = (req.query?.runId || "").trim();
    const fileKey = (req.query?.file || "").trim();
    const optionalPrefix = (req.query?.prefix || "").trim();

    // Validate inputs
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

    // Resolve prefix (fast path: ?prefix=..., else list to discover)
    let prefix;
    if (isLikelyValidPrefix(optionalPrefix, runId)) {
      // Normalize to container-relative: strip the actual container name if present
      if (optionalPrefix.startsWith(`${RESULTS_CONTAINER}/`)) {
        prefix = optionalPrefix.slice(`${RESULTS_CONTAINER}/`.length);
      } else {
        prefix = optionalPrefix;
      }
      if (!prefix.endsWith("/")) prefix += "/";
    } else {
      prefix = await locateRunPrefix(containerClient, runId);
    }

    if (!prefix) {
      // Run not found (no status.json under any page/date for this runId)
      context.res = {
        status: 404,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "not_found", message: "Run not found" },
      };
      return;
    }

    const blobName = `${prefix}${VALID_MAP[fileKey]}`; // container-relative
    const client = containerClient.getBlockBlobClient(blobName);

    if (!(await client.exists())) {
      // Prefix exists (run known), but the specific file is missing
      context.res = {
        status: 404,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "not_found", message: "File not found" },
      };
      return;
    }

    // Stream back the blob (memory efficient)
    const dl = await client.download();
    const bodyStream = dl.readableStreamBody || Buffer.from("{}", "utf8");

    context.log(
      JSON.stringify({
        event: "campaign_fetch_stream",
        runId,
        file: fileKey,
        correlationId,
        outcome: "OK",
      })
    );

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
      body: bodyStream,
      isRaw: true,
    };
  } catch (err) {
    context.log.error(
      JSON.stringify({
        event: "campaign_fetch_error",
        correlationId,
        error: String(err?.message || err),
      })
    );
    context.res = internalError(correlationId);
  }
};

