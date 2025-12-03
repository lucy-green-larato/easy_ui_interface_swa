// /api/campaign-status/index.js 03-12-2025 v10.2
// GET /api/campaign-status?runId=... [&page=campaign]
//
// Canonical status resolver for Campaign runs.
// - Single canonical layout based on canonicalPrefix({ runId, userId, page })
// - No legacy runs/<runId>/status.json fallback
// - CORS + conditional GET (ETag / Last-Modified)
// - Safe flag normalisation using lib/featureFlags.getFlags
//
// Node 20 / Azure Functions v4 / CommonJS

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { canonicalPrefix } = require("../lib/prefix");
const { getFlags } = require("../lib/featureFlags");

const STORAGE_CONN =
  process.env.AzureWebJobsStorage || process.env.AZURE_STORAGE_CONNECTION_STRING;

const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER || "results";

// ---------- CORS ----------
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers":
    "Content-Type, X-Correlation-Id, Authorization, If-None-Match",
};

// ---------- utils ----------
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
  const chunks = [];
  for await (const chunk of readable || []) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

// ---- SWA auth helpers (for canonicalPrefix userId) ----
function decodeClientPrincipal(req) {
  const b64 = readHeader(req, "x-ms-client-principal");
  if (!b64) return null;
  try {
    const json = Buffer.from(b64, "base64").toString("utf8");
    return JSON.parse(json);
  } catch {
    return null;
  }
}

function userIdFromReq(req) {
  const cp = decodeClientPrincipal(req) || {};
  const claims = Array.isArray(cp.claims) ? cp.claims : [];
  const byType = Object.create(null);
  for (const c of claims) {
    const key = (c.typ || c.type || "").toLowerCase();
    byType[key] = c.val || c.value;
  }

  const oid =
    byType["http://schemas.microsoft.com/identity/claims/objectidentifier"] ||
    byType["oid"] ||
    null;
  const sub = byType["sub"] || null;
  const email =
    (byType["emails"] || byType["email"] || cp.userDetails || "").toLowerCase();

  const chosen = oid || sub || email || "anonymous";
  return String(chosen)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_.@-]/g, "-")
    .replace(/-+/g, "-");
}

module.exports = async function (context, req) {
  const cid = correlationIdFrom(req);
  const method = String(req?.method || "").toUpperCase();

  // OPTIONS (preflight)
  if (method === "OPTIONS") {
    context.res = { status: 204, headers: CORS };
    return;
  }

  // Enforce GET
  if (method !== "GET") {
    context.res = {
      status: 405,
      headers: {
        ...CORS,
        "Content-Type": "application/json",
        "x-correlation-id": cid,
        Allow: "GET, OPTIONS",
      },
      body: {
        error: "method_not_allowed",
        message: "Only GET is supported",
      },
    };
    return;
  }

  const correlationId = cid;

  try {
    if (!STORAGE_CONN) {
      context.res = {
        status: 500,
        headers: {
          ...CORS,
          "Content-Type": "application/json",
          "x-correlation-id": correlationId,
        },
        body: {
          error: "config",
          message: "AzureWebJobsStorage / storage connection is missing",
        },
      };
      return;
    }

    const query = req.query || {};
    const routeRunId =
      (req.params && req.params.runId && String(req.params.runId).trim()) || "";
    const queryRunId = (query.runId && String(query.runId).trim()) || "";
    const runId = routeRunId || queryRunId;

    if (!runId) {
      context.res = {
        status: 400,
        headers: {
          ...CORS,
          "Content-Type": "application/json",
          "x-correlation-id": correlationId,
        },
        body: { error: "bad_request", message: "Missing runId" },
      };
      return;
    }

    // Accept UUID-like and short ids; keep practical bounds
    const RUNID_RE = /^[A-Za-z0-9_-]{6,72}$/;
    if (!RUNID_RE.test(runId)) {
      context.res = {
        status: 400,
        headers: {
          ...CORS,
          "Content-Type": "application/json",
          "x-correlation-id": correlationId,
        },
        body: { error: "bad_request", message: "Invalid runId format" },
      };
      return;
    }

    const page =
      (query.page && String(query.page).trim().toLowerCase()) || "campaign";

    let prefix;
    const providedPrefix = query.prefix || (req.body && req.body.prefix);

    if (providedPrefix) {
      // Caller supplied an explicit prefix → trust it but normalise shape
      prefix = String(providedPrefix).trim();
    } else {
      // Fallback: reconstruct using canonicalPrefix
      const userId = userIdFromReq(req); // only used for fallback
      prefix = canonicalPrefix({ runId, userId, page });
    }

    // Final normalisation for ALL prefixes:
    // - strip container name if present
    // - strip leading slashes
    // - strip leading "results/" if present
    // - ensure trailing slash
    if (prefix.toLowerCase().startsWith(`${RESULTS_CONTAINER}/`)) {
      prefix = prefix.slice(`${RESULTS_CONTAINER}/`.length);
    }
    prefix = prefix.replace(/^\/+/, "");
    prefix = prefix.replace(/^results\//i, "");
    if (!prefix.endsWith("/")) prefix += "/";

    const statusPath = `${prefix}status.json`;
    const blobService = BlobServiceClient.fromConnectionString(STORAGE_CONN);
    const container = blobService.getContainerClient(RESULTS_CONTAINER);
    const client = container.getBlockBlobClient(statusPath);

    if (!(await client.exists())) {
      context.res = {
        status: 404,
        headers: {
          ...CORS,
          "Content-Type": "application/json",
          "x-correlation-id": correlationId,
          "Cache-Control": "no-cache",
        },
        body: {
          runId,
          state: "Unknown",
          error: "not_found",
          message: `No status.json found at ${statusPath}`,
          statusPath
        },
      };
      return;
    }

    // Properties for validators
    const props = await client.getProperties();
    const etag = props.etag;
    const lastModified = props.lastModified
      ? props.lastModified.toUTCString()
      : undefined;

    // Conditional GET via If-None-Match
    const ifNoneMatch = readHeader(req, "if-none-match");
    if (ifNoneMatch && etag && ifNoneMatch === etag) {
      context.res = {
        status: 304,
        headers: {
          ...CORS,
          ETag: etag,
          ...(lastModified ? { "Last-Modified": lastModified } : {}),
          "Cache-Control": "no-cache",
          "x-correlation-id": correlationId,
        },
      };
      return;
    }

    // Download JSON text
    const dl = await client.download();
    const bodyText = await streamToString(dl.readableStreamBody);

    // Validate + parse JSON (log but avoid leaking payload)
    let statusPayload;
    try {
      statusPayload = JSON.parse(bodyText);
    } catch (e) {
      context.log.warn("[campaign-status] status.json is not valid JSON", {
        runId,
        statusPath,
        error: String(e?.message || e),
      });
      context.res = {
        status: 502,
        headers: {
          ...CORS,
          "Content-Type": "application/json",
          "x-correlation-id": correlationId,
        },
        body: {
          error: "bad_status_payload",
          message: "status.json is not valid JSON",
        },
      };
      return;
    }

    // ---- Feature-flag normalisation (using lib/featureFlags) ----
    try {
      const inputFlags =
        statusPayload &&
          typeof statusPayload === "object" &&
          statusPayload.input &&
          typeof statusPayload.input.flags === "object"
          ? statusPayload.input.flags
          : {};

      const topFlags =
        statusPayload &&
          typeof statusPayload === "object" &&
          typeof statusPayload.flags === "object"
          ? statusPayload.flags
          : {};

      // Merge input.flags (earlier) and top-level flags (override) then normalise
      const merged = { ...inputFlags, ...topFlags };
      const normalised = getFlags({ flags: merged });

      if (normalised && Object.keys(normalised).length > 0) {
        statusPayload.flags = normalised;
      }
    } catch (e) {
      context.log.warn("[campaign-status] flag normalisation failed", {
        runId,
        statusPath,
        error: String(e?.message || e),
      });
    }

    const responseText = JSON.stringify(statusPayload);

    context.res = {
      status: 200,
      headers: {
        ...CORS,
        "Content-Type": "application/json",
        ETag: etag,
        ...(lastModified ? { "Last-Modified": lastModified } : {}),
        "Cache-Control": "no-cache",
        "x-correlation-id": correlationId,
      },
      body: responseText,
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
      headers: {
        ...CORS,
        "Content-Type": "application/json",
        "x-correlation-id": correlationId,
      },
      body: { error: "internal", message: "Unexpected error" },
    };
  }
};
