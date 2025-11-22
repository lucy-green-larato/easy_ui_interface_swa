// /api/campaign-status/index.js 22-11-2025 v5
// GET /api/campaign-status?runId=... [&prefix=containerOrRelativePrefix]
//
// Resolves status.json in priority order:
//   1) Explicit ?prefix=… (container-relative normalised; or absolute starting with <container>/)
//   2) New layout via users/<userId>/recent.json → prefix
//   3) Legacy fallback runs/<runId>/status.json
//
// Node 20 / Azure Functions v4 / CommonJS

const { BlobServiceClient } = require("@azure/storage-blob");

// ---------- config ----------
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";

// ---------- CORS ----------
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, X-Correlation-Id, Authorization, If-None-Match",
};

// ---------- utils ----------
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
async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}
function normalizePrefix(p) {
  let x = String(p || "").trim();
  if (!x) return null;
  // Allow "results/…"
  if (x.startsWith(`${RESULTS_CONTAINER}/`)) x = x.slice(`${RESULTS_CONTAINER}/`.length);
  x = x.replace(/^\/+/, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}

// ---- SWA auth helpers (to locate users/<userId>/recent.json) ----
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
  for (const c of claims) byType[(c.typ || c.type || "").toLowerCase()] = c.val || c.value;

  const oid = byType["http://schemas.microsoft.com/identity/claims/objectidentifier"]
    || byType["oid"] || null;
  const sub = byType["sub"] || null;
  const email = (byType["emails"] || byType["email"] || cp.userDetails || "").toLowerCase();

  const chosen = oid || sub || email || "anonymous";
  return String(chosen).trim().toLowerCase().replace(/[^a-z0-9_.@-]/g, "-").replace(/-+/g, "-");
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
      headers: { ...CORS, "Content-Type": "application/json", "x-correlation-id": cid, "Allow": "GET, OPTIONS" },
      body: { error: "method_not_allowed", message: "Only GET is supported" }
    };
    return;
  }

  const correlationId = cid;

  try {
    if (!process.env.AzureWebJobsStorage) {
      context.res = {
        status: 500,
        headers: { ...CORS, "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "config", message: "AzureWebJobsStorage app setting is missing" }
      };
      return;
    }

    const query = req.query || {};
    const routeRunId = (req.params && req.params.runId) ? String(req.params.runId).trim() : "";
    const queryRunId = query.runId ? String(query.runId).trim() : "";
    const runId = routeRunId || queryRunId;

    if (!runId) {
      context.res = {
        status: 400,
        headers: { ...CORS, "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: "Missing runId" }
      };
      return;
    }

    // Accept UUID-like and short ids; keep practical bounds
    const RUNID_RE = /^[A-Za-z0-9_-]{6,72}$/;
    if (!RUNID_RE.test(runId)) {
      context.res = {
        status: 400,
        headers: { ...CORS, "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: "Invalid runId format" }
      };
      return;
    }

    const blobService = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
    const container = blobService.getContainerClient(RESULTS_CONTAINER);

    // -------- Resolve prefix (explicit > recent.json > legacy) Prefix must point at run folder. --------
    let resolvedPrefix = null;

    // 1) Explicit ?prefix=...
    if (typeof query.prefix === "string" && query.prefix.trim()) {
      resolvedPrefix = normalizePrefix(query.prefix);
    }

    // 2) New layout via users/<userId>/recent.json (if no explicit prefix)
    if (!resolvedPrefix) {
      const userId = userIdFromReq(req);
      const userIdxPath = `users/${userId}/recent.json`;
      try {
        const idxClient = container.getBlockBlobClient(userIdxPath);
        if (await idxClient.exists()) {
          const dl = await idxClient.download();
          const txt = await streamToString(dl.readableStreamBody);
          const idx = JSON.parse(txt);
          const hit = (Array.isArray(idx?.items) ? idx.items : []).find(x => String(x?.runId || "") === runId);
          if (hit && typeof hit.prefix === "string") {
            const norm = normalizePrefix(hit.prefix);
            if (norm) resolvedPrefix = norm;
          }
        }
      } catch (e) {
        context.log.warn("campaign-status: recent.json lookup failed", { runId, err: String(e?.message || e) });
      }
    }

    // 3) Legacy fallback
    const candidates = [];
    if (resolvedPrefix) candidates.push(`${resolvedPrefix}status.json`);
    candidates.push(`runs/${runId}/status.json`);

    // -------- Try candidates in order --------
    let found = null;
    for (const blobName of candidates) {
      const client = container.getBlockBlobClient(blobName);
      if (await client.exists()) {
        found = client;
        context.log({ event: "campaign_status_target", blob: `${RESULTS_CONTAINER}/${blobName}`, correlationId });
        break;
      }
    }

    if (!found) {
      context.res = {
        status: 404,
        headers: { ...CORS, "Content-Type": "application/json", "x-correlation-id": correlationId, "Cache-Control": "no-cache" },
        body: { state: "Unknown", runId }
      };
      return;
    }

    // Read properties for validators
    const props = await found.getProperties();
    const etag = props.etag;
    const lastModified = props.lastModified ? props.lastModified.toUTCString() : undefined;

    // Conditional GET via If-None-Match
    const ifNoneMatch = readHeader(req, "if-none-match");
    if (ifNoneMatch && etag && ifNoneMatch === etag) {
      context.res = {
        status: 304,
        headers: {
          ...CORS,
          "ETag": etag,
          ...(lastModified ? { "Last-Modified": lastModified } : {}),
          "Cache-Control": "no-cache",
          "x-correlation-id": correlationId
        }
      };
      return;
    }

   // Download JSON text
const dl = await found.download();
const bodyText = await streamToString(dl.readableStreamBody);

// Validate + parse JSON (log but avoid leaking payload)
let statusPayload;
try {
  statusPayload = JSON.parse(bodyText);
} catch (e) {
  context.log.warn("[campaign-status] status.json is not valid JSON", {
    runId,
    error: String(e?.message || e)
  });
  context.res = {
    status: 502,
    headers: {
      ...CORS,
      "Content-Type": "application/json",
      "x-correlation-id": correlationId
    },
    body: {
      error: "bad_status_payload",
      message: "status.json is not valid JSON"
    }
  };
  return;
}

// ---- Flag normalisation + derived terminal state for strategy-only runs ----
try {
  // Merge flags from root + input.flags so callers have a single source
  const topFlags =
    statusPayload && typeof statusPayload === "object" && statusPayload.flags
      ? statusPayload.flags
      : {};
  const inputFlags =
    statusPayload &&
    typeof statusPayload === "object" &&
    statusPayload.input &&
    statusPayload.input.flags
      ? statusPayload.input.flags
      : {};

  const flags = { ...inputFlags, ...topFlags };

  if (Object.keys(flags).length > 0) {
    statusPayload.flags = flags;
  }

  // If strategy is ready and writer is disabled, treat as a completed run
  if (
    statusPayload.state === "strategy_ready" &&
    flags.use_writer_assembler === false
  ) {
    const history = Array.isArray(statusPayload.history)
      ? statusPayload.history
      : [];

    const last = history[history.length - 1] || null;
    if (!last || last.state !== "Completed") {
      history.push({
        at: new Date().toISOString(),
        state: "Completed",
        note: "Strategy-only run completed (writer disabled)"
      });
    }

    statusPayload.history = history;
    statusPayload.state = "Completed";
  }
} catch (e) {
  context.log.warn("[campaign-status] flag/terminal-state normalisation failed", {
    runId,
    error: String(e?.message || e)
  });
}

const responseText = JSON.stringify(statusPayload);

context.res = {
  status: 200,
  headers: {
    ...CORS,
    "Content-Type": "application/json",
    "ETag": etag,
    ...(lastModified ? { "Last-Modified": lastModified } : {}),
    "Cache-Control": "no-cache",
    "x-correlation-id": correlationId
  },
  // Return the parsed+normalised JSON, not the raw blob text
  body: responseText
};
    
  } catch (err) {
    context.log.error(JSON.stringify({
      event: "campaign_status_error",
      correlationId,
      error: String(err?.message || err)
    }));
    context.res = {
      status: 500,
      headers: { ...CORS, "Content-Type": "application/json", "x-correlation-id": correlationId },
      body: { error: "internal", message: "Unexpected error" }
    };
  }
};
