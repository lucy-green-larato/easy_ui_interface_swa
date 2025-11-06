// /api/campaign-fetch/index.js  · 06-11-2025 v6 (prefix-aware + robust returns)
// GET /api/campaign-fetch?runId=<id>&file=<campaign|evidence_log|csv|status|outline>

const { BlobServiceClient } = require("@azure/storage-blob");

// --- tiny utils --------------------------------------------------------------
const genId = () => `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function streamToString(readable) {
  const chunks = [];
  for await (const ch of readable) chunks.push(ch);
  return Buffer.concat(chunks).toString("utf8");
}
async function jsonIfExists(bc) {
  if (!(await bc.exists())) return null;
  const dl = await bc.download();
  const txt = await streamToString(dl.readableStreamBody);
  try { return txt ? JSON.parse(txt) : null; } catch { return null; }
}
async function existsWithTinyRetry(bc, doRetry = false) {
  let ok = await bc.exists();
  for (let i = 0; doRetry && !ok && i < 3; i += 1) {
    await sleep(150 * (i + 1));          // 150 / 300 / 450 ms
    ok = await bc.exists();
  }
  return ok;
}

// Optional auth (for correlation id only here)
let requireAuth;
try { ({ requireAuth } = require("../lib/auth")); }
catch { requireAuth = async () => ({ correlationId: genId(), userId: "anonymous" }); }

// --- config ------------------------------------------------------------------
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const VALID_MAP = Object.freeze({
  campaign: "campaign.json",
  evidence_log: "evidence_log.json",
  csv: "csv_normalized.json",
  status: "status.json",
  outline: "outline.json",
});

// Resolve container-relative prefix for a run
async function resolvePrefix(container, runId, userIdHint) {
  // 1) Per-user recent index (fast path)
  if (userIdHint) {
    try {
      const idx = container.getBlockBlobClient(`users/${userIdHint}/recent.json`);
      const obj = await jsonIfExists(idx);
      const hit = (obj?.items || []).find((x) => String(x?.runId) === runId && typeof x?.prefix === "string");
      if (hit?.prefix && hit.prefix.endsWith("/")) return hit.prefix;
    } catch { /* noop */ }
  }

  // 2) Bounded scan for status.json that contains the runId (defensive)
  let seen = 0;
  for await (const it of container.listBlobsFlat({ prefix: "runs/" })) {
    if (!it.name.toLowerCase().endsWith("/status.json")) continue;
    if (++seen > 500) break; // hard cap
    try {
      const bb = container.getBlockBlobClient(it.name);
      const dl = await bb.download();
      const txt = await streamToString(dl.readableStreamBody);
      if (txt && txt.includes(`"runId":"${runId}"`)) {
        return it.name.slice(0, -("status.json".length)); // keep trailing slash
      }
    } catch { /* continue */ }
  }

  // 3) Legacy layout fallback
  return `runs/${runId}/`;
}

// --- handler -----------------------------------------------------------------
module.exports = async function (context, req) {
  let correlationId = genId();
  try {
    const auth = await requireAuth(context, req);
    if (auth?.correlationId) correlationId = auth.correlationId;
  } catch { /* noop */ }

  const extra = { "x-correlation-id": correlationId, "cache-control": "no-store" };
  const method = String(req?.method || "GET").toUpperCase();
  if (method !== "GET") {
    context.res = { status: 405, headers: { ...extra, "content-type": "application/json" },
      body: { error: "method_not_allowed" } };
    return;
  }

  const runId = String(req.query?.runId || "").trim();
  const fileKey = String(req.query?.file || "").trim().toLowerCase();
  const relName = VALID_MAP[fileKey];

  if (!runId || !/^[a-z0-9-]{10,}$/i.test(runId)) {
    context.res = { status: 400, headers: { ...extra, "content-type": "application/json" },
      body: { error: "bad_request", message: "Missing or invalid runId" } };
    return;
  }
  if (!relName) {
    context.res = { status: 400, headers: { ...extra, "content-type": "application/json" },
      body: { error: "bad_request", message: "Unknown file parameter" } };
    return;
  }
  if (!STORAGE_CONN) {
    context.res = { status: 500, headers: { ...extra, "content-type": "application/json" },
      body: { error: "config", message: "AzureWebJobsStorage is not configured" } };
    return;
  }

  const blobSvc = BlobServiceClient.fromConnectionString(STORAGE_CONN);
  const container = blobSvc.getContainerClient(RESULTS_CONTAINER);

  // Best-effort userId (only for recent.json fast-path)
  let userIdHint = "anonymous";
  try {
    const b64 = req.headers["x-ms-client-principal"];
    if (b64) {
      const cp = JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
      const claims = cp?.claims || [];
      const by = Object.create(null);
      for (const c of claims) by[c.typ?.toLowerCase?.() || ""] = c.val;
      userIdHint = (by["oid"] || by["sub"] || by["emails"] || by["email"] || "anonymous").toLowerCase();
    }
  } catch { /* noop */ }

  const base = await resolvePrefix(container, runId, userIdHint);
  const blob = container.getBlockBlobClient(`${base}${relName}`);

  // Tiny retry for freshly-written artifacts
  const shouldRetry = fileKey === "campaign" || fileKey === "evidence_log";
  const ok = await existsWithTinyRetry(blob, shouldRetry);
  if (!ok) {
    // friendly 404 – UI will keep polling
    context.res = { status: 404, headers: { ...extra, "content-type": "application/json" },
      body: { error: "not_found", message: "File not found" } };
    return;
  }

  const dl = await blob.download();
  const text = await streamToString(dl.readableStreamBody);
  const isJson = relName.endsWith(".json");
  const contentType = isJson ? "application/json; charset=utf-8" : (dl?.contentType || "application/octet-stream");

  let bodyOut = text;
  if (isJson) {
    let parsed = null;
    try { parsed = text ? JSON.parse(text) : null; } catch { parsed = null; }
    if (fileKey === "evidence_log") {
      bodyOut = Array.isArray(parsed) ? parsed
              : (parsed && Array.isArray(parsed.evidence_log) ? parsed.evidence_log : []);
    } else {
      bodyOut = parsed;
    }
  }

  context.res = { status: 200, headers: { ...extra, "content-type": contentType }, body: bodyOut };
};
