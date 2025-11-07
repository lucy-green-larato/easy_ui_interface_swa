// /api/campaign-fetch/index.js  · 07-11-2025 v7 (adds file=final)
// GET /api/campaign-fetch?runId=<id>&file=<campaign|evidence_log|csv|status|outline|final>

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
  return JSON.parse(await streamToString(dl.readableStreamBody));
}

const VALID_MAP = Object.freeze({
  campaign: "campaign.json",
  evidence_log: "evidence_log.json",
  csv: "csv_normalized.json",
  status: "status.json",
  outline: "outline.json",
});

async function readSection(container, prefix, key) {
  const name = `sections/${key}.json`;
  const bc = container.getBlockBlobClient(`${prefix}${name}`);
  if (!(await bc.exists())) return null;
  const obj = await jsonIfExists(bc);
  return obj && obj[key] != null ? obj[key] : obj;
}

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
        return it.name.replace(/status\.json$/, "");
      }
    } catch { /* ignore */ }
  }
  return null;
}

module.exports = async function (context, req) {
  const runId = String(req.query.runId || "").trim();
  const fileKey = String(req.query.file || "campaign").trim().toLowerCase();
  const userId = String(req.query.user || "").trim();

  if (!runId) {
    context.res = { status: 400, body: { error: "bad_request", message: "Missing runId" } };
    return;
  }

  const svc = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
  const container = svc.getContainerClient(process.env.CAMPAIGN_RESULTS_CONTAINER || "results");

  const prefix = (await resolvePrefix(container, runId, userId)) || `runs/${runId}/`;
  const extra = {
    "x-run-id": runId,
    "x-prefix": prefix,
    "x-request-id": genId()
  };

  if (fileKey === "final") {
    // Prefer campaign.json if present; otherwise stitch from sections/*
    const campaignBC = container.getBlockBlobClient(`${prefix}campaign.json`);
    if (await campaignBC.exists()) {
      const obj = await jsonIfExists(campaignBC);
      context.res = { status: 200, headers: { ...extra, "content-type": "application/json" }, body: obj };
      return;
    }
    // Stitch
    const merged = {};
    const keys = [
      "executive_summary",
      "positioning_and_differentiation",
      "offer_strategy",
      "messaging_matrix",
      "channel_plan",
      "sales_enablement",
      "measurement_and_learning",
      "risks_and_contingencies",
      "compliance_and_governance",
      "one_pager_summary"
    ];
    for (const k of keys) {
      const v = await readSection(container, prefix, k);
      if (v != null) merged[k] = v;
    }
    context.res = { status: 200, headers: { ...extra, "content-type": "application/json" }, body: merged };
    return;
  }

  const mapPath = VALID_MAP[fileKey];
  if (!mapPath) {
    context.res = { status: 400, headers: extra, body: { error: "bad_request", message: "Unknown file parameter" } };
    return;
  }

  const bc = container.getBlockBlobClient(`${prefix}${mapPath}`);
  const isJson = mapPath.endsWith(".json");
  const contentType = isJson ? "application/json" : "text/plain; charset=utf-8";

  if (!(await bc.exists())) {
    context.res = { status: 404, headers: extra, body: { error: "not_found", message: `${mapPath} not found` } };
    return;
  }

  const text = isJson ? JSON.stringify(await jsonIfExists(bc)) : await streamToString((await bc.download()).readableStreamBody);

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
