// /api/campaign-evidence/index.js — STUB for wiring test
// Writes a minimal evidence pack + marks status, then hands off to router.

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueClient } = require("@azure/storage-queue");

const STORAGE_CONN = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const START_QUEUE_NAME = process.env.CAMPAIGN_QUEUE_NAME || "campaign";

function normalisePrefix(prefixRaw) {
  let x = String(prefixRaw || "").trim();
  if (!x) return null;
  // strip container name if someone passed "results/..."
  if (x.startsWith(RESULTS_CONTAINER + "/")) x = x.slice((RESULTS_CONTAINER + "/").length);
  x = x.replace(/^\/+/, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}

function streamToString(readable) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readable.on("data", (c) => chunks.push(c));
    readable.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    readable.on("error", reject);
  });
}

async function getJson(container, rel) {
  const b = container.getBlockBlobClient(rel);
  if (!(await b.exists())) return null;
  const dl = await b.download();
  const txt = await streamToString(dl.readableStreamBody);
  try { return txt ? JSON.parse(txt) : null; } catch { return null; }
}

async function putJson(container, rel, obj) {
  const bb = container.getBlockBlobClient(rel);
  const data = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await bb.uploadData(data, {
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }
  });
}

module.exports = async function (context, job) {
  if (!STORAGE_CONN) {
    context.log.error("[evidence-stub] AzureWebJobsStorage missing");
    return;
  }

  // Parse message from queue
  let msg = job;
  if (typeof msg === "string") {
    try { msg = JSON.parse(msg); } catch { msg = {}; }
  } else if (!msg || typeof msg !== "object") {
    msg = {};
  }

  const runId = String(msg.runId || "").trim()
    || (Date.now().toString(36) + "-" + Math.random().toString(16).slice(2));
  const page = (msg.page && String(msg.page).trim()) || "campaign";

  let prefix = normalisePrefix(msg.prefix || `runs/${page}/anonymous/${runId}/`);
  if (!prefix) {
    prefix = `runs/${page}/anonymous/${runId}/`;
  }

  context.log("[evidence-stub] start", { runId, prefix, page });

  const blobSvc = BlobServiceClient.fromConnectionString(STORAGE_CONN);
  const container = blobSvc.getContainerClient(RESULTS_CONTAINER);
  await container.createIfNotExists();

  const statusPath = `${prefix}status.json`;
  const status0 = (await getJson(container, statusPath)) || { runId, history: [], markers: {} };
  status0.history = Array.isArray(status0.history) ? status0.history : [];
  status0.markers = status0.markers || {};

  // 1) Write a tiny evidence log + bundle
  const claims = [
    {
      claim_id: "CLM-001",
      title: "Stub evidence record",
      summary: "Stub evidence pack written by campaign-evidence stub. Replace with real engine once wiring is confirmed.",
      quote: "",
      source_type: "Directory",
      url: `${container.url}/${prefix}status.json`,
      tag: "",
      date: null
    }
  ];
  const evidenceBundle = {
    claims,
    counts: {
      website: 0,
      linkedin: 0,
      pdf: 0,
      directories: claims.length,
      ixbrl: 0,
      csv: 0
    }
  };

  await putJson(container, `${prefix}evidence_log.json`, claims);
  await putJson(container, `${prefix}evidence.json`, evidenceBundle);

  // 2) Mark status as EvidenceDigest completed
  status0.history.push({
    at: new Date().toISOString(),
    phase: "EvidenceDigest",
    op: "stub_completed",
    note: "Stub evidence pack written"
  });
  status0.markers.evidenceDigestCompleted = true;
  await putJson(container, statusPath, status0);

  // 3) Hand off to router (afterevidence)
  try {
    const q = new QueueClient(STORAGE_CONN, START_QUEUE_NAME);
    await q.createIfNotExists();
    await q.sendMessage(JSON.stringify({ op: "afterevidence", runId, page, prefix }));
    context.log("[evidence-stub] enqueued afterevidence", { runId });
  } catch (err) {
    context.log.error("[evidence-stub] afterevidence enqueue failed", String(err?.message || err));
  }

  context.log("[evidence-stub] done", { runId, prefix });
};
