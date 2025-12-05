// /api/campaign-router/index.js — Gold v7.3 canonical prefix router 05-12-2025
"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { enqueueTo } = require("../lib/campaign-queue");   // ✅ use shared queue helper
const { canonicalPrefix } = require("../lib/prefix");     // (not strictly needed but harmless)

// ---- ENV ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

// These are only used for logging / defaults; the real enqueue uses env again
const OUTLINE_QUEUE = process.env.Q_CAMPAIGN_OUTLINE || "campaign-outline";
const WORKER_QUEUE = process.env.Q_CAMPAIGN_WORKER || "campaign-worker-jobs";
const WRITE_QUEUE = process.env.Q_CAMPAIGN_WRITE || "campaign-write";

// ---- Blob helpers ----
function getResultsContainerClient() {
  if (!STORAGE_CONN) {
    throw new Error(
      "AzureWebJobsStorage not configured for campaign-router"
    );
  }
  const service = BlobServiceClient.fromConnectionString(STORAGE_CONN);
  return service.getContainerClient(RESULTS_CONTAINER);
}

async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const c of readable) {
    chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function getJson(container, rel) {
  const b = container.getBlockBlobClient(rel);
  const exists = await b.exists();
  if (!exists) return null;
  const dl = await b.download();
  const text = await streamToString(dl.readableStreamBody);
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

async function putJson(container, rel, obj) {
  const bb = container.getBlockBlobClient(rel);
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await bb.uploadData(body, {
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }
  });
}

// ---- Optional: race-safe existence check (currently not used) ----
async function waitForEvidence(prefix, container, attempts = 3, delayMs = 150) {
  const evBlob = container.getBlockBlobClient(`${prefix}evidence.json`);
  const logBlob = container.getBlockBlobClient(`${prefix}evidence_log.json`);

  for (let i = 0; i < attempts; i++) {
    const ev = await evBlob.exists();
    const evLog = await logBlob.exists();
    if (ev || evLog) return true;
    await new Promise((r) => setTimeout(r, delayMs));
  }
  return false;
}

// ======================================================================
//                               ROUTER
// ======================================================================

module.exports = async function (context, queueItem) {
  const log = context.log;

  // Parse message safely (string or object)
  const msg =
    typeof queueItem === "string"
      ? (() => {
          try {
            return JSON.parse(queueItem);
          } catch {
            return {};
          }
        })()
      : queueItem && typeof queueItem === "object"
      ? queueItem
      : {};

  const op = msg.op || "afterevidence";
  let runId = msg.runId || msg.run_id || "unknown";
  let prefix = msg.prefix || "";
  const page = msg.page || "campaign";

  log("[campaign-router] starting", { op, runId, prefix, page });

  // Only handle after-evidence style messages
  if (op !== "afterevidence" && op !== "route" && op !== "reroute") {
    log("[campaign-router] unsupported op; skipping", { op });
    return;
  }

  if (!prefix) {
    log("[campaign-router] missing prefix; cannot route", { runId });
    return;
  }

  // Normalise prefix to container-relative, ensure trailing slash
  if (prefix.startsWith(`${RESULTS_CONTAINER}/`)) {
    prefix = prefix.slice(`${RESULTS_CONTAINER}/`.length);
  }
  prefix = prefix.replace(/^\/+/, "");
  if (!prefix.endsWith("/")) prefix = `${prefix}/`;

  const container = getResultsContainerClient();

  // Artefact blobs
  const evidenceBlob = container.getBlockBlobClient(`${prefix}evidence.json`);
  const csvBlob = container.getBlockBlobClient(`${prefix}csv_normalized.json`);

  const hasEvidence = await evidenceBlob.exists();
  const hasCsv = await csvBlob.exists();

  // SAFETY CHECK — only run worker when both artefacts exist
  if (!hasEvidence || !hasCsv) {
    log("[campaign-router] evidence not ready; deferring worker", {
      runId,
      hasEvidence,
      hasCsv,
      prefix
    });
    return;
  }

  // STATE CHECK — Prevent duplicate worker runs
  const statusPath = `${prefix}status.json`;
  const status = (await getJson(container, statusPath)) || {};
  const markers = status.markers || {};

  if (markers.workerStarted) {
    log("[campaign-router] worker already started — skipping enqueue", {
      runId,
      prefix
    });
    return;
  }

  // ENQUEUE WORKER (single-time, idempotent)
  const workerQueueName =
    process.env.Q_CAMPAIGN_WORKER || WORKER_QUEUE || "campaign-worker-jobs";

  await enqueueTo(workerQueueName, {
    op: "run_strategy",
    runId,
    page,
    prefix
  });

  // Mark so router does not double-trigger
  status.markers = { ...markers, workerStarted: true };
  await putJson(container, statusPath, status);

  log("[campaign-router] worker enqueued successfully", {
    runId,
    prefix,
    workerQueueName
  });
};
