// /api/campaign-router/index.js — Gold v7.2 canonical prefix router 04-12-2025
"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const { getFlags } = require("../lib/featureFlags");
const { canonicalPrefix } = require("../lib/prefix");

// ---- ENV ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const OUTLINE_QUEUE = process.env.Q_CAMPAIGN_OUTLINE || "campaign-outline";
const WORKER_QUEUE = process.env.Q_CAMPAIGN_WORKER || "campaign-worker-jobs";
const WRITE_QUEUE = process.env.Q_CAMPAIGN_WRITE || "campaign-write";

// ---- Helpers ----
async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const c of readable) chunks.push(c);
  return Buffer.concat(chunks).toString("utf8");
}

async function getJson(container, rel) {
  const b = container.getBlockBlobClient(rel);
  if (!(await b.exists())) return null;
  const dl = await b.download();
  const txt = await streamToString(dl.readableStreamBody);
  try { return JSON.parse(txt); } catch { return null; }
}

async function putJson(container, rel, obj) {
  const bb = container.getBlockBlobClient(rel);
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await bb.uploadData(body, {
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }
  });
}

function nowISO() {
  return new Date().toISOString();
}

// ---- Robust evidence existence check (race-safe) ----
async function waitForEvidence(prefix, container, attempts = 3, delayMs = 150) {
  const evBlob = container.getBlockBlobClient(`${prefix}evidence.json`);
  const logBlob = container.getBlockBlobClient(`${prefix}evidence_log.json`);

  for (let i = 0; i < attempts; i++) {
    const ev = await evBlob.exists();
    const evLog = await logBlob.exists();
    if (ev || evLog) return true;
    await new Promise(r => setTimeout(r, delayMs));
  }
  return false;
}

// ======================================================================
//                               ROUTER
// ======================================================================
// ---------------------------------------------------------------
// PATCH 5 — Safe Router Trigger
// ---------------------------------------------------------------
module.exports = async function (context, job) {
  const log = context.log;

  // Parse message safely (string or object)
  const msg =
    typeof job === "string"
      ? (() => {
        try {
          return JSON.parse(job);
        } catch {
          return {};
        }
      })()
      : (job && typeof job === "object" ? job : {});

  const op = msg.op || "afterevidence";
  let runId = msg.runId || msg.run_id || "unknown";
  let prefix = msg.prefix || "";
  const page = msg.page || "campaign";

  log("[campaign-router] starting", { op, runId, prefix, page });

  // Only handle afterevidence-style messages
  if (op !== "afterevidence" && op !== "route" && op !== "reroute") {
    log("[campaign-router] unsupported op; skipping", { op });
    return;
  }

  if (!prefix) {
    log("[campaign-router] missing prefix; cannot route", { runId });
    return;
  }

  const container = getResultsContainerClient(); // your existing helper

  // Artefact blobs
  const evidenceBlob = container.getBlockBlobClient(`${prefix}evidence.json`);
  const csvBlob = container.getBlockBlobClient(`${prefix}csv_normalized.json`);

  const hasEvidence = await evidenceBlob.exists();
  const hasCsv = await csvBlob.exists();

  // -----------------------------------------------------------
  // SAFETY CHECK — Only run worker when *both* artefacts exist
  // -----------------------------------------------------------
  if (!hasEvidence || !hasCsv) {
    log("[campaign-router] evidence not ready; deferring worker", {
      runId,
      hasEvidence,
      hasCsv,
      prefix
    });
    return; // router exits without sending worker
  }

  // -----------------------------------------------------------
  // STATE CHECK — Prevent duplicate worker runs
  // -----------------------------------------------------------
  const status = (await getJson(container, `${prefix}status.json`)) || {};
  const markers = status.markers || {};

  if (markers.workerStarted) {
    log("[campaign-router] worker already started — skipping enqueue", {
      runId,
      prefix
    });
    return;
  }

  // -----------------------------------------------------------
  // ENQUEUE WORKER (single-time, idempotent)
  // -----------------------------------------------------------
  await enqueueTo(process.env.Q_CAMPAIGN_WORKER || "campaign-worker-jobs", {
    op: "run_strategy",
    runId,
    page,
    prefix
  });

  // Mark so router does not double-trigger
  status.markers = { ...markers, workerStarted: true };
  await putJson(container, `${prefix}status.json`, status);

  log("[campaign-router] worker enqueued successfully", {
    runId,
    prefix
  });
};
