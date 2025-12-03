// /api/campaign-router/index.js — Gold v7.1 canonical prefix router 03-12-2025
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
module.exports = async function (context, queueItem) {
  // ==================================================================
  // 1. Parse Queue Message
  // ==================================================================
  if (typeof queueItem === "string") {
    try {
      queueItem = JSON.parse(queueItem);
    } catch {
      context.log.error("[router] invalid JSON", queueItem);
      return;
    }
  }

  const op = queueItem.op || "";
  let runId;

  if (queueItem.runId && queueItem.id && queueItem.runId !== queueItem.id) {
    context.log.warn("[router] conflicting runId/id", queueItem);
  }

  runId = queueItem.runId || queueItem.id;
  const page = queueItem.page || "campaign";
  const user = queueItem.userId || queueItem.user || "anonymous";

  if (!runId) {
    context.log.error("[router] missing runId");
    return;
  }

  // ==================================================================
  // 2. Compute canonical prefix
  // ==================================================================
  const prefix = canonicalPrefix({
    runId,
    userId: user,
    page,
    date: queueItem.date ? new Date(queueItem.date) : undefined
  });
  context.log("[router] parsed", { op, runId, page, prefix });

  // ==================================================================
  // 3. Blob + Queue Clients
  // ==================================================================
  if (!STORAGE_CONN) {
    context.log.error("[router] STORAGE_CONN missing");
    return;
  }

  const blobSvc = BlobServiceClient.fromConnectionString(STORAGE_CONN);
  const container = blobSvc.getContainerClient(RESULTS_CONTAINER);
  await container.createIfNotExists();

  const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
  const outlineQ = qs.getQueueClient(OUTLINE_QUEUE);
  const workerQ = qs.getQueueClient(WORKER_QUEUE);
  const writeQ = qs.getQueueClient(WRITE_QUEUE);

  await outlineQ.createIfNotExists();
  await workerQ.createIfNotExists();
  await writeQ.createIfNotExists();

  // ==================================================================
  // 4. Load + restore status.json semantics
  // ==================================================================
  const statusPath = `${prefix}status.json`;

  let status0 =
    (await getJson(container, statusPath)) || {
      runId,
      state: "Router",
      history: [],
      markers: {}
    };

  // ensure correct shapes
  status0.history = Array.isArray(status0.history) ? status0.history : [];
  status0.markers = status0.markers || {};
  status0.flags = getFlags(status0);

  async function saveStatus(extra = {}) {
    const next = { ...status0, ...extra };
    next.flags = getFlags(next);
    await putJson(container, statusPath, next);
    status0 = next;
  }

  function pushHistory(phase, note, error = null) {
    status0.history.push({
      at: nowISO(),
      phase,
      note,
      error: error ? String(error) : undefined
    });
  }

  // ==================================================================
  // 5. Writer gating: enqueue once strategy_v2 exists
  // ==================================================================
  async function attemptEnqueueWriter(phaseLabel) {
    // Check strategy_v2/campaign_strategy.json presence
    const strategyBlob = container.getBlockBlobClient(
      `${prefix}strategy_v2/campaign_strategy.json`
    );
    const hasStrategy = await strategyBlob.exists();

    if (!hasStrategy) {
      pushHistory(phaseLabel, "writer_not_ready_missing_strategy_v2");
      await saveStatus({ state: "Router" });
      return false;
    }

    if (status0.markers.writerEnqueued) {
      pushHistory(phaseLabel, "writer_already_enqueued");
      await saveStatus({ state: "Router" });
      return false;
    }

    // Single-shot enqueue to writer queue
    await writeQ.sendMessage(
      JSON.stringify({
        op: "write",
        runId,
        page,
        prefix
      })
    );

    status0.markers.writerEnqueued = true;
    pushHistory(phaseLabel, "writer_enqueued");
    await saveStatus({ state: "Router" });
    context.log("[router] writer_enqueued", { runId, prefix });
    return true;
  }

  // ==================================================================
  // 6. ROUTING: afterevidence
  // ==================================================================
  if (op === "afterevidence") {
    const phase = "Router.afterevidence";

    // Robust, retry-aware check for evidence blobs
    const evidenceReady = await waitForEvidence(prefix, container);
    if (!evidenceReady) {
      status0.markers.waitingForEvidence = true;
      pushHistory(phase, "evidence_missing");
      await saveStatus({ state: "Router" });
      return;
    }

    // Outline queue (kick off outline once)
    if (!status0.markers.outlineEnqueued) {
      await outlineQ.sendMessage(JSON.stringify({ runId, page, prefix }));
      status0.markers.outlineEnqueued = true;
      pushHistory(phase, "outline_enqueued");
    }

    // Strategy/viability worker queue (once)
    if (!status0.markers.strategyEnqueued) {
      await workerQ.sendMessage(
        JSON.stringify({
          op: "run_strategy",
          runId,
          page,
          prefix
        })
      );
      status0.markers.strategyEnqueued = true;
      pushHistory(phase, "strategy_enqueued");
    }

    await saveStatus({ state: "Router" });
    return;
  }

  // ==================================================================
  // 7. ROUTING: afteroutline
  // ==================================================================
  if (op === "afteroutline") {
    const phase = "Router.afteroutline";

    // Outline has completed; record and attempt writer enqueue
    pushHistory(phase, "outline_completed_signal");
    await saveStatus({ state: "Router" });

    await attemptEnqueueWriter(phase);
    return;
  }

  // ==================================================================
  // 8. Any other op → best-effort writer gating + safe drop
  //    (e.g. future 'afterstrategy', 'afterviability', etc.)
  // ==================================================================
  const phase = `Router.${op || "unknown"}`;
  pushHistory("Router", `op:${op || "none"}`);
  await saveStatus({ state: "Router" });

  // Opportunistically try to enqueue writer if strategy_v2 is ready
  await attemptEnqueueWriter(phase);

  if (!op) {
    context.log.warn("[router] no op on message → treated as generic signal", {
      runId,
      prefix
    });
  } else {
    context.log.warn("[router] unhandled op (no special routing)", { op });
  }
};
