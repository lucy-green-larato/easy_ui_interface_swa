// /api/campaign-router/index.js — Gold v8.3 canonical prefix router 06-12-2025
"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");

const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

module.exports = async function (context, queueItem) {
  const log = context.log;

  // ---------- Parse incoming message ----------
  const msg =
    typeof queueItem === "string"
      ? (() => { try { return JSON.parse(queueItem); } catch { return {}; } })()
      : queueItem && typeof queueItem === "object"
      ? queueItem
      : {};

  const op = msg.op || "";
  const runId = msg.runId || msg.run_id || "unknown";
  const page = msg.page || "campaign";
  let prefix = msg.prefix || "";

  log("[campaign-router] received", { op, runId, prefix });

  // ---------- Validate & normalise prefix ----------
  if (!prefix) {
    log("[campaign-router] missing prefix; cannot route");
    return;
  }

  if (prefix.startsWith(`${RESULTS_CONTAINER}/`)) {
    prefix = prefix.slice(`${RESULTS_CONTAINER}/`.length);
  }
  prefix = prefix.replace(/^\/+/, "");
  if (!prefix.endsWith("/")) prefix += "/";

  const container = await getResultsContainerClient();
  const statusPath = `${prefix}status.json`;

  let status =
    (await getJson(container, statusPath)) || { runId, markers: {}, history: [] };
  status.markers = status.markers || {};

  // ==============================================================
  // 1. afterstart → no-op
  // ==============================================================
  if (op === "afterstart") {
    log("[router] afterstart → no-op");
    return;
  }

  // ==============================================================
  // 2. afterevidence → enqueue outline ONCE
  // ==============================================================
  if (op === "afterevidence") {
    if (status.markers.outlineEnqueued) {
      log("[router] afterevidence: outline already enqueued; skipping");
      return;
    }

    const outlineQueue = process.env.Q_CAMPAIGN_OUTLINE || "campaign-outline";

    await enqueueTo(outlineQueue, {
      op: "run_outline",
      runId,
      page,
      prefix
    });

    status.markers.outlineEnqueued = true;
    await putJson(container, statusPath, status);

    log("[router] afterevidence → enqueued outline", { runId, prefix });
    return;
  }

  // ==============================================================
  // 3. afteroutline → enqueue WORKER (strategy engine)
  // ==============================================================

  if (op === "afteroutline") {
    if (status.markers.workerEnqueued) {
      log("[router] afteroutline: worker already enqueued; skipping");
      return;
    }

    const workerQueue =
      process.env.Q_CAMPAIGN_WORKER || "campaign-worker-jobs";

    await enqueueTo(workerQueue, {
      op: "run_strategy",   // This triggers strategy_v2 + viability generation
      runId,
      page,
      prefix
    });

    status.markers.workerEnqueued = true;
    await putJson(container, statusPath, status);

    log("[router] afteroutline → enqueued worker", { runId, prefix });
    return;
  }

  // ==============================================================
  // 4. afterworker → enqueue writer
  // ==============================================================

  if (op === "afterworker") {
    if (status.markers.writerEnqueued) {
      log("[router] afterworker: writer already enqueued; skipping");
      return;
    }

    const writerQueue = process.env.Q_CAMPAIGN_WRITE || "campaign-write";

    await enqueueTo(writerQueue, {
      op: "run_writer",
      runId,
      page,
      prefix
    });

    status.markers.writerEnqueued = true;
    await putJson(container, statusPath, status);

    log("[router] afterworker → enqueued writer", { runId, prefix });
    return;
  }

  // ==============================================================
  // 5. afterwrite → mark Completed
  // ==============================================================

  if (op === "afterwrite") {
    status.state = "Completed";
    status.markers.pipelineCompleted = true;

    await putJson(container, statusPath, status);

    log("[router] afterwrite → Completed", { runId, prefix });
    return;
  }

  // ==============================================================
  // 6. Fallback → unsupported op
  // ==============================================================

  log("[router] unsupported op; skipping", { op, runId, prefix });
};
