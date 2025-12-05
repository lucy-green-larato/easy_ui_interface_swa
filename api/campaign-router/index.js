// /api/campaign-router/index.js — Gold v8.0 canonical prefix router (Option B)
// Responsibility:
//   - Listen on campaign-router-jobs
//   - Route between stages using queue ops:
//       * afterstart     → (no-op; start already enqueued evidence)
//       * afterevidence  → enqueue campaign-outline
//       * afteroutline   → enqueue campaign-write
//       * afterwrite     → mark pipeline Completed
//
//   - No direct calls to campaign-worker.
//   - Idempotent via status.json.markers flags.

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const {
  getResultsContainerClient,
  getJson,
  putJson
} = require("../shared/storage");

const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

module.exports = async function (context, queueItem) {
  const log = context.log;

  // -------- Parse message safely --------
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

  const op = msg.op || "";
  const runId = msg.runId || msg.run_id || "unknown";
  const page = msg.page || "campaign";
  let prefix = msg.prefix || "";

  log("[campaign-router] received", { op, runId, prefix, page });

  // -------- Require prefix for all routing --------
  if (!prefix) {
    log("[campaign-router] missing prefix; cannot route", { runId });
    return;
  }

  // Normalise prefix: strip container name if present, strip leading slash, ensure trailing slash
  if (prefix.startsWith(`${RESULTS_CONTAINER}/`)) {
    prefix = prefix.slice(`${RESULTS_CONTAINER}/`.length);
  }
  prefix = String(prefix).replace(/^\/+/, "");
  if (!prefix.endsWith("/")) prefix += "/";

  const container = await getResultsContainerClient();
  const statusPath = `${prefix}status.json`;

  // Load current status (or skeleton)
  const status =
    (await getJson(container, statusPath)) || { runId, history: [], markers: {} };
  status.markers = status.markers || {};

  // =====================================================================
  // 1. afterstart → no-op (start already enqueued evidence)
  // =====================================================================
  if (op === "afterstart") {
    log("[campaign-router] afterstart → no-op (evidence already queued by start)", {
      runId,
      prefix
    });
    return;
  }

  // =====================================================================
  // 2. afterevidence → enqueue outline (once)
  // =====================================================================
  if (op === "afterevidence") {
    if (status.markers.outlineEnqueued) {
      log(
        "[campaign-router] afterevidence: outline already enqueued; skipping",
        { runId, prefix }
      );
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

    log("[campaign-router] afterevidence → enqueued outline", {
      runId,
      prefix,
      outlineQueue
    });
    return;
  }

  // =====================================================================
  // 3. afteroutline → enqueue writer (once)
  // =====================================================================
  if (op === "afteroutline") {
    if (status.markers.writerEnqueued) {
      log(
        "[campaign-router] afteroutline: writer already enqueued; skipping",
        { runId, prefix }
      );
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

    log("[campaign-router] afteroutline → enqueued writer", {
      runId,
      prefix,
      writerQueue
    });
    return;
  }

  // =====================================================================
  // 4. afterwrite → mark Completed
  // =====================================================================
  if (op === "afterwrite") {
    status.state = "Completed";
    status.markers.pipelineCompleted = true;
    await putJson(container, statusPath, status);

    log("[campaign-router] afterwrite → marked pipeline Completed", {
      runId,
      prefix
    });
    return;
  }

  // =====================================================================
  // 5. Unsupported ops → log + return
  // =====================================================================
  log("[campaign-router] unsupported op; skipping", { op, runId, prefix });
};
