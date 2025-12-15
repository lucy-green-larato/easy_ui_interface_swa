// /api/campaign-router/index.js — Gold v8.6 canonical prefix router (15-12-2025)

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

  log("[router] received", { op, runId, prefix });

  // ---------- Validate & normalise prefix ----------
  if (!prefix) {
    log("[router] missing prefix; cannot route");
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

  if (!Array.isArray(status.history)) status.history = [];
  if (!status.markers) status.markers = {};

  // ==============================================================
  // 1. afterstart → wait for packsload
  // ==============================================================
  if (op === "afterstart") {
    if (status.markers.packsloadEnqueued) return;

    const packsloadQueue =
      process.env.Q_CAMPAIGN_PACKSLOAD || "campaign-packsload";

    await enqueueTo(packsloadQueue, {
      op: "run_packsload",
      runId,
      page,
      prefix
    });

    status.markers.packsloadEnqueued = true;
    status.state = "packsload_queued";
    await putJson(container, statusPath, status);

    return;
  }

  // ==============================================================
  // 1a. afterpacksload → enqueue markdown_pack ONCE
  // ==============================================================
  if (op === "afterpacksload") {
    if (status.markers.markdownPackEnqueued) {
      log("[router] afterpacksload: markdown_pack already enqueued; skipping");
      return;
    }

    const markdownQueue =
      process.env.Q_CAMPAIGN_MARKDOWN || "campaign-markdown-pack";

    await enqueueTo(markdownQueue, {
      op: "run_markdown_pack",
      runId,
      page,
      prefix
    });

    status.markers.markdownPackEnqueued = true;
    status.state = "markdown_pack_queued";
    await putJson(container, statusPath, status);

    log("[router] afterpacksload → enqueued markdown_pack", { runId, prefix });
    return;
  }

  // ==============================================================
  // 1b. aftermarkdown → enqueue evidence ONCE
  // ==============================================================
  if (op === "aftermarkdown") {
    if (status.markers.evidenceEnqueued) {
      log("[router] aftermarkdown: evidence already enqueued; skipping");
      return;
    }

    const evidenceQueue =
      process.env.Q_CAMPAIGN_EVIDENCE || "campaign-evidence";

    await enqueueTo(evidenceQueue, {
      op: "run_evidence",
      runId,
      page,
      prefix
    });

    status.markers.evidenceEnqueued = true;
    status.state = "evidence_queued";
    await putJson(container, statusPath, status);

    log("[router] aftermarkdown → enqueued evidence", { runId, prefix });
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
    status.state = "outline_queued";
    await putJson(container, statusPath, status);

    log("[router] afterevidence → enqueued outline", { runId, prefix });
    return;
  }

  // ==============================================================
  // 3. afteroutline → enqueue strategy WORKER ONCE
  // ==============================================================
  if (op === "afteroutline") {
    if (status.markers.workerEnqueued) {
      log("[router] afteroutline: worker already enqueued; skipping");
      return;
    }

    const workerQueue = process.env.Q_CAMPAIGN_WORKER || "campaign-worker-jobs";

    await enqueueTo(workerQueue, {
      op: "run_strategy",
      runId,
      page,
      prefix
    });

    status.markers.workerEnqueued = true;
    status.state = "worker_queued";
    await putJson(container, statusPath, status);

    log("[router] afteroutline → enqueued worker", { runId, prefix });
    return;
  }

  // ==============================================================
  // 4. afterworker → enqueue writer ONCE
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
    status.state = "writer_queued";
    await putJson(container, statusPath, status);

    log("[router] afterworker → enqueued writer", { runId, prefix });
    return;
  }

  // ==============================================================
  // 5. afterwrite → mark pipeline Completed
  // ==============================================================
  if (op === "afterwrite") {
    status.state = "Completed";
    status.markers.pipelineCompleted = true;

    await putJson(container, statusPath, status);

    log("[router] afterwrite → pipeline Completed", { runId, prefix });
    return;
  }

  // ==============================================================
  // 6. Unsupported ops → log + ignore
  // ==============================================================
  log("[router] unsupported op; skipping", { op, runId, prefix });
};

