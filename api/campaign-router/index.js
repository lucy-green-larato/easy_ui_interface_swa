// /api/campaign-router/index.js
// Gold v9.7 — canonical, idempotent, prefix-safe router
// 29-12-2025
//
"use strict";

const {
  enqueueTo,
  PACKSLOAD_QUEUE_NAME,
  MARKDOWN_QUEUE_NAME,
  EVIDENCE_QUEUE_NAME,
  OUTLINE_QUEUE_NAME,
  WORKER_QUEUE_NAME,
  WRITE_QUEUE_NAME,
  LINKEDIN_QUEUE_NAME,
  COMPETITOR_SCORE_QUEUE_NAME
} = require("../lib/campaign-queue");

const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");

const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

// Phase 3 queue (local, explicit)
const COMPETITOR_ENRICH_QUEUE_NAME =
  process.env.Q_CAMPAIGN_COMPETITOR_ENRICH ||
  "campaign-competitor-enrich-jobs";

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------
function parseQueueItem(queueItem) {
  if (!queueItem) return {};
  if (typeof queueItem === "string") {
    try { return JSON.parse(queueItem); } catch { return {}; }
  }
  return (queueItem && typeof queueItem === "object") ? queueItem : {};
}

function normPrefix(prefix) {
  let p = String(prefix || "").trim();
  if (!p) return "";
  if (p.startsWith(`${RESULTS_CONTAINER}/`)) {
    p = p.slice(`${RESULTS_CONTAINER}/`.length);
  }
  p = p.replace(/^\/+/, "");
  if (!p.endsWith("/")) p += "/";
  return p;
}

function pushHistory(status, phase, note) {
  if (!Array.isArray(status.history)) status.history = [];
  status.history.push({
    at: new Date().toISOString(),
    phase: String(phase || "status"),
    note: note ? String(note) : ""
  });
}

async function persistStatus(container, path, status) {
  await putJson(container, path, status);
}

// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------
module.exports = async function (context, queueItem) {
  const log = context.log;
  const msg = parseQueueItem(queueItem);

  const op = String(msg.op || "").trim();
  let runId = (typeof msg.runId === "string" && msg.runId.trim())
    ? msg.runId.trim()
    : (typeof msg.run_id === "string" && msg.run_id.trim())
      ? msg.run_id.trim()
      : null;

  if (!runId) {
    try {
      const parts = prefix.split("/").filter(Boolean);
      if (parts.length) runId = parts[parts.length - 1];
    } catch { /* noop */ }
  }
  if (!runId) runId = "unknown";
  const page = msg.page || "campaign";
  const prefix = normPrefix(msg.prefix || "");

  log("[router] received", { op, runId, prefix });

  if (!prefix) {
    log("[router] missing prefix; aborting");
    return;
  }

  const container = await getResultsContainerClient();
  const statusPath = `${prefix}status.json`;

  let status = {};
  try {
    status = (await getJson(container, statusPath)) || {};
  } catch (e) {
    log("[router] status read failed", {
      runId,
      prefix,
      statusPath,
      err: String(e?.message || e)
    });
    return;
  }

  if (!status || typeof status !== "object") status = {};
  if (!status.markers || typeof status.markers !== "object") status.markers = {};
  if (!Array.isArray(status.history)) status.history = [];
  if (!status.runId) status.runId = runId;

  pushHistory(status, "router_received", `op=${op || "(none)"}`);

  // ---------------------------------------------------------------------------
  // afterstart → packsload (ONCE)
  // ---------------------------------------------------------------------------
  if (op === "afterstart") {
    if (status.markers.packsloadEnqueued) {
      pushHistory(status, "packsload_skip", "already enqueued");
      await persistStatus(container, statusPath, status);
      return;
    }

    await enqueueTo(PACKSLOAD_QUEUE_NAME, {
      op: "run_packsload",
      runId,
      page,
      prefix
    });

    status.markers.packsloadEnqueued = true;
    status.state = "packsload_queued";
    pushHistory(status, "packsload_queued");
    await persistStatus(container, statusPath, status);
    return;
  }

  // ---------------------------------------------------------------------------
  // afterpacksload → markdown_pack (ONCE)
  // ---------------------------------------------------------------------------
  if (op === "afterpacksload") {
    if (status.markers.markdownPackEnqueued) {
      pushHistory(status, "markdown_pack_skip", "already enqueued");
      await persistStatus(container, statusPath, status);
      return;
    }

    await enqueueTo(MARKDOWN_QUEUE_NAME, {
      op: "run_markdown_pack",
      runId,
      page,
      prefix,
      industry_slug: msg.industry_slug ?? null,
      supplier_slug: msg.supplier_slug ?? null,
      competitor_slugs: Array.isArray(msg.competitor_slugs) ? msg.competitor_slugs : []
    });

    status.markers.markdownPackEnqueued = true;
    status.state = "markdown_pack_queued";
    pushHistory(status, "markdown_pack_queued");
    await persistStatus(container, statusPath, status);
    return;
  }

  // ---------------------------------------------------------------------------
  // aftermarkdown → evidence (ONCE)
  // ---------------------------------------------------------------------------
  if (op === "aftermarkdown") {
    if (status.markers.evidenceEnqueued) {
      pushHistory(status, "evidence_skip", "already enqueued");
      await persistStatus(container, statusPath, status);
      return;
    }

    await enqueueTo(EVIDENCE_QUEUE_NAME, {
      op: "run_evidence",
      runId,
      page,
      prefix
    });

    status.markers.evidenceEnqueued = true;
    status.state = "evidence_queued";
    pushHistory(status, "evidence_queued");
    await persistStatus(container, statusPath, status);
    return;
  }
  // ---------------------------------------------------------------------------
  // afterevidence → competitor enrich (ONCE)
  // ---------------------------------------------------------------------------
  if (op === "afterevidence") {
    if (status.markers.competitorEnrichEnqueued) {
      pushHistory(status, "competitor_enrich_skip", "already enqueued");
      await persistStatus(container, statusPath, status);
      return;
    }

    await enqueueTo(COMPETITOR_ENRICH_QUEUE_NAME, {
      op: "enrich_competitors",
      runId,
      page,
      prefix
    });

    status.markers.competitorEnrichEnqueued = true;
    status.state = "competitor_enrich_queued";
    pushHistory(status, "competitor_enrich_queued");
    await persistStatus(container, statusPath, status);
    return;
  }

  // ---------------------------------------------------------------------------
  // aftercompetitorenrich → competitor scoring (ONCE)
  // ---------------------------------------------------------------------------
  if (op === "aftercompetitorenrich") {
    if (status.markers.competitorScoreEnqueued) {
      pushHistory(status, "competitor_scoring_skip", "already enqueued");
      await persistStatus(container, statusPath, status);
      return;
    }

    await enqueueTo(COMPETITOR_SCORE_QUEUE_NAME, {
      op: "score_competitors",
      runId,
      page,
      prefix
    });

    status.markers.competitorScoreEnqueued = true;
    status.state = "competitor_scoring_queued";
    pushHistory(status, "competitor_scoring_queued");
    await persistStatus(container, statusPath, status);
    return;
  }

  // ---------------------------------------------------------------------------
  // aftercompetitorscored → outline (ONCE)
  // ---------------------------------------------------------------------------
  if (op === "aftercompetitorscored") {
    if (status.markers.outlineEnqueued) {
      pushHistory(status, "outline_skip", "already enqueued");
      await persistStatus(container, statusPath, status);
      return;
    }

    await enqueueTo(OUTLINE_QUEUE_NAME, {
      op: "run_outline",
      runId,
      page,
      prefix
    });

    status.markers.outlineEnqueued = true;
    status.state = "outline_queued";
    pushHistory(status, "outline_queued");
    await persistStatus(container, statusPath, status);
    return;
  }

  // ---------------------------------------------------------------------------
  // afteroutline → worker (ONCE)
  // ---------------------------------------------------------------------------
  if (op === "afteroutline") {
    if (status.markers.workerEnqueued) {
      pushHistory(status, "worker_skip", "already enqueued");
      await persistStatus(container, statusPath, status);
      return;
    }

    await enqueueTo(WORKER_QUEUE_NAME, {
      op: "run_strategy",
      runId,
      page,
      prefix
    });

    status.markers.workerEnqueued = true;
    status.state = "worker_queued";
    pushHistory(status, "worker_queued");
    await persistStatus(container, statusPath, status);
    return;
  }

  // ---------------------------------------------------------------------------
  // afterworker → writer (ONCE)
  // ---------------------------------------------------------------------------
  if (op === "afterworker") {
    if (status.markers.writerEnqueued) {
      pushHistory(status, "writer_skip", "already enqueued");
      await persistStatus(container, statusPath, status);
      return;
    }

    await enqueueTo(WRITE_QUEUE_NAME, {
      op: "run_writer",
      runId,
      page,
      prefix
    });

    status.markers.writerEnqueued = true;
    status.state = "writer_queued";
    pushHistory(status, "writer_queued");
    await persistStatus(container, statusPath, status);
    return;
  }

  // ---------------------------------------------------------------------------
  // afterwrite → completed (+ linkedin gated) (ONCE)
  // ---------------------------------------------------------------------------
  if (op === "afterwrite") {
    status.state = "completed";
    status.markers.pipelineCompleted = true;
    pushHistory(status, "completed");

    const strategyChanged = status.markers.strategyChanged === true;

    if (strategyChanged && !status.markers.linkedinEnqueued) {
      await enqueueTo(LINKEDIN_QUEUE_NAME, {
        op: "run_linkedin_activation",
        runId,
        page,
        prefix
      });
      status.markers.linkedinEnqueued = true;
      pushHistory(status, "linkedin_queued", "strategyChanged=true");
    } else if (!strategyChanged) {
      pushHistory(status, "linkedin_skip", "strategyChanged=false");
    }

    await persistStatus(container, statusPath, status);
    return;
  }

  // ---------------------------------------------------------------------------
  // unsupported op
  // ---------------------------------------------------------------------------
  log("[router] unsupported op; skipping", { op, runId, prefix });
  pushHistory(status, "unsupported_op", op || "(none)");
  await persistStatus(container, statusPath, status);
};
