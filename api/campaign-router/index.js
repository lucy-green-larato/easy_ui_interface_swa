// /api/campaign-router/index.js — Gold v9.2 canonical prefix router (17-12-2025) 

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

// ✅ Phase 3 queue: local constant so router works even if lib/campaign-queue is unchanged
const COMPETITOR_ENRICH_QUEUE_NAME =
  process.env.Q_CAMPAIGN_COMPETITOR_ENRICH ||
  "campaign-competitor-enrich-jobs";

// ---------- helpers ----------
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
  if (p.startsWith(`${RESULTS_CONTAINER}/`)) p = p.slice(`${RESULTS_CONTAINER}/`.length);
  p = p.replace(/^\/+/, "");
  if (!p.endsWith("/")) p += "/";
  return p;
}

function pushHistory(status, phase, note) {
  if (!status.history || !Array.isArray(status.history)) status.history = [];
  status.history.push({
    at: new Date().toISOString(),
    phase: String(phase || "status"),
    note: note ? String(note) : ""
  });
}

async function persistStatus(container, statusPath, status) {
  await putJson(container, statusPath, status);
}

// ---------- main ----------
module.exports = async function (context, queueItem) {
  const log = context.log;

  const msg = parseQueueItem(queueItem);

  const op = String(msg.op || "").trim();
  const runId = msg.runId || msg.run_id || "unknown";
  const page = msg.page || "campaign";
  const prefix = normPrefix(msg.prefix || "");

  log("[router] received", { op, runId, prefix });

  if (!prefix) {
    log("[router] missing prefix; cannot route");
    return;
  }

  const container = await getResultsContainerClient();
  const statusPath = `${prefix}status.json`;

  let status = (await getJson(container, statusPath)) || { runId, markers: {}, history: [] };
  if (!status || typeof status !== "object") status = { runId, markers: {}, history: [] };
  if (!status.markers || typeof status.markers !== "object") status.markers = {};
  if (!Array.isArray(status.history)) status.history = [];

  pushHistory(status, "router_received", `op=${op || "(none)"}`);

  // ==============================================================
  // 1. afterstart → enqueue packsload (ONCE)
  // ==============================================================
  if (op === "afterstart") {
    if (status.markers.packsloadEnqueued) {
      log("[router] afterstart: packsload already enqueued; skipping");
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

  // ==============================================================
  // 1a. afterpacksload → enqueue markdown_pack (ONCE)
  // ==============================================================
  if (op === "afterpacksload") {
    if (status.markers.markdownPackEnqueued) {
      log("[router] afterpacksload: markdown_pack already enqueued; skipping");
      pushHistory(status, "markdown_pack_skip", "already enqueued");
      await persistStatus(container, statusPath, status);
      return;
    }

    // Prefer slugs provided by packsload message, fallback to status.context
    const industry_slug = msg.industry_slug ?? status?.context?.industry_slug ?? null;
    const supplier_slug = msg.supplier_slug ?? status?.context?.supplier_slug ?? null;
    const competitor_slugs = Array.isArray(msg.competitor_slugs)
      ? msg.competitor_slugs
      : (Array.isArray(status?.context?.competitor_slugs) ? status.context.competitor_slugs : []);

    await enqueueTo(MARKDOWN_QUEUE_NAME, {
      op: "run_markdown_pack",
      runId,
      page,
      prefix,
      industry_slug,
      supplier_slug,
      competitor_slugs
    });

    status.markers.markdownPackEnqueued = true;
    status.state = "markdown_pack_queued";
    pushHistory(status, "markdown_pack_queued");
    await persistStatus(container, statusPath, status);
    return;
  }

  // ==============================================================
  // 1b. aftermarkdown → enqueue evidence (ONCE)
  // ==============================================================
  if (op === "aftermarkdown") {
    if (status.markers.evidenceEnqueued) {
      log("[router] aftermarkdown: evidence already enqueued; skipping");
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

  // ==============================================================
  // 2. afterevidence → enqueue competitor enrichment (PHASE 3) (ONCE)
  // ==============================================================
  if (op === "afterevidence") {
    if (status.markers.competitorEnrichEnqueued) {
      log("[router] afterevidence: competitor enrichment already enqueued; skipping");
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

  // ==============================================================
  // 2a. aftercompetitorenrich → enqueue competitor scoring (ONCE)
  //      Phase 4a — deterministic competitor scoring
  // ==============================================================

  if (op === "aftercompetitorenrich") {
    if (status.markers.competitorScoreEnqueued) {
      log("[router] aftercompetitorenrich: competitor scoring already enqueued; skipping");
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

  // ==============================================================
  // 2b. aftercompetitorscored → enqueue outline (ONCE)
  //      Phase 4b → Phase 5 transition
  // ==============================================================

  if (op === "aftercompetitorscored") {
    if (status.markers.outlineEnqueued) {
      log("[router] aftercompetitorscored: outline already enqueued; skipping");
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

  // ==============================================================
  // 3. afteroutline → enqueue strategy WORKER (ONCE)
  // ==============================================================
  if (op === "afteroutline") {
    if (status.markers.workerEnqueued) {
      log("[router] afteroutline: worker already enqueued; skipping");
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

  // ==============================================================
  // 4. afterworker → enqueue writer (ONCE)
  // ==============================================================
  if (op === "afterworker") {
    if (status.markers.writerEnqueued) {
      log("[router] afterworker: writer already enqueued; skipping");
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

  // ==============================================================
  // 5. afterwrite → mark Completed AND enqueue LinkedIn (doctrine-gated) (ONCE)
  // ==============================================================
  if (op === "afterwrite") {
    status.state = "completed";
    status.markers.pipelineCompleted = true;
    pushHistory(status, "completed");

    // ✅ Doctrine: LinkedIn is downstream activation only,
    // gated by status.markers.strategyChanged === true
    const strategyChanged = status?.markers?.strategyChanged === true;

    if (strategyChanged) {
      if (!status.markers.linkedinEnqueued) {
        await enqueueTo(LINKEDIN_QUEUE_NAME, {
          op: "run_linkedin_activation",
          runId,
          page,
          prefix
        });
        status.markers.linkedinEnqueued = true;
        pushHistory(status, "linkedin_queued", "strategyChanged=true");
      } else {
        pushHistory(status, "linkedin_skip", "already enqueued");
      }
    } else {
      pushHistory(status, "linkedin_skip", "strategyChanged=false");
    }

    await persistStatus(container, statusPath, status);

    log("[router] afterwrite → completed (+linkedin gated)", { runId, prefix, strategyChanged });
    return;
  }

  // ==============================================================
  // 6. Unsupported ops → log + ignore (but persist receipt)
  // ==============================================================
  log("[router] unsupported op; skipping", { op, runId, prefix });
  pushHistory(status, "unsupported_op", op || "(none)");
  await persistStatus(container, statusPath, status);
};
