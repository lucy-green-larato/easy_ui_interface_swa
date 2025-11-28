// /api/campaign-router/index.js — Option 1 unified queue client version
// 29-11-2025 — Verified working build

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const { getFlags } = require("../lib/featureFlags");
const { getRunPrefix } = require("../lib/paths");

// ---- ENV ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";

const MAIN_QUEUE = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
const OUTLINE_QUEUE = process.env.Q_CAMPAIGN_OUTLINE || "campaign-outline";
const WORKER_QUEUE = process.env.Q_CAMPAIGN_WORKER || "campaign-worker-jobs";
const WRITE_QUEUE = process.env.Q_CAMPAIGN_WRITE || "campaign-write";

// ---- Sections for writer fan-out ----
const SECTION_KEYS = [
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

// ---- Helpers ----
function normalizePrefix(p) {
  let x = String(p || "").trim();
  if (!x) return null;
  if (x.startsWith(`${RESULTS_CONTAINER}/`)) {
    x = x.slice(`${RESULTS_CONTAINER}/`.length);
  }
  x = x.replace(/^\/+/, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}

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
  try {
    return JSON.parse(txt);
  } catch {
    return null;
  }
}

async function putJson(container, rel, obj) {
  const bb = container.getBlockBlobClient(rel);
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await bb.uploadData(body, {
    blobHTTPHeaders: {
      blobContentType: "application/json; charset=utf-8"
    }
  });
}

function nowISO() {
  return new Date().toISOString();
}

module.exports = async function (context, queueItem) {
  context.log("[router] trigger", {
    type: typeof queueItem,
    raw: typeof queueItem === "string" ? queueItem.slice(0, 500) : queueItem
  });

  if (!STORAGE_CONN) {
    context.log.error("[router] AzureWebJobsStorage missing");
    return;
  }

  // Parse message
  let msg = queueItem;
  if (typeof msg === "string") {
    try {
      msg = JSON.parse(msg);
    } catch (e) {
      context.log.error("[router] failed to parse queueItem JSON", {
        error: String(e?.message || e),
        raw: msg.slice(0, 500)
      });
      msg = {};
    }
  }

  const op = msg.op || "";
  const runId = msg.runId || "";
  const page = msg.page || "campaign";

  const defaultPrefix = runId ? getRunPrefix(runId) : "";
  let prefix = normalizePrefix(msg.prefix || defaultPrefix);

  context.log("[router] parsed message", {
    op,
    runId,
    page,
    prefix
  });

  if (!op || !runId || !prefix) {
    context.log.warn("[router] invalid payload; dropping", {
      reason: "missing_op_runId_or_prefix",
      op,
      runId,
      prefix
    });
    return;
  }

  // ---- Blob + Queue Clients ----
  const blobSvc = BlobServiceClient.fromConnectionString(STORAGE_CONN);
  const container = blobSvc.getContainerClient(RESULTS_CONTAINER);

  const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
  const outlineQ = qs.getQueueClient(OUTLINE_QUEUE);
  const workerQ = qs.getQueueClient(WORKER_QUEUE);
  const writeQ = qs.getQueueClient(WRITE_QUEUE);

  await outlineQ.createIfNotExists();
  await workerQ.createIfNotExists();
  await writeQ.createIfNotExists();

  // ---- Load and normalise status ----
  const statusPath = `${prefix}status.json`;
  const status0 =
    (await getJson(container, statusPath)) || { runId, history: [], markers: {} };

  status0.history = Array.isArray(status0.history)
    ? status0.history
    : [];
  status0.markers = status0.markers || {};

  const flags = getFlags(status0);
  status0.flags = flags;

  context.log("[router] status snapshot", {
    runId,
    prefix,
    op,
    state: status0.state || null,
    markers: status0.markers,
    flags
  });

  async function saveStatus(patch = {}) {
    const next = { ...status0, ...patch };
    next.flags = getFlags(next);
    await putJson(container, statusPath, next);
    context.log("[router] status saved", {
      runId,
      prefix,
      state: next.state,
      markers: next.markers
    });
  }

  // ------------------------------------------------------------------
  //                        afterevidence
  // ------------------------------------------------------------------
  if (op === "afterevidence") {
    context.log("[router] handling afterevidence", { runId, prefix });

    // Check evidence
    const evBlobName = `${prefix}evidence.json`;
    const evLogBlobName = `${prefix}evidence_log.json`;

    const evExists = await container.getBlockBlobClient(evBlobName).exists();
    const evLogExists = await container.getBlockBlobClient(evLogBlobName).exists();

    context.log("[router] evidence existence", {
      evExists,
      evLogExists
    });

    if (!evExists && !evLogExists) {
      status0.markers.waitingForEvidence = true;
      status0.history.push({
        at: nowISO(),
        phase: "Router",
        op: "afterevidence",
        note: "evidence_missing"
      });
      await saveStatus();
      return;
    }

    // ---- Enqueue outline once ----
    if (!status0.markers.outlineEnqueued) {
      const outlinePayload = { runId, page, prefix };
      await outlineQ.sendMessage(JSON.stringify(outlinePayload));

      context.log("[router] enqueued outline", {
        runId,
        prefix,
        queue: OUTLINE_QUEUE
      });

      status0.markers.outlineEnqueued = true;
      status0.history.push({
        at: nowISO(),
        phase: "Router",
        op: "afterevidence→outline"
      });
    }

    // ---- Enqueue strategy once ----
    if (!status0.markers.strategyEnqueued) {
      const strategyPayload = { op: "run_strategy", runId, page, prefix };
      await workerQ.sendMessage(JSON.stringify(strategyPayload));

      context.log("[router] enqueued strategy", {
        runId,
        prefix,
        queue: WORKER_QUEUE
      });

      status0.markers.strategyEnqueued = true;
      status0.history.push({
        at: nowISO(),
        phase: "Router",
        op: "afterevidence→strategy"
      });
    }

    await saveStatus();
    return;
  }

  // ------------------------------------------------------------------
  //                          afteroutline
  // ------------------------------------------------------------------
  if (op === "afteroutline") {
    context.log("[router] handling afteroutline", { runId, prefix });

    // ---- Fan out sections once ----
    if (!status0.markers.sectionsEnqueued) {
      for (const key of SECTION_KEYS) {
        const payload = { op: "section", runId, page, prefix, section: key };
        await writeQ.sendMessage(JSON.stringify(payload));

        context.log("[router] enqueued section", {
          runId,
          prefix,
          section: key
        });
      }

      status0.markers.sectionsEnqueued = true;
      status0.history.push({
        at: nowISO(),
        phase: "Router",
        op: "afteroutline→sections",
        count: SECTION_KEYS.length
      });
    }

    // ---- Assemble once ----
    if (!status0.markers.assembleEnqueued) {
      const assemblePayload = { op: "assemble", runId, page, prefix };
      await writeQ.sendMessage(JSON.stringify(assemblePayload));

      context.log("[router] enqueued assemble", {
        runId,
        prefix
      });

      status0.markers.assembleEnqueued = true;
      status0.history.push({
        at: nowISO(),
        phase: "Router",
        op: "afteroutline→assemble"
      });
    }

    await saveStatus();
    return;
  }

  // ------------------------------------------------------------------
  //                     Unknown op → drop
  // ------------------------------------------------------------------
  context.log.warn("[router] unhandled op; dropping", { op, runId, prefix });
};
