// /api/campaign-router/index.js — Unified canonical prefix version 30-11-2025 v 2
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

// ---- Section keys for Writer (unchanged) ----
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



// ======================================================================
//                               ROUTER
// ======================================================================
module.exports = async function (context, queueItem) {

  // ==========================================================================
  // 1. Parse Queue Message
  // ==========================================================================
  if (typeof queueItem === "string") {
    try { queueItem = JSON.parse(queueItem); }
    catch {
      context.log.error("[router] invalid JSON", queueItem);
      return;
    }
  }

  const op = queueItem.op || "";
  const runId = queueItem.runId || queueItem.id;
  const page = queueItem.page || "campaign";
  const user = queueItem.userId || queueItem.user || "anonymous";

  if (!runId) {
    context.log.error("[router] missing runId");
    return;
  }

  // ==========================================================================
  // 2. Compute canonical prefix
  // ==========================================================================
  const prefix = canonicalPrefix({
    runId,
    userId: user,
    page
  });
  context.log("[router] parsed", { op, runId, page, prefix });


  // ==========================================================================
  // 3. Blob + Queue Clients
  // ==========================================================================
  if (!STORAGE_CONN) {
    context.log.error("[router] STORAGE_CONN missing");
    return;
  }

  const blobSvc = BlobServiceClient.fromConnectionString(STORAGE_CONN);
  const container = blobSvc.getContainerClient(RESULTS_CONTAINER);

  const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
  const outlineQ = qs.getQueueClient(OUTLINE_QUEUE);
  const workerQ = qs.getQueueClient(WORKER_QUEUE);
  const writeQ = qs.getQueueClient(WRITE_QUEUE);

  await outlineQ.createIfNotExists();
  await workerQ.createIfNotExists();
  await writeQ.createIfNotExists();


  // ==========================================================================
  // 4. Load + restore original status.json semantics
  // ==========================================================================
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



  // ==========================================================================
  // 5. ROUTING: afterevidence
  // ==========================================================================
  if (op === "afterevidence") {
    const phase = "Router.afterevidence";

    // Check evidence
    const ev = await container.getBlockBlobClient(`${prefix}evidence.json`).exists();
    const evLog = await container.getBlockBlobClient(`${prefix}evidence_log.json`).exists();

    if (!ev && !evLog) {
      status0.markers.waitingForEvidence = true;
      pushHistory(phase, "evidence_missing");
      await saveStatus();
      return;
    }

    // ---- Outline queue ----
    if (!status0.markers.outlineEnqueued) {
      await outlineQ.sendMessage(JSON.stringify({ runId, page, prefix }));
      status0.markers.outlineEnqueued = true;
      pushHistory(phase, "outline_enqueued");
    }

    // ---- Strategy queue ----
    if (!status0.markers.strategyEnqueued) {
      await workerQ.sendMessage(JSON.stringify({ op: "run_strategy", runId, page, prefix }));
      status0.markers.strategyEnqueued = true;
      pushHistory(phase, "strategy_enqueued");
    }

    await saveStatus({ state: "Router" });
    return;
  }



  // ==========================================================================
  // 6. ROUTING: afteroutline
  // ==========================================================================
  if (op === "afteroutline") {
    const phase = "Router.afteroutline";

    // ---- Section fan-out ----
    if (!status0.markers.sectionsEnqueued) {
      for (const key of SECTION_KEYS) {
        await writeQ.sendMessage(JSON.stringify({
          op: "section",
          runId,
          page,
          prefix,
          section: key
        }));
      }
      status0.markers.sectionsEnqueued = true;
      pushHistory(phase, "sections_enqueued");
    }

    // ---- Assemble once ----
    if (!status0.markers.assembleEnqueued) {
      await writeQ.sendMessage(JSON.stringify({
        op: "assemble",
        runId,
        page,
        prefix
      }));
      status0.markers.assembleEnqueued = true;
      pushHistory(phase, "assemble_enqueued");
    }

    await saveStatus({ state: "Router" });
    return;
  }



  // ==========================================================================
  // 7. Unknown op → safe drop
  // ==========================================================================
  pushHistory("Router", `unknown_op:${op}`);
  await saveStatus();
  context.log.warn("[router] unhandled op → drop", { op });
};
