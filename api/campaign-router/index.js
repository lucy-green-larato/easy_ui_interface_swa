// /api/campaign-router/index.js 24-11-2025 v16 (with tracing)
// Trigger: queue %CAMPAIGN_QUEUE_NAME%
// Routes:
//   - { op:"afterevidence", runId, page, prefix }  → enqueue to %Q_CAMPAIGN_OUTLINE%
//   - { op:"afteroutline",  runId, page, prefix }  → enqueue N section jobs + one {op:"assemble"} to %Q_CAMPAIGN_WRITE%
//
// Idempotence:
//   - Uses status.json markers: outlineEnqueued, sectionsEnqueued, assembleEnqueued
//   - Never re-enqueues itself; never loops
//
// Paths are container-relative under results/<prefix>/…

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const { getFlags } = require("../lib/featureFlags");
const { getRunPrefix } = require("../lib/paths");

// ---- ENV ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const MAIN_QUEUE = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
const OUTLINE_QUEUE = process.env.Q_CAMPAIGN_OUTLINE || "campaign-outline";
const WRITE_QUEUE = process.env.Q_CAMPAIGN_WRITE || "campaign-write";

// ---- sections for writer fan-out (preserve names) ----
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

// ---- utils ----
function normalizePrefix(p) {
  let x = String(p || "").trim();
  if (!x) return null;
  if (x.startsWith(`${RESULTS_CONTAINER}/`)) x = x.slice(`${RESULTS_CONTAINER}/`.length);
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
  try { return JSON.parse(txt); } catch { return null; }
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

function nowISO() { return new Date().toISOString(); }

// ---- router ----
module.exports = async function (context, queueItem) {
  // TRACE 1: raw trigger payload
  context.log("[router] trigger", {
    type: typeof queueItem,
    raw: typeof queueItem === "string" ? queueItem.slice(0, 500) : queueItem
  });

  if (!STORAGE_CONN) {
    context.log.error("[router] AzureWebJobsStorage missing");
    return;
  }

  // Parse message (support string/object)
  let msg = queueItem;
  if (typeof msg === "string") {
    try { msg = JSON.parse(msg); } catch (e) {
      context.log.error("[router] failed to parse queueItem JSON", {
        error: String(e?.message || e),
        raw: msg.slice(0, 500)
      });
      msg = {};
    }
  }

  const op = (msg && msg.op) || "";
  const runId = (msg && msg.runId) || "";
  const page = (msg && msg.page) || "campaign";

  // Use getRunPrefix for the default, then normalize
  const defaultPrefix = runId ? getRunPrefix(runId) : "";
  let prefix = normalizePrefix((msg && msg.prefix) || defaultPrefix);

  // TRACE 2: parsed message
  context.log("[router] parsed message", {
    op,
    runId,
    page,
    prefix,
    defaultPrefix,
    MAIN_QUEUE,
    OUTLINE_QUEUE,
    WRITE_QUEUE
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

  const blobSvc = BlobServiceClient.fromConnectionString(STORAGE_CONN);
  const container = blobSvc.getContainerClient(RESULTS_CONTAINER);

  const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
  const outlineQ = qs.getQueueClient(OUTLINE_QUEUE);
  const writeQ = qs.getQueueClient(WRITE_QUEUE);

  await outlineQ.createIfNotExists();
  await writeQ.createIfNotExists();

  // Read/patch status for idempotence markers
  const statusPath = `${prefix}status.json`;
  const status0 = (await getJson(container, statusPath)) || { runId, history: [], markers: {} };

  status0.history = Array.isArray(status0.history) ? status0.history : [];
  status0.markers = status0.markers || {};

  // ---- Phase 0: normalise and persist feature flags ----
  const flags = getFlags(status0);
  status0.flags = flags;

  // TRACE 3: status + flags snapshot
  context.log("[router] status snapshot", {
    runId,
    prefix,
    op,
    state: status0.state || null,
    markers: status0.markers,
    flags
  });

  // Helper: persist status changes (while preserving flags normalisation)
  async function saveStatus(notePatch = {}) {
    const next = { ...status0, ...notePatch };
    next.flags = getFlags(next);
    await putJson(container, statusPath, next);

    context.log("[router] status saved", {
      runId,
      prefix,
      state: next.state || null,
      markers: next.markers,
      notePatch
    });
  }

  try {
    if (op === "afterevidence") {
      context.log("[router] handling afterevidence", { runId, prefix });

      // Only route to outline ONCE
      if (status0.markers.outlineEnqueued) {
        context.log("[router] outline already enqueued; skipping", {
          runId,
          prefix
        });
        return;
      }

      // Strong guard: require evidence.json or evidence_log.json to exist
      const evBlobName = `${prefix}evidence.json`;
      const evLogBlobName = `${prefix}evidence_log.json`;
      const evBlob = container.getBlockBlobClient(evBlobName);
      const evLogBlob = container.getBlockBlobClient(evLogBlobName);

      const evExists = await evBlob.exists();
      const evLogExists = await evLogBlob.exists();

      // TRACE 4: evidence existence check
      context.log("[router] evidence existence", {
        runId,
        prefix,
        evBlobName,
        evLogBlobName,
        evExists,
        evLogExists
      });

      const evOk = evExists || evLogExists;

      if (!evOk) {
        // Persist a forward-only marker that we are waiting on Evidence
        status0.history.push({
          at: nowISO(),
          phase: "Router",
          op: "afterevidence",
          note: "evidence_missing"
        });
        status0.markers.waitingForEvidence = true;
        await saveStatus();
        context.log.warn("[router] evidence missing; not enqueuing outline", {
          runId,
          prefix
        });
        return;
      }

      // Optional sanity marker if evidence finished
      status0.markers.evidenceDigestCompleted =
        status0.markers.evidenceDigestCompleted === true ||
        status0.state === "EvidenceDigest";

      const msgPayload = { runId, page, prefix };
      await outlineQ.sendMessage(JSON.stringify(msgPayload));

      // TRACE 5: outline enqueued
      context.log("[router] enqueued outline", {
        runId,
        prefix,
        queue: OUTLINE_QUEUE,
        payload: msgPayload
      });

      status0.history.push({
        at: nowISO(),
        phase: "Router",
        op: "afterevidence→outline",
        page
      });
      status0.markers.outlineEnqueued = true;
      await saveStatus();

      return;
    }

    if (op === "afteroutline") {
      context.log("[router] handling afteroutline", { runId, prefix });

      // Fan-out sections exactly once
      if (!status0.markers.sectionsEnqueued) {
        for (const key of SECTION_KEYS) {
          const payload = { op: "section", runId, page, prefix, section: key };
          await writeQ.sendMessage(JSON.stringify(payload));

          // TRACE 6: per-section enqueue
          context.log("[router] enqueued section", {
            runId,
            prefix,
            section: key,
            queue: WRITE_QUEUE
          });
        }
        status0.markers.sectionsEnqueued = true;
        status0.history.push({
          at: nowISO(),
          phase: "Router",
          op: "afteroutline→sections",
          count: SECTION_KEYS.length
        });
        await saveStatus();
        context.log("[router] all sections enqueued", {
          runId,
          count: SECTION_KEYS.length
        });
      } else {
        context.log("[router] sections already enqueued; skipping", {
          runId,
          prefix
        });
      }

      // Enqueue a single assemble once
      if (!status0.markers.assembleEnqueued) {
        const assemblePayload = { op: "assemble", runId, page, prefix };
        await writeQ.sendMessage(JSON.stringify(assemblePayload));

        // TRACE 7: assemble enqueue
        context.log("[router] enqueued assemble", {
          runId,
          prefix,
          queue: WRITE_QUEUE,
          payload: assemblePayload
        });

        status0.markers.assembleEnqueued = true;
        status0.history.push({
          at: nowISO(),
          phase: "Router",
          op: "afteroutline→assemble"
        });
        await saveStatus();
      } else {
        context.log("[router] assemble already enqueued; skipping", {
          runId,
          prefix
        });
      }
      return;
    }

    if (op === "run_strategy") {
      const msgOut = {
        runId,
        prefix,
        input: baseInput || {},
        op: "run_strategy"
      };

      context.log("[router] Enqueueing strategy worker job", {
        queue: WORKER_QUEUE,
        runId,
        prefix
      });

      await context.bindings.queueOutput.push(msgOut);
      return;
    }

    // Unknown op: log and drop (no loops)
    context.log.warn("[router] unhandled op; dropping", { op, runId, prefix });
  } catch (err) {
    const msgText = String(err?.message || err);
    context.log.error("[router] failure", msgText);
    try {
      const cur = (await getJson(container, statusPath)) || { runId, history: [], markers: {} };
      cur.history = Array.isArray(cur.history) ? cur.history : [];
      cur.markers = cur.markers || {};
      cur.history.push({
        at: nowISO(),
        phase: "Router",
        op,
        error: msgText
      });
      await putJson(container, statusPath, cur);
    } catch {
      // ignore secondary failure
    }
  }
};
