// /api/campaign-worker/index.js 07-11-2025 v19 (Option B — Orchestrator, expanded)
// Classic Azure Functions (function.json + scriptFile), CommonJS.
//
// Purpose:
//   Orchestrates the multi-function pipeline:
//   kickoff → afterevidence → outline → afteroutline → write all sections → assemble → afterassemble.
//   Provides robust idempotency (blob locks), artifact verification, consistent status history,
//   resilient queue enqueues with retries, and detailed logging for diagnostics.
//
// Contracts respected (no schema/paths renamed):
//   - Container: process.env.CAMPAIGN_RESULTS_CONTAINER || "results"
//   - Prefix: container-relative (already computed by start), preserved through all messages
//   - Evidence job is triggered by /api/campaign-start into Q_CAMPAIGN_EVIDENCE
//   - Outline job goes to Q_CAMPAIGN_OUTLINE; Section writes + assemble go to Q_CAMPAIGN_WRITE
//   - Final completion flips status to "Completed"
//   - Section keys include sales_enablement (explicitly)

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");

// ---------- ENV ----------
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const MAIN_QUEUE = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
const OUTLINE_QUEUE = process.env.Q_CAMPAIGN_OUTLINE || "campaign-outline";
const WRITE_QUEUE = process.env.Q_CAMPAIGN_WRITE || "campaign-write";

// ---------- Pipeline sections (fixed order not required, but kept stable) ----------
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

// ---------- Small utilities ----------
function nowISO() { return new Date().toISOString(); }
function normalizePrefix(p) {
  let x = String(p || "").trim();
  if (!x) return null;
  if (x.startsWith(`${RESULTS_CONTAINER}/`)) x = x.slice(`${RESULTS_CONTAINER}/`.length);
  if (x.startsWith("/")) x = x.replace(/^\/+/, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}
function svcBlob() {
  if (!STORAGE_CONN) throw new Error("AzureWebJobsStorage not configured");
  return BlobServiceClient.fromConnectionString(STORAGE_CONN);
}
function svcQueue() {
  if (!STORAGE_CONN) throw new Error("AzureWebJobsStorage not configured");
  return QueueServiceClient.fromConnectionString(STORAGE_CONN);
}
async function streamToString(readable) {
  const chunks = [];
  for await (const c of readable) chunks.push(c);
  return Buffer.concat(chunks).toString("utf8");
}
async function getJson(container, relPath) {
  const bc = container.getBlockBlobClient(relPath);
  if (!(await bc.exists())) return null;
  const dl = await bc.download();
  try { return JSON.parse(await streamToString(dl.readableStreamBody)); } catch { return null; }
}
async function putJson(container, relPath, obj) {
  const body = Buffer.from(JSON.stringify(obj, null, 2));
  await container.getBlockBlobClient(relPath)
    .uploadData(body, { blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" } });
}
async function appendStatusHistory(container, prefix, state, extra = {}) {
  const path = `${prefix}status.json`;
  const cur = (await getJson(container, path)) || {};
  const next = { ...cur };
  const stamp = nowISO();

  // maintain state and a lightweight history trail
  next.state = state;
  next.history = Array.isArray(cur.history) ? cur.history.slice() : [];
  next.history.push({ phase: state, at: stamp, ...extra });

  // keep existing 'input' if present; never grow it in orchestrator
  if (cur.input != null) next.input = cur.input;

  await putJson(container, path, next);
}

// Use tiny lock blobs to dedupe work across retries (idempotent)
// Returns true if we created the lock (i.e., caller should perform the action)
async function ensureLock(container, relPath, payload = undefined) {
  const bb = container.getBlockBlobClient(relPath);
  if (await bb.exists()) return false;
  const body = Buffer.from(JSON.stringify(payload || { at: nowISO() }));
  await bb.uploadData(body, { blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" } });
  return true;
}

// Queue send with small retry (handles transient 409/5xx)
async function sendQueue(queueName, messageObject, context, label = "enqueue") {
  const qs = svcQueue();
  const qc = qs.getQueueClient(queueName);
  await qc.createIfNotExists();

  const payload = JSON.stringify(messageObject);
  let attempt = 0; let lastErr;
  while (attempt < 3) {
    try {
      const res = await qc.sendMessage(payload); // SDK base64-encodes
      if (context) context.log({ event: label, queue: queueName, messageId: res?.messageId });
      return res;
    } catch (e) {
      lastErr = e;
      await new Promise(r => setTimeout(r, 200 * (1 << attempt))); // 200/400/800ms
      attempt++;
    }
  }
  throw lastErr;
}

// Artifact checks to prevent “loop on missing file”
// These are non-fatal (we log + annotate status), but they give you strong signals during ops.
async function assertArtifactExists(container, prefix, file, context, phase) {
  const bc = container.getBlockBlobClient(`${prefix}${file}`);
  const exists = await bc.exists();
  if (!exists) {
    context.log.warn("orchestrator_artifact_missing", { prefix, file, phase });
    await appendStatusHistory(container, prefix, "Warning", {
      code: "artifact_missing",
      file, during: phase, at: nowISO()
    });
  }
  return exists;
}

// ---------- Azure Function entry ----------
module.exports = async function (context, queueItem) {
  const started = Date.now();

  // Defensive parse
  let msg = queueItem;
  if (typeof msg === "string") { try { msg = JSON.parse(msg); } catch { msg = {}; } }
  const op = String(msg.op || "").toLowerCase();
  const runId = msg.runId || msg.id || null;
  const page = msg.page || "campaign";
  const prefix = normalizePrefix(msg.prefix) || (runId ? `runs/${runId}/` : null);

  // Early guards
  if (!runId || !prefix) {
    context.log.warn("orchestrator_ignored_message", { reason: !runId ? "missing_runId" : "missing_prefix", op });
    return;
  }

  // Storage
  const blob = svcBlob();
  const container = blob.getContainerClient(RESULTS_CONTAINER);
  await container.createIfNotExists();

  // Unified op switch
  try {
    // --- kickoff ---
    if (op === "kickoff") {
      await appendStatusHistory(container, prefix, "Evidence", { kickedAt: nowISO(), page });
      // No enqueue here; /api/campaign-start already enqueued the evidence job.
      context.log("orchestrator_kickoff", { runId, prefix, page, mainQueue: MAIN_QUEUE });
      return;
    }

    // --- afterevidence ---
    if (op === "afterevidence") {
      // Optional: verify evidence exists (non-fatal)
      await assertArtifactExists(container, prefix, "evidence_log.json", context, "afterevidence");

      await appendStatusHistory(container, prefix, "Outline", {
        outlineQueuedAt: nowISO(), page
      });

      // Queue outline exactly once
      const lockCreated = await ensureLock(container, `${prefix}locks/outline.queued.json`, { runId, at: nowISO() });
      if (lockCreated) {
        await sendQueue(OUTLINE_QUEUE, { op: "outline", runId, page, prefix }, context, "outline_enqueue");
        context.log("orchestrator_outline_enqueued", { runId, queue: OUTLINE_QUEUE });
      } else {
        context.log("orchestrator_outline_skip_duplicate", { runId });
      }
      return;
    }

    // --- afteroutline ---
    if (op === "afteroutline") {
      // Optional: verify outline exists (non-fatal)
      await assertArtifactExists(container, prefix, "outline.json", context, "afteroutline");

      await appendStatusHistory(container, prefix, "SectionWrites", {
        writeQueuedAt: nowISO(), page
      });

      // Enqueue section writers exactly once
      const wroteLock = await ensureLock(container, `${prefix}locks/write_all.queued.json`, { runId, at: nowISO() });
      if (wroteLock) {
        for (const section of SECTION_KEYS) {
          await sendQueue(WRITE_QUEUE, { op: "write_section", runId, page, prefix, section }, context, "write_section_enqueue");
        }
        context.log("orchestrator_write_all_enqueued", { runId, count: SECTION_KEYS.length, queue: WRITE_QUEUE });
      } else {
        context.log("orchestrator_write_all_skip_duplicate", { runId });
      }

      // Enqueue assemble exactly once
      const asmLock = await ensureLock(container, `${prefix}locks/assemble.queued.json`, { runId, at: nowISO() });
      if (asmLock) {
        await sendQueue(WRITE_QUEUE, { op: "assemble", runId, page, prefix }, context, "assemble_enqueue");
        context.log("orchestrator_assemble_enqueued", { runId, queue: WRITE_QUEUE });
      } else {
        context.log("orchestrator_assemble_skip_duplicate", { runId });
      }

      return;
    }

    // --- afterassemble ---
    if (op === "afterassemble") {
      // Optional: verify final exists (non-fatal)
      await assertArtifactExists(container, prefix, "campaign.json", context, "afterassemble");

      await appendStatusHistory(container, prefix, "Completed", { completedAt: nowISO(), page });
      context.log("orchestrator_completed", { runId, durationMs: Date.now() - started });
      return;
    }

    // Unknown op — do not throw (avoids poison queue). Log and exit.
    context.log.warn("orchestrator_unknown_op_ignored", { op, runId, prefix });
  } catch (err) {
    // Best-effort failure path: mark status and keep the message consumed
    context.log.error("orchestrator_failed", { runId, prefix, op, error: String(err?.message || err) });
    try {
      await appendStatusHistory(container, prefix, "Failed", {
        code: "orchestrator_error",
        message: String(err?.message || err),
        at: nowISO(),
        op, page
      });
    } catch { /* swallow */ }
  }
};
