// 26/09/2025 v1 /api/campaign-start/index.js
// POST /api/campaign/start -> enqueue queue work item and return { runId }.
// Enforces roles, echoes x-correlation-id, and logs structured events.

const { QueueClient } = require("@azure/storage-queue");
const { BlobServiceClient } = require("@azure/storage-blob");
const { requireAuth, jsonError, cryptoRandomId } = require("../lib/auth");

const ALLOWED_ROLES_CAMPAIGN = ["campaign", "campaign-admin"];
const DEFAULT_QUEUE = process.env.CAMPAIGN_JOBS_QUEUE || "campaign-jobs";
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";

function correlationIdFrom(req) {
  return (req.headers?.["x-correlation-id"] || "").toString() || cryptoRandomId();
}

function utcParts(date = new Date()) {
  const yyyy = date.getUTCFullYear();
  const MM = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  return { yyyy, MM, dd };
}

function computePrefix({ page = "default", runId, now }) {
  const { yyyy, MM, dd } = utcParts(now);
  return `results/campaign/${page}/${yyyy}/${MM}/${dd}/${runId}/`; // per required pattern
}

module.exports = async function (context, req) {
  if (req.method !== "POST") {
    context.res = jsonError(400, "bad_request", "Invalid input", correlationIdFrom(req));
    return;
  }

  const auth = await requireAuth(context, req, ALLOWED_ROLES_CAMPAIGN);
  if (!auth.ok) return;
  const correlationId = auth.correlationId;

  try {
    const body = typeof req.body === "object" && req.body ? req.body : {};
    const { page, rowCount, filters, notes } = body;
    const runId = cryptoRandomId();
    const enqueuedAt = new Date().toISOString();
    const prefix = computePrefix({ page: page || "default", runId, now: new Date() });

    // Build message (plain JSON string, no base64)
    const message = {
      runId,
      page: page || "default",
      rowCount: typeof rowCount === "number" ? rowCount : undefined,
      filters: filters ?? undefined,
      notes: notes ?? undefined,
      enqueuedAt,
      prefix,
      correlationId,
    };
    const payload = JSON.stringify(message);

    // Enqueue
    const queueClient = new QueueClient(process.env.AzureWebJobsStorage, DEFAULT_QUEUE);
    await queueClient.createIfNotExists();
    await queueClient.sendMessage(payload);

    // (Optional but helpful) Write initial "Queued" status so status endpoint can find without delay
    const blobService = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
    const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);
    await containerClient.createIfNotExists();
    const statusClient = containerClient.getBlockBlobClient(`${prefix}status.json`);
    const initialStatus = {
      runId,
      state: "Queued",
      input: { page: page || "default", rowCount, filters, notes },
      enqueuedAt,
    };
    const initialJson = JSON.stringify(initialStatus);
    await statusClient.upload(initialJson, Buffer.byteLength(initialJson), {
      blobHTTPHeaders: { blobContentType: "application/json" },
    });

    context.log({
      event: "campaign_start_enqueued",
      runId,
      correlationId,
      queue: DEFAULT_QUEUE,
      outcome: "OK",
    });

    context.res = {
      status: 202,
      headers: {
        "Content-Type": "application/json",
        "x-correlation-id": correlationId,
      },
      body: { runId },
    };
  } catch (err) {
    context.log.error("campaign_start_error", err);
    context.res = jsonError(500, "internal", "Unexpected error", correlationId);
  }
};
