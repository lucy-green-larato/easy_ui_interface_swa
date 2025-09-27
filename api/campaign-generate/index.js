// 26-09-2025 v1 /api/campaign-generate/index.js
// Single HTTP function that handles: 
//  POST /api/campaign/start
//  GET  /api/campaign/status?runId=...
//  GET  /api/campaign/fetch?runId=...&file=campaign|evidence_log|status

const { QueueClient } = require("@azure/storage-queue");
const { BlobServiceClient } = require("@azure/storage-blob");
const path = require("path");
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
  const { yyyy, MM, dd } = utcParts(now || new Date());
  return `results/campaign/${page}/${yyyy}/${MM}/${dd}/${runId}/`;
}
async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}
async function findStatusBlob(containerClient, runId) {
  const suffix = `/${runId}/status.json`;
  const prefixRoot = `results/campaign/`;
  for await (const item of containerClient.listBlobsFlat({ prefix: prefixRoot })) {
    if (item.name.endsWith(suffix)) return item.name;
  }
  return null;
}
async function locateRunPrefix(containerClient, runId) {
  const name = await findStatusBlob(containerClient, runId);
  if (!name) return null;
  return name.slice(0, name.length - "status.json".length);
}

module.exports = async function (context, req) {
  const action = (context.bindingData?.action || "").toLowerCase();
  const method = req.method?.toUpperCase();
  const correlationId = correlationIdFrom(req);

  // Enforce roles (anonymous HTTP + app-level role check)
  const auth = await requireAuth(context, req, ALLOWED_ROLES_CAMPAIGN);
  if (!auth.ok) return; // requireAuth sets context.res on failure

  try {
    const blobService = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
    const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);
    await containerClient.createIfNotExists();

    // ---------- ACTION: start ----------
    if (action === "start" && method === "POST") {
      const body = typeof req.body === "object" && req.body ? req.body : {};
      const { page, rowCount, filters, notes } = body;
      const runId = cryptoRandomId();
      const enqueuedAt = new Date().toISOString();
      const prefix = computePrefix({ page: page || "default", runId, now: new Date() });

      // Enqueue plain JSON
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
      const queueClient = new QueueClient(process.env.AzureWebJobsStorage, DEFAULT_QUEUE);
      await queueClient.createIfNotExists();
      await queueClient.sendMessage(payload);

      // Seed status.json as Queued
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

      context.log({ event: "campaign_start_enqueued", runId, correlationId, queue: DEFAULT_QUEUE, outcome: "OK" });
      context.res = {
        status: 202,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { runId },
      };
      return;
    }

    // ---------- ACTION: status ----------
    if (action === "status" && method === "GET") {
      const runId = (req.query?.runId || "").trim();
      if (!runId) {
        context.res = jsonError(400, "bad_request", "Invalid input", correlationId);
        return;
      }
      const blobName = await findStatusBlob(containerClient, runId);
      if (!blobName) {
        context.res = {
          status: 404,
          headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
          body: { state: "Unknown", runId },
        };
        return;
      }
      const block = containerClient.getBlockBlobClient(blobName);
      const dl = await block.download();
      const body = await streamToString(dl.readableStreamBody);
      context.log({ event: "campaign_status_read", runId, correlationId, outcome: "OK" });
      context.res = { status: 200, headers: { "Content-Type": "application/json", "x-correlation-id": correlationId }, body };
      return;
    }

    // ---------- ACTION: fetch ----------
    if (action === "fetch" && method === "GET") {
      const runId = (req.query?.runId || "").trim();
      const fileKey = (req.query?.file || "").trim();
      const VALID_MAP = {
        campaign: "campaign.json",
        evidence_log: "evidence_log.json",
        status: "status.json",
      };
      if (!runId || !fileKey || !Object.prototype.hasOwnProperty.call(VALID_MAP, fileKey)) {
        context.res = jsonError(400, "bad_request", "Invalid input", correlationId);
        return;
      }
      const prefix = await locateRunPrefix(containerClient, runId);
      if (!prefix) {
        context.res = {
          status: 404,
          headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
          body: { error: "not_found", message: "Run not found" },
        };
        return;
      }
      const blobName = `${prefix}${VALID_MAP[fileKey]}`;
      const client = containerClient.getBlockBlobClient(blobName);
      if (!(await client.exists())) {
        context.res = {
          status: 404,
          headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
          body: { error: "not_found", message: "File not found" },
        };
        return;
      }
      const dl = await client.download();
      context.log({ event: "campaign_fetch_stream", runId, file: fileKey, correlationId, outcome: "OK" });
      context.res = {
        status: 200,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: dl.readableStreamBody,
        isRaw: true,
      };
      return;
    }

    // No matching action
    context.res = jsonError(400, "bad_request", "Invalid input", correlationId);
  } catch (err) {
    context.log.error("campaign_generate_error", err);
    context.res = jsonError(500, "internal", "Unexpected error", correlationId);
  }
};
