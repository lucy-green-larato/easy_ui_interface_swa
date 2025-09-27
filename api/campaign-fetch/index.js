// 26-09-2025 v1 /api/campaign-fetch/index.js
// GET /api/campaign/fetch?runId=...&file=campaign|evidence_log|status -> streams blob JSON back.

const { BlobServiceClient } = require("@azure/storage-blob");
const { requireAuth, jsonError, cryptoRandomId } = require("../lib/auth");

const ALLOWED_ROLES_CAMPAIGN = ["campaign", "campaign-admin"];
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const VALID_MAP = {
  campaign: "campaign.json",
  evidence_log: "evidence_log.json",
  status: "status.json",
};

function correlationIdFrom(req) {
  return (req.headers?.["x-correlation-id"] || "").toString() || cryptoRandomId();
}

async function locateRunPrefix(containerClient, runId) {
  const suffix = `/${runId}/status.json`;
  const prefixRoot = `results/campaign/`;
  for await (const item of containerClient.listBlobsFlat({ prefix: prefixRoot })) {
    if (item.name.endsWith(suffix)) {
      return item.name.slice(0, item.name.length - "status.json".length);
    }
  }
  return null;
}

module.exports = async function (context, req) {
  if (req.method !== "GET") {
    context.res = jsonError(400, "bad_request", "Invalid input", correlationIdFrom(req));
    return;
  }

  const auth = await requireAuth(context, req, ALLOWED_ROLES_CAMPAIGN);
  if (!auth.ok) return;
  const correlationId = auth.correlationId;

  try {
    const runId = (req.query?.runId || "").trim();
    const fileKey = (req.query?.file || "").trim();

    if (!runId || !fileKey || !Object.prototype.hasOwnProperty.call(VALID_MAP, fileKey)) {
      context.res = jsonError(400, "bad_request", "Invalid input", correlationId);
      return;
    }

    const blobService = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
    const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);

    const prefix = await locateRunPrefix(containerClient, runId);
    if (!prefix) {
      context.res = {
        status: 404,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "not_found", message: "Run not found" }
      };
      return;
    }

    const blobName = `${prefix}${VALID_MAP[fileKey]}`;
    const client = containerClient.getBlockBlobClient(blobName);
    if (!(await client.exists())) {
      context.res = {
        status: 404,
        headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
        body: { error: "not_found", message: "File not found" }
      };
      return;
    }

    const dl = await client.download();
    context.log({ event: "campaign_fetch_stream", runId, file: fileKey, correlationId, outcome: "OK" });

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json", "x-correlation-id": correlationId },
      body: dl.readableStreamBody,
      isRaw: true
    };
  } catch (err) {
    context.log.error("campaign_fetch_error", err);
    context.res = jsonError(500, "internal", "Unexpected error", correlationIdFrom(req));
  }
};
