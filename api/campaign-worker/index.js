//  26/09/2025 V1/api/campaign-worker/index.js
// Queue-triggered worker that runs the 4 phases and writes artifacts under:
// results/campaign/{page}/{yyyy}/{MM}/{dd}/{runId}/(status.json|evidence_log.json|campaign.json)

const { BlobServiceClient } = require("@azure/storage-blob");
const path = require("path");

// Reuse existing repo JS helpers:
const promptHarness = require("../lib/prompt-harness"); // assumed present
// Evidence helpers (assumed folder). We'll attempt index.js default export buildEvidence(input)
let buildEvidence;
try {
  // eslint-disable-next-line import/no-dynamic-require, global-require
  const evidenceModule = require("../lib/evidence");
  buildEvidence =
    evidenceModule?.buildEvidence ||
    evidenceModule?.default ||
    evidenceModule;
} catch {
  buildEvidence = null;
}

const { loadPacks } = require("../shared/packloader");
const packs = require("../shared/packs");
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || "45000");

// util
function utcParts(date = new Date()) {
  const yyyy = date.getUTCFullYear();
  const MM = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  return { yyyy, MM, dd };
}

function computePrefix({ page = "default", runId, enqueuedAt }) {
  const now = enqueuedAt ? new Date(enqueuedAt) : new Date();
  const { yyyy, MM, dd } = utcParts(now);
  return `results/campaign/${page}/${yyyy}/${MM}/${dd}/${runId}/`;
}

async function putJson(containerClient, blobPath, obj) {
  const client = containerClient.getBlockBlobClient(blobPath);
  let payload;
  if (typeof obj === "string") {
    // If already a JSON string, upload as-is
    payload = obj;
  } else {
    payload = JSON.stringify(obj);
  }
  await client.upload(payload, Buffer.byteLength(payload), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });
}

module.exports = async function (context, queueItem) {
  const started = Date.now();
  let message = queueItem;

  // Normalize: trigger typically gives a stringified JSON message (no base64).
  if (typeof message === "string") {
    try { message = JSON.parse(message); } catch { /* keep as-is if not JSON */ }
  }

  const { runId, page = "default", rowCount, filters, notes, enqueuedAt, prefix: msgPrefix, correlationId } = message || {};
  const prefix = msgPrefix || computePrefix({ page, runId, enqueuedAt });

  const blobService = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
  const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);
  await containerClient.createIfNotExists();

  const input = { page, rowCount, filters, notes };

  async function updateStatus(state, extra = {}) {
    const status = {
      runId,
      state,
      input,
      ...(extra || {}),
    };
    await putJson(containerClient, `${prefix}status.json`, status);
  }

  const logEvent = (event, outcome, extra = {}) => {
    context.log({ event, runId, correlationId, durationMs: Date.now() - started, outcome, ...extra });
  };

  try {
    // Phase 1 – Validate input
    await updateStatus("ValidatingInput", { startedAt: new Date().toISOString() });
    if (!runId) throw new Error("Missing runId");
    if (rowCount != null && (typeof rowCount !== "number" || rowCount < 0)) {
      throw new Error("rowCount must be a non-negative number when provided");
    }

    // Phase 2 – Evidence builder
    await updateStatus("BuildingEvidence");
    let evidence = [];
    if (typeof buildEvidence === "function") {
      const packsConfig = await loadPacks(packs);
      evidence = (await buildEvidence({ input, packs: packsConfig, runId, correlationId })) || [];
    } else {
      evidence = [];
    }
    await putJson(containerClient, `${prefix}evidence_log.json`, evidence);

    // Phase 3 – Campaign draft (JSON-only mode)
    await updateStatus("DraftingCampaign");
    const packsConfig = await loadPacks(packs);
    const schemaPath = path.join(__dirname, "..", "schemas", "campaign.schema.json");
    let draft = await promptHarness.generate({
      schemaPath,
      packs: packsConfig,
      input: { runId, page, rowCount, filters, notes, evidence },
      options: { jsonOnly: true, timeoutMs: LLM_TIMEOUT_MS },
    });
    if (typeof draft === "string") {
      try { draft = JSON.parse(draft); } catch { /* leave as string if non-JSON; still valid JSON text */ }
    }
    await putJson(containerClient, `${prefix}campaign.json`, draft);

    // Phase 4 – Completed
    await updateStatus("Completed", { completedAt: new Date().toISOString() });

    logEvent("campaign_worker_completed", "OK");
  } catch (error) {
    context.log.error("campaign_worker_failed", error);
    await updateStatus("Failed", {
      error: { code: "worker_error", message: String(error?.message || error) },
      failedAt: new Date().toISOString(),
    });
    logEvent("campaign_worker_completed", "Failed", { error: String(error?.message || error) });
  }
};
