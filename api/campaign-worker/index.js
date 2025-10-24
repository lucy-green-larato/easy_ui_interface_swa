// /api/campaign-worker/index.js 2025-10-24 v8
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// Writes under results/campaign/{page}/{yyyy}/{MM}/{dd}/{runId}/(status.json|evidence_log.json|campaign.json)

const { BlobServiceClient } = require("@azure/storage-blob");
const path = require("path");

// ----- Repo helpers (guarded imports; never throw at module load) -----
const promptHarness = require("../lib/prompt-harness"); // expected present
const schemaPath = path.join(__dirname, "../schemas/campaign.schema.json");

let buildEvidence = null;
try {
  // Prefer ../lib/evidence with common shapes: { buildEvidence } or default export
  // eslint-disable-next-line import/no-dynamic-require, global-require
  const evidenceModule = require("../lib/evidence");
  buildEvidence =
    evidenceModule?.buildEvidence ||
    evidenceModule?.default ||
    evidenceModule;
} catch {
  buildEvidence = null; // tolerate absence
}

// ---- Robust loader that supports CJS or ESM packloader without top-level throw
async function loadPackModule() {
  // Try CommonJS first (fast path)
  try {
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const cjs = require("../shared/packloader");
    const fn = cjs?.loadPacks ?? cjs?.default ?? cjs;
    if (typeof fn === "function") return fn;
  } catch (e) {
    // fall through to ESM try
  }
  // Try ESM dynamically
  try {
    const modUrl = new URL("../shared/packloader.js", `file://${__dirname}/`);
    const esm = await import(modUrl.href);
    const fn = esm?.loadPacks ?? esm?.default ?? esm;
    if (typeof fn === "function") return fn;
  } catch (e) {
    // ignore
  }
  // Last resort: no-op pack loader
  return async () => ({ packs: {} });
}

const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || "45000");

// ----- Utils -----
function utcParts(date = new Date()) {
  const yyyy = date.getUTCFullYear();
  const MM = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  return { yyyy, MM, dd };
}

function sanitizePage(page) {
  const s = String(page || "default").trim().toLowerCase();
  const cleaned = s.replace(/[^a-z0-9._-]/g, "-").replace(/-+/g, "-");
  return cleaned || "default";
}

function computePrefix({ page = "default", runId, enqueuedAt }) {
  const now = enqueuedAt ? new Date(enqueuedAt) : new Date();
  const { yyyy, MM, dd } = utcParts(now);
  const p = sanitizePage(page);
  // Container-relative path (RESULTS_CONTAINER is the container)
  return `campaign/${p}/${yyyy}/${MM}/${dd}/${runId}/`;
}

async function putJson(containerClient, blobPath, obj) {
  const client = containerClient.getBlockBlobClient(blobPath);
  const payload = typeof obj === "string" ? obj : JSON.stringify(obj);
  const bytes = Buffer.byteLength(payload);
  const opts = { blobHTTPHeaders: { blobContentType: "application/json" } };

  let attempt = 0, lastErr;
  while (attempt < 3) {
    try {
      await client.upload(payload, bytes, opts);
      return;
    } catch (e) {
      lastErr = e;
      await new Promise(r => setTimeout(r, 200 * (1 << attempt))); // 200ms, 400ms, 800ms
      attempt++;
    }
  }
  throw lastErr;
}

module.exports = async function (context, queueItem) {
  const started = Date.now();

  // Normalize queue message (string -> JSON)
  let message = queueItem;
  if (typeof message === "string") {
    try { message = JSON.parse(message); } catch { /* keep as string if not JSON */ }
  }

  const {
    runId,
    page,
    rowCount,
    filters,
    notes,
    enqueuedAt,
    prefix: msgPrefix,
    salesModel,
    call_type,
    correlationId: msgCorrelationId
  } = message || {};

  const correlationId = msgCorrelationId || `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;

  let containerClient;
  let prefix;

  // Build input envelope up-front (used in status.json and harness)
  const input = {
    page,
    rowCount,
    filters,
    notes,
    // normalised keys for harness
    sales_model: salesModel ?? filters?.salesModel ?? null,
    call_type: call_type ?? filters?.call_type ?? null
  };

  async function updateStatus(state, extra = {}) {
    try {
      if (!containerClient || !prefix) return; // cannot write before setup
      const status = {
        runId,
        state,
        input,
        ...(extra || {}),
        correlationId
      };
      await putJson(containerClient, `${prefix}status.json`, status);
    } catch (e) {
      context.log.warn("status_write_failed", { runId, correlationId, message: String(e?.message || e) });
    }
  }

  const logEvent = (event, outcome, extra = {}) => {
    context.log({
      event,
      runId,
      correlationId,
      durationMs: Date.now() - started,
      outcome,
      ...extra,
    });
  };

  try {
    // Guard configuration before any storage use
    const conn = process.env.AzureWebJobsStorage;
    if (!conn) {
      context.log.error("AzureWebJobsStorage not configured");
      return; // Can't write status.json without storage; bail quietly (no poison)
    }

    // Storage client + container
    const blobService = BlobServiceClient.fromConnectionString(conn);
    containerClient = blobService.getContainerClient(RESULTS_CONTAINER);
    await containerClient.createIfNotExists();

    // Prefix (prefer provided, else compute)
    if (msgPrefix && typeof msgPrefix === "string") {
      prefix = msgPrefix.startsWith(`${RESULTS_CONTAINER}/`)
        ? msgPrefix.slice(`${RESULTS_CONTAINER}/`.length)
        : msgPrefix;
      if (!prefix.endsWith("/")) prefix += "/";
    } else {
      prefix = computePrefix({ page, runId, enqueuedAt });
    }

    // Load packs (works with CJS or ESM)
    let packsConfig = {};
    try {
      const loadPacks = await loadPackModule();
      const loaded = await loadPacks();
      packsConfig = loaded?.packs || {};
    } catch (e) {
      context.log.warn("packs_load_failed", { runId, correlationId, message: String(e?.message || e) });
      packsConfig = {};
    }

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
      try {
        evidence = (await buildEvidence({ input, packs: packsConfig, runId, correlationId })) || [];
      } catch (e) {
        await updateStatus("BuildingEvidence", {
          warning: { code: "evidence_error", message: String(e?.message || e) }
        });
        evidence = [];
      }
    }
    await putJson(containerClient, `${prefix}evidence_log.json`, evidence);

    // Phase 3 – Drafting
    await updateStatus("DraftingCampaign");

    // Effective Azure OpenAI config (explicit, to avoid env drift)
    const AZO_ENDPOINT   = process.env.AZURE_OPENAI_ENDPOINT;
    const AZO_API_KEY    = process.env.AZURE_OPENAI_API_KEY;
    const AZO_API_VER    = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";
    const AZO_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT;

    // One line debug (safe—no key)
    context.log("[campaign-worker env]", {
      endpoint: AZO_ENDPOINT,
      deployment: AZO_DEPLOYMENT,
      apiVersion: AZO_API_VER
    });

    if (!AZO_ENDPOINT || !AZO_API_KEY || !AZO_DEPLOYMENT) {
      await updateStatus("Failed", {
        error: { code: "config_missing", message: "Missing Azure OpenAI configuration (endpoint/apiKey/deployment)" }
      });
      logEvent("campaign_worker_completed", "Failed", { error: "Missing Azure OpenAI config" });
      return;
    }

    let draft;
    try {
      draft = await promptHarness.generate({
        schemaPath,
        packs: packsConfig,
        input: { runId, page: sanitizePage(page), rowCount, filters, notes, evidence },
        options: {
          timeoutMs: LLM_TIMEOUT_MS,
          azure: {
            endpoint:   AZO_ENDPOINT,
            apiKey:     AZO_API_KEY,
            apiVersion: AZO_API_VER,
            deployment: AZO_DEPLOYMENT,
            api: "responses" // or "chat" if you standardise on Chat Completions
          },
          retry: { attempts: 2, backoffMs: 500 }
        }
      });

      if (typeof draft === "string") {
        try { draft = JSON.parse(draft); } catch { /* keep as string */ }
      }

      await putJson(containerClient, `${prefix}campaign.json`, draft);
    } catch (e) {
      await updateStatus("Failed", {
        error: { code: "draft_error", message: String(e?.message || e) },
        failedAt: new Date().toISOString()
      });
      logEvent("campaign_worker_completed", "Failed", { error: String(e?.message || e) });
      return;
    }

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
