// /api/campaign-worker/index.js 2025-10-25 v11
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// Writes under results/campaign/{page}/{yyyy}/{MM}/{dd}/{runId}/(status.json|evidence_log.json|campaign.json)

const { BlobServiceClient } = require("@azure/storage-blob");
const path = require("path");

const schemaPath = path.join(__dirname, "../schemas/campaign.schema.json");

// ---------- Guarded, lazy loaders (no top-level throws) ----------
let _promptHarness;
async function loadPromptHarness() {
  if (_promptHarness) return _promptHarness;
  try {
    // CJS fast path
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const mod = require("../lib/prompt-harness");
    _promptHarness = mod?.generate ? mod : { generate: mod?.default ?? mod };
    return _promptHarness;
  } catch (e1) {
    try {
      // ESM fallback
      const modUrl = new URL("../lib/prompt-harness.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      _promptHarness = esm?.generate ? esm : { generate: esm?.default ?? esm };
      return _promptHarness;
    } catch (e2) {
      throw new Error(`prompt-harness load failed: ${e1?.message || e1} | ${e2?.message || e2}`);
    }
  }
}

let _evidence;
async function loadEvidence() {
  if (_evidence) return _evidence;
  try {
    // CJS fast path
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const mod = require("../lib/evidence");
    _evidence = { buildEvidence: mod.buildEvidence ?? mod.default ?? mod };
    return _evidence;
  } catch (e1) {
    try {
      // ESM fallback
      const modUrl = new URL("../lib/evidence.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      _evidence = { buildEvidence: esm.buildEvidence ?? esm.default ?? esm };
      return _evidence;
    } catch (e2) {
      throw new Error(`evidence load failed: ${e1?.message || e1} | ${e2?.message || e2}`);
    }
  }
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

// Idempotent write with small retries
async function putJson(containerClient, blobPath, obj) {
  const client = containerClient.getBlockBlobClient(blobPath);
  const payload = typeof obj === "string" ? obj : JSON.stringify(obj);
  const bytes = Buffer.byteLength(payload);
  const opts = { blobHTTPHeaders: { blobContentType: "application/json" } };
  let attempt = 0, lastErr;
  while (attempt < 3) {
    try { await client.upload(payload, bytes, opts); return; }
    catch (e) { lastErr = e; await new Promise(r => setTimeout(r, 200 * (1 << attempt))); attempt++; }
  }
  throw lastErr;
}

module.exports = async function (context, queueItem) {
  try {
    context.log("campaign-worker TRIGGERED");
    context.log("campaign-worker received", {
      type: typeof queueItem,
      sample: typeof queueItem === "string" ? queueItem.slice(0, 200) : queueItem
    });
    const started = Date.now();

    // Force object; try best-effort parse if string
    let message = queueItem;
    if (typeof message === "string") {
      try { message = JSON.parse(message); } catch { /* leave as string */ }
    }
    if (!message || typeof message !== "object") {
      // bail out early – don’t write under 'default/undefined/'
      throw new Error("Invalid queue payload: expected JSON object");
    }

    const {
      runId,
      page,
      rowCount,
      filters,
      notes,
      prefix: msgPrefix,
      salesModel,
      call_type,
      callType,
      correlationId: msgCorrelationId
    } = message;

    const correlationId = msgCorrelationId || `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
    const filtersObj = (filters && typeof filters === "object" && !Array.isArray(filters)) ? filters : null;
    let containerClient;
    let prefix;

    // Build input envelope up-front (used in status.json and harness)
    const input = {
      page,
      rowCount,
      filters,
      notes,
      // normalised keys for harness
      sales_model: (salesModel ?? filtersObj?.salesModel) ?? null,
      call_type: (call_type ?? callType ?? filtersObj?.call_type ?? filtersObj?.callType) ?? null
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

    // ---- Guard configuration BEFORE any storage use ----
    const conn = process.env.AzureWebJobsStorage;
    if (!conn) {
      const msg = "AzureWebJobsStorage not configured";
      context.log.error(msg);

      // Best-effort: write a Failed status so the UI doesn’t stay “Queued”
      try {
        const parsed = (typeof queueItem === "string")
          ? (() => { try { return JSON.parse(queueItem); } catch { return undefined; } })()
          : queueItem;
        const runId2 = parsed?.runId;
        const page2 = parsed?.page || "default";
        const enq2 = parsed?.enqueuedAt;

        const conn2 = process.env.AzureWebJobsStorage || "";
        if (conn2 && runId2) {
          const bs = BlobServiceClient.fromConnectionString(conn2);
          const cc = bs.getContainerClient(RESULTS_CONTAINER);
          await cc.createIfNotExists();
          const pfx = `${computePrefix({ page: page2, runId: runId2, enqueuedAt: enq2 })}`;
          await putJson(cc, `${pfx}status.json`, {
            runId: runId2,
            state: "Failed",
            error: { code: "config", message: msg },
            failedAt: new Date().toISOString()
          });
        }
      } catch { /* best effort; ignore */ }

      // Throw so the message retries (prevents false "success" + poison)
      throw new Error(msg);
    }

    // ---- Storage client + container ----
    const blobService = BlobServiceClient.fromConnectionString(conn);
    containerClient = blobService.getContainerClient(RESULTS_CONTAINER);
    await containerClient.createIfNotExists();

    // --- REQUIRE a container-relative prefix from the starter message ---
    if (!msgPrefix || typeof msgPrefix !== "string") {
      throw new Error("Missing prefix in message");
    }
    if (msgPrefix.startsWith(`${RESULTS_CONTAINER}/`)) {
      throw new Error("Prefix must be container-relative (do not include the container name)");
    }
    prefix = msgPrefix.endsWith("/") ? msgPrefix : (msgPrefix + "/");

    // Optional: log where we will write, for quick diagnostics
    context.log("campaign-worker resolved path", { runId, prefix });

    // ---- Phase 1 – Validate input ----
    await updateStatus("ValidatingInput", { startedAt: new Date().toISOString() });
    if (!runId) throw new Error("Missing runId");
    if (rowCount != null && (typeof rowCount !== "number" || rowCount < 0)) {
      throw new Error("rowCount must be a non-negative number when provided");
    }

    // ---- Packs (optional) ----
    let packsConfig = {};
    try {
      const loadPacks = await loadPackModule();
      const loaded = await loadPacks();
      packsConfig = loaded?.packs || {};
    } catch (e) {
      context.log.warn("packs_load_failed", { runId, correlationId, message: String(e?.message || e) });
      packsConfig = {};
    }

    // ---- Phase 2 – Evidence builder ----
    await updateStatus("EvidenceBuilder");
    let evidence = [];
    try {
      const { buildEvidence } = await loadEvidence();
      if (typeof buildEvidence === "function") {
        const ev = await buildEvidence({ input, packs: packsConfig, runId, correlationId });
        evidence = Array.isArray(ev) ? ev : [];
      }
    } catch (e) {
      await updateStatus("EvidenceBuilder", {
        warning: { code: "evidence_error", message: String(e?.message || e) }
      });
      evidence = [];
    }
    await putJson(containerClient, `${prefix}evidence_log.json`, evidence);

    // ---- Phase 3 – Draft campaign (LLM) ----
    await updateStatus("DraftCampaign");
    let promptHarness;
    try {
      promptHarness = await loadPromptHarness();
    } catch (e) {
      await updateStatus("Failed", {
        error: { code: "loader_error", message: String(e?.message || e) },
        failedAt: new Date().toISOString()
      });
      logEvent("campaign_worker_completed", "Failed", { error: "prompt harness load failed" });
      return;
    }

    // Effective Azure OpenAI config (explicit, to avoid env drift)
    const AZO_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT;
    const AZO_API_KEY = process.env.AZURE_OPENAI_API_KEY;
    const AZO_API_VER = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";
    const AZO_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT;

    context.log("[campaign-worker env]", { endpoint: AZO_ENDPOINT, deployment: AZO_DEPLOYMENT, apiVersion: AZO_API_VER });

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
        input: {
          runId,
          page: sanitizePage(page),
          rowCount,
          filters,
          notes,
          evidence,
          sales_model: input.sales_model,
          call_type: input.call_type
        },
        options: {
          timeoutMs: LLM_TIMEOUT_MS,
          azure: {
            endpoint: AZO_ENDPOINT,
            apiKey: AZO_API_KEY,
            apiVersion: AZO_API_VER,
            deployment: AZO_DEPLOYMENT,
            api: "chat"
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

    // ---- Phase 4 – Quality Gate (no-op placeholder, but visible to UI) ----
    await updateStatus("QualityGate");

    // ---- Phase 5 – Completed ----
    await updateStatus("Completed", { completedAt: new Date().toISOString() });
    logEvent("campaign_worker_completed", "OK");
  } catch (error) {
    // Catch absolutely everything (including early-phase exceptions)
    context.log.error("campaign_worker_failed", error);
    try {
      // Best-effort: write a clear Failed status where the UI is polling
      const conn = process.env.AzureWebJobsStorage;
      if (!conn) return;

      const blobService = BlobServiceClient.fromConnectionString(conn);
      const container = blobService.getContainerClient(RESULTS_CONTAINER);
      await container.createIfNotExists();

      // Recover routing from original message
      const raw = typeof queueItem === "string" ? queueItem : JSON.stringify(queueItem || {});
      let parsed;
      try { parsed = JSON.parse(raw); } catch { parsed = undefined; }

      const runIdSafe = parsed?.runId ?? "unknown";
      const prefixSafe = typeof parsed?.prefix === "string" && parsed.prefix.length > 0
        ? (parsed.prefix.endsWith("/") ? parsed.prefix : `${parsed.prefix}/`)
        : computePrefix({ page: parsed?.page || "default", runId: runIdSafe, enqueuedAt: parsed?.enqueuedAt });

      await putJson(container, `${prefixSafe}status.json`, {
        runId: runIdSafe,
        state: "Failed",
        error: { code: "worker_error", message: String(error?.message || error) },
        failedAt: new Date().toISOString()
      });
    } catch (writeErr) {
      // Best effort; do not rethrow
      context.log.warn("campaign_worker_failure_status_write_failed", String(writeErr?.message || writeErr));
    }
  }
};
