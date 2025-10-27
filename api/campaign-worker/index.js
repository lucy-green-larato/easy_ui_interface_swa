// /api/campaign-worker/index.js 2025-10-27 v12
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
    const out = mod?.generate ? mod : { generate: mod?.default ?? mod };
    if (typeof out.generate !== "function") throw new Error("prompt-harness has no generate()");
    _promptHarness = out;
    return _promptHarness;
  } catch (e1) {
    try {
      // ESM fallback
      const modUrl = new URL("../lib/prompt-harness.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      const out = esm?.generate ? esm : { generate: esm?.default ?? esm };
      if (typeof out.generate !== "function") throw new Error("prompt-harness has no generate()");
      _promptHarness = out;
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
    const buildEvidence = mod.buildEvidence ?? mod.default ?? mod;
    if (typeof buildEvidence !== "function") throw new Error("evidence module has no buildEvidence()");
    _evidence = { buildEvidence };
    return _evidence;
  } catch (e1) {
    try {
      // ESM fallback
      const modUrl = new URL("../lib/evidence.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      const buildEvidence = esm.buildEvidence ?? esm.default ?? esm;
      if (typeof buildEvidence !== "function") throw new Error("evidence module has no buildEvidence()");
      _evidence = { buildEvidence };
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
  } catch { /* fall through to ESM */ }
  // Try ESM dynamically
  try {
    const modUrl = new URL("../shared/packloader.js", `file://${__dirname}/`);
    const esm = await import(modUrl.href);
    const fn = esm?.loadPacks ?? esm?.default ?? esm;
    if (typeof fn === "function") return fn;
  } catch { /* ignore */ }
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

// Idempotent write with small retries (overwrite allowed by default)
async function putJson(containerClient, blobPath, obj) {
  const client = containerClient.getBlockBlobClient(blobPath);
  const payload = typeof obj === "string" ? obj : JSON.stringify(obj);
  const bytes = Buffer.byteLength(payload);
  const opts = { blobHTTPHeaders: { blobContentType: "application/json" } };

  let attempt = 0;
  let lastErr;
  while (attempt < 3) {
    try {
      await client.upload(payload, bytes, opts); // overwrite by default
      return;
    } catch (e) {
      lastErr = e;
      // 200ms, 400ms, 800ms (cap ~1s total for responsiveness)
      const backoff = 200 * (1 << attempt);
      await new Promise(r => setTimeout(r, backoff));
      attempt++;
    }
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
    const __startedAtMs = Date.now();

    // -------- Parse and validate the queue message --------
    let __message = queueItem;
    if (typeof __message === "string") {
      try { __message = JSON.parse(__message); } catch { /* keep as string; handled below */ }
    }
    if (!__message || typeof __message !== "object") {
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
    } = __message;

    const correlationId = msgCorrelationId || `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
    const __filtersObj = (filters && typeof filters === "object" && !Array.isArray(filters)) ? filters : null;

    // -------- Helpers (function-scoped, unique names to avoid collisions) --------
    const __normNonEmpty = (v) => {
      if (v == null) return undefined;
      const s = String(v).trim();
      return s.length ? s : undefined; // never return "" as a value
    };
    const __pickFirstPresent = (...vals) => {
      for (const v of vals) {
        if (v == null) continue;
        if (typeof v === "string") {
          const t = v.trim();
          if (t) return t;
        } else {
          return v;
        }
      }
      return undefined;
    };

    // -------- Build the harness input WITHOUT clobbering good values --------
    // Source of truth = the parsed queue message (not an undefined 'job').
    const __src = __message;

    // Canonical scalars
    const __pageVal = __normNonEmpty(page) || "campaign";
    const __rowCountVal = Number.isFinite(Number(rowCount)) ? Number(rowCount) : undefined;
    const __notesVal = __normNonEmpty(notes);

    // Sales model / call type (normalise to 'direct' | 'partner' when possible; otherwise omit)
    const __rawSales = __normNonEmpty(__pickFirstPresent(
      salesModel, __filtersObj?.salesModel, __src.sales_model, __src.salesModel
    ));
    const __salesModelVal = (__rawSales && ["direct", "partner"].includes(__rawSales.toLowerCase()))
      ? __rawSales.toLowerCase()
      : undefined;

    const __rawCall = __normNonEmpty(__pickFirstPresent(
      call_type, callType, __filtersObj?.call_type, __filtersObj?.callType, __src.call_type, __src.callType
    ));
    const __callTypeVal = (__rawCall && ["direct", "partner"].includes(__rawCall.toLowerCase()))
      ? __rawCall.toLowerCase()
      : undefined;

    // Company inputs (prefer canonical; fall back to legacy/alt names)
    const __companyVal = __normNonEmpty(__pickFirstPresent(__src.prospect_company, __src.company_name, __src.company));
    const __websiteVal = __normNonEmpty(__pickFirstPresent(__src.prospect_website, __src.company_website, __src.website));
    const __linkedinVal = __normNonEmpty(__pickFirstPresent(__src.prospect_linkedin, __src.company_linkedin, __src.linkedin));

    // USPs: accept array or comma-separated string; omit if none
    let __uspsArr;
    if (Array.isArray(__src.user_usps)) {
      __uspsArr = __src.user_usps.map(s => String(s ?? "").trim()).filter(Boolean);
    } else if (typeof __src.user_usps === "string") {
      __uspsArr = __src.user_usps.split(",").map(s => s.trim()).filter(Boolean);
    } else if (Array.isArray(__src.usps)) {
      __uspsArr = __src.usps.map(s => String(s ?? "").trim()).filter(Boolean);
    } else if (typeof __src.usps === "string") {
      __uspsArr = __src.usps.split(",").map(s => s.trim()).filter(Boolean);
    }

    // Optional CSV summary object
    const __csvSummaryVal = (typeof __src.csvSummary === "object" && __src.csvSummary)
      || (typeof __src.csv_summary === "object" && __src.csv_summary)
      || undefined;

    // Assemble payload for the harness: only set keys that have meaningful values
    const inputPayload = {};
    inputPayload.page = __pageVal;
    if (__rowCountVal !== undefined) inputPayload.rowCount = __rowCountVal;
    if (__filtersObj) inputPayload.filters = __filtersObj;
    if (__notesVal) inputPayload.notes = __notesVal;

    if (__salesModelVal) inputPayload.sales_model = __salesModelVal;
    if (__callTypeVal) inputPayload.call_type = __callTypeVal;

    if (__companyVal) inputPayload.prospect_company = __companyVal;
    if (__websiteVal) inputPayload.prospect_website = __websiteVal;
    if (__linkedinVal) inputPayload.prospect_linkedin = __linkedinVal;

    if (__uspsArr && __uspsArr.length) inputPayload.user_usps = __uspsArr;
    if (__csvSummaryVal) inputPayload.csvSummary = __csvSummaryVal;

    // Legacy aliases only when canonical exists (prevents duplicate empty fields)
    if (inputPayload.prospect_company) inputPayload.company_name = inputPayload.prospect_company;
    if (inputPayload.prospect_website) inputPayload.company_website = inputPayload.prospect_website;
    if (inputPayload.prospect_linkedin) inputPayload.company_linkedin = inputPayload.prospect_linkedin;

    // Quick snapshot for diagnostics
    context.log("worker→harness input snapshot", {
      runId,
      page: inputPayload.page,
      sales_model: inputPayload.sales_model || null,
      call_type: inputPayload.call_type || null,
      company: inputPayload.prospect_company || null,
      website_present: !!inputPayload.prospect_website,
      usps: Array.isArray(inputPayload.user_usps) ? inputPayload.user_usps.length : 0
    });

    // -------- Status writer & event logger (use your existing helpers) --------
    let containerClient;
    let prefix;

    async function updateStatus(state, extra = {}) {
      try {
        if (!containerClient || !prefix) return; // cannot write before setup
        const status = {
          runId,
          state,
          input: inputPayload,   // <— ensures UI sees the exact inputs used
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
        durationMs: Date.now() - __startedAtMs,
        outcome,
        ...extra,
      });
    };

    // -------- Guard configuration BEFORE any storage use --------
    const conn = process.env.AzureWebJobsStorage;
    if (!conn) {
      const msg = "AzureWebJobsStorage not configured";
      context.log.error(msg);

      // Best-effort: write a Failed status so the UI doesn’t stay “Queued”
      try {
        const raw = typeof queueItem === "string" ? queueItem : JSON.stringify(queueItem || {});
        let parsed;
        try { parsed = JSON.parse(raw); } catch { parsed = undefined; }

        const runId2 = parsed?.runId;
        const page2 = parsed?.page || "default";
        const enq2 = parsed?.enqueuedAt;

        if (runId2) {
          const bs = BlobServiceClient.fromConnectionString(conn);
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

    // -------- Storage client + container --------
    const blobService = BlobServiceClient.fromConnectionString(conn);
    containerClient = blobService.getContainerClient(RESULTS_CONTAINER);
    await containerClient.createIfNotExists();

    // Require a container-relative prefix from the starter
    if (!msgPrefix || typeof msgPrefix !== "string") {
      throw new Error("Missing prefix in message");
    }
    if (msgPrefix.startsWith(`${RESULTS_CONTAINER}/`)) {
      throw new Error("Prefix must be container-relative (do not include the container name)");
    }
    prefix = msgPrefix.endsWith("/") ? msgPrefix : (msgPrefix + "/");
    context.log("campaign-worker resolved path", { runId, prefix });

    // -------- Phase 1 – Validate input --------
    await updateStatus("ValidatingInput", { startedAt: new Date().toISOString() });
    if (!runId) throw new Error("Missing runId");
    if (rowCount != null && (typeof rowCount !== "number" || rowCount < 0)) {
      throw new Error("rowCount must be a non-negative number when provided");
    }

    // -------- Packs (optional) --------
    let packsConfig = {};
    try {
      const loadPacks = await loadPackModule();
      const loaded = await loadPacks();
      packsConfig = loaded?.packs || {};
    } catch (e) {
      context.log.warn("packs_load_failed", { runId, correlationId, message: String(e?.message || e) });
      packsConfig = {};
    }

    // -------- Phase 2 – Evidence builder --------
    await updateStatus("EvidenceBuilder");
    let evidence = [];
    try {
      const { buildEvidence } = await loadEvidence();
      if (typeof buildEvidence === "function") {
        const ev = await buildEvidence({ input: inputPayload, packs: packsConfig, runId, correlationId });
        evidence = Array.isArray(ev) ? ev : [];
      }
    } catch (e) {
      await updateStatus("EvidenceBuilder", {
        warning: { code: "evidence_error", message: String(e?.message || e) }
      });
      evidence = [];
    }
    await putJson(containerClient, `${prefix}evidence_log.json`, evidence);

    // -------- Phase 3 – Draft campaign (LLM) --------
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

    // Effective Azure OpenAI config
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

    // IMPORTANT: pass the normalised inputs into the harness
    let draft;
    try {
      draft = await promptHarness.generate({
        schemaPath,               // keep your existing schema wiring if present in module scope
        packs: packsConfig,
        input: {
          ...inputPayload,        // <-- pass everything the model needs
          runId,
          page: sanitizePage(inputPayload.page), // in case sanitizePage is defined in module scope
          evidence               // keep evidence alongside input for traceability (harmless if unused by harness)
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
          retry: { attempts: 2, backoffMs: 500 },
          temperature: 0
        }
      });

      if (typeof draft === "string") {
        try { draft = JSON.parse(draft); } catch { /* leave as string */ }
      }

      const prospectSite = inputPayload.prospect_website || inputPayload.company_website || "";
      draft = __sanitizeCaseStudyLibrary(draft, evidence, prospectSite, context);

      await putJson(containerClient, `${prefix}campaign.json`, draft);
    } catch (e) {
      await updateStatus("Failed", {
        error: { code: "draft_error", message: String(e?.message || e) },
        failedAt: new Date().toISOString()
      });
      logEvent("campaign_worker_completed", "Failed", { error: String(e?.message || e) });
      return;
    }

    // -------- Phase 4 – Quality Gate (placeholder) --------
    await updateStatus("QualityGate");

    // -------- Phase 5 – Completed --------
    await updateStatus("Completed", { completedAt: new Date().toISOString() });
    logEvent("campaign_worker_completed", "OK");
  } catch (error) {
    context.log.error("campaign_worker_failed", error);
    try {
      const conn = process.env.AzureWebJobsStorage;
      if (!conn) return;

      const blobService = BlobServiceClient.fromConnectionString(conn);
      const container = blobService.getContainerClient(RESULTS_CONTAINER);
      await container.createIfNotExists();

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
      context.log.warn("campaign_worker_failure_status_write_failed", String(writeErr?.message || writeErr));
    }
  }
};
