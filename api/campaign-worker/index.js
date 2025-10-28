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

// ---- Case study sanitizer helpers (host-verified; safe) ----
function hostnameOf(u) {
  try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return null; }
}

function buildAllowedSets(evidence, prospectWebsite) {
  const allowedUrls = new Set();
  const allowedHosts = new Set();

  const siteHost = hostnameOf(prospectWebsite);
  if (siteHost) allowedHosts.add(siteHost);

  const list = Array.isArray(evidence) ? evidence : [];
  for (const it of list) {
    const url = it?.url || it?.link;
    if (typeof url === "string" && url) {
      allowedUrls.add(url);
      const h = hostnameOf(url);
      if (h) allowedHosts.add(h);
    }
  }
  return { allowedUrls, allowedHosts };
}

function sanitizeCaseStudyLibrary(draft, evidence, prospectWebsite, context) {
  if (!draft || typeof draft !== "object") return draft;

  const original =
    Array.isArray(draft.case_study_library) ? draft.case_study_library
      : Array.isArray(draft.case_studies) ? draft.case_studies
        : null;

  if (!original) return draft;

  const { allowedUrls, allowedHosts } = buildAllowedSets(evidence, prospectWebsite);

  const filtered = original.filter((row) => {
    if (!row || typeof row !== "object") return false;

    const url = row.url || row.link || "";
    const host = hostnameOf(url);
    if (!url || !host) return false;

    // must be same host as company site and/or present in evidence URLs
    const hostOK = allowedHosts.has(host);
    const urlOK = allowedUrls.size ? allowedUrls.has(url) : true;

    const headlineOK = typeof row.headline === "string" && row.headline.trim().length > 0;
    const bulletsOK = Array.isArray(row.bullets) && row.bullets.filter(b => String(b).trim()).length >= 2;

    return hostOK && urlOK && headlineOK && bulletsOK;
  }).map(row => ({ ...row, verified: true }));

  // Prefer the new key; keep shape consistent
  draft.case_study_library = filtered;

  if ("case_studies" in draft) {
    draft.case_studies = filtered;
  }

  if (context && typeof context.log === "function") {
    context.log({
      event: "case_study_sanitizer",
      removed: (original.length - filtered.length),
      kept: filtered.length
    });
  }
  return draft;
}



module.exports = async function (context, queueItem) {
  try {
    context.log("campaign-worker TRIGGERED");
    context.log("campaign-worker received", {
      type: typeof queueItem,
      sample: typeof queueItem === "string" ? queueItem.slice(0, 200) : queueItem
    });
    const __startedAtMs = Date.now();
    const HOST_FUNCTION_BUDGET_MS = Number(process.env.HOST_FUNCTION_BUDGET_MS || 0); // 0 = disabled

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
    // Build a bucketed pack the harness expects (all env-driven limits remain in the harness)
    const evidencePack = { website: [], linkedin: [], ixbrl: {}, pdf: [], directories: [], csv: {} };
    for (const item of (Array.isArray(evidence) ? evidence : [])) {
      const t = String(item?.type || "").toLowerCase();
      if (t.includes("linkedin")) evidencePack.linkedin.push(item);
      else if (t.includes("ixbrl")) Object.assign(evidencePack.ixbrl, item.data || {});
      else if (t.includes("pdf")) evidencePack.pdf.push(item);
      else if (t.includes("directory")) evidencePack.directories.push(item);
      else if (t.includes("csv")) Object.assign(evidencePack.csv, item.data || {});
      else evidencePack.website.push(item);
    }

    await putJson(containerClient, `${prefix}evidence_log.json`, evidence);

    // -------- Phase 3 – Draft campaign (LLM) --------
    await updateStatus("DraftCampaign");
    let promptHarness;
    try {
      promptHarness = await loadPromptHarness();
    } catch (e) {
      // Capture harness signals (e.g., draft_json_parse_error with details.head/tail)
      const code = e?.code || "draft_error";
      const details = e?.details && typeof e.details === "object" ? e.details : undefined;

      // Write a small debug artifact alongside status to inspect parse failures
      try {
        if (details) {
          await putJson(containerClient, `${prefix}draft_parse_debug.json`, {
            code,
            ...details,
            // ensure we don’t write megabytes by accident
            head: String(details.head || "").slice(0, 4000),
            tail: String(details.tail || "").slice(-4000)
          });
        }
      } catch (writeDbgErr) {
        context.log.warn("draft_parse_debug_write_failed", String(writeDbgErr?.message || writeDbgErr));
      }
      await updateStatus("Failed", {
        error: {
          code,
          message: String(e?.message || e),
          ...(details ? { details: { length: details.length || null } } : {})
        },
        failedAt: new Date().toISOString()
      });

      logEvent("campaign_worker_completed", "Failed", { error: String(e?.message || e), code });
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
      context.log("worker evidencePack summary", {
        website: Array.isArray(evidencePack.website) ? evidencePack.website.length : 0,
        linkedin: Array.isArray(evidencePack.linkedin) ? evidencePack.linkedin.length : 0,
        pdf: Array.isArray(evidencePack.pdf) ? evidencePack.pdf.length : 0,
        directories: Array.isArray(evidencePack.directories) ? evidencePack.directories.length : 0,
        ixbrlKeys: Object.keys(evidencePack.ixbrl || {}).length,
        csvKeys: Object.keys(evidencePack.csv || {}).length
      });
      draft = await promptHarness.generate({
        schemaPath,
        packs: packsConfig,
        input: {
          ...inputPayload,
          runId,
          page: sanitizePage(inputPayload.page)
        },
        // pass evidence at the top level (not inside input)
        evidencePack,
        options: {
          timeoutMs: Number(LLM_TIMEOUT_MS), // env-driven
          azure: {
            endpoint: AZO_ENDPOINT,
            apiKey: AZO_API_KEY,
            apiVersion: AZO_API_VER,
            deployment: AZO_DEPLOYMENT,
            api: "chat"
          },
          retry: {
            attempts: Number(process.env.LLM_ATTEMPTS ?? 2),
            backoffMs: Number(process.env.LLM_BACKOFF_MS ?? 500)
          },
          temperature: Number(process.env.LLM_TEMPERATURE ?? 0)
        }
      });

      if (typeof draft === "string") {
        try { draft = JSON.parse(draft); } catch { /* leave as string */ }
      }
      const prospectSite = inputPayload.prospect_website || inputPayload.company_website || "";
      try {
        draft = sanitizeCaseStudyLibrary(draft, evidence, prospectSite, context);
      } catch (sanErr) {
        // Don’t fail the run if sanitizer has an issue; just log and continue
        context.log.warn("case_study_sanitizer_failed", String(sanErr?.message || sanErr));
      }

      await putJson(containerClient, `${prefix}campaign.json`, draft);
    } catch (e) {
      const code = e?.code || "draft_error";
      const details = (e && typeof e.details === "object") ? e.details : undefined;

      // Persist head/tail when the harness says JSON parsing failed
      try {
        if (code === "draft_json_parse_error" && details) {
          await putJson(containerClient, `${prefix}draft_parse_debug.json`, {
            code,
            ...details,
            head: String(details.head || "").slice(0, 4000),
            tail: String(details.tail || "").slice(-4000)
          });
        }
      } catch (writeDbgErr) {
        context.log.warn("draft_parse_debug_write_failed", String(writeDbgErr?.message || writeDbgErr));
      }

      await updateStatus("Failed", {
        error: {
          code,
          message: String(e?.message || e),
          ...(details ? { details: { length: details.length ?? null } } : {})
        },
        failedAt: new Date().toISOString()
      });

      logEvent("campaign_worker_completed", "Failed", { error: String(e?.message || e), code });
      return;
    }
    // -------- Phase 4 – Quality Gate (placeholder) --------
    await updateStatus("QualityGate");

    if (HOST_FUNCTION_BUDGET_MS > 0 && (Date.now() - __startedAtMs) > HOST_FUNCTION_BUDGET_MS) {
      await updateStatus("Failed", {
        error: { code: "wall_clock_budget", message: "Exceeded worker wall-clock budget" },
        failedAt: new Date().toISOString()
      });
      logEvent("campaign_worker_completed", "Failed", { error: "wall_clock_budget" });
      return;
    }

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
