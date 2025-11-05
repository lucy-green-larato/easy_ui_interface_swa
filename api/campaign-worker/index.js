// /api/campaign-worker/index.js 05-11-2025 v15
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// Writes under results/<prefix> (status.json | campaign.json). Uses prebuilt evidence_log.json when present.

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
function sanitizePage(page) {
  const s = String(page || "default").trim().toLowerCase();
  const cleaned = s.replace(/[^a-z0-9._-]/g, "-").replace(/-+/g, "-");
  return cleaned || "default";
}

function computePrefix({ runId }) {
  return `runs/${runId}/`;
}

async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const c of readable) chunks.push(c);
  return Buffer.concat(chunks).toString("utf8");
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
      await new Promise(r => setTimeout(r, 200 * (1 << attempt))); // 200/400/800ms
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
    const hostOK = allowedHosts.has(host);
    const urlOK = allowedUrls.size ? allowedUrls.has(url) : true;
    const headlineOK = typeof row.headline === "string" && row.headline.trim().length > 0;
    const bulletsOK = Array.isArray(row.bullets) && row.bullets.filter(b => String(b).trim()).length >= 2;
    return hostOK && urlOK && headlineOK && bulletsOK;
  }).map(row => ({ ...row, verified: true }));

  draft.case_study_library = filtered;
  if ("case_studies" in draft) draft.case_studies = filtered;

  context?.log?.({
    event: "case_study_sanitizer",
    removed: (original.length - filtered.length),
    kept: filtered.length
  });
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
      correlationId: msgCorrelationId,

      // CRITICAL: new inputs carried from start
      supplier_company,
      supplier_website,
      supplier_linkedin,
      supplier_usps,
      campaign_industry,
      selected_industry,
      campaign_requirement,
      relevant_competitors,

      // And/or nested runConfig for back-compat
      runConfig
    } = __message;

    const correlationId = msgCorrelationId || `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
    const __filtersObj = (filters && typeof filters === "object" && !Array.isArray(filters)) ? filters : null;

    // -------- Helpers (function-scoped) --------
    const __normNonEmpty = (v) => {
      if (v == null) return undefined;
      const s = String(v).trim();
      return s.length ? s : undefined;
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

    // -------- Build harness input (fully-populated) --------
    const __pageVal = __normNonEmpty(page) || "campaign";
    const __rowCountVal = Number.isFinite(Number(rowCount)) ? Number(rowCount) : undefined;
    const __notesVal = __normNonEmpty(notes);

    const __rawSales = __normNonEmpty(__pickFirstPresent(
      salesModel, __filtersObj?.salesModel, __message.sales_model, __message.salesModel
    ));
    const __salesModelVal = (__rawSales && ["direct", "partner"].includes(__rawSales.toLowerCase()))
      ? __rawSales.toLowerCase()
      : undefined;

    const __rawCall = __normNonEmpty(__pickFirstPresent(
      call_type, callType, __filtersObj?.call_type, __filtersObj?.callType, __message.call_type, __message.callType
    ));
    const __callTypeVal = (__rawCall && ["direct", "partner"].includes(__rawCall.toLowerCase()))
      ? __rawCall.toLowerCase()
      : undefined;

    const __companyVal = __normNonEmpty(__pickFirstPresent(supplier_company, __message.company_name, __message.prospect_company, __message.company));
    const __websiteVal = __normNonEmpty(__pickFirstPresent(supplier_website, __message.company_website, __message.prospect_website, __message.website));
    const __linkedinVal = __normNonEmpty(__pickFirstPresent(supplier_linkedin, __message.company_linkedin, __message.prospect_linkedin, __message.linkedin));

    // USPs (array or delimited string)
    let __uspsArr;
    if (Array.isArray(supplier_usps)) {
      __uspsArr = supplier_usps.map(s => String(s ?? "").trim()).filter(Boolean);
    } else if (typeof supplier_usps === "string") {
      __uspsArr = supplier_usps.split(/[,;\n]/).map(s => s.trim()).filter(Boolean);
    } else if (Array.isArray(__message.usps)) {
      __uspsArr = __message.usps.map(s => String(s ?? "").trim()).filter(Boolean);
    } else if (typeof __message.usps === "string") {
      __uspsArr = __message.usps.split(/[,;\n]/).map(s => s.trim()).filter(Boolean);
    }

    // Objective + competitors (prefer top-level; fallback to runConfig)
    const __objectiveEffective =
      (__normNonEmpty(campaign_requirement) || __normNonEmpty(runConfig?.campaign_requirement) || "unspecified").toLowerCase();

    let __competitors = [];
    if (Array.isArray(relevant_competitors)) __competitors = relevant_competitors;
    else if (Array.isArray(runConfig?.relevant_competitors)) __competitors = runConfig.relevant_competitors;
    __competitors = (__competitors || []).map(s => String(s || "").trim()).filter(Boolean).slice(0, 8);

    const __campaignIndustry = __normNonEmpty(campaign_industry) || __normNonEmpty(selected_industry);
    const __selectedIndustry = __normNonEmpty(selected_industry) || __normNonEmpty(campaign_industry);

    const inputPayload = {};
    inputPayload.page = __pageVal;
    if (__rowCountVal !== undefined) inputPayload.rowCount = __rowCountVal;
    if (__filtersObj) inputPayload.filters = __filtersObj;
    if (__notesVal) inputPayload.notes = __notesVal;

    if (__salesModelVal) inputPayload.sales_model = __salesModelVal;
    if (__callTypeVal) inputPayload.call_type = __callTypeVal;

    if (__companyVal) inputPayload.supplier_company = __companyVal;
    if (__websiteVal) inputPayload.supplier_website = __websiteVal;
    if (__linkedinVal) inputPayload.supplier_linkedin = __linkedinVal;
    if (__uspsArr && __uspsArr.length) inputPayload.supplier_usps = __uspsArr;

    // schema-critical signals:
    if (__objectiveEffective) inputPayload.campaign_requirement = __objectiveEffective;
    if (__competitors.length) inputPayload.relevant_competitors = __competitors;
    if (__campaignIndustry) inputPayload.campaign_industry = __campaignIndustry;
    if (__selectedIndustry) inputPayload.selected_industry = __selectedIndustry;

    // Legacy mirrors
    if (inputPayload.supplier_company) inputPayload.company_name = inputPayload.supplier_company;
    if (inputPayload.supplier_website) inputPayload.company_website = inputPayload.supplier_website;
    if (inputPayload.supplier_linkedin) inputPayload.company_linkedin = inputPayload.supplier_linkedin;

    context.log("worker→harness input snapshot", {
      runId,
      page: inputPayload.page,
      sales_model: inputPayload.sales_model || null,
      call_type: inputPayload.call_type || null,
      company: inputPayload.supplier_company || null,
      website_present: !!inputPayload.supplier_website,
      usps: Array.isArray(inputPayload.supplier_usps) ? inputPayload.supplier_usps.length : 0,
      objective: inputPayload.campaign_requirement || "unspecified",
      competitors: Array.isArray(inputPayload.relevant_competitors) ? inputPayload.relevant_competitors.length : 0,
      campaign_industry: inputPayload.campaign_industry || null,
      selected_industry: inputPayload.selected_industry || null
    });

    // -------- Guard configuration BEFORE any storage use --------
    const conn = process.env.AzureWebJobsStorage;
    if (!conn) {
      const msg = "AzureWebJobsStorage not configured";
      context.log.error(msg);
      throw new Error(msg);
    }

    // -------- Storage client + container --------
    const blobService = BlobServiceClient.fromConnectionString(conn);
    const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);
    await containerClient.createIfNotExists();

    // Helper now that containerClient exists
    async function readJsonIfExists(blobPath) {
      const b = containerClient.getBlockBlobClient(blobPath);
      if (!(await b.exists())) return null;
      const dl = await b.download();
      const txt = await streamToString(dl.readableStreamBody);
      try { return JSON.parse(txt); } catch { return null; }
    }

    // Authoritative container-relative prefix: trust message, normalise
    const prefix = (() => {
      let p = (typeof msgPrefix === "string" && msgPrefix.trim()) ? msgPrefix.trim() : `runs/${runId}/`;
      if (p.startsWith(`${RESULTS_CONTAINER}/`)) p = p.slice(`${RESULTS_CONTAINER}/`.length);
      if (p.startsWith("/")) p = p.replace(/^\/+/, "");
      if (!p.endsWith("/")) p += "/";
      return p;
    })();

    async function updateStatus(state, extra = {}) {
      try {
        const path = `${prefix}status.json`;
        let cur = await readJsonIfExists(path);
        const now = new Date().toISOString();
        const next = cur && typeof cur === "object" ? cur : { runId, history: [] };
        next.state = state;
        if (!Array.isArray(next.history)) next.history = [];
        next.history.push({ phase: state, at: now, correlationId, ...extra, input: undefined });
        next.input = inputPayload;
        await putJson(containerClient, path, next);
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
        ...extra
      });
    };

    // Validate prefix (container-relative)
    if (!prefix || typeof prefix !== "string") throw new Error("Missing prefix after normalisation");
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

    // -------- Phase 2 – Evidence ingest (prefer prebuilt evidence_log.json) --------
    await updateStatus("EvidenceBuilder", { phase: "ingest", note: "prefer prebuilt evidence_log.json" });

    let evidence = await readJsonIfExists(`${prefix}evidence_log.json`);
    if (Array.isArray(evidence) && evidence.length) {
      context.log("worker: using prebuilt evidence_log.json", { runId, count: evidence.length });
    } else {
      context.log.warn("worker: prebuilt evidence_log.json missing/empty; invoking fallback builder", { runId });
      try {
        const { buildEvidence } = await loadEvidence();
        const built = await buildEvidence({ input: inputPayload, packs: packsConfig, runId, prefix, correlationId });
        evidence = Array.isArray(built) ? built : [];
        await updateStatus("EvidenceBuilder", {
          warning: { code: "fallback_evidence_builder", count: evidence.length }
        });
      } catch (err) {
        context.log.error("worker: fallback buildEvidence failed", String(err?.message || err));
        evidence = [];
      }
    }

    // Bucket evidence by new schema enums
    const evidencePack = { website: [], linkedin: [], ixbrl: {}, pdf: [], directories: [], csv: {} };
    for (const item of (Array.isArray(evidence) ? evidence : [])) {
      const st = String(item?.source_type || "").toLowerCase();
      if (st === "company site") evidencePack.website.push(item);
      else if (st === "pdf extract") evidencePack.pdf.push(item);
      else if (st === "linkedin") evidencePack.linkedin.push(item);
      else if (st === "directory") evidencePack.directories.push(item);
      else evidencePack.directories.push(item); // conservative bucket
    }

    // Load csv_normalized.json to populate evidencePack.csv
    try {
      const cn = await readJsonIfExists(`${prefix}csv_normalized.json`);
      if (cn && typeof cn === "object") evidencePack.csv = cn;
    } catch { /* leave csv as {} */ }

    // Helpful signals for the harness & schema (addressable market + CSV signals)
    const csvMeta = (evidencePack.csv && evidencePack.csv.meta) || {};
    const addressable_market = Number.isFinite(Number(csvMeta.rows)) ? Number(csvMeta.rows) : null;

    const csvSignals = (() => {
      const sig = {};
      const cn = evidencePack.csv || {};
      if (typeof cn.industry_mode === "string") sig.industry_mode = cn.industry_mode;
      if (typeof cn.selected_industry === "string") sig.selected_industry = cn.selected_industry;
      if (cn.signals && typeof cn.signals === "object") sig.signals = cn.signals;
      if (cn.global_signals && typeof cn.global_signals === "object") sig.global_signals = cn.global_signals;
      if (cn.meta && typeof cn.meta === "object") sig.meta = cn.meta;
      return sig;
    })();

    // -------- Phase 3 – Draft campaign (LLM) --------
    await updateStatus("DraftCampaign", {
      evidence_items: Array.isArray(evidence) ? evidence.length : 0,
      at: new Date().toISOString()
    });

    let promptHarness;
    try {
      promptHarness = await loadPromptHarness();
    } catch (e) {
      const code = e?.code || "draft_error";
      const details = e?.details && typeof e.details === "object" ? e.details : undefined;
      try {
        if (details) {
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
          ...(details ? { details: { length: details.length || null } } : {})
        },
        failedAt: new Date().toISOString()
      });
      logEvent("campaign_worker_completed", "Failed", { error: String(e?.message || e), code });
      return;
    }

    // Azure OpenAI config
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

    // Generate draft via harness
    let draft;
    try {
      context.log("worker evidencePack summary", {
        website: Array.isArray(evidencePack.website) ? evidencePack.website.length : 0,
        linkedin: Array.isArray(evidencePack.linkedin) ? evidencePack.linkedin.length : 0,
        pdf: Array.isArray(evidencePack.pdf) ? evidencePack.pdf.length : 0,
        directories: Array.isArray(evidencePack.directories) ? evidencePack.directories.length : 0,
        ixbrlKeys: Object.keys(evidencePack.ixbrl || {}).length,
        csvKeys: Object.keys(evidencePack.csv || {}).length,
        addressable_market
      });

      draft = await promptHarness.generate({
        schemaPath,
        packs: packsConfig,
        input: {
          ...inputPayload,
          runId,
          page: sanitizePage(inputPayload.page),
          addressable_market,              // number | null
          csv_signals: csvSignals          // includes industry_mode, selected_industry, signals/global_signals, meta
        },
        evidencePack,
        options: {
          timeoutMs: Number(LLM_TIMEOUT_MS),
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

      const prospectSite = inputPayload.supplier_website || inputPayload.company_website || "";
      try {
        draft = sanitizeCaseStudyLibrary(draft, evidence, prospectSite, context);
      } catch (sanErr) {
        context.log.warn("case_study_sanitizer_failed", String(sanErr?.message || sanErr));
      }

      await putJson(containerClient, `${prefix}campaign.json`, draft);
    } catch (e) {
      const code = e?.code || "draft_error";
      const details = (e && typeof e.details === "object") ? e.details : undefined;
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
      let prefixSafe = typeof parsed?.prefix === "string" && parsed.prefix.length > 0
        ? parsed.prefix : computePrefix({ runId: runIdSafe });
      if (prefixSafe.startsWith(`${RESULTS_CONTAINER}/`)) prefixSafe = prefixSafe.slice(`${RESULTS_CONTAINER}/`.length);
      if (!prefixSafe.endsWith("/")) prefixSafe += "/";

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
