// /api/campaign-worker/index.js 07-11-2025 v15
// Option B pipeline — worker fast path (draft campaign.json) with business-leader Executive Summary shaping
// Preserves v14 structure: phased status, append-only event logger, robust loaders, and sanitizer.
// Writes under results/<prefix> (container-relative, trailing slash). No renames of queues/ops/keys.

// ------------------------------- Imports -----------------------------------
const { BlobServiceClient } = require("@azure/storage-blob");
const path = require("path");

// Keep schema path (no rename)
const schemaPath = path.join(__dirname, "../schemas/campaign.schema.json");

// ---------- Guarded, lazy loaders (no top-level throws) ----------
let _promptHarness;
async function loadPromptHarness(context) {
  if (_promptHarness) return _promptHarness;
  try {
    // CJS first
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const mod = require("../lib/prompt-harness");
    const out = mod?.generate ? mod : { generate: mod?.default ?? mod };
    if (typeof out.generate !== "function") throw new Error("prompt-harness has no generate()");
    _promptHarness = out;
  } catch (e1) {
    try {
      const modUrl = new URL("../lib/prompt-harness.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      const out = esm?.generate ? esm : { generate: esm?.default ?? esm };
      if (typeof out.generate !== "function") throw new Error("prompt-harness has no generate()");
      _promptHarness = out;
    } catch (e2) {
      throw new Error(`prompt-harness load failed: ${e1?.message || e1} | ${e2?.message || e2}`);
    }
  }
  context.log("prompt-harness loaded");
  return _promptHarness;
}

let _evidence;
async function loadEvidenceBuilder(context) {
  if (_evidence) return _evidence;
  try {
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const mod = require("../lib/evidence");
    const buildEvidence = mod.buildEvidence ?? mod.default ?? mod;
    if (typeof buildEvidence !== "function") throw new Error("evidence module has no buildEvidence()");
    _evidence = { buildEvidence };
  } catch (e1) {
    try {
      const modUrl = new URL("../lib/evidence.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      const buildEvidence = esm.buildEvidence ?? esm.default ?? esm;
      if (typeof buildEvidence !== "function") throw new Error("evidence module has no buildEvidence()");
      _evidence = { buildEvidence };
    } catch (e2) {
      throw new Error(`evidence load failed: ${e1?.message || e1} | ${e2?.message || e2}`);
    }
  }
  context.log("evidence builder loaded");
  return _evidence;
}

// ---- Robust loader that supports CJS or ESM packloader without top-level throw
async function loadPackModule(context) {
  try {
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const cjs = require("../shared/packloader");
    const fn = cjs?.loadPacks ?? cjs?.default ?? cjs;
    if (typeof fn === "function") { context.log("packloader (CJS)"); return fn; }
  } catch { /* fall through */ }
  try {
    const modUrl = new URL("../shared/packloader.js", `file://${__dirname}/`);
    const esm = await import(modUrl.href);
    const fn = esm?.loadPacks ?? esm?.default ?? esm;
    if (typeof fn === "function") { context.log("packloader (ESM)"); return fn; }
  } catch { /* ignore */ }
  context.log.warn("packloader missing, returning stub");
  return async () => ({ packs: {} });
}

// ----- Utils -----
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || "45000");
const LLM_ATTEMPTS = Number(process.env.LLM_ATTEMPTS ?? 2);
const LLM_BACKOFF_MS = Number(process.env.LLM_BACKOFF_MS ?? 600);
const LLM_TEMPERATURE = Number(process.env.LLM_TEMPERATURE ?? 0);

const safe = (v) => (typeof v === "string" ? v.trim() : "");
const nonEmpty = (s) => typeof s === "string" && s.trim().length > 0;
const firstNonEmpty = (...vals) => { for (const v of vals) { const s = safe(v); if (s) return s; } return ""; };
const cap = (s) => (s ? s.charAt(0).toUpperCase() + s.slice(1) : s);
const hostnameOf = (u) => { try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return null; } };
const sanitizePage = (page) => String(page || "campaign").trim().toLowerCase().replace(/[^a-z0-9._-]/g, "-");
const computePrefix = ({ runId }) => `runs/${runId}/`;

async function streamToString(rs) {
  const chunks = [];
  for await (const c of rs) chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
  return Buffer.concat(chunks).toString("utf-8");
}

async function readJsonIfExists(container, blobPath) {
  const bb = container.getBlockBlobClient(blobPath);
  if (!(await bb.exists())) return undefined;
  const dl = await bb.download();
  try { return JSON.parse(await streamToString(dl.readableStreamBody)); } catch { return undefined; }
}

async function putJson(container, blobPath, obj) {
  const client = container.getBlockBlobClient(blobPath);
  const payload = typeof obj === "string" ? obj : JSON.stringify(obj);
  await client.upload(payload, Buffer.byteLength(payload), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });
}

// ---- Case study sanitizer helpers (host-verified; safe) ----
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
    const headlineOK = nonEmpty(row.headline);
    const bulletsOK = Array.isArray(row.bullets) && row.bullets.filter(b => nonEmpty(b)).length >= 2;
    const hostOK = !!host && allowedHosts.has(host);
    const urlOK = allowedUrls.size ? allowedUrls.has(url) : true;
    return url && hostOK && urlOK && headlineOK && bulletsOK;
  }).map(row => ({ ...row, verified: true }));

  draft.case_study_library = filtered;
  if ("case_studies" in draft) draft.case_studies = filtered;

  context?.log?.({ event: "case_study_sanitizer", removed: (original.length - filtered.length), kept: filtered.length });
  return draft;
}

// ---------------- Executive Summary shaping + Title (v15) ------------------
// Business-leader sign-off paragraph + bullets with safe fallbacks.
// No fabrication: only input.json, csv_normalized.meta, and verified evidence.
function deriveCampaignTitle({ company, industry, requirement }) {
  const parts = [];
  if (company) parts.push(company);
  if (industry) parts.push(industry);
  const mid = parts.join(" | ");
  const suffix = requirement ? `${cap(requirement)} Campaign` : "Campaign";
  return [mid || "Campaign", suffix].filter(Boolean).join(" ");
}

function buildMooreValueProp({ company, industry, usps, claimId }) {
  const who = industry ? `senior decision-makers in ${industry}` : "senior decision-makers";
  const offer = company ? `${company} offers` : "This campaign offers";
  const uspText = Array.isArray(usps) && usps.length
    ? usps.slice(0, 3).join("; ")
    : "a focused proposition aligned to measurable outcomes";
  const base = `For ${who}, ${offer} ${uspText} — unlike generic telco options.`;
  return claimId ? `${base} [${claimId}]` : base;
}

function buildLeadParagraph({ company, industry, requirement }) {
  const a = company ? `${company} can capture` : "This campaign can capture";
  const b = industry ? ` near-term ${industry} ` : " near-term ";
  const c = requirement ? `${requirement} ` : "";
  return `${a}${b}${c}opportunities with a focused, evidence-led campaign. This plan concentrates on one ICP, proves traction quickly, and scales once signal is validated.`;
}

function shapeExecutiveSummary({ existing, input, csvMeta, evidence }) {
  // Keep valid shape
  if (Array.isArray(existing) && existing.length >= 1 && existing.every(x => nonEmpty(x))) return existing;

  const company = firstNonEmpty(
    input.supplier_company, input.company_name, existing?.supplier_company, existing?.company_name
  );
  const industry = firstNonEmpty(
    input.selected_industry, input.campaign_industry, csvMeta?.selected_industry, csvMeta?.industry_mode
  );
  const requirement = firstNonEmpty(
    (input.campaign_requirement || "").toLowerCase(), (existing?.campaign_requirement || "").toLowerCase()
  );

  const rows = Number.isFinite(Number(csvMeta?.rows)) ? Number(csvMeta.rows) : null;
  const amBullet = rows !== null
    ? `Addressable market in scope: **${rows.toLocaleString()}** organisations (from CSV scope).`
    : `Addressable market in scope: will be populated from the uploaded CSV.`;

  const csvClaim = Array.isArray(evidence) && evidence[0]?.claim_id ? evidence[0].claim_id : null;
  const usps = Array.isArray(input.supplier_usps) ? input.supplier_usps.filter(Boolean) : [];

  const paragraph = buildLeadParagraph({ company, industry, requirement });
  const bullets = [
    buildMooreValueProp({ company, industry, usps, claimId: csvClaim }),
    amBullet,
    `Dependencies: ${[
      (input.sales_model || input.salesModel || "").toLowerCase() === "partner"
        ? "Confirm partner route and co-marketing MDF availability"
        : "Confirm direct route and SDR capacity",
      "Access to verified customer references (2–3) with measurable outcomes",
      "Web landing path and lead capture instrumented (UTM, CRM)"
    ].join("; ")}.`,
    `Decision points: ${[
      "Select primary ICP sub-segment for first wave",
      "Choose success metric & reporting cadence (weekly)"
    ].join("; ")}.`,
    `CTA: ${((input.sales_model || input.salesModel || "").toLowerCase() === "partner")
      ? "Approve partner-led campaign kickoff (content + joint outreach) and release MDF."
      : "Approve direct campaign kickoff (content + SDR outreach) with 6-week runway."
    }`,
  ];

  return [paragraph, ...bullets];
}

// -------- Parse and validate the queue message --------
function parseQueueMessage(queueItem) {
  let msg = queueItem;
  if (typeof msg === "string") { try { msg = JSON.parse(msg); } catch { /* ignore */ } }
  if (!msg || typeof msg !== "object") throw new Error("Invalid queue payload: expected JSON object");
  return msg;
}

// -------- Helpers (function-scoped) --------
function mergeInput(base, msg) {
  return {
    ...base,
    supplier_company: base.supplier_company ?? msg.supplier_company ?? msg.company_name,
    supplier_website: base.supplier_website ?? msg.supplier_website ?? msg.company_website,
    supplier_linkedin: base.supplier_linkedin ?? msg.supplier_linkedin,
    supplier_usps: Array.isArray(base.supplier_usps) ? base.supplier_usps
      : Array.isArray(msg.supplier_usps) ? msg.supplier_usps : undefined,
    campaign_industry: base.campaign_industry ?? msg.campaign_industry,
    selected_industry: base.selected_industry ?? msg.selected_industry,
    campaign_requirement: base.campaign_requirement ?? msg.campaign_requirement,
    sales_model: (base.sales_model ?? base.salesModel ?? msg.sales_model ?? msg.salesModel),
    call_type: (base.call_type ?? base.callType ?? msg.call_type ?? msg.callType),
  };
}

function pickCsvMeta(csvNormalized) {
  return csvNormalized?.meta || {};
}

// -------- Build harness input (fully-populated) --------
function buildHarnessInput({ page, mergedInput, csvNormalized }) {
  return {
    ...mergedInput,
    page,
    addressable_market: csvNormalized?.meta?.rows ?? null,
    csv_signals: csvNormalized || {},
  };
}

// -------- Status writer & event logger (append model) --------
async function appendStatus(container, prefix, updater) {
  const statusPath = `${prefix}status.json`;
  let cur = await readJsonIfExists(container, statusPath);
  if (!cur || typeof cur !== "object") cur = { runId: undefined, history: [] };
  const updated = await updater(cur);
  await putJson(container, statusPath, updated);
}

function pushHistory(cur, phase, extra = {}) {
  if (!Array.isArray(cur.history)) cur.history = [];
  cur.history.push({ phase, at: new Date().toISOString(), ...extra });
}

async function setPhase(container, prefix, phase, extra = {}) {
  await appendStatus(container, prefix, (cur) => {
    cur.state = phase;
    pushHistory(cur, phase, extra);
    return cur;
  });
}

// -------- Guard configuration BEFORE any storage use --------
function ensureConfig() {
  if (!process.env.AzureWebJobsStorage) throw new Error("AzureWebJobsStorage not configured");
}

// -------- Storage client + container --------
async function getContainer() {
  const blobService = BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
  const container = blobService.getContainerClient(RESULTS_CONTAINER);
  await container.createIfNotExists();
  return container;
}

// -------- Phase 1 – Validate input --------
// -------- Packs (optional) --------
// -------- Phase 2 – Evidence ingest (prefer prebuilt evidence_log.json) --------
// -------- Phase 3 – Draft campaign (LLM) --------
// -------- Phase 4 – Quality Gate (placeholder) --------
// -------- Phase 5 – Completed --------

module.exports = async function (context, queueItem) {
  const startedAt = Date.now();
  let runId = "unknown";
  try {
    ensureConfig();
    const container = await getContainer();

    // Phase 1
    const msg = parseQueueMessage(queueItem);
    runId = msg.runId || runId;
    let prefix = safe(msg.prefix) || computePrefix({ runId });
    if (!prefix.endsWith("/")) prefix += "/";
    if (prefix.startsWith("/")) prefix = prefix.replace(/^\/+/, "");
    const page = sanitizePage("campaign");

    await setPhase(container, prefix, "ValidatingInput");

    // Load input.json (if present) and merge with queue aliases
    const baseInput = (await readJsonIfExists(container, `${prefix}input.json`)) || {};
    const mergedInput = mergeInput(baseInput, msg);

    // Packs
    await setPhase(container, prefix, "PacksLoad");
    let packs = {};
    try { const loadPacks = await loadPackModule(context); packs = (await loadPacks())?.packs || {}; }
    catch (e) { context.log.warn("packs load failed", String(e?.message || e)); }

    // Phase 2 — Evidence
    await setPhase(container, prefix, "EvidenceBuilder", { phase: "ingest" });
    let evidence = await readJsonIfExists(container, `${prefix}evidence_log.json`);
    if (!Array.isArray(evidence) || !evidence.length) {
      context.log.warn("worker: prebuilt evidence_log.json missing/empty; invoking fallback builder");
      try {
        const { buildEvidence } = await loadEvidenceBuilder(context);
        evidence = await buildEvidence({ input: { page, ...mergedInput }, packs, runId, prefix });
        if (!Array.isArray(evidence)) evidence = [];
      } catch (e) {
        context.log.error("worker: fallback buildEvidence failed", String(e?.message || e));
        evidence = [];
      }
    }

    // CSV normalized (meta.rows => TAM)
    let csvNormalized = await readJsonIfExists(container, `${prefix}csv_normalized.json`);
    const csvMeta = pickCsvMeta(csvNormalized);

    // Phase 3 — Draft campaign (LLM)
    await setPhase(container, prefix, "DraftCampaign", { evidence_items: Array.isArray(evidence) ? evidence.length : 0 });
    const harness = await loadPromptHarness(context);
    let draft = await harness.generate({
      schemaPath,
      packs,
      input: buildHarnessInput({ page, mergedInput, csvNormalized }),
      evidencePack: {
        csv: csvNormalized || {},
        evidence
      },
      options: {
        timeoutMs: LLM_TIMEOUT_MS,
        azure: {
          endpoint: process.env.AZURE_OPENAI_ENDPOINT,
          apiKey: process.env.AZURE_OPENAI_API_KEY,
          apiVersion: process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview",
          deployment: process.env.AZURE_OPENAI_DEPLOYMENT,
          api: "chat"
        },
        retry: { attempts: LLM_ATTEMPTS, backoffMs: LLM_BACKOFF_MS },
        temperature: LLM_TEMPERATURE
      }
    });
    if (typeof draft === "string") { try { draft = JSON.parse(draft); } catch { draft = {}; } }
    if (!draft || typeof draft !== "object") draft = {};

    // Safety: sanitize case studies (host-verified only)
    const prospectSite = mergedInput.supplier_website || mergedInput.company_website || "";
    draft = sanitizeCaseStudyLibrary(draft, evidence, prospectSite, context);

    // v15 fix: Title + Executive Summary coercion with safe fallbacks
    try {
      draft.campaign_title = draft.campaign_title || deriveCampaignTitle({
        company: firstNonEmpty(mergedInput.supplier_company, draft.supplier_company, draft.company_name),
        industry: firstNonEmpty(mergedInput.selected_industry, mergedInput.campaign_industry, csvMeta?.selected_industry, csvMeta?.industry_mode),
        requirement: firstNonEmpty(mergedInput.campaign_requirement, draft.campaign_requirement)
      });

      draft.executive_summary = shapeExecutiveSummary({
        existing: draft.executive_summary,
        input: mergedInput,
        csvMeta,
        evidence
      });

      draft.markers = Object.assign({}, draft.markers, { workerDraft: true });
    } catch (shapeErr) {
      context.log.warn("executive_summary_shape_coercion_failed", String(shapeErr?.message || shapeErr));
    }

    // Write campaign.json
    await putJson(container, `${prefix}campaign.json`, draft);

    // Phase 4 — Quality Gate (placeholder)
    await setPhase(container, prefix, "QualityGate", { durationMs: Date.now() - startedAt });

    // Phase 5 — Completed (worker fast path; writer may overwrite later)
    await setPhase(container, prefix, "Completed", { completedAt: new Date().toISOString() });

  } catch (err) {
    context.log.error("campaign-worker error", err?.message || err);
    try {
      ensureConfig();
      const container = await getContainer();
      const prefix = computePrefix({ runId });
      await putJson(container, `${prefix}status.json`, {
        runId,
        state: "Failed",
        error: { code: "worker_error", message: String(err?.message || err) },
        failedAt: new Date().toISOString()
      });
    } catch { /* best-effort */ }
  }
};
