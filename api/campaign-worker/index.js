// /api/campaign-worker/index.js
// Option B pipeline — worker fast path (draft campaign.json) with strict Executive Summary shaping
// Keeps names/paths/aliases; writes under results/<prefix>; no queue renames.
// Produces: campaign.json (with campaign_title) and status.json updates.
// Evidence: HTTPS only, deterministic claim_ids CLM-001..; never fabricates.

// --- Imports ---------------------------------------------------------------
const { BlobServiceClient } = require("@azure/storage-blob");
const path = require("path");

// Schemas & harness
const schemaPath = path.join(__dirname, "../schemas/campaign.schema.json");

// --- Lazy loaders (CJS first, ESM fallback) --------------------------------
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
    // ESM fallback
    try {
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
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const mod = require("../lib/evidence");
    const buildEvidence = mod.buildEvidence ?? mod.default ?? mod;
    if (typeof buildEvidence !== "function") throw new Error("evidence module has no buildEvidence()");
    _evidence = { buildEvidence };
    return _evidence;
  } catch (e1) {
    try {
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

// Optional pack loader (CJS/ESM tolerant)
async function loadPackModule() {
  try {
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const cjs = require("../shared/packloader");
    const fn = cjs?.loadPacks ?? cjs?.default ?? cjs;
    if (typeof fn === "function") return fn;
  } catch { /* fall through */ }
  try {
    const modUrl = new URL("../shared/packloader.js", `file://${__dirname}/`);
    const esm = await import(modUrl.href);
    const fn = esm?.loadPacks ?? esm?.default ?? esm;
    if (typeof fn === "function") return fn;
  } catch { /* ignore */ }
  return async () => ({ packs: {} });
}

// --- Constants -------------------------------------------------------------
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || "45000");

// --- Utility helpers -------------------------------------------------------
const sanitizePage = (page) => String(page || "campaign").trim().toLowerCase().replace(/[^a-z0-9._-]/g, "-");
const computePrefix = ({ runId }) => `runs/${runId}/`;

async function putJson(containerClient, blobPath, obj) {
  const client = containerClient.getBlockBlobClient(blobPath);
  const payload = typeof obj === "string" ? obj : JSON.stringify(obj);
  const bytes = Buffer.byteLength(payload);
  const opts = { blobHTTPHeaders: { blobContentType: "application/json" } };
  await client.upload(payload, bytes, opts);
}

async function readJsonIfExists(containerClient, blobPath) {
  const bb = containerClient.getBlockBlobClient(blobPath);
  if (!(await bb.exists())) return undefined;
  const dl = await bb.download();
  const chunks = [];
  for await (const c of dl.readableStreamBody) chunks.push(c);
  try { return JSON.parse(Buffer.concat(chunks).toString("utf-8")); } catch { return undefined; }
}

async function streamToString(rs) {
  const chunks = [];
  for await (const c of rs) chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
  return Buffer.concat(chunks).toString("utf-8");
}

const hostnameOf = (u) => { try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return null; } };

// --- Executive Summary shaping --------------------------------------------
// Builds a business-leader sign-off paragraph + bullets from structured inputs.
// Uses only deterministic signals (inputs, CSV meta, verified evidence).
function buildCampaignTitle({ company, industry, requirement }) {
  const co = (company || "").trim();
  const ind = (industry || "").trim();
  const req = (requirement || "").trim();
  const parts = [co].filter(Boolean);
  if (ind) parts.push(ind);
  const mid = parts.join(" | ");
  const suffix = req ? `${req.charAt(0).toUpperCase()}${req.slice(1)} Campaign` : "Campaign";
  return [mid, suffix].filter(Boolean).join(" ");
}

function coerceExecutiveSummary({ draft, input, addressable_market, evidence }) {
  // If draft already has a valid array: [ paragraph, ...bullets ] keep it.
  const es = draft && draft.executive_summary;
  const validArray = Array.isArray(es) && es.length >= 1 && es.every(x => typeof x === "string" && x.trim().length > 0);
  if (validArray) return draft.executive_summary;

  const company = input.supplier_company || input.company_name || "";
  const industry = input.selected_industry || input.campaign_industry || "";
  const requirement = (input.campaign_requirement || "growth").toLowerCase(); // upsell | win-back | growth | unspecified
  const salesModel = input.sales_model || input.salesModel || "direct";

  // Evidence-backed value proposition line (Moore format)
  // Only use verified USPs if present; otherwise keep it generic but truthful.
  const usps = Array.isArray(input.supplier_usps) ? input.supplier_usps.filter(Boolean).slice(0, 3) : [];
  const valueProp = usps.length
    ? `For ${industry || "your market"} decision-makers, ${company} offers ${usps.join("; ")} — unlike generic telco options.`
    : `For ${industry || "your market"} decision-makers, ${company} offers a focused proposition aligned to measurable outcomes — unlike generic telco options.`;

  // Addressable market bullet from CSV meta
  const amBullet = Number.isFinite(addressable_market)
    ? `Addressable market in scope: **${addressable_market.toLocaleString()}** organisations (from CSV scope).`
    : `Addressable market in scope: derived from the uploaded CSV once available.`;

  // Dependencies / enablers — conservative defaults; do not fabricate
  const deps = [
    salesModel === "partner" ? "Confirm partner route and co-marketing MDF availability" : "Confirm direct route and SDR capacity",
    "Access to verified customer references (2–3) with measurable outcomes",
    "Web landing path and lead capture instrumented (UTM, CRM)",
  ];

  const decisions = [
    "Select primary ICP sub-segment for first wave",
    "Choose success metric & reporting cadence (weekly)",
  ];

  const cta = salesModel === "partner"
    ? "Approve partner-led campaign kickoff (content + joint outreach) and release MDF."
    : "Approve direct campaign kickoff (content + SDR outreach) with 6-week runway.";

  const paragraph = [
    `${company} can capture near-term ${industry ? `${industry} ` : ""}${requirement} opportunities with a focused, evidence-led campaign.`,
    "This plan concentrates on one ICP, proves traction quickly, and scales once signal is validated.",
  ].join(" ");

  // Optional claim_id references if present (CSV summary must appear at evidence[0])
  const claims = [];
  if (Array.isArray(evidence) && evidence.length > 0 && evidence[0]?.claim_id) {
    claims.push(evidence[0].claim_id);
  }
  const claimSuffix = claims.length ? ` [${claims.join(", ")}]` : "";

  const bullets = [
    valueProp + claimSuffix,
    amBullet,
    `Dependencies: ${deps.join("; ")}.`,
    `Decision points: ${decisions.join("; ")}.`,
    `CTA: ${cta}`,
  ];

  return [paragraph, ...bullets];
}

// Case-study library sanitizer (host-verified; safe)
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

  if (context && typeof context.log === "function") {
    context.log({ event: "case_study_sanitizer", removed: (original.length - filtered.length), kept: filtered.length });
  }
  return draft;
}

// --- Azure Function entry ---------------------------------------------------
module.exports = async function (context, queueItem) {
  const startedAt = Date.now();
  try {
    // ---------- Parse payload ----------
    let msg = queueItem;
    if (typeof msg === "string") { try { msg = JSON.parse(msg); } catch { /* leave string */ } }
    if (!msg || typeof msg !== "object") throw new Error("Invalid queue payload: expected JSON object");

    const {
      runId,
      prefix: msgPrefix,

      // Inputs & aliases (do not rename)
      supplier_company, supplier_website, supplier_linkedin, supplier_usps,
      campaign_industry, selected_industry, campaign_requirement,
      relevant_competitors, salesModel, sales_model, call_type, callType,
      rowCount, filters, notes,
    } = msg;

    const input = {};
    if (supplier_company) input.supplier_company = String(supplier_company);
    if (supplier_website) input.supplier_website = String(supplier_website);
    if (supplier_linkedin) input.supplier_linkedin = String(supplier_linkedin);
    if (Array.isArray(supplier_usps)) input.supplier_usps = supplier_usps.map(s => String(s).trim()).filter(Boolean);

    if (campaign_industry) input.campaign_industry = String(campaign_industry);
    if (selected_industry) input.selected_industry = String(selected_industry);
    if (campaign_requirement) input.campaign_requirement = String(campaign_requirement);

    if (Array.isArray(relevant_competitors)) input.relevant_competitors = relevant_competitors.map(s => String(s).trim()).filter(Boolean).slice(0, 8);
    const sm = (sales_model || salesModel || "").toString().toLowerCase();
    if (sm === "direct" || sm === "partner") input.sales_model = sm;
    const ct = (call_type || callType || "").toString().toLowerCase();
    if (ct === "direct" || ct === "partner") input.call_type = ct;

    if (Number.isFinite(Number(rowCount))) input.rowCount = Number(rowCount);
    if (filters && typeof filters === "object" && !Array.isArray(filters)) input.filters = filters;
    if (typeof notes === "string" && notes.trim()) input.notes = notes.trim();

    const page = sanitizePage("campaign");

    // ---------- Storage ----------
    const conn = process.env.AzureWebJobsStorage;
    if (!conn) throw new Error("AzureWebJobsStorage not configured");
    const blobService = BlobServiceClient.fromConnectionString(conn);
    const container = blobService.getContainerClient(RESULTS_CONTAINER);
    await container.createIfNotExists();

    // Prefix (container-relative, trailing slash)
    let prefix = typeof msgPrefix === "string" && msgPrefix.trim() ? msgPrefix.trim() : computePrefix({ runId });
    if (!prefix.endsWith("/")) prefix += "/";
    if (prefix.startsWith("/")) prefix = prefix.replace(/^\/+/, "");

    // ---------- Status helper ----------
    async function updateStatus(state, extra = {}) {
      const path = `${prefix}status.json`;
      let cur = await readJsonIfExists(container, path);
      if (!cur || typeof cur !== "object") cur = { runId, history: [] };
      cur.state = state;
      if (!Array.isArray(cur.history)) cur.history = [];
      cur.history.push({ phase: state, at: new Date().toISOString(), ...extra, input: undefined });
      cur.input = { page, ...input }; // canonical input snapshot once
      await putJson(container, path, cur);
    }

    await updateStatus("ValidatingInput");
    if (!runId) throw new Error("Missing runId");

    // ---------- Packs (optional) ----------
    let packs = {};
    try { const loadPacks = await loadPackModule(); const loaded = await loadPacks(); packs = loaded?.packs || {}; }
    catch (e) { context.log.warn("packs_load_failed", String(e?.message || e)); }

    // ---------- Evidence ingest (prefer prebuilt) ----------
    await updateStatus("EvidenceBuilder", { phase: "ingest" });

    let evidence = await readJsonIfExists(container, `${prefix}evidence_log.json`);
    if (!Array.isArray(evidence) || !evidence.length) {
      context.log.warn("worker: prebuilt evidence_log.json missing/empty; invoking fallback builder");
      try {
        const { buildEvidence } = await loadEvidence();
        evidence = await buildEvidence({ input: { page, ...input }, packs, runId, prefix });
        if (!Array.isArray(evidence)) evidence = [];
      } catch (e) {
        context.log.error("worker: fallback buildEvidence failed", String(e?.message || e));
        evidence = [];
      }
    }

    // Build evidencePack buckets
    const evidencePack = { website: [], linkedin: [], ixbrl: {}, pdf: [], directories: [], csv: {} };
    for (const item of (evidence || [])) {
      const st = String(item?.source_type || "").toLowerCase();
      if (st === "company site") evidencePack.website.push(item);
      else if (st === "pdf extract") evidencePack.pdf.push(item);
      else if (st === "linkedin") evidencePack.linkedin.push(item);
      else if (st === "directory") evidencePack.directories.push(item);
      else evidencePack.directories.push(item);
    }
    try {
      const csvBlob = container.getBlockBlobClient(`${prefix}csv_normalized.json`);
      if (await csvBlob.exists()) {
        const dl = await csvBlob.download();
        evidencePack.csv = JSON.parse(await streamToString(dl.readableStreamBody));
      }
    } catch { /* ignore */ }

    const csvMeta = (evidencePack.csv && evidencePack.csv.meta) || {};
    const addressable_market = Number.isFinite(Number(csvMeta.rows)) ? Number(csvMeta.rows) : null;

    // ---------- Draft via prompt-harness ----------
    await updateStatus("DraftCampaign", { evidence_items: Array.isArray(evidence) ? evidence.length : 0 });

    const harness = await loadPromptHarness();
    let draft = await harness.generate({
      schemaPath,
      packs,
      input: { page, ...input, addressable_market, csv_signals: {
        industry_mode: evidencePack.csv?.industry_mode,
        selected_industry: evidencePack.csv?.selected_industry,
        signals: evidencePack.csv?.signals,
        global_signals: evidencePack.csv?.global_signals,
        meta: evidencePack.csv?.meta
      }},
      evidencePack,
      options: {
        timeoutMs: LLM_TIMEOUT_MS,
        azure: {
          endpoint: process.env.AZURE_OPENAI_ENDPOINT,
          apiKey: process.env.AZURE_OPENAI_API_KEY,
          apiVersion: process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview",
          deployment: process.env.AZURE_OPENAI_DEPLOYMENT,
          api: "chat"
        },
        retry: { attempts: Number(process.env.LLM_ATTEMPTS ?? 2), backoffMs: Number(process.env.LLM_BACKOFF_MS ?? 500) },
        temperature: Number(process.env.LLM_TEMPERATURE ?? 0)
      }
    });
    if (typeof draft === "string") { try { draft = JSON.parse(draft); } catch { draft = {}; } }
    if (!draft || typeof draft !== "object") draft = {};

    // ---------- Case study library: verify & trim ----------
    const prospectSite = input.supplier_website || input.company_website || "";
    draft = sanitizeCaseStudyLibrary(draft, evidence, prospectSite, context);

    // ---------- Title + Executive Summary coercion (THIS IS THE FIX) ----------
    try {
      // Title
      draft.campaign_title = draft.campaign_title || buildCampaignTitle({
        company: input.supplier_company || input.company_name,
        industry: input.selected_industry || input.campaign_industry,
        requirement: input.campaign_requirement
      });

      // Executive Summary (array: [paragraph, bullets...])
      draft.executive_summary = coerceExecutiveSummary({
        draft, input, addressable_market, evidence
      });

      // Marker so UI can optionally show "draft" until writer assemble completes
      draft.markers = Object.assign({}, draft.markers, { workerDraft: true });
    } catch (shapeErr) {
      context.log.warn("executive_summary_shape_coercion_failed", String(shapeErr?.message || shapeErr));
    }

    // ---------- Write draft campaign.json ----------
    await putJson(container, `${prefix}campaign.json`, draft);

    // ---------- Quality gate + status ----------
    await updateStatus("QualityGate", { durationMs: Date.now() - startedAt });
    await updateStatus("Completed", { completedAt: new Date().toISOString() }); // keep state machine intact

  } catch (error) {
    context.log.error("campaign-worker failed", error);
    try {
      const conn = process.env.AzureWebJobsStorage;
      if (!conn) return;
      const blobService = BlobServiceClient.fromConnectionString(conn);
      const container = blobService.getContainerClient(RESULTS_CONTAINER);
      await container.createIfNotExists();

      let parsed; try { parsed = typeof queueItem === "string" ? JSON.parse(queueItem) : queueItem; } catch { parsed = {}; }
      const runId = parsed?.runId || "unknown";
      let prefix = typeof parsed?.prefix === "string" && parsed.prefix ? parsed.prefix : computePrefix({ runId });
      if (!prefix.endsWith("/")) prefix += "/";

      await putJson(container, `${prefix}status.json`, {
        runId,
        state: "Failed",
        error: { code: "worker_error", message: String(error?.message || error) },
        failedAt: new Date().toISOString()
      });
    } catch { /* best-effort */ }
  }
};
