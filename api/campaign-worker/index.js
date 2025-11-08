// /api/campaign-worker/index.js 07-11-2025 v16.1
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

// ---------------- Strategy synthesis ------------------------
function pickClaim(evidence, predicate) {
  const it = (Array.isArray(evidence) ? evidence : []).find(predicate);
  return it?.claim_id || null;
}
function topN(arr, n) { return (Array.isArray(arr) ? arr : []).filter(s => typeof s === "string" && s.trim()).slice(0, n); }

function deriveOutcomeByTam(rows, salesModel) {
  const r = Number.isFinite(Number(rows)) ? Number(rows) : 0;
  if (r <= 50) return salesModel === "partner" ? "Create 4–6 partner-sourced qualified opportunities in 6 weeks" : "Create 4–6 qualified opportunities in 6 weeks";
  if (r >= 400) return salesModel === "partner" ? "Create 10–15 partner-sourced qualified opportunities in 6 weeks" : "Create 10–15 qualified opportunities in 6 weeks";
  return salesModel === "partner" ? "Create 6–10 partner-sourced qualified opportunities in 6 weeks" : "Create 6–10 qualified opportunities in 6 weeks";
}

function diffVsCompetitors({ evidence, competitors, usps }) {
  const outs = [];
  const compSet = new Set((Array.isArray(competitors) ? competitors : []).map(c => String(c || "").toLowerCase()));
  const ev = Array.isArray(evidence) ? evidence : [];
  for (const c of compSet) {
    const hit = ev.find(e =>
      (e.title && e.title.toLowerCase().includes(c)) ||
      (e.url && e.url.toLowerCase().includes(c))
    );
    if (hit) outs.push(`Stronger proof vs ${c}: ${topN(usps, 2).join("; ") || "supplier differentiators"}`);
  }
  return topN(outs, 3);
}

function buildStrategyObject({ input, csvNormalized, needsMap, evidence }) {
  const company = firstNonEmpty(input.supplier_company, input.company_name);
  const industry = firstNonEmpty(input.selected_industry, input.campaign_industry, csvNormalized?.selected_industry);
  const route = (firstNonEmpty(input.sales_model, input.salesModel, input.call_type, input.callType) || "direct").toLowerCase();
  const requirement = (firstNonEmpty(input.campaign_requirement) || "growth").toLowerCase();
  const usps = Array.isArray(input.supplier_usps) ? input.supplier_usps.filter(nonEmpty) : [];

  const rows = Number(csvNormalized?.meta?.rows || 0);
  const csvClaim = pickClaim(evidence, (x) => /csv/i.test(x?.source_type) && /csv population/i.test(x?.title || ""));
  const packClaim = pickClaim(evidence, (x) => /Ofcom|ONS|DSIT|LinkedIn|PDF extract|Company site/.test(x?.source_type || ""));

  const problems = topN(
    (csvNormalized?.signals?.top_blockers?.length ? csvNormalized.signals.top_blockers : csvNormalized?.global_signals?.top_blockers) || [],
    2
  );

  const cov = needsMap?.coverage || { matched: 0, partial: 0, gap: 0, coverage: 0 };
  const gaps = topN((needsMap?.items || []).filter(i => i?.status === "gap").map(i => i.need), 4);

  const competitors = Array.isArray(input.relevant_competitors) ? input.relevant_competitors.filter(nonEmpty) : [];
  const vsCompetitors = diffVsCompetitors({ evidence, competitors, usps });

  const whyPlayBase = rows
    ? `There is a defined, addressable market of ${rows} organisations [${csvClaim || "CLM-001"}].`
    : `There is a defined addressable market in the uploaded CSV [${csvClaim || "CLM-001"}].`;
  const whyNow = packClaim ? " External sources indicate momentum and adoption drivers." : "";

  const howWinPosition = industry
    ? `Focus on ${industry} with quick proof of value and measurable results`
    : "Focus on a tight ICP with quick proof of value and measurable results";

  const specificOutcome = deriveOutcomeByTam(rows, route);

  return {
    why_play: (whyPlayBase + whyNow).trim(),
    prospect_base: {
      icp: industry ? `leaders in ${industry} projects` : "leaders in target projects",
      subsegment: "first-wave sub-segment to be selected",
      tam: rows
    },
    buyer_problems: problems,
    fit_analysis: { coverage: cov, gaps },
    how_win: {
      position: howWinPosition,
      differentiators: topN(usps, 3),
      vs_competitors: vsCompetitors
    },
    go_to_market: { route, first_wave: route === "partner" ? "co-marketing with named partners" : "direct SDR + content" },
    specific_outcome: specificOutcome,
    execution_checks: [
      route === "partner" ? "Confirm partner route and MDF availability" : "Confirm direct route and SDR capacity",
      "Provide 2–3 customer references with measurable outcomes",
      "Publish tracked landing page (UTM + CRM)",
      gaps.length ? `Mitigate capability gaps: ${gaps.join(", ")}` : "Validate coverage against top needs"
    ],
    evidence_links: [csvClaim, packClaim].filter(Boolean),
    meta: { company, industry, route, requirement, usps }
  };
}

// Render a plain-English lead paragraph from the strategy
function leadParagraphFromStrategy({ strategy }) {
  const requirement = (strategy?.meta?.requirement || "growth").toLowerCase();
  const industry = strategy?.meta?.industry || "";
  const objective =
    requirement === "upsell" ? "upsell services to existing customers"
      : requirement === "win-back" ? "win back high-potential lapsed customers"
        : "create near-term growth";

  const sector = industry ? ` in ${industry}` : "";
  const icp = strategy?.prospect_base?.icp
    ? `We will start with ${strategy.prospect_base.icp} and prove results quickly on agreed measures.`
    : "We will start with one clear audience first and prove results quickly on agreed measures.";

  const problems = topN(strategy?.buyer_problems, 2);
  const problemsText = problems.length ? `They face ${problems.join("; ")}.` : "";

  const cov = strategy?.fit_analysis?.coverage || { gap: 0 };
  const gapNote = (cov.gap > 0) ? "We will highlight any capability gaps and address them explicitly." : "";

  return [
    `This campaign’s objective is to ${objective}${sector}.`,
    icp,
    problemsText,
    gapNote
  ].filter(Boolean).join(" ");
}
// ---------------- Strategy synthesis (NEW in v16.1) ------------------------
function pickClaim(evidence, predicate) {
  const it = (Array.isArray(evidence) ? evidence : []).find(predicate);
  return it?.claim_id || null;
}
function topN(arr, n) { return (Array.isArray(arr) ? arr : []).filter(s => typeof s === "string" && s.trim()).slice(0, n); }

function deriveOutcomeByTam(rows, salesModel) {
  const r = Number.isFinite(Number(rows)) ? Number(rows) : 0;
  if (r <= 50) return salesModel === "partner" ? "Create 4–6 partner-sourced qualified opportunities in 6 weeks" : "Create 4–6 qualified opportunities in 6 weeks";
  if (r >= 400) return salesModel === "partner" ? "Create 10–15 partner-sourced qualified opportunities in 6 weeks" : "Create 10–15 qualified opportunities in 6 weeks";
  return salesModel === "partner" ? "Create 6–10 partner-sourced qualified opportunities in 6 weeks" : "Create 6–10 qualified opportunities in 6 weeks";
}

function diffVsCompetitors({ evidence, competitors, usps }) {
  const outs = [];
  const compSet = new Set((Array.isArray(competitors) ? competitors : []).map(c => String(c || "").toLowerCase()));
  const ev = Array.isArray(evidence) ? evidence : [];
  for (const c of compSet) {
    const hit = ev.find(e =>
      (e.title && e.title.toLowerCase().includes(c)) ||
      (e.url && e.url.toLowerCase().includes(c))
    );
    if (hit) outs.push(`Stronger proof vs ${c}: ${topN(usps, 2).join("; ") || "supplier differentiators"}`);
  }
  return topN(outs, 3);
}

function buildStrategyObject({ input, csvNormalized, needsMap, evidence }) {
  const company = firstNonEmpty(input.supplier_company, input.company_name);
  const industry = firstNonEmpty(input.selected_industry, input.campaign_industry, csvNormalized?.selected_industry);
  const route = (firstNonEmpty(input.sales_model, input.salesModel, input.call_type, input.callType) || "direct").toLowerCase();
  const requirement = (firstNonEmpty(input.campaign_requirement) || "growth").toLowerCase();
  const usps = Array.isArray(input.supplier_usps) ? input.supplier_usps.filter(nonEmpty) : [];

  const rows = Number(csvNormalized?.meta?.rows || 0);
  const csvClaim = pickClaim(evidence, (x) => /csv/i.test(x?.source_type) && /csv population/i.test(x?.title || ""));
  const packClaim = pickClaim(evidence, (x) => /Ofcom|ONS|DSIT|LinkedIn|PDF extract|Company site/.test(x?.source_type || ""));

  const problems = topN(
    (csvNormalized?.signals?.top_blockers?.length ? csvNormalized.signals.top_blockers : csvNormalized?.global_signals?.top_blockers) || [],
    2
  );

  const cov = needsMap?.coverage || { matched: 0, partial: 0, gap: 0, coverage: 0 };
  const gaps = topN((needsMap?.items || []).filter(i => i?.status === "gap").map(i => i.need), 4);

  const competitors = Array.isArray(input.relevant_competitors) ? input.relevant_competitors.filter(nonEmpty) : [];
  const vsCompetitors = diffVsCompetitors({ evidence, competitors, usps });

  const whyPlayBase = rows
    ? `There is a defined, addressable market of ${rows} organisations [${csvClaim || "CLM-001"}].`
    : `There is a defined addressable market in the uploaded CSV [${csvClaim || "CLM-001"}].`;
  const whyNow = packClaim ? " External sources indicate momentum and adoption drivers." : "";

  const howWinPosition = industry
    ? `Focus on ${industry} with quick proof of value and measurable results`
    : "Focus on a tight ICP with quick proof of value and measurable results";

  const specificOutcome = deriveOutcomeByTam(rows, route);

  return {
    why_play: (whyPlayBase + whyNow).trim(),
    prospect_base: {
      icp: industry ? `leaders in ${industry} projects` : "leaders in target projects",
      subsegment: "first-wave sub-segment to be selected",
      tam: rows
    },
    buyer_problems: problems,
    fit_analysis: { coverage: cov, gaps },
    how_win: {
      position: howWinPosition,
      differentiators: topN(usps, 3),
      vs_competitors: vsCompetitors
    },
    go_to_market: { route, first_wave: route === "partner" ? "co-marketing with named partners" : "direct SDR + content" },
    specific_outcome: specificOutcome,
    execution_checks: [
      route === "partner" ? "Confirm partner route and MDF availability" : "Confirm direct route and SDR capacity",
      "Provide 2–3 customer references with measurable outcomes",
      "Publish tracked landing page (UTM + CRM)",
      gaps.length ? `Mitigate capability gaps: ${gaps.join(", ")}` : "Validate coverage against top needs"
    ],
    evidence_links: [csvClaim, packClaim].filter(Boolean),
    meta: { company, industry, route, requirement, usps }
  };
}

// Render a plain-English lead paragraph from the strategy
function leadParagraphFromStrategy({ strategy }) {
  const requirement = (strategy?.meta?.requirement || "growth").toLowerCase();
  const industry = strategy?.meta?.industry || "";
  const objective =
    requirement === "upsell" ? "upsell services to existing customers"
      : requirement === "win-back" ? "win back high-potential lapsed customers"
        : "create near-term growth";

  const sector = industry ? ` in ${industry}` : "";
  const icp = strategy?.prospect_base?.icp
    ? `We will start with ${strategy.prospect_base.icp} and prove results quickly on agreed measures.`
    : "We will start with one clear audience first and prove results quickly on agreed measures.";

  const problems = topN(strategy?.buyer_problems, 2);
  const problemsText = problems.length ? `They face ${problems.join("; ")}.` : "";

  const cov = strategy?.fit_analysis?.coverage || { gap: 0 };
  const gapNote = (cov.gap > 0) ? "We will highlight any capability gaps and address them explicitly." : "";

  return [
    `This campaign’s objective is to ${objective}${sector}.`,
    icp,
    problemsText,
    gapNote
  ].filter(Boolean).join(" ");
}

// ---------------- Executive Summary shaping + Title (v16) ------------------
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

// Build an exec-summary intro grounded in CSV signals and needs coverage, plain business tone.
function buildLeadParagraph({ company, industry, requirement, csvNormalized, needsMap }) {
  const objective =
    (String(requirement || "").toLowerCase() === "upsell")
      ? "upsell services to existing customers"
      : (String(requirement || "").toLowerCase() === "win-back")
        ? "win back high-potential lapsed customers"
        : "create near-term growth";

  // ICP phrase
  const icp =
    industry
      ? `one clear audience first: leaders on ${industry} projects`
      : "one clear audience first";

  // Buyer problems from CSV signals (prefer industry-specific, fall back to global)
  const sig = csvNormalized?.signals;
  const gs = csvNormalized?.global_signals;
  const topProblems = (Array.isArray(sig?.top_blockers) && sig.top_blockers.length
    ? sig.top_blockers
    : (gs?.top_blockers || []))
    .filter(Boolean)
    .slice(0, 2);

  const problemsText = topProblems.length
    ? `They face ${topProblems.join("; ")}.`
    : "";

  // Fit/coverage from needs_map.json
  const cov = needsMap?.coverage || { matched: 0, partial: 0, gap: 0 };
  const gapNote = (cov.gap > 0)
    ? "We will highlight any capability gaps and address them explicitly."
    : "";

  const supplier = company ? `${company} ` : "";
  const sector = industry ? ` in ${industry}` : "";

  // Keep it non-jargon, sign-off intent
  return [
    `This campaign’s objective is to ${objective}${sector}.`,
    `We will start with ${icp} and prove results quickly on agreed measures.`,
    problemsText,
    gapNote
  ].filter(Boolean).join(" ");
}

function shapeExecutiveSummary({ existing, input, csvMeta, strategy, evidence }) {
  if (Array.isArray(existing) && existing.length >= 1 && existing.every(x => nonEmpty(x))) return existing;

  const company = firstNonEmpty(input.supplier_company, input.company_name, strategy?.meta?.company);
  const industry = firstNonEmpty(input.selected_industry, input.campaign_industry, csvMeta?.selected_industry, strategy?.meta?.industry);
  const requirement = firstNonEmpty((input.campaign_requirement || "").toLowerCase(), (strategy?.meta?.requirement || "").toLowerCase());

  const rows = Number.isFinite(Number(csvMeta?.rows)) ? Number(csvMeta.rows) : (strategy?.prospect_base?.tam ?? null);
  const amBullet = rows !== null
    ? `Addressable market in scope: **${Number(rows).toLocaleString()}** organisations (from CSV scope).`
    : `Addressable market in scope: will be populated from the uploaded CSV.`;

  const csvClaim = Array.isArray(evidence) && evidence[0]?.claim_id ? evidence[0].claim_id : (strategy?.evidence_links?.[0] || null);
  const usps = Array.isArray(input.supplier_usps) ? input.supplier_usps.filter(Boolean) : (strategy?.meta?.usps || []);

  const paragraph = leadParagraphFromStrategy({ strategy });

  const deps = Array.isArray(strategy?.execution_checks) && strategy.execution_checks.length
    ? strategy.execution_checks.slice(0, 3).join("; ")
    : [
      (input.sales_model || input.salesModel || "").toLowerCase() === "partner"
        ? "Confirm partner route and co-marketing MDF availability"
        : "Confirm direct route and SDR capacity",
      "Access to verified customer references (2–3) with measurable outcomes",
      "Web landing path and lead capture instrumented (UTM, CRM)"
    ].join("; ");

  const route = (strategy?.go_to_market?.route || (input.sales_model || input.salesModel || "")).toLowerCase();
  const cta = route === "partner"
    ? "Approve partner-led campaign kickoff (content + joint outreach) and release MDF."
    : "Approve direct campaign kickoff (content + SDR outreach) with a 6-week runway.";

  const bullets = [
    buildMooreValueProp({ company, industry, usps, claimId: csvClaim }),
    amBullet,
    `Dependencies: ${deps}.`,
    `Decision points: ${[
      "Select primary ICP sub-segment for first wave",
      "Choose success metric & reporting cadence (weekly)"
    ].join("; ")}.`,
    `CTA: ${cta}`
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
    const needsMap = await readJsonIfExists(container, `${prefix}needs_map.json`) || {
      coverage: { total: 0, matched: 0, partial: 0, gap: 0, coverage: 0 },
      items: []
    };

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
    // NEW Phase — Strategy synthesis (deterministic, from artifacts)
    await setPhase(container, prefix, "StrategySynthesis");
    const strategy = buildStrategyObject({
      input: mergedInput,
      csvNormalized: csvNormalized || {},
      needsMap,
      evidence
    });
    await putJson(container, `${prefix}campaign_strategy.json`, strategy);

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
        strategy,
        evidence
      });

      draft.markers = Object.assign({}, draft.markers, { workerDraft: true, strategyV: "v16.1" });
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
