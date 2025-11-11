// /api/campaign-worker/index.js 12-11-2025 v25
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

async function getJson(container, blobPath) {
  const bb = container.getBlockBlobClient(blobPath);
  if (!(await bb.exists())) return null;
  const dl = await bb.download();
  try {
    return JSON.parse(await streamToString(dl.readableStreamBody));
  } catch {
    return null;
  }
}

async function putJson(container, blobPath, obj) {
  const client = container.getBlockBlobClient(blobPath);
  const payload = typeof obj === "string" ? obj : JSON.stringify(obj);
  await client.upload(payload, Buffer.byteLength(payload), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });
}
// -------- VP Utils (deterministic; no model calls) ---------------------------
function _vpTop(arr, n = 6) {
  return (Array.isArray(arr) ? arr : [])
    .map(s => String(s || "").trim())
    .filter(Boolean)
    .slice(0, n);
}
function _vpPick(list, fallback = "") {
  return (Array.isArray(list) ? list.map(x => String(x || "").trim()).find(Boolean) : "") || fallback;
}
function _vpHost(u) { try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return ""; } }
function _vpUniq(list) { const s = new Set(); return (Array.isArray(list) ? list : []).filter(x => (x && !s.has(x) && s.add(x))); }
function _vpProductNames(productsJson) {
  const arr = Array.isArray(productsJson) ? productsJson : [];
  return _vpUniq(
    arr
      .map(p => (typeof p === "string" ? p : (p?.name || p?.title || "")))
      .filter(Boolean)
  ).slice(0, 6);
}
function _vpProofPoints(evidenceLog) {
  const items = Array.isArray(evidenceLog) ? evidenceLog : [];
  const score = (e) => {
    const st = String(e?.source_type || "").toLowerCase();
    let s = 0;
    if (/^pack:\s*supplier/.test(st)) s += 40;
    if (st.includes("company site")) s += 30;
    if (st.includes("pdf") || /case/i.test(e?.title || "")) s += 12;
    if (/ofcom|ons|dsit|gov\.uk/.test((e?.url || "").toLowerCase())) s += 8;
    if (e?.quote && e.quote.length > 0) s += 2;
    return s;
  };
  return items
    .filter(e => e?.url && /^https:\/\//.test(e.url))
    .sort((a, b) => score(b) - score(a))
    .slice(0, 6)
    .map(e => (e.title || _vpHost(e.url) || "Evidence"));
}

function buildMooreVP({ outline, csvNormalized, evidenceLog, productsJson }) {
  // Supplier / industry
  const supplier =
    (outline?.input_notes?.supplier_name) ||
    (outline?.input_notes?.supplier) ||
    (outline?.input_notes?.company) || "";

  const industry =
    (csvNormalized?.selected_industry) ||
    (csvNormalized?.meta?.selected_industry) ||
    (outline?.input_notes?.industry) || "";
  const forWhoHint = industry ? `leaders in ${industry}` : "";

  // Signals (needs & blockers) — accept either local or global summaries
  const sig = csvNormalized?.signals || {};
  const gsig = csvNormalized?.global_signals || {};
  const buyerNeeds = _vpTop(sig.top_needs_supplier?.length ? sig.top_needs_supplier : gsig.top_needs_supplier, 3);
  const blockers = _vpTop(sig.top_blockers?.length ? sig.top_blockers : gsig.top_blockers, 3);

  // Products/capabilities
  const productNames = _vpProductNames(productsJson);
  const mainProduct = _vpPick(productNames, "the supplier’s service");

  // Competitors — user input only; never inject defaults here
  const userComps =
    (Array.isArray(outline?.input_notes?.competitors) && outline.input_notes.competitors) ||
    (Array.isArray(outline?.input_notes?.relevant_competitors) && outline.input_notes.relevant_competitors) || [];
  const unlikeTarget = _vpPick(userComps, "generic alternatives");

  // Proof points from evidence
  const proofPoints = _vpProofPoints(evidenceLog);

  const fields = {
    for_who: forWhoHint || (outline?.input_notes?.for_who || ""),
    who_need: buyerNeeds.length ? buyerNeeds.join("; ").replace(/; ([^;]*)$/, " and $1")
      : "to solve their priority operational needs",
    the: `${supplier || "The supplier"} ${mainProduct}`,
    is_a: productNames.length ? "managed offering" : "solution",
    that: blockers.length ? `resolves ${blockers.join("; ").replace(/; ([^;]*)$/, " and $1")}`
      : "delivers clear, measurable outcomes",
    unlike: unlikeTarget,
    provides: productNames.length ? productNames.join(", ") : "differentiated capabilities",
    proof_points: proofPoints
  };

  const paragraph = [
    `For ${fields.for_who}`,
    `who need ${fields.who_need},`,
    `the ${fields.the}`,
    `is a ${fields.is_a}`,
    `that ${fields.that}.`,
    `Unlike ${fields.unlike}, ${supplier || "the supplier"} provides ${fields.provides}.`
  ].join(" ");

  return { paragraph, fields };
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

// -------- Helper — Market context from evidence + CSV signals (no hard-wiring) --------
function deriveMarketContext({ evidence, csvSignals, usps, company, industry }) {
  // Preconditions: need some industry pack evidence and at least one of {need, usp}
  const ev = Array.isArray(evidence) ? evidence : [];
  if (!ev.length) return "";

  // 1) Choose the most relevant industry pack items (ranked earlier in evidence builder)
  //    Prefer regulator/industry items; then anything mentioning the industry term.
  const industryL = String(industry || "").toLowerCase();
  const isReg = (t) => /(ofcom|gov\.uk|ons|citb|regulator|industry)/.test(t);
  const score = (it) => {
    const t = `${(it.title || "")} ${(it.summary || it.quote || "")}`.toLowerCase();
    let s = 0;
    if (industryL && t.includes(industryL)) s += 5;
    if (isReg(t) || isReg(String(it.source_type || "").toLowerCase())) s += 8;
    return s;
  };
  const top = ev
    .map(it => ({ it, s: score(it) }))
    .sort((a, b) => b.s - a.s)
    .slice(0, 8)
    .map(x => x.it);

  if (!top.length) return "";

  // 2) Extract key terms and benefits from summaries only (no fixed keyword list)
  const text = top.map(it => String(it.summary || it.quote || it.title || "")).join(" ").toLowerCase();

  // Build a simple token frequency without stopwords; keep neutral nouns/adjectives
  const STOP = new Set([
    "the", "and", "for", "with", "from", "that", "this", "are", "is", "of", "to", "in", "on", "as", "by", "at", "an", "or", "a",
    "into", "across", "more", "most", "over", "under", "within", "their", "your", "our", "it", "they", "be", "was", "were"
  ]);
  const tokens = text.split(/[^a-z0-9+]+/).filter(t => t && t.length > 2 && !STOP.has(t));
  const freq = new Map();
  for (const t of tokens) freq.set(t, (freq.get(t) || 0) + 1);

  // Remove industry name and company name tokens to avoid tautology
  const rm = (s) => String(s || "").toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
  for (const k of rm(industry)) freq.delete(k);
  for (const k of rm(company)) freq.delete(k);

  // Pick top terms (neutral, evidence-derived)
  const topTerms = [...freq.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map(([w]) => w)
    .filter(Boolean);

  if (!topTerms.length) return "";

  // 3) Buyer need (CSV signals) and supplier capability (USPs)
  const needs = Array.isArray(csvSignals?.top_needs_supplier) && csvSignals.top_needs_supplier.length
    ? csvSignals.top_needs_supplier.filter(Boolean)
    : (Array.isArray(csvSignals?.top_needs) ? csvSignals.top_needs.filter(Boolean) : []);
  const primaryNeed = needs[0] || "";

  const uspArr = Array.isArray(usps) ? usps.filter(Boolean) : [];
  const uspPhrase = uspArr.slice(0, 3).join("; ");

  // Compose two short, factual sentences. If any element is missing, omit that clause.
  const parts = [];

  // Supplier fit sentence (only if we have either a need or USPs)
  if (company && (primaryNeed || uspPhrase)) {
    const needPart = primaryNeed ? `meet buyers’ needs for ${primaryNeed}` : "address priority buyer needs";
    const uspPart = uspPhrase ? ` via ${uspPhrase}` : "";
    parts.push(`${company} can ${needPart}${uspPart}.`);
  }

  // Peer practice sentence from evidence terms (no vendor words, purely evidence-derived)
  // Join 3–4 highest-signal terms to keep it readable
  const peerTerms = topTerms.slice(0, 4).join(", ");
  if (peerTerms) {
    parts.push(`Peers in the sector emphasise ${peerTerms} to secure specific operational benefits.`);
  }

  return parts.join(" ").trim();
}

// -------- Helper — extract top buyer blockers directly from CSV "TopBlockers" column --------
function extractTopBlockersFromCsv(csvCanonical) {
  // Accept any of: csvCanonical.records, csvCanonical.rows, or csvCanonical (AoA)
  const rows =
    Array.isArray(csvCanonical?.records) ? csvCanonical.records :
      Array.isArray(csvCanonical?.rows) ? csvCanonical.rows :
        (Array.isArray(csvCanonical) ? csvCanonical : []);

  if (!Array.isArray(rows) || rows.length < 2) return [];

  // Find column index for TopBlockers (case/spacing/underscore tolerant)
  const norm = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const header = rows[0].map((h) => String(h || ""));
  const idx = header.findIndex((h) => {
    const n = norm(h);
    return n === "topblockers" || (n.includes("top") && n.includes("block"));
  });
  if (idx === -1) return [];

  // Split cells by common delimiters, count frequency (keep original casing)
  const counts = new Map();
  const push = (txt) => {
    const key = txt.trim();
    if (!key) return;
    const low = key.toLowerCase();
    counts.set(low, { text: key, n: (counts.get(low)?.n || 0) + 1 });
  };

  for (let r = 1; r < rows.length; r++) {
    const cell = rows[r]?.[idx];
    if (!cell) continue;
    String(cell)
      .split(/[,;/|•]+/g)
      .map(s => s.trim())
      .filter(Boolean)
      .forEach(push);
  }

  // Return top 3 distinct blockers, preserving original text from the most common variant
  return [...counts.values()]
    .sort((a, b) => b.n - a.n)
    .slice(0, 3)
    .map(x => x.text);
}
// PATCH W-PROD-H: products_meta helpers (no external deps)
function _pm_cleanStrArr(arr) {
  return Array.isArray(arr)
    ? arr.map(v => (typeof v === "string" ? v.trim() : "")).filter(Boolean)
    : [];
}
function _pm_uniqPreserve(arr) {
  const seen = new Set(); const out = [];
  for (const s of (arr || [])) { const k = String(s).toLowerCase(); if (k && !seen.has(k)) { seen.add(k); out.push(String(s)); } }
  return out;
}
function _pm_inferObservedFromEvidence(claims = []) {
  // Heuristic from topic/title — extend keywords as needed
  const out = new Set();
  for (const c of (Array.isArray(claims) ? claims : [])) {
    const txt = `${c?.topic || ""} ${c?.title || ""}`.toLowerCase();
    if (!txt) continue;
    if (txt.includes("4g") || txt.includes("lte") || txt.includes("failover") || txt.includes("backup")) out.add("4G backup");
    if (txt.includes("apn")) out.add("Private APN");
    if (txt.includes("sim pool") || txt.includes("pooling")) out.add("SIM pooling");
    if (txt.includes("sd-wan")) out.add("SD-WAN");
    if (txt.includes("private 5g")) out.add("Private 5G");
  }
  return [...out];
}
function _pm_validateProducts({ declared, observed, csvNeeds, evidenceClaims }) {
  const candidates = _pm_uniqPreserve([...(declared || []), ...(observed || [])]);
  const vals = [];
  for (const name of candidates) {
    const k = name.toLowerCase();
    let score = 0;
    if ((observed || []).some(x => String(x).toLowerCase() === k)) score += 0.4;               // Observed
    if ((declared || []).some(x => String(x).toLowerCase() === k)) score += 0.2;               // Declared
    if ((csvNeeds || []).some(n => String(n).toLowerCase().includes(k) || k.includes(String(n).toLowerCase()))) score += 0.2; // CSV needs
    if ((evidenceClaims || []).some(c => `${c?.topic || ""} ${c?.title || ""}`.toLowerCase().includes(k))) score += 0.2;       // Evidence match
    if (score > 0) vals.push({ name, score: Math.round(score * 100) / 100, proof: [] });
  }
  return vals.filter(v => v.score >= 0.5).sort((a, b) => b.score - a.score);
}
function _pm_chooseProducts({ validated, input }) {
  const preferred = new Set(_pm_cleanStrArr(input?.campaign_products));
  const names = (validated || []).map(v => v.name);
  if (preferred.size) {
    const picked = names.filter(n => preferred.has(n));
    if (picked.length) return picked;
  }
  return names.slice(0, Math.min(2, names.length));
}
async function buildProductsMeta(container, prefix, { input, csvNormalized, evidence }) {
  const declared = _pm_cleanStrArr(input?.supplier_products);
  const evidenceClaims = Array.isArray(evidence) ? evidence : [];
  const observed = _pm_uniqPreserve(_pm_inferObservedFromEvidence(evidenceClaims));
  const csvNeeds = _pm_cleanStrArr(
    Array.isArray(csvNormalized?.signals?.top_needs_supplier) && csvNormalized.signals.top_needs_supplier.length
      ? csvNormalized.signals.top_needs_supplier
      : csvNormalized?.signals?.top_needs
  );
  const validated = _pm_validateProducts({ declared, observed, csvNeeds, evidenceClaims });
  const chosen = _pm_chooseProducts({ validated, input });
  const meta = { declared, observed, validated, chosen };
  await putJson(container, `${prefix}products_meta.json`, meta);
  return meta;
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

function leadParagraphFromStrategy({ strategy, evidence, industryOverride, input }) {
  // --- local helpers (no external deps) ---
  const listInline = (arr) => {
    const a = (Array.isArray(arr) ? arr : []).map(s => String(s || "").trim()).filter(Boolean);
    if (!a.length) return "";
    if (a.length === 1) return a[0];
    if (a.length === 2) return `${a[0]} and ${a[1]}`;
    return `${a.slice(0, -1).join(", ")}, and ${a[a.length - 1]}`;
  };
  const cap = (s) => (s ? s.charAt(0).toUpperCase() + s.slice(1) : "");

  // --- inputs from strategy / input ---
  const company = strategy?.meta?.company || "";
  const industry = (industryOverride || strategy?.meta?.industry || "").trim();
  const requirement = (strategy?.meta?.requirement || "growth").toLowerCase();
  const objective =
    requirement === "upsell" ? "upsell services to existing customers"
      : requirement === "win-back" ? "win back high-potential lapsed customers"
        : "create near-term growth";

  // Sector-insight (from ranked evidence: Ofcom/ONS/GOV.UK/CITB etc.) — optional, no placeholders
  let sectorLead = "";
  if (industry && Array.isArray(evidence) && evidence.length) {
    const picks = evidence.filter(ev => {
      const t = `${(ev.title || "")} ${(ev.summary || "")}`.toLowerCase();
      const src = String(ev.source_type || "").toLowerCase();
      const isReg = /(ofcom|gov\.uk|ons|citb)/.test(t) || /(ofcom|gov\.uk|ons|citb)/.test(src);
      return isReg || t.includes(industry.toLowerCase());
    }).slice(0, 2);

    if (picks.length) {
      const phrases = picks.map(ev => {
        const s = String(ev.summary || ev.title || "").split(/[.?!]/)[0].trim();
        const tag = (ev.source_type || "source").toString();
        return s ? `${s} (${tag})` : "";
      }).filter(Boolean);
      if (phrases.length) sectorLead = phrases.join(" ");
    }
  }

  // Buyer **motivations/needs** and **barriers** (no placeholders)
  // Prefer strategy-derived fields (deterministic model), fall back to input if present.
  const needsFromStrategy =
    Array.isArray(strategy?.buyer_needs) ? strategy.buyer_needs.filter(Boolean) : [];
  const needsFromInput =
    Array.isArray(input?.top_needs_supplier) ? input.top_needs_supplier.filter(Boolean) :
      Array.isArray(input?.top_needs) ? input.top_needs.filter(Boolean) : [];
  const needs = (needsFromStrategy.length ? needsFromStrategy : needsFromInput).slice(0, 2);

  const blockersFromStrategy =
    Array.isArray(strategy?.buyer_problems) ? strategy.buyer_problems.filter(Boolean) : [];
  const blockersFromInput =
    Array.isArray(input?.top_blockers) ? input.top_blockers.filter(Boolean) : [];
  const blockers = (blockersFromStrategy.length ? blockersFromStrategy : blockersFromInput).slice(0, 2);

  // One-sentence **decision context** (only if we have real data)
  let decisionLine = "";
  if (needs.length || blockers.length) {
    const actor = industry ? `${cap(industry)} leaders` : "Decision-makers";
    const want = needs.length ? `want ${listInline(needs)}` : "";
    const held = blockers.length ? `but are held back by ${listInline(blockers)}` : "";
    const clause = [want, held].filter(Boolean).join(" ");
    if (clause) decisionLine = `${actor} ${clause}.`;
  }

  // Buyer pains → sentence (optional, only if present)
  const pains = Array.isArray(strategy?.buyer_problems) ? strategy.buyer_problems.filter(Boolean).slice(0, 2) : [];
  const painsText = pains.length ? `Based on current evidence, they need ${listInline(pains)}.` : "";

  // Supplier capabilities (USPs) — only real values
  const usps = Array.isArray(strategy?.meta?.usps) ? strategy.meta.usps.filter(Boolean).slice(0, 3) : [];
  const uspText = usps.length ? `${company || "The supplier"} provides ${listInline(usps)}.` : "";

  // Expected business outcome (deterministic if provided)
  const expected = strategy?.specific_outcome ? `Expected outcome: ${strategy.specific_outcome}.` : "";

  // Core paragraph (kept from your model)
  const core = [
    `This campaign’s objective is to ${objective}${industry ? ` in ${industry}` : ""}.`,
    strategy?.prospect_base?.icp
      ? `We will start with ${strategy.prospect_base.icp} and report weekly against agreed measures.`
      : "We will start with one clear audience and report weekly against agreed measures.",
    painsText,
    uspText,
    expected
  ].filter(Boolean).join(" ");

  // Order: sector insight (if present) → decision context (if present) → core
  return [sectorLead, decisionLine, core].filter(Boolean).join(" ");
}

// -------- Helper — Data-driven Feature → Outcome → Business Value (no hard-wiring) --------
function mapFeaturesToBenefits({ usps, evidence, csvSignals, strategy }) {
  const toText = (v) => String(v || "").toLowerCase();
  const split = (s) => toText(s).split(/[^a-z0-9]+/).filter(Boolean);
  const jaccard = (a, b) => {
    const A = new Set(a), B = new Set(b);
    if (!A.size || !B.size) return 0;
    let inter = 0;
    for (const x of A) if (B.has(x)) inter++;
    return inter / (A.size + B.size - inter);
  };

  const needs =
    Array.isArray(csvSignals?.top_needs_supplier) && csvSignals.top_needs_supplier.length
      ? csvSignals.top_needs_supplier.filter(Boolean)
      : (Array.isArray(csvSignals?.top_needs) ? csvSignals.top_needs.filter(Boolean) : []);
  const blockers = Array.isArray(csvSignals?.top_blockers) ? csvSignals.top_blockers.filter(Boolean) : [];
  const uspsArr = Array.isArray(usps) ? usps.filter(Boolean) : [];

  // Optional evidence gate: allow a USP if it appears in evidence text OR accept it from strategy meta
  const evidenceText = (Array.isArray(evidence) ? evidence : [])
    .map(ev => `${ev.title || ""} ${ev.summary || ev.quote || ""}`)
    .join(" ")
    .toLowerCase();

  const lines = [];

  for (const usp of uspsArr) {
    // Only proceed if the USP is non-empty and either present in evidence text OR we trust strategy meta
    const uspTokens = split(usp);
    if (!uspTokens.length) continue;

    // Find best matching need & blocker by token overlap (no fixed keywords)
    let bestNeed = null, bestNeedScore = 0;
    for (const n of needs) {
      const s = jaccard(uspTokens, split(n));
      if (s > bestNeedScore) { bestNeedScore = s; bestNeed = n; }
    }

    let bestBlocker = null, bestBlockerScore = 0;
    for (const b of blockers) {
      const s = jaccard(uspTokens, split(b));
      if (s > bestBlockerScore) { bestBlockerScore = s; bestBlocker = b; }
    }

    // Build only if we have at least one alignment (need OR blocker)
    if (!bestNeed && !bestBlocker) continue;

    const parts = [];
    // Feature
    parts.push(String(usp).trim());
    // Outcome (why it matters operationally)
    if (bestNeed) parts.push(`to advance ${bestNeed}`);
    // Business value (link to blocker risk reduction or the stated specific outcome)
    const specific = strategy?.specific_outcome ? String(strategy.specific_outcome).trim() : "";
    if (bestBlocker) {
      parts.push(`→ Business value: reduces risk from ${bestBlocker}`);
    } else if (specific) {
      parts.push(`→ Business value: supports ${specific}`);
    }

    // Join: "<USP> to advance <need> → Business value: ..."
    const line = parts.join(" ");
    if (line) lines.push(line);
  }

  // Keep the ES tight
  return lines.slice(0, 3);
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

function shapeExecutiveSummary({ existing, input, csvNormalized, strategy, evidence }) {
  if (Array.isArray(existing) && existing.length >= 1 && existing.every(x => nonEmpty(x))) return existing;
  const company = firstNonEmpty(input.supplier_company, input.company_name, strategy?.meta?.company);
  const industry = firstNonEmpty(
    input.selected_industry,
    input.campaign_industry,
    csvNormalized?.selected_industry,
    strategy?.meta?.industry
  );

  // Route (declare ONCE; used in both deps + CTA)
  const route = (strategy?.go_to_market?.route || (input.sales_model || input.salesModel || "")).toLowerCase();

  // Detection flags from evidence/input to make dependencies conditional
  const evArr = Array.isArray(evidence) ? evidence : [];
  const toText = (v) => String(v || "").toLowerCase();

  const hasReferences =
    evArr.some(e =>
      /reference|case study|testimonial|customer/.test(toText(e?.title)) ||
      /reference|case study|testimonial|customer/.test(toText(e?.source_type)) ||
      /reference|case study|testimonial|customer/.test(toText(e?.tags))
    );

  const trackingText = [
    input?.landing_url,
    input?.utm,
    input?.tracking_notes,
    input?.crm,
    input?.crm_system
  ].map(toText).join(" ");

  const hasTracking =
    /utm|crm|salesforce|hubspot|tracked landing|landing page/.test(trackingText) ||
    evArr.some(e =>
      /utm|crm|tracked landing|landing page/.test(toText(e?.title)) ||
      /utm|crm|tracked landing|landing page/.test(toText(e?.source_type))
    );

  // ---- Declare USPs BEFORE any usage ----
  const usps = Array.isArray(input.supplier_usps)
    ? input.supplier_usps.filter(Boolean)
    : (Array.isArray(strategy?.meta?.usps) ? strategy.meta.usps.filter(Boolean) : []);

  // Addressable market: prefer input.addressable_market, then csvMeta.rows, then strategy TAM
  const rowsRaw = Number.isFinite(Number(input?.addressable_market)) && Number(input.addressable_market) > 0
    ? Number(input.addressable_market)
    : (Number.isFinite(Number(csvNormalized?.meta?.rows)) && Number(csvNormalized.meta.rows) > 0
      ? Number(csvNormalized.meta.rows)
      : (Number.isFinite(Number(strategy?.prospect_base?.tam)) ? Number(strategy.prospect_base.tam) : null));

  const amBullet = rowsRaw !== null
    ? `Addressable market in scope: **${rowsRaw.toLocaleString()}** organisations (from campaign source data).`
    : `Addressable market in scope: will be populated from the uploaded CSV.`;

  // Buyer blockers (prefer industry-specific, then global signals; no CSV rows dependency here)
  const sig = csvNormalized?.signals || {};
  const gsig = csvNormalized?.global_signals || {};
  const csvBlockers = (Array.isArray(sig.top_blockers) && sig.top_blockers.length)
    ? sig.top_blockers
    : (Array.isArray(gsig.top_blockers) ? gsig.top_blockers : []);
  const blockersLine = csvBlockers.length
    ? `Key buyer blockers to investment: ${csvBlockers.slice(0, 3).join("; ")}.`
    : "";
  // Market context (data-led, optional; prints only when we have evidence/signals)
  const marketContextLine = deriveMarketContext({
    evidence,
    csvSignals: (csvNormalized && csvNormalized.signals) ? csvNormalized.signals : undefined,
    usps,
    company,
    industry
  });
  // Optional evidence reference (use it or omit it entirely)
  const csvClaim = (Array.isArray(evidence) && evidence[0]?.claim_id)
    ? evidence[0].claim_id
    : (strategy?.evidence_links?.[0] || null);

  // Lead paragraph (deterministic)
  const paragraph = leadParagraphFromStrategy({ strategy, evidence, industry, input });

  // Dependencies: conditional and sign-off oriented (no SDR jargon)
  const depItems = [];
  if (route === "partner") {
    depItems.push("Confirm partner route and co-marketing funds (MDF)");
  } else {
    depItems.push("Confirm sufficient, enabled, salespeople for outbound first wave");
  }
  if (!hasReferences) depItems.push("Access 2–3 verified customer references with measurable outcomes");
  if (!hasTracking) depItems.push("Publish tracked landing path and lead capture (UTM, CRM)");
  const deps = depItems.join("; ");

  // Buyer pains & competitive note (data-driven; no placeholders)
  const pains = Array.isArray(strategy?.buyer_problems) ? strategy.buyer_problems.filter(Boolean).slice(0, 2) : [];

  // Audience: prefer ICP; else industry; else omit subject entirely
  const icp = strategy?.prospect_base?.icp && String(strategy.prospect_base.icp).trim();
  const audience = icp
    ? icp
    : (industry ? `${industry} decision-makers` : "");

  // Offer: only if we have company and/or USPs; never print a placeholder
  const uspArr = Array.isArray(usps) ? usps.filter(Boolean) : [];
  const offerParts = [];
  if (company) offerParts.push(company);
  if (uspArr.length) offerParts.push(uspArr.slice(0, 3).join("; "));
  const offerPhrase = offerParts.join(" — "); // e.g., "Comms365 — Bonded Internet; SD-One; Continuum"

  // Competitive alternative: only if provided; otherwise omit the clause
  const vsList = Array.isArray(strategy?.how_win?.vs_competitors) ? strategy.how_win.vs_competitors.filter(Boolean) : [];
  const diffClause = vsList.length ? ` — unlike ${vsList[0]}` : "";

  // Compose first bullet with only evidenced parts
  const firstParts = [];
  if (audience) {
    // "For <audience>, <offer> ..."
    const head = offerPhrase ? `For ${audience}, ${offerPhrase}` : `For ${audience}`;
    firstParts.push(head);
  } else if (offerPhrase) {
    // No audience string; start with offer
    firstParts.push(offerPhrase);
  }
  if (pains.length) firstParts.push(`to address ${pains.join("; ")}`);
  const firstBullet = (firstParts.join(" ") + diffClause).trim();

  // Evidence-checked Feature→Outcome→Business Value lines (no placeholders)
  const fovLines = mapFeaturesToBenefits({
    usps,
    evidence,
    csvSignals: (csvNormalized && csvNormalized.signals) ? csvNormalized.signals : undefined,
    strategy
  }).slice(0, 3);

  // ---- Define CTA deterministically (with safe overrides) ----
  let cta =
    (typeof input?.cta === "string" && input.cta.trim()) ||
    (typeof strategy?.meta?.cta === "string" && strategy.meta.cta.trim()) ||
    (route === "partner"
      ? "Initiate the joint campaign with named partners and MDF plan"
      : "Launch first-wave outreach to the named cohort with tracked landing and CRM capture");

  const bullets = [
    // First bullet only if we have something substantive
    ...(firstBullet ? [firstBullet] : []),

    // Evidence-backed Feature → Outcome → Business Value lines (0–3, no placeholders)
    ...fovLines,

    // Addressable market (always present if CSV parsed)
    amBullet,

    // Market context line (only when derived from evidence + CSV signals + USPs)
    ...(marketContextLine ? [marketContextLine] : []),
    ...(blockersLine ? [blockersLine] : []),

    // Pre-conditions expressed for sign-off (deps already computed upstream)
    `Pre-conditions: ${deps}.`,

    // Decision phrased for a budget holder (no jargon)
    `Decision: approve the target segment and success metric, with weekly reporting.`,

    // Clear next step (CTA already route-aware and non-jargon)
    `Next step: ${cta}`
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
    // PATCH W-PROD-CALL: build products_meta.json (Declared → Observed → Validated → Chosen)
    await buildProductsMeta(container, prefix, {
      input: mergedInput,
      csvNormalized: csvNormalized || {},
      evidence: Array.isArray(evidence) ? evidence : []
    });


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
    await setPhase(container, prefix, "StrategySynthesis");
    const strategy = buildStrategyObject({
      input: mergedInput,
      csvNormalized: csvNormalized || {},
      needsMap,
      evidence
    });
    // -------- Phase — Moore Value Proposition synthesis --------------------------

    // Safe loader (uses your existing getJson/putJson, container, prefix)
    const safeGetJson = async (rel) => (await getJson(container, `${prefix}${rel}`)) || null;

    // Normalise all inputs so buildMooreVP receives valid, in-scope values
    const outlineFixed =
      (await safeGetJson("outline.json")) || {};

    const csvNormalizedFixed =
      (csvNormalized && typeof csvNormalized === "object" ? csvNormalized : null) ||
      (await safeGetJson("csv_normalized.json")) ||
      {};

    // evidence_log.json can be either ARRAY or { evidence_log: ARRAY }
    const evAny = await safeGetJson("evidence_log.json");
    const evidenceLogFixed = Array.isArray(evidence)
      ? evidence
      : (Array.isArray(evAny) ? evAny : (Array.isArray(evAny?.evidence_log) ? evAny.evidence_log : []));

    // products_meta.json (best-effort): prefer validated → chosen
    const metaAny = await safeGetJson("products_meta.json");
    const productsValidated = Array.isArray(metaAny?.validated) ? metaAny.validated : [];
    const productsChosen = Array.isArray(metaAny?.chosen) ? metaAny.chosen : [];

    // products.json may be { products: [] } or a raw array; normalise to array
    const prodAny = await safeGetJson("products.json");
    const productsJsonFixed = Array.isArray(prodAny?.products)
      ? prodAny.products
      : (Array.isArray(prodAny) ? prodAny : []);

    // Final list used by worker: validated → chosen → products.json
    const productsFinal = productsValidated.length
      ? productsValidated
      : (productsChosen.length ? productsChosen : productsJsonFixed);

    // PATCH INS-1: enrich Strategy with validated products + coverage + evidence
    (function attachInsight() {
      // 1) Prospect-base products (names only for validated; keep chosen as provided)
      const validatedNames = Array.isArray(productsValidated) ? productsValidated.map(v => v?.name).filter(Boolean) : [];
      strategy.prospect_base = strategy.prospect_base || {};
      strategy.prospect_base.products = {
        declared: Array.isArray(metaAny?.declared) ? metaAny.declared : [],
        observed: Array.isArray(metaAny?.observed) ? metaAny.observed : [],
        validated: validatedNames,
        chosen: Array.isArray(productsChosen) ? productsChosen : []
      };

      // 2) Coverage (from needs_map.json you already loaded)
      strategy.insight = strategy.insight || {};
      strategy.insight.coverage = (needsMap && needsMap.coverage) ? needsMap.coverage : { total: 0, matched: 0, partial: 0, gap: 0, coverage: 0 };

      // 3) Evidence counters (lightweight, from evidenceLogFixed in this scope)
      const counts = { website: 0, linkedin: 0, pdf: 0, directories: 0, ixbrl: 0, csv: 0 };
      const ev = Array.isArray(evidenceLogFixed) ? evidenceLogFixed : [];
      for (const c of ev) {
        const t = String(c?.source_type || "").toLowerCase();
        if (t.includes("site") || t.includes("website") || t.includes("company site")) counts.website++;
        else if (t.includes("linkedin")) counts.linkedin++;
        else if (t.includes("pdf")) counts.pdf++;
        else if (t.includes("directory")) counts.directories++;
        else if (t.includes("ixbrl")) counts.ixbrl++;
        else if (t.includes("csv")) counts.csv++;
      }
      strategy.insight.evidence = { total: ev.length, counts };

      // 4) Product-fit credibility score (avg of validated scores, 0–1 rounded 2dp)
      let credibility = 0;
      if (Array.isArray(productsValidated) && productsValidated.length) {
        let sum = 0;
        for (const v of productsValidated) sum += Number(v?.score || 0);
        credibility = Math.round((sum / productsValidated.length) * 100) / 100;
      }

      // 5) Reasoned rationale (short, audit-friendly)
      const tam = Number(csvNormalizedFixed?.meta?.rows || 0);
      const whyPlay = tam
        ? `Defined in-scope market of ${tam} organisations from CSV.`
        : `Defined in-scope market present in uploaded CSV.`;
      const whyCredible = credibility
        ? `Validated product fit (avg score ${credibility}).`
        : `Product fit inferred from declared/observed signals; validation pending.`;
      const chosenLine = (strategy.prospect_base.products.chosen || []).length
        ? `Chosen focus: ${strategy.prospect_base.products.chosen.join(", ")}.`
        : ``;

      strategy.insight.product_fit = {
        credibility_score: credibility,
        top_validated: validatedNames.slice(0, 2),
        rationale: [whyPlay, whyCredible, chosenLine].filter(Boolean)
      };
    })();

    // -------- Phase — Moore Value Proposition synthesis --------------------------
    const _moore = buildMooreVP({
      outline: outlineFixed,
      csvNormalized: csvNormalizedFixed,
      evidenceLog: evidenceLogFixed,
      productsJson: productsFinal // <-- now passing the preferred list
    });

    // Attach to strategy without changing existing schema
    strategy.value_proposition_moore = {
      paragraph: _moore.paragraph,
      fields: _moore.fields,
      chosen_products: Array.isArray(productsChosen) ? productsChosen.slice(0, 4) : []
    };

    // Keep legacy one-liner populated for current UI if missing
    strategy.positioning_and_differentiation = strategy.positioning_and_differentiation || {};
    if (!strategy.positioning_and_differentiation.value_prop) {
      strategy.positioning_and_differentiation.value_prop = _moore.paragraph;
    }

    // -------- Strategy persist + status (scoped, fail-safe) --------
    try {
      // In-progress (idempotent, forward-only)
      try {
        const nowIso = new Date().toISOString();
        const st0 = (await getJson(container, `${prefix}status.json`)) || {};
        const prevState = String(st0.state || "");
        const terminal = new Set(["assembled", "strategy_ready", "error", "Failed"]);
        if (!terminal.has(prevState)) {
          st0.state = "strategy_working";
          st0.last_op = "worker";
          st0.updated = nowIso;
          st0.markers = (st0.markers && typeof st0.markers === "object") ? st0.markers : {};
          st0.history = Array.isArray(st0.history) ? st0.history : [];
          st0.history.push({ t: nowIso, op: "worker_start" });
          if (st0.history.length > 100) st0.history = st0.history.slice(-100);
          await putJson(container, `${prefix}status.json`, st0);
        }
      } catch { /* non-fatal */ }

      // Persist the strategy
      await putJson(container, `${prefix}campaign_strategy.json`, strategy);

      // Success (strategy_ready, forward-only)
      try {
        const nowIso = new Date().toISOString();
        const st = (await getJson(container, `${prefix}status.json`)) || {};
        const prevState = String(st.state || "");
        const terminal = new Set(["assembled", "error", "Failed"]);
        if (!terminal.has(prevState)) {
          st.state = "strategy_ready";     // frontend treats this as ready-to-write
          st.last_op = "worker_done";
          st.updated = nowIso;
          st.markers = (st.markers && typeof st.markers === "object") ? st.markers : {};
          st.markers.afterStrategy = true;
          st.history = Array.isArray(st.history) ? st.history : [];
          st.history.push({ t: nowIso, op: "worker_done" });
          if (st.history.length > 100) st.history = st.history.slice(-100);
          await putJson(container, `${prefix}status.json`, st);
        }
      } catch { /* non-fatal; do not block enqueue */ }

    } catch (err) {
      // Error (localized): mark terminal "Failed" to match your existing scheme
      try {
        const nowIso = new Date().toISOString();
        const stE = (await getJson(container, `${prefix}status.json`)) || {};
        const prevState = String(stE.state || "");
        if (prevState !== "assembled") {
          const msg = (err && (err.message || String(err))) || "worker_error";
          stE.state = "Failed";
          stE.last_op = "worker_error";
          stE.error = { code: "worker_error", message: msg.length > 2000 ? (msg.slice(0, 1997) + "…") : msg };
          stE.updated = nowIso;
          stE.history = Array.isArray(stE.history) ? stE.history : [];
          stE.history.push({ t: nowIso, op: "worker_error" });
          if (stE.history.length > 100) stE.history = stE.history.slice(-100);
          await putJson(container, `${prefix}status.json`, stE);
        }
      } catch { /* best-effort */ }
      throw err; // rethrow so your outer handler behaves as before
    }

    // v15 fix: Title + Executive Summary coercion with safe fallbacks
    try {
      draft.campaign_title = draft.campaign_title || deriveCampaignTitle({
        company: firstNonEmpty(mergedInput.supplier_company, draft.supplier_company, draft.company_name),
        industry: firstNonEmpty(mergedInput.selected_industry, mergedInput.campaign_industry, csvNormalized?.selected_industry, csvNormalized?.meta?.selected_industry),
        requirement: firstNonEmpty(mergedInput.campaign_requirement, draft.campaign_requirement)
      });

      draft.executive_summary = shapeExecutiveSummary({
        existing: draft.executive_summary,
        input: mergedInput,
        csvNormalized,
        strategy,
        evidence,
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
