// /api/campaign-evidence/index.js — Split campaign build. 11-11-2025 — v21.0
// Artifacts written under: <prefix>/
//   - site.json               (homepage snapshot + lightweight link graph — array, legacy compatible)
//   - products.json           (product analysis for ability to solve buyers' problems)
//   - linkedin.json           (reference link only; no scraping)
//   - pdf_extracts.json       (links discovered incl. PDFs/case studies; shallow metadata)
//   - directories.json        (placeholder; empty array)
//   - csv_normalized.json     (canonical: industry_mode + signals + meta)
//   - evidence_log.json       (ARRAY; CSV summary + supplier artefacts + regulator/sector seeds + LI hooks + PACKS)
//   - status.json             (state=EvidenceDigest or Failed; with history[])

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueClient } = require("@azure/storage-queue");
const crypto = require("node:crypto");
const fs = require("fs");
const path = require("path");

// ---------- Config ----------
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const START_QUEUE_NAME = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
const FETCH_TIMEOUT_MS = Number(process.env.HTTP_FETCH_TIMEOUT_MS || 8000);
const MAX_SITE_PAGES = 8;                  // crawl budget (homepage + up to 7 pages)
const MAX_CASESTUDIES = 8;                 // cap case study items stored
let MAX_EVIDENCE_ITEMS = parseInt(process.env.MAX_EVIDENCE_ITEMS || "24", 10);
if (!Number.isFinite(MAX_EVIDENCE_ITEMS) || MAX_EVIDENCE_ITEMS <= 0) MAX_EVIDENCE_ITEMS = 24;
if (MAX_EVIDENCE_ITEMS > 128) MAX_EVIDENCE_ITEMS = 128;

// ---------- Guarded, lazy loaders (added) ----------
let _packLoaderFn;
async function loadPacksFn() {
  if (_packLoaderFn) return _packLoaderFn;
  try {
    // CJS fast path
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const cjs = require("../shared/packloader");
    const fn = cjs?.loadPacks ?? cjs?.default ?? cjs;
    if (typeof fn === "function") { _packLoaderFn = fn; return _packLoaderFn; }
  } catch { /* fall through */ }
  try {
    // ESM fallback
    const modUrl = new URL("../shared/packloader.js", `file://${__dirname}/`);
    const esm = await import(modUrl.href);
    const fn = esm?.loadPacks ?? esm?.default ?? esm;
    if (typeof fn === "function") { _packLoaderFn = fn; return _packLoaderFn; }
  } catch { /* ignore */ }
  _packLoaderFn = async () => ({ packs: {} }); // safe no-op
  return _packLoaderFn;
}

let _evidenceLib;
async function loadEvidenceLib() {
  if (_evidenceLib) return _evidenceLib;
  try {
    // CJS fast path
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const mod = require("../lib/evidence");
    const buildEvidence = mod.buildEvidence ?? mod.default ?? mod;
    if (typeof buildEvidence !== "function") throw new Error("evidence.buildEvidence missing");
    _evidenceLib = { buildEvidence };
    return _evidenceLib;
  } catch (e1) {
    try {
      // ESM fallback
      const modUrl = new URL("../lib/evidence.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      const buildEvidence = esm.buildEvidence ?? esm.default ?? esm;
      if (typeof buildEvidence !== "function") throw new Error("evidence.buildEvidence missing");
      _evidenceLib = { buildEvidence };
      return _evidenceLib;
    } catch (e2) {
      throw new Error(`evidence lib load failed: ${e1?.message || e1} | ${e2?.message || e2}`);
    }
  }
}

// ---------- Azure helpers ----------
function getContainerClient() {
  const conn = process.env.AzureWebJobsStorage;
  if (!conn) throw new Error("AzureWebJobsStorage not configured");
  const svc = BlobServiceClient.fromConnectionString(conn);
  return svc.getContainerClient(RESULTS_CONTAINER);
}

// --- CSV holders for later evidence composition ---
let csvRowsRaw = null;        // raw parsed rows (array-of-arrays)
let csvFocusInsight = {       // computed later from rows + input focus
  totalRows: 0,
  focusLabel: "",
  focusCount: null
};

async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

async function getJson(containerClient, blobPath) {
  try {
    const blob = containerClient.getBlobClient(blobPath);
    if (!(await blob.exists())) return null;
    const dl = await blob.download();
    const text = await streamToString(dl.readableStreamBody);
    return JSON.parse(text);
  } catch {
    return null;
  }
}

async function readJsonIfExists(containerClient, blobPath) {
  return getJson(containerClient, blobPath);
}

async function putJson(containerClient, blobPath, obj) {
  const b = containerClient.getBlockBlobClient(blobPath);
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await b.uploadData(body, {
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }
  });
}

async function putText(containerClient, blobPath, text) {
  const b = containerClient.getBlockBlobClient(blobPath);
  const body = Buffer.from(String(text ?? ""), "utf-8");
  await b.uploadData(body, {
    blobHTTPHeaders: { blobContentType: "text/plain; charset=utf-8" }
  });
}

async function listCsvUnderPrefix(containerClient, prefix) {
  const out = [];
  for await (const item of containerClient.listBlobsFlat({ prefix })) {
    if (item.name.toLowerCase().endsWith(".csv")) out.push(item.name);
  }
  return out;
}

async function downloadText(containerClient, blobPath) {
  const blob = containerClient.getBlobClient(blobPath);
  if (!(await blob.exists())) return null;
  const resp = await blob.download();
  return streamToString(resp.readableStreamBody);
}

async function getText(containerClient, blobPath) {
  return downloadText(containerClient, blobPath);
}


// ---------- Generic utils ----------
function nowIso() { return new Date().toISOString(); }
function hostnameOf(u) { try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return null; } }
function toAbsUrl(base, href) { try { return new URL(href, base).toString(); } catch { return null; } }
function sha1(s) { return crypto.createHash("sha1").update(String(s || "")).digest("hex"); }

// Enforce https scheme for all evidence URLs
function toHttps(u) {
  if (!u) return u;
  const s = String(u).trim();
  if (!s) return s;
  if (/^https:\/\//i.test(s)) return s;
  if (/^http:\/\//i.test(s)) return s.replace(/^http:\/\//i, "https://");
  return `https://${s}`;
}

// ---------- Status helpers ----------
async function updateStatus(containerClient, prefix, patch, historyNode) {
  const statusPath = `${prefix}status.json`;
  const cur = (await getJson(containerClient, statusPath)) || { runId: patch?.runId, history: [] };
  const next = { ...cur, ...patch };
  if (!Array.isArray(next.history)) next.history = [];
  if (historyNode) next.history.push({ at: nowIso(), ...historyNode });
  await putJson(containerClient, statusPath, next);
}

// ==== BEGIN: product validation helpers ====
function tokenize(s) {
  return (s || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s\-\+\/]/g, ' ')
    .split(/\s+/)
    .filter(Boolean);
}

function jaccard(aTokens, bTokens) {
  const a = new Set(aTokens), b = new Set(bTokens);
  let inter = 0;
  for (const t of a) if (b.has(t)) inter++;
  const union = a.size + b.size - inter;
  return union ? inter / union : 0;
}

/**
 * Build problem/need signals from:
 *  - CSV (top_needs_supplier, top_needs, top_purchases)
 *  - Packs: industry/generic/company (fields: problems, pains, needs if present)
 *  - Supplier site: headings/nav snippets that look like problem statements
 */
function buildProblemSignals({ input, packs, site }) {
  const sigs = [];

  // CSV
  const csv = input?.signals || {};
  for (const k of ['top_needs_supplier', 'top_needs', 'top_purchases']) {
    const v = csv[k];
    if (Array.isArray(v)) v.forEach(x => sigs.push(String(x)));
    else if (typeof v === 'string') v.split(/[;,\n]/).forEach(x => sigs.push(x.trim()));
  }

  // Packs
  const packSets = [
    packs?.industry, packs?.generic, packs?.company
  ].filter(Boolean);

  for (const p of packSets) {
    for (const k of ['problems', 'pains', 'needs']) {
      const v = p[k];
      if (Array.isArray(v)) v.forEach(x => sigs.push(String(x)));
      else if (typeof v === 'string') v.split(/[;,\n]/).forEach(x => sigs.push(x.trim()));
    }
  }

  // Site (very light heuristic: sentences with “latency”, “outage”, “security”, etc.)
  const siteText = (site?.home_text || '') + ' ' + (site?.titles?.join(' ') || '');
  siteText.split(/[.\n]/).forEach(s => {
    const t = s.trim();
    if (!t) return;
    if (/\b(latency|outage|resilien|failover|security|compliance|sla|uptime|coverage|support|integration|cost|efficien|visibility|monitor|redundan)/i.test(t)) {
      sigs.push(t);
    }
  });

  // dedupe and normalise
  return [...new Set(sigs.map(s => s.trim()).filter(Boolean))].slice(0, 200);
}

/**
 * Score a product candidate against problem signals. Returns {score, matches[]}
 */
function scoreProductAgainstSignals(name, signals) {
  const nt = tokenize(name);
  let best = { score: 0, match: '' };
  for (const s of signals) {
    const st = tokenize(s);
    const sc = jaccard(nt, st);
    if (sc > best.score) best = { score: sc, match: s };
  }
  return { score: best.score, matches: best.score ? [best.match] : [] };
}

// ---------- Network helper (resilient) ----------
async function httpGet(url, timeoutMs = FETCH_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort("timeout"), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: ctrl.signal,
      headers: {
        "user-agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
      }
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, text };
  } catch (e) {
    return { ok: false, status: 0, text: String(e?.message || e) };
  } finally {
    clearTimeout(t);
  }
}

// ---------- CSV normaliser (no external deps) ----------
function headerIndexByPatterns(headerRow, patterns) {
  const norm = (s) => String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^a-z0-9 ]+/g, "")
    .trim();

  const head = headerRow.map(norm);
  for (let i = 0; i < head.length; i++) {
    const h = head[i];
    for (const p of patterns) {
      if (typeof p === "function") { if (p(h)) return i; continue; }
      const needle = norm(p);
      if (h === needle) return i;
      if (h.includes(needle)) return i;
    }
  }
  return -1;
}
function parseCsvLoose(csvText) {
  const rows = [];
  if (!csvText || !csvText.trim()) return rows;

  let i = 0, cur = "", inQ = false, row = [];
  const pushCell = () => { row.push(cur); cur = ""; };
  const pushRow = () => { rows.push(row.map(s => s.trim())); row = []; };

  const s = csvText.replace(/\r\n/g, "\n");
  while (i < s.length) {
    const ch = s[i];
    if (inQ) {
      if (ch === '"') {
        if (s[i + 1] === '"') { cur += '"'; i += 2; continue; }
        inQ = false; i++; continue;
      }
      cur += ch; i++; continue;
    } else {
      if (ch === '"') { inQ = true; i++; continue; }
      if (ch === "," || ch === ";") { pushCell(); i++; continue; }
      if (ch === "\n") { pushCell(); pushRow(); i++; continue; }
      cur += ch; i++; continue;
    }
  }
  pushCell();
  pushRow();
  return rows;
}

function normalizeCsv(rows) {
  if (!rows.length) {
    return { industries: [], dominant_industry: null, by_industry: {} };
  }
  const header = rows[0].map(h => String(h || "").trim());

  // More tolerant header detection
  const iIndustry = headerIndexByPatterns(header, [
    "SimplifiedIndustry", "Industry", "buyer industry", "vertical", "sector", "industry sector"
  ]);

  const iBlockers = headerIndexByPatterns(header, [
    "TopBlockers", "Top Blockers", "blockers", "top obstacles", "barriers"
  ]);
  const iPurchases = headerIndexByPatterns(header, [
    "TopPurchases", "Top Purchases", "intended purchases", "purchase intent", "plan to purchase", "purchases"
  ]);
  const iNeeds = headerIndexByPatterns(header, [
    "TopNeedsSupplier", "Top Needs Supplier", "supplier needs", "top needs", "needs (supplier)", "needs"
  ]);
  const iSpend = headerIndexByPatterns(header, [
    "SpendDistribution", "Spend Distribution", "it spend", "spend band", "spend"
  ]);

  const agg = new Map(); // industry -> { TopBlockers:Set, TopPurchases:Set, TopNeedsSupplier:Set, SpendDistribution:Object, SampleSize:Number }

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const industry = (iIndustry >= 0 ? (row[iIndustry] || "") : "").toString().trim() || "Unknown";
    if (!agg.has(industry)) {
      agg.set(industry, {
        TopBlockers: new Set(),
        TopPurchases: new Set(),
        TopNeedsSupplier: new Set(),
        SpendDistribution: {},
        SampleSize: 0
      });
    }
    const bucket = agg.get(industry);

    const addList = (val, set) => {
      const items = String(val || "")
        .split(/\r?\n|;|,|\|/)
        .map(s => s.trim())
        .filter(Boolean);
      items.forEach(x => set.add(x));
    };

    if (iBlockers >= 0) addList(row[iBlockers], bucket.TopBlockers);
    if (iPurchases >= 0) addList(row[iPurchases], bucket.TopPurchases);
    if (iNeeds >= 0) addList(row[iNeeds], bucket.TopNeedsSupplier);

    if (iSpend >= 0 && row[iSpend]) {
      let obj = {};
      const raw = String(row[iSpend]).trim();
      try { obj = JSON.parse(raw); }
      catch {
        raw.split("|").forEach(kv => {
          const [k, v] = kv.split(":");
          if (k && v && Number.isFinite(Number(v))) obj[k.trim()] = Number(v);
        });
      }
      for (const [k, v] of Object.entries(obj)) {
        bucket.SpendDistribution[k] = (bucket.SpendDistribution[k] || 0) + Number(v || 0);
      }
    }

    bucket.SampleSize++;
  }

  const by_industry = {};
  let dominant = null, bestN = -1;
  for (const [ind, b] of agg.entries()) {
    const pack = {
      TopBlockers: Array.from(b.TopBlockers),
      TopPurchases: Array.from(b.TopPurchases),
      TopNeedsSupplier: Array.from(b.TopNeedsSupplier),
      SpendDistribution: b.SpendDistribution,
      SampleSize: b.SampleSize
    };
    by_industry[ind] = pack;
    if (b.SampleSize > bestN) { bestN = b.SampleSize; dominant = ind; }
  }

  return {
    industries: Object.keys(by_industry),
    dominant_industry: dominant,
    by_industry
  };
}

function topByFrequency(arr, limit = 8) {
  const freq = new Map();
  for (const s of Array.isArray(arr) ? arr : []) {
    const t = String(s || "").trim();
    if (!t) continue;
    freq.set(t, (freq.get(t) || 0) + 1);
  }
  return [...freq.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([k]) => k)
    .slice(0, limit);
}

function collectProductNames(productsObj, siteJson, usps) {
  const out = new Set();

  // products.json (v7.1 stores { products: [...] })
  if (Array.isArray(productsObj?.products)) {
    for (const p of productsObj.products) {
      const name = typeof p === "string" ? p : (p?.name || p?.title || "");
      if (name && name.trim()) out.add(name.trim());
    }
  }

  // site.json (array, legacy compatible) — harvest obvious headings/titles from page 0 snippet
  const page0 = Array.isArray(siteJson) ? siteJson[0] : null;
  const html = page0?.snippet || "";
  if (html) {
    const m = html.match(/<(h1|h2|h3|li)[^>]*>([^<]{3,120})</gi) || [];
    for (const tag of m) {
      const t = tag.replace(/<[^>]+>/g, "").trim();
      if (t && !/^(home|about|contact|support|blog|learn|login|privacy|terms)$/i.test(t)) out.add(t);
    }
  }

  // supplier_usps hint at capabilities
  if (Array.isArray(usps)) {
    for (const u of usps) if (u && u.trim()) out.add(u.trim());
  }

  return Array.from(out);
}

function matchNeedToCapabilities(need, productNames, usps) {
  const tokens = String(need || "").toLowerCase().split(/\W+/).filter(Boolean);
  const hits = [];

  for (const name of productNames) {
    const nlow = name.toLowerCase();
    if (tokens.some(tok => nlow.includes(tok))) hits.push({ type: "product", name });
  }
  for (const u of (usps || [])) {
    const ulow = String(u || "").toLowerCase();
    if (tokens.some(tok => ulow.includes(tok))) hits.push({ type: "usp", name: u });
  }

  let status = "gap";
  if (hits.length >= 2) status = "matched";
  else if (hits.length === 1) status = "partial";

  return { need, status, hits };
}

function summariseCoverage(map) {
  const total = Array.isArray(map) ? map.length : 0;
  const matched = map.filter(x => x.status === "matched").length;
  const partial = map.filter(x => x.status === "partial").length;
  const gap = map.filter(x => x.status === "gap").length;
  const coverage = total ? Math.round(((matched + 0.5 * partial) / total) * 100) : 0;
  return { total, matched, partial, gap, coverage };
}

function resolveIndustry({ userIndustry, csvIndustry, csvHasMultipleSectors }) {
  const u = (userIndustry || "").trim().toLowerCase();
  if (u) return u;
  const c = (csvIndustry || "").trim().toLowerCase();
  if (c && !csvHasMultipleSectors) return c;
  return null; // agnostic
}

// --- Product extraction (lightweight heuristic, homepage) ---
try {
  const ldMatches = Array.from(String(html || "").matchAll(
    /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  ));
  for (const m of ldMatches) {
    try {
      const node = JSON.parse(m[1]);
      const nodes = Array.isArray(node) ? node : [node];
      for (const n of nodes) {
        if (n && (n['@type'] === 'Product' || (Array.isArray(n['@type']) && n['@type'].includes('Product')))) {
          const title = String(n.name || n.alternateName || '').trim();
          const summary = String(n.description || '').trim();
          if (title || summary) {
            products.add(JSON.stringify({
              topic: 'product_detail',
              title: title || 'Product',
              summary: summary || 'Product (JSON-LD)',
              weight: 2
            }));
          }
        }
      }
    } catch { /* ignore bad JSON */ }
  }
} catch { /* no-op */ }
async function extractProductsFromSite(html) {
  if (!html || typeof html !== "string") return [];
  const lines = html.split(/\r?\n/).slice(0, 2000); // safety cap
  const out = new Set();

  for (const line of lines) {
    if (/(<h1|<h2|<h3|<li|product|solutions|services)/i.test(line)) {
      const m = line.match(/>([^<>]{3,120})</);
      if (m) {
        const val = m[1].trim();
        if (
          val &&
          !/^(home|about|contact|login|support|learn|blog|cookie|privacy|terms|partners|resources)$/i.test(val) &&
          !/^(read more|learn more)$/i.test(val)
        ) {
          out.add(val);
        }
      }
    }
  }
  return Array.from(out);
}

// ---------- Simple link extraction & crawl ----------
function extractLinks(html, baseUrl) {
  if (!html || !baseUrl) return [];
  const hrefs = new Set();
  const re = /href\s*=\s*"(.*?)"/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const abs = toAbsUrl(baseUrl, m[1]);
    if (abs) hrefs.add(toHttps(abs.split("#")[0]));
  }
  return Array.from(hrefs);
}

function sameHost(a, b) {
  try { return new URL(a).hostname === new URL(b).hostname; } catch { return false; }
}

function isCaseStudyUrl(u) {
  const low = String(u || "").toLowerCase();
  return (
    low.includes("case-study") ||
    low.includes("case-studies") ||
    low.includes("/customers/") ||
    low.includes("/customer/") ||
    low.includes("/success/") ||
    low.includes("/stories/") ||
    low.endsWith(".pdf") ||
    /\b(success|story|customer|deployment|implementation|results|case)\b/.test(low)
  );
}
// Product URL heuristic (same host; obvious product/solution/service paths)
// PATCH PRD-URL-1: include deeper product/solution paths
// Matches: /products/, /product/, /solutions/, and deep SKUs: /solutions/<category>/<sku>
const DEEP_PRODUCT_RE = /\/(products?|solutions?)\/[^\/]+\/[^\/]+/i;

// Wrap original helper to include deep patterns (idempotent)
const _isProductUrl = typeof isProductUrl === "function" ? isProductUrl : (u) => false;
function isProductUrl(u) {
  try {
    if (DEEP_PRODUCT_RE.test(u)) return true;
    return _isProductUrl(u);
  } catch { return _isProductUrl(u); }
}
function isProductUrl(u) {
  const low = String(u || "").toLowerCase();
  return /product|products|solution|solutions|service|services|continuum|constellation|sd-?one|bond(ed|ing)|starlink/.test(low);
}

// Parse product/solution claims (H1/H2/LI → short bullet texts)
function parseProductClaims(html) {
  if (!html || typeof html !== "string") return [];
  const claims = [];

  const h1 = (html.match(/<h1[^>]*>(.*?)<\/h1>/i) || [, ""])[1];
  if (h1) claims.push(h1.replace(/<[^>]+>/g, "").trim());

  const h2s = [...html.matchAll(/<h2[^>]*>(.*?)<\/h2>/gim)].map(m => (m[1] || "").replace(/<[^>]+>/g, "").trim());
  for (const h of h2s) if (h) claims.push(h);

  const lis = [...html.matchAll(/<li[^>]*>(.*?)<\/li>/gim)].map(m => (m[1] || "").replace(/<[^>]+>/g, "").trim());
  for (const li of lis) if (li && li.length <= 200) claims.push(li);

  // De-dupe + trim + cap
  const seen = new Set();
  const out = [];
  for (const c of claims) {
    const t = (c || "").replace(/\s+/g, " ").trim();
    if (!t) continue;
    const key = t.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(t);
    if (out.length >= 12) break; // safety cap per page
  }
  return out;
}

async function collectCaseStudyLinks(pages, allLinks, rootHost) {
  const candidates = new Set();
  for (const p of pages) {
    if (isCaseStudyUrl(p.url)) candidates.add(p.url);
    const pageLinks = extractLinks(p.snippet || "", p.url);
    for (const l of pageLinks) {
      if (sameHost(l, `https://${rootHost}`) && isCaseStudyUrl(l)) candidates.add(l);
    }
  }
  for (const l of allLinks) {
    if (sameHost(l, `https://${rootHost}`) && isCaseStudyUrl(l)) candidates.add(l);
  }
  return Array.from(candidates).slice(0, MAX_CASESTUDIES);
}

async function fetchCaseStudySummaries(urls) {
  const out = [];
  for (const raw of urls) {
    const url = toHttps(raw);
    const isPdf = url.toLowerCase().endsWith(".pdf");
    if (isPdf) {
      out.push({
        url,
        host: hostnameOf(url),
        fetchedAt: nowIso(),
        type: "pdf",
        title: decodeURIComponent(url.split("/").pop() || "PDF"),
        quote: "PDF referenced; text not extracted in this phase."
      });
      continue;
    }
    const res = await httpGet(url);
    if (!res.ok) continue;
    const html = (res.text || "").slice(0, 120_000);
    const titleMatch = html.match(/<title[^>]*>([^<]{3,200})<\/title>/i);
    const title = titleMatch ? titleMatch[1].trim() : (url.split("/").slice(-1)[0] || "Case study");
    const bodySnippet = html.replace(/\s+/g, " ").slice(0, 500);
    out.push({
      url,
      host: hostnameOf(url),
      fetchedAt: nowIso(),
      type: "html",
      title,
      quote: bodySnippet
    });
  }
  return out;
}

// ---------- Evidence log composer ----------

// Sequential CLM-### generator (001..999)
function makeClaimIdFactory() {
  let n = 0;
  return function nextId() {
    n = Math.min(999, n + 1);
    return `CLM-${String(n).padStart(3, "0")}`;
  };
}
const nextClaimId = makeClaimIdFactory();

function pushIfRoom(arr, item, cap = MAX_EVIDENCE_ITEMS) {
  if (arr.length < cap) arr.push(item);
}
function safePushIfRoom(arr, item, cap) {
  if (!item) return;
  if (isPlaceholderEvidence(item)) return;    // central guard
  pushIfRoom(arr, item, cap);
}

// --- Industry sources loader (deterministic evidence seeds) ---
function loadIndustrySources(industryRaw) {
  const industry = String(industryRaw || "").toLowerCase().replace(/\s+/g, "-");
  const baseDir = path.join(__dirname, "..", "packs", "industry-sources");
  const generalPath = path.join(baseDir, "sources.md");
  const sectorPath = path.join(baseDir, `${industry}.md`);
  const general = fs.existsSync(generalPath) ? fs.readFileSync(generalPath, "utf8") : "";
  const sector = fs.existsSync(sectorPath) ? fs.readFileSync(sectorPath, "utf8") : "";
  return { general, sector };
}

// Parse markdown bullets like: "- Title : https://url"
function mdToSourceItems(md) {
  const items = [];
  const lines = String(md || "").split(/\r?\n/);
  for (const line of lines) {
    const m = line.match(/^\s*-\s*([^:]+?)\s*:\s*(https?:\/\/\S+)/i); // "Title : URL"
    if (!m) continue;
    const title = m[1].trim();
    const url = m[2].trim();
    try {
      const u = new URL(toHttps(url));
      if (u.href.includes("...")) continue; // skip obviously truncated placeholders
      items.push({ title, url: u.href });
    } catch { /* skip invalid url */ }
  }
  return items;
}

// Reject invented placeholders and junk (stronger guard)
// Reject invented placeholders and junk (stronger guard)
// NOTE: Allow "Customer profile" items without URLs (internal doc, not external link).
function isPlaceholderEvidence(it) {
  if (!it) return true;

  const src = String(it.source_type || "").trim();
  const url = (it.url || "").trim();
  const title = (it.title || "").trim();
  const summary = (it.summary || "").trim();

  // 1) No substance at all
  if (!title && !summary && !url) return true;

  // --- Special allowance: Customer profile items may omit URL ---
  if (src.toLowerCase() === "customer profile") {
    // Require at least some meaningful text
    return !(title || summary);
  }

  // 2) Weak scaffold titles
  if (/^(?:products|product|placeholder|lorem ipsum)$/i.test(title)) return true;

  // 3) URL hygiene for everything else: must be https and non-junk hostnames
  if (!/^https:\/\//i.test(url)) return true;
  const u = url.toLowerCase();
  if (u.startsWith("about:")) return true;
  if (u.includes("example.com")) return true;
  if (/\b(companywebsite\.com|companyx\.com|competitor\d+\.com|vendora\.com|vendorb\.com|vendorc\.com|vendord\.com|vendore\.com|test\.(com|net)|localhost|127\.0\.0\.1)\b/i.test(u)) {
    return true;
  }

  return false;
}


function dedupeEvidence(list) {
  const seen = new Set();
  const out = [];
  for (const it of list) {
    const key = `${(it.title || "").toLowerCase()}|${(it.url || "").toLowerCase()}|${(it.source_type || "")}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(it);
  }
  return out;
}

function addCitation(text, tag) {
  const t = String(text || "").trim();
  if (!t) return `(${tag})`;
  return /\([^()]{2,}\)\.?$/.test(t) ? t : `${t} (${tag})`;
}

function classifySourceType(url) {
  const u = String(url || "").toLowerCase();
  if (u.includes("linkedin.com")) return "LinkedIn";
  if (u.includes("ofcom")) return "Ofcom";
  if (u.includes("ons.gov")) return "ONS";
  if (u.includes("dsit.gov") || (u.includes("gov.uk") && u.includes("dsit"))) return "DSIT";
  if (u.endsWith(".pdf")) return "PDF extract";
  if (u.includes("google.")) return "Directory";
  return "Company site";
}

// -------- Markdown profile parsing (no dependencies) --------

// Extract footnote link refs: [1]: https://...
function extractMdRefs(md) {
  const refs = {};
  const rx = /^\s*\[(\d+)\]\s*:\s*(https?:\/\/\S+)\s*.*$/gim;
  let m;
  while ((m = rx.exec(md))) refs[m[1]] = m[2];
  return refs;
}

function mdSection(md, titlePattern) {
  // Accept a literal string or a simple regex-like pattern
  // We anchor to H2 lines that match: ^## <pattern>$
  const rx = new RegExp("^\\s*##\\s+" + titlePattern + "\\s*$", "im");
  const m = rx.exec(md);
  if (!m) return "";
  const start = m.index + m[0].length;
  const rest = md.slice(start);
  const nx = /^\s*##\s+/im.exec(rest);
  return nx ? rest.slice(0, nx.index) : rest;
}

// Pull first bold **...** phrase from a line
function firstBold(line) {
  const m = /\*\*(.+?)\*\*/.exec(line);
  return m ? m[1].trim() : "";
}

// Normalise a bullet line (strip "*", "-", "•")
function cleanBullet(line) {
  return String(line || "").replace(/^\s*[*\-•]\s*/, "").trim();
}

// Split section text into bullet lines
function bullets(sectionText) {
  return sectionText.split(/\r?\n/)
    .map(s => s.trim())
    .filter(s => /^(\*|\-|\u2022)\s+/.test(s))
    .map(cleanBullet);
}

// Find first non-empty paragraph in a section (ignores headings and hr)
function firstParagraph(sectionText) {
  const blocks = sectionText.split(/\n{2,}/).map(s => s.trim());
  for (const b of blocks) {
    if (!b || /^(\*|\-|\u2022)\s/.test(b) || /^#{1,6}\s/.test(b) || /^---+$/.test(b)) continue;
    return b;
  }
  return "";
}

// Kebab slug for packs/<slug>
function toSlug(name) {
  return String(name || "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

function csvSummaryEvidence(csvCanonical, input, prefix, containerUrl, focusInsight, industry) {
  const rows = Number(csvCanonical?.meta?.rows || input?.rowCount || 0);
  const fileName = csvCanonical?.meta?.source || input?.csvFilename || "inline";
  const root = String(containerUrl).replace(/\/+$/, "");
  const pfx = String(prefix).replace(/^\/+/, "");
  const cleanName = String(fileName || "").replace(/^\/+/, "");
  const csvUrl = (cleanName && cleanName !== "inline")
    ? `${root}/${pfx}${cleanName}`
    : `${root}/${pfx}csv_normalized.json`;

  // If we have a credible focus subset, print the full segment insight
  const f = focusInsight || {};
  const indPrefix = (industry && String(industry).trim()) ? `${industry} ` : "";

  let summaryLine = "";
  if (Number.isFinite(f.totalRows) && f.totalRows > 0 && f.focusLabel && Number.isFinite(f.focusCount)) {
    summaryLine =
      `Campaign addressable market: there are ${f.totalRows.toLocaleString()} ${indPrefix}companies in your campaign cohort. ` +
      `${f.focusCount.toLocaleString()} of them plan to purchase ${f.focusLabel}.`;
  } else if (rows > 0) {
    // fallback: credible total only
    let focusNote = "";
    if (rows >= 180 && rows <= 320) focusNote = " Good focus range for a single campaign (≈200–300).";
    else if (rows > 320) focusNote = " Large set; consider segmenting into waves for focus.";
    else focusNote = " Very narrow set; consider broadening if volume is required.";
    summaryLine = `Addressable market: ${rows} companies in ${fileName}.${focusNote}`;
  } else {
    summaryLine = "Addressable market: to be populated from the uploaded CSV.";
  }

  return {
    claim_id: nextClaimId(),
    source_type: "CSV",
    title: "CSV population",
    url: csvUrl,
    summary: addCitation(summaryLine.trim(), "CSV"),
    quote: "CSV-derived population and segment insight."
  };
}

function csvSignalsEvidence(csvCanonical, prefix, containerUrl) {
  if (!csvCanonical || typeof csvCanonical !== "object") return null;

  const sig = csvCanonical?.signals || {};
  const gsig = csvCanonical?.global_signals || {};
  const selInd = csvCanonical?.selected_industry || null;
  const rowCount = Number(csvCanonical?.meta?.rows || 0);

  const topBlockers = (Array.isArray(sig.top_blockers) && sig.top_blockers.length ? sig.top_blockers : gsig.top_blockers) || [];
  const topNeeds = (Array.isArray(sig.top_needs_supplier) && sig.top_needs_supplier.length ? sig.top_needs_supplier : gsig.top_needs_supplier) || [];
  const topPurch = (Array.isArray(sig.top_purchases) && sig.top_purchases.length ? sig.top_purchases : gsig.top_purchases) || [];

  const parts = [];
  if (selInd) parts.push(`Selected industry: ${selInd}`);
  if (rowCount > 0) parts.push(`Row count: ${rowCount}`);
  if (topBlockers.length) parts.push(`Top blockers: ${topBlockers.slice(0, 8).join("; ")}`);
  if (topNeeds.length) parts.push(`Top needs from supplier: ${topNeeds.slice(0, 8).join("; ")}`);
  if (topPurch.length) parts.push(`Top intended purchases: ${topPurch.slice(0, 8).join("; ")}`);

  if (!parts.length) return null;

  const csvUrl = `${String(containerUrl).replace(/\/+$/, "")}/${String(prefix).replace(/^\/+/, "")}csv_normalized.json`;

  return {
    claim_id: nextClaimId(),
    source_type: "CSV",
    title: "CSV signals (buyer problems & intent)",
    url: csvUrl,
    summary: addCitation(parts.join(" — "), "CSV"),
    quote: ""
  };
}

function supplierIdentityEvidence(company, website, fallbackUrl) {
  const host = hostnameOf(website || "");
  const name = (company || "").trim();
  if (!name && !host) return null;
  const summary = [name ? `Supplier: ${name}` : null, host ? `Website host: ${host}` : null]
    .filter(Boolean).join(" — ");
  return {
    claim_id: nextClaimId(),
    source_type: "Company site",
    title: "Supplier identity",
    url: toHttps(website || fallbackUrl || "https://inside-track.local/site"),
    summary: addCitation(summary, "Company site"),
    quote: ""
  };
}

function siteProductsEvidence(products, website, containerUrl, prefix) {
  if (!Array.isArray(products) || !products.length) return null;
  const base = website ? toHttps(website).replace(/\/+$/, "") : null;
  const items = products.slice(0, 12).map(p => {
    const name = (typeof p === "string" ? p : (p?.name || p?.title || "")).trim();
    if (!name) return null;
    return { name };
  }).filter(Boolean);
  if (!items.length) return null;
  return {
    claim_id: nextClaimId(),
    source_type: "Company site",
    title: "Products (homepage-derived)",
    url: base || (containerUrl && prefix ? `${containerUrl.replace(/\/+$/, "")}/${prefix}products.json` : undefined),
    summary: addCitation(items.map(i => i.name).slice(0, 8).join(", "), "Company site"),
    quote: ""
  };
}

function linkedinEvidence(linkedin) {
  if (!linkedin) return null;
  return {
    claim_id: nextClaimId(),
    source_type: "LinkedIn",
    title: "Supplier LinkedIn (reference)",
    url: toHttps(linkedin),
    summary: addCitation("Company LinkedIn profile reference for employer facts and recent posts.", "LinkedIn"),
    quote: ""
  };
}

// --- LinkedIn relevance helpers (search URLs; no scraping) ---
function liCompanySearch(company) {
  if (!company) return null;
  const q = encodeURIComponent(company);
  return `https://www.linkedin.com/search/results/content/?keywords=${q}&origin=GLOBAL_SEARCH_HEADER`;
}
function liProductSearch(name, company) {
  const q = encodeURIComponent([company || "", name || ""].filter(Boolean).join(" "));
  return `https://www.linkedin.com/search/results/content/?keywords=${q}&origin=GLOBAL_SEARCH_HEADER`;
}
function liCompetitorSearch(name) {
  if (!name) return null;
  const q = encodeURIComponent(name);
  return `https://www.linkedin.com/search/results/content/?keywords=${q}&origin=GLOBAL_SEARCH_HEADER`;
}
function linkedinSearchEvidence({ url, title }) {
  if (!url) return null;
  return {
    claim_id: nextClaimId(),
    source_type: "LinkedIn",
    title,
    url: toHttps(url),
    summary: addCitation(`${title} — relevance scan link.`, "LinkedIn"),
    quote: ""
  };
}

function summarizeClaims(all = []) {
  const counts = {
    website: 0,
    linkedin: 0,
    pdf: 0,
    directories: 0,
    ixbrl: 0,
    csv: 0
  };
  for (const it of all) {
    const t = String(it?.source_type || "").toLowerCase();
    if (t.includes("site") || t.includes("website") || t.includes("company site")) counts.website++;
    else if (t.includes("linkedin")) counts.linkedin++;
    else if (t.includes("pdf")) counts.pdf++;
    else if (t.includes("directory")) counts.directories++;
    else if (t.includes("ixbrl")) counts.ixbrl++;
    else if (t.includes("csv")) counts.csv++;
  }
  return counts;
}


module.exports = async function (context, job) {
  const container = getContainerClient();

  // ---- Normalise incoming queue payload (object or string) ----
  const msg = (typeof job === "string")
    ? (() => { try { return JSON.parse(job); } catch { return {}; } })()
    : (job || {});

  // Canonical fields
  const runId = (msg.runId && String(msg.runId)) || (crypto.randomUUID ? crypto.randomUUID() : String(Date.now()));
  const input = msg.input || msg.inputs || msg || {};
  // Prefix: trust message.prefix if present (from /campaign-start), otherwise fallback
  try {
    const persisted = await getJson(container, `${prefix}input.json`);
    if (persisted && typeof persisted === "object") {
      // Only backfill if the earlier attempt failed or fields are empty
      const keys = ["csvSummary", "csvText", "selected_industry", "buyer_industry", "campaign_industry", "relevant_competitors", "competitors", "supplier_website", "company_website", "prospect_website"];
      for (const k of keys) {
        if (input[k] == null || (typeof input[k] === "string" && input[k].trim() === "")) {
          input[k] = persisted[k] ?? input[k];
        }
      }
      // Prefer persisted csvSummary/csvText if queue message was slimmed
      if (!input.csvSummary && persisted.csvSummary) input.csvSummary = persisted.csvSummary;
      if (!input.csvText && persisted.csvText) input.csvText = persisted.csvText;
    }
  } catch { /* non-fatal; continue */ }
  let prefix = (msg.prefix && String(msg.prefix).trim()) || `runs/${runId}/`;
  // Normalise to container-relative (strip container name, strip leading slash, ensure trailing slash)
  if (prefix.startsWith(`${RESULTS_CONTAINER}/`)) prefix = prefix.slice(`${RESULTS_CONTAINER}/`.length);
  if (prefix.startsWith("/")) prefix = prefix.replace(/^\/+/, "");
  if (!prefix.endsWith("/")) prefix = `${prefix}/`;

  // ---- Enter EvidenceDigest  ----
  // PATCH EV-PROFILE-1: robust profile loader (supplier + competitors), produces structured, high-value claims
  async function downloadTextIfExists(relPath) {
    try {
      const blob = container.getBlockBlobClient(relPath);
      if (!(await blob.exists())) return null;
      const buf = await blob.downloadToBuffer();
      return buf.toString("utf8");
    } catch { return null; }
  }

  function parseProfileMarkdown(md) {
    // Tiny, deterministic parser: map top-level headings into topics; produce compact claims
    const lines = String(md || "").split(/\r?\n/);
    const claims = [];
    let section = "profile";
    const push = (title, summary, topic) => claims.push({
      source_type: "profile",
      title, summary, topic, weight: 3
    });
    for (const raw of lines) {
      const s = raw.trim();
      if (/^#{1,3}\s+/.test(s)) {
        const h = s.replace(/^#{1,3}\s+/, "").trim().toLowerCase();
        if (/(proposition|value|positioning)/i.test(h)) section = "proposition";
        else if (/(offers|products|solutions)/i.test(h)) section = "offers";
        else if (/(segments|verticals|industr(y|ies)|use cases?)/i.test(h)) section = "verticals";
        else if (/(proof|evidence|case studies|outcomes)/i.test(h)) section = "proof_points";
        else if (/(routes|go to market|sales|partners?)/i.test(h)) section = "routes_to_market";
        else section = "profile";
      } else if (s) {
        push("Profile insight", s, section);
      }
    }
    return claims;
  }

  async function loadSupplierProfileClaims() {
    const md = await downloadTextIfExists(`${prefix}profile.md`);
    return md ? parseProfileMarkdown(md) : [];
  }

  async function loadCompetitorProfileClaims(names = []) {
    const out = [];
    for (const n of (Array.isArray(names) ? names : [])) {
      const slug = String(n).trim().toLowerCase().replace(/\s+/g, "-");
      // Common locations; only push if found
      const rel = `${prefix}profiles/${slug}/profile.md`;
      const md = await downloadTextIfExists(rel);
      if (md) {
        const claims = parseProfileMarkdown(md).map(c => ({ ...c, vendor: n, source_type: "profile_competitor" }));
        out.push(...claims);
      }
    }
    return out;
  }

  // Supplier profile (high priority)
  try {
    const profClaims = await loadSupplierProfileClaims();
    for (const c of profClaims) safePushIfRoom(evidenceLog, c, MAX_EVIDENCE_ITEMS);
  } catch { /* non-fatal */ }

  // Competitor profiles (if user named competitors)
  const userComps = Array.isArray(input?.relevant_competitors) ? input.relevant_competitors
    : (Array.isArray(input?.competitors) ? input.competitors : []);
  try {
    const compClaims = await loadCompetitorProfileClaims(userComps);
    for (const c of compClaims) safePushIfRoom(evidenceLog, c, MAX_EVIDENCE_ITEMS);
  } catch { /* non-fatal */ }
  await updateStatus(
    container,
    prefix,
    {
      runId,
      state: "EvidenceDigest",
      input: {
        page: input.page || null,
        rowCount: Number(input.rowCount || 0),
        supplier_company: input.supplier_company || input.company_name || input.prospect_company || null,
        supplier_website: input.supplier_website || input.company_website || input.prospect_website || null,
        supplier_linkedin: input.supplier_linkedin || input.company_linkedin || input.prospect_linkedin || null,
        supplier_usps: Array.isArray(input.supplier_usps)
          ? input.supplier_usps
          : (input.supplier_usps
            ? String(input.supplier_usps).split(/[;,\n]/).map(s => s.trim()).filter(Boolean)
            : []),
        selected_industry:
          (input.selected_industry || input.campaign_industry || input.company_industry || input.industry || "")
            .trim().toLowerCase() || null
      },
      updatedAt: nowIso()
    },
    { phase: "EvidenceDigest", note: "start" }
  );

  try {
    const supplierWebsiteRaw = (input.supplier_website || input.company_website || input.prospect_website || "").trim();
    const supplierLinkedInRaw = (input.supplier_linkedin || input.company_linkedin || input.prospect_linkedin || "").trim();
    const supplierWebsite = toHttps(supplierWebsiteRaw);
    const supplierLinkedIn = supplierLinkedInRaw ? toHttps(supplierLinkedInRaw) : "";
    const supplierCompany = (input.supplier_company || input.company_name || input.prospect_company || "").trim();

    // 1) WEBSITE: crawl small site graph (homepage + few internal pages)
    let siteGraph = { pages: [], links: [] };
    if (supplierWebsite) {
      try {
        siteGraph = await crawlSiteGraph(supplierWebsite, MAX_SITE_PAGES, FETCH_TIMEOUT_MS);
      } catch (e) {
        context.log.warn("[campaign-evidence] crawlSiteGraph failed", String(e?.message || e));
      }
    }

    // Store homepage snapshot array-shaped (prev format compatibility)
    const siteJson = siteGraph.pages.length ? [siteGraph.pages[0]] : [];
    await putJson(container, `${prefix}site.json`, siteJson);
    // PATCH EV-PAGES-1: derive product-page claims from crawled HTML
    let productPageEvidence = [];
    try {
      const pages = Array.isArray(siteGraph?.pages) ? siteGraph.pages : [];
      for (const p of pages) {
        const html = String(p?.snippet || "");
        const claims = parseProductClaims(html); // uses your helper
        for (const c of claims.slice(0, 12)) {
          productPageEvidence.push({
            claim_id: nextClaimId(),
            source_type: "Company site",
            title: c.length > 120 ? `${c.slice(0, 117)}…` : c,
            url: toHttps(p?.url || (input?.supplier_website || "")),
            summary: addCitation(c, "Company site"),
            quote: ""
          });
        }
      }
    } catch (e) {
      context.log.warn("[campaign-evidence] productPageEvidence build skipped", String(e?.message || e));
    }

    // 1a-USER) Seed products from user input (string or array), if provided
    try {
      const userProducts =
        Array.isArray(input?.supplier_products)
          ? input.supplier_products
          : (typeof input?.supplier_products === "string"
            ? input.supplier_products.split(/[;,\n]/).map(s => s.trim()).filter(Boolean)
            : []);

      if (userProducts.length) {
        const uniq = Array.from(new Set(userProducts.map(s => String(s).trim()).filter(Boolean))).slice(0, 12);
        if (uniq.length) {
          await putJson(container, `${prefix}products.json`, { products: uniq });
        }
      }
    } catch { /* best-effort; continue */ }

    // ---- Products: Declared → Observed → Validated → Chosen (init) ----
    const productsMeta = { declared: [], observed: [], validated: [], chosen: [], notes: Object.create(null) };

    // Declared products (user-specified): array OR a single string (split on ; , or newline)
    try {
      const declared =
        Array.isArray(input?.supplier_products)
          ? input.supplier_products
          : (typeof input?.supplier_products === "string"
            ? input.supplier_products.split(/[;,\n]/).map(s => s.trim()).filter(Boolean)
            : []);

      productsMeta.declared = Array.from(new Set(declared.map(s => String(s).trim()).filter(Boolean))).slice(0, 24);
    } catch { /* best-effort */ }

    // ---- 1a) Observed products (site & profile) ----
    try {
      // Gather candidates from: homepage snippet extractor, site graph titles/snippets/nav, and profile.md sections
      const observed = new Set();

      // A) Homepage snippet extractor (your existing util)
      // ==== Build fused problem signals (safe timing: after site crawl) ====
      const declared = Array.isArray(productsMeta.declared) ? productsMeta.declared : [];
      const site_home_text =
        (siteGraph && Array.isArray(siteGraph.pages) && siteGraph.pages[0] && siteGraph.pages[0].snippet)
          ? siteGraph.pages[0].snippet
          : "";
      const site_titles =
        (siteGraph && Array.isArray(siteGraph.pages))
          ? siteGraph.pages.map(p => (p && p.title) ? String(p.title) : "").filter(Boolean)
          : [];

      const problemSignals = buildProblemSignals({
        input,
        packs: {}, // packs are integrated later; not needed for initial validation
        site: { home_text: site_home_text, titles: site_titles }
      });
      try {
        const html = siteGraph?.pages?.[0]?.snippet || "";
        if (html) {
          const names = await extractProductsFromSite(html);
          for (const n of (names || [])) if (n) observed.add(String(n).trim().replace(/\.$/, ""));
        }
      } catch (e) {
        context.log.warn("[campaign-evidence] observed: homepage extractor skipped", String(e?.message || e));
      }

      // ==== BEGIN fused validation (Declared ∪ Observed against problem signals) ====
      // Use the productsMeta.declared (always in-scope) and the local observed Set
      const declaredList = Array.isArray(productsMeta.declared) ? productsMeta.declared : [];
      const observedList = Array.isArray(observed) ? observed : Array.from(observed);

      // Build candidate list
      const PRODUCT_CANDIDATES = [...new Set(
        [...declaredList, ...observedList].map(x => String(x).trim()).filter(Boolean)
      )];

      const validated = [];
      const diag = [];

      for (const name of PRODUCT_CANDIDATES) {
        const { score, matches } = scoreProductAgainstSignals(name, problemSignals);
        // Light threshold to allow UK naming variance; declared items always pass
        const pass = score >= 0.18 || declaredList.includes(name);
        diag.push({ name, score: Number(score.toFixed(3)), pass, matches });
        if (pass) validated.push({ name, score, matches });
      }

      // Choose top-scoring validated products; fall back to declared for continuity
      let chosen = validated
        .sort((a, b) => b.score - a.score)
        .map(v => v.name)
        .slice(0, 12);

      if (chosen.length === 0 && declaredList.length) {
        chosen = [...new Set(declaredList.map(s => String(s).trim()).filter(Boolean))].slice(0, 6);
        // note: don't touch 'evidence' (not defined); stamp later in productsMeta.notes
      }

      // Persist diagnostics now (use putJson; writeJson/runPath don't exist here)
      await putJson(container, `${prefix}products_meta.json`, {
        declared: declaredList,
        observed: observedList,
        validated: validated.map(v => ({ name: v.name, score: Number(v.score.toFixed(3)), matches: v.matches })),
        chosen,
        problem_signals_sample: Array.isArray(problemSignals) ? problemSignals.slice(0, 25) : []
      });
      // ==== END fused validation ====

      // B) Site graph titles/snippets/nav (light heuristic: short name-like tokens)
      try {
        const bins = [];
        const pages = Array.isArray(siteGraph?.pages) ? siteGraph.pages : [];
        for (const p of pages) {
          if (p?.title) bins.push(String(p.title));
          if (p?.snippet) bins.push(String(p.snippet));
        }
        const nav = Array.isArray(siteGraph?.nav) ? siteGraph.nav : [];
        for (const n of nav) if (n?.text) bins.push(String(n.text));

        for (const bin of bins) {
          const parts = String(bin).split(/[\|\u2013\u2014>\-•·\n\r\t]+/);
          for (let t of parts) {
            t = String(t || "").trim();
            if (!t || t.length > 80) continue;
            if (/contact|about|blog|privacy|terms|download|learn more|read more/i.test(t)) continue;
            const candidates = t.match(/\b([A-Z][A-Za-z0-9]+(?:[ -][A-Za-z0-9][A-Za-z0-9\-]+){0,4})\b/g);
            if (candidates) {
              for (const c of candidates) {
                if (/^(Home|Services?|Solutions?|Products?|Resources?)$/i.test(c)) continue;
                observed.add(c.trim().replace(/\s{2,}/g, " ").replace(/\.$/, ""));
              }
            }
          }
        }
      } catch (e) {
        context.log.warn("[campaign-evidence] observed: site graph sweep skipped", String(e?.message || e));
      }

      // C) Profile.md sections: Products|Solutions|Services|Offerings
      try {
        const companyName = (input?.supplier_company || input?.company_name || "").trim();
        const slug = toSlug(companyName);
        if (slug) {
          const md = await getText(container, `${prefix}packs/${slug}/profile.md`);
          if (md) {
            const sec = mdSection(md, "Products|Solutions|Services|Offerings");
            if (sec) {
              const blts = bullets(sec).slice(0, 24);
              for (const b of blts) {
                const s = String(b || "").trim().replace(/\.$/, "");
                if (s) observed.add(s);
              }
            }
          }
        }
      } catch (e) {
        context.log.warn("[campaign-evidence] observed: profile.md sweep skipped", String(e?.message || e));
      }

      productsMeta.observed = Array.from(observed).slice(0, 24);

      // If user already seeded products.json upstream, don't overwrite it.
      const existing = await getJson(container, `${prefix}products.json`);
      const hasSeed = Array.isArray(existing?.products) && existing.products.length > 0;
      if (!hasSeed) {
        await putJson(container, `${prefix}products.json`, { products: productsMeta.observed });
      }
    } catch (e) {
      context.log.warn("[campaign-evidence] observed phase skipped", String(e?.message || e));
    }

    // CSV → csv_normalized.json (canonical)
    let csvText = null;
    let chosenCsvName = null;

    if (typeof input.csvText === "string" && input.csvText.trim()) {
      csvText = input.csvText;
      chosenCsvName = (typeof input.csvFilename === "string" && input.csvFilename.trim()) ? input.csvFilename.trim() : "inline";
    }

    if (!csvText) {
      const csvNames = await listCsvUnderPrefix(container, prefix);
      if (csvNames.length) {
        let chosen = csvNames[0];
        if (input.csvFilename && typeof input.csvFilename === "string") {
          const want = input.csvFilename.trim().toLowerCase();
          const exact = csvNames.find(n => n.toLowerCase().endsWith(want));
          if (exact) chosen = exact;
        }
        csvText = await downloadText(container, chosen);
        chosenCsvName = chosen;
      }
    }

    let csvNormalizedCanonical = {
      selected_industry: null,
      industry_mode: "agnostic",
      signals: { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] },
      global_signals: { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] },
      meta: { rows: 0, source: null, csv_has_multiple_sectors: false }
    };

    if (csvText && csvText.trim()) {
      const rows = parseCsvLoose(csvText);
      csvRowsRaw = rows;

      const focusLabelRaw = String(
        input?.selected_product ??
        input?.campaign_focus ??
        input?.offer ??
        input?.sales_focus ??
        ""
      ).trim();

      (function computeCsvFocus() {
        try {
          const totalRows = Array.isArray(rows) && rows.length > 1 ? (rows.length - 1) : 0;

          let focusCount = null;
          if (focusLabelRaw && totalRows > 0) {
            const header = rows[0].map(h => String(h || "").trim().toLowerCase());
            const intentCols = header
              .map((h, i) => (/\b(top|purchase|intent|plan|priority|buy|evaluate|mobile|connect)\b/.test(h) ? i : -1))
              .filter(i => i >= 0);

            const tokens = focusLabelRaw.toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
            let count = 0;
            for (let r = 1; r < rows.length; r++) {
              const row = rows[r].map(v => String(v || "").toLowerCase());
              const hay = intentCols.length
                ? intentCols.map(i => row[i] || "").join(" ")
                : row.join(" ");
              if (tokens.some(t => hay.includes(t))) count++;
            }
            focusCount = count;
          }

          csvFocusInsight = {
            totalRows: Number.isFinite(totalRows) ? totalRows : 0,
            focusLabel: focusLabelRaw,
            focusCount: Number.isFinite(focusCount) ? focusCount : null
          };
        } catch {
          csvFocusInsight = { totalRows: 0, focusLabel: "", focusCount: null };
        }
      })();

      const agg = normalizeCsv(rows);

      const industries = Array.isArray(agg.industries) ? agg.industries : [];
      const csvHasMultipleSectors = industries.filter(x => x && x !== "Unknown").length > 1;
      const userSelectedIndustry = (input.selected_industry || input.campaign_industry || input.company_industry || input.industry || "").trim().toLowerCase();
      const csvIndustry = (agg.dominant_industry || "").trim().toLowerCase();
      const selectedIndustry = resolveIndustry({
        userIndustry: userSelectedIndustry,
        csvIndustry,
        csvHasMultipleSectors
      });

      const allBlockers = [];
      const allNeeds = [];
      const allPurchases = [];
      let totalRows = 0;

      for (const ind of industries) {
        const pack = agg.by_industry[ind] || {};
        allBlockers.push(...(Array.isArray(pack.TopBlockers) ? pack.TopBlockers : []));
        allNeeds.push(...(Array.isArray(pack.TopNeedsSupplier) ? pack.TopNeedsSupplier : []));
        allPurchases.push(...(Array.isArray(pack.TopPurchases) ? pack.TopPurchases : []));
        totalRows += Number(pack.SampleSize || 0);
      }

      const globalSignals = {
        spend_band: null,
        top_blockers: topByFrequency(allBlockers, 8),
        top_needs_supplier: topByFrequency(allNeeds, 8),
        top_purchases: topByFrequency(allPurchases, 8)
      };

      let industrySignals = { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] };
      if (selectedIndustry) {
        const matchKey = industries.find(x => (x || "").toLowerCase() === selectedIndustry);
        const pack = agg.by_industry[matchKey] || {};
        industrySignals = {
          spend_band: null,
          top_blockers: Array.isArray(pack.TopBlockers) ? pack.TopBlockers : [],
          top_needs_supplier: Array.isArray(pack.TopNeedsSupplier) ? pack.TopNeedsSupplier : [],
          top_purchases: Array.isArray(pack.TopPurchases) ? pack.TopPurchases : []
        };
      }

      csvNormalizedCanonical = {
        selected_industry: selectedIndustry,
        industry_mode: selectedIndustry ? "specific" : "agnostic",
        signals: industrySignals,
        global_signals: globalSignals,
        meta: {
          rows: Math.max(0, totalRows),
          source: chosenCsvName || "inline",
          csv_has_multiple_sectors: !!csvHasMultipleSectors
        }
      };
    }

    await putJson(container, `${prefix}csv_normalized.json`, csvNormalizedCanonical);

    // Validated products (Observed/Declared cross-checked with CSV signals)
    try {
      productsMeta.notes = productsMeta.notes || Object.create(null);

      const sig = (csvNormalizedCanonical && csvNormalizedCanonical.signals) ? csvNormalizedCanonical.signals : {};
      const topPurchases = Array.isArray(sig.top_purchases) ? sig.top_purchases : [];
      const topNeeds = Array.isArray(sig.top_needs_supplier) ? sig.top_needs_supplier
        : (Array.isArray(sig.top_needs) ? sig.top_needs : []);
      const universeRaw = [...topPurchases, ...topNeeds].map(s => String(s || "").toLowerCase().trim()).filter(Boolean);
      const universe = Array.from(new Set(universeRaw));

      const tok = s => String(s || "").toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
      const jacc = (a, b) => {
        const A = new Set(a), B = new Set(b);
        let i = 0; for (const x of A) if (B.has(x)) i++;
        const d = A.size + B.size - i;
        return d ? (i / d) : 0;
      };

      const declared = productsMeta.declared || [];
      const observed = productsMeta.observed || [];

      if (universe.length === 0) {
        productsMeta.validated = Array.from(new Set([...declared, ...observed])).slice(0, 24);
        productsMeta.notes.validation = "no_csv_signals";
      } else {
        const THRESH = 0.18;
        const score = (name) => {
          const t = tok(name);
          let best = 0;
          for (const u of universe) { const s = jacc(t, tok(u)); if (s > best) best = s; }
          return best;
        };
        const vd = declared.map(n => ({ n, s: score(n) })).filter(x => x.s >= THRESH).sort((a, b) => b.s - a.s).map(x => x.n);
        const vo = observed.map(n => ({ n, s: score(n) })).filter(x => x.s >= THRESH).sort((a, b) => b.s - a.s).map(x => x.n);
        const validated = Array.from(new Set([...vd, ...vo])).slice(0, 24);

        productsMeta.validated = validated;
        productsMeta.notes.validation = validated.length ? "csv_signals_matched" : "csv_signals_present_but_no_match";
      }
    } catch (e) {
      context.log.warn("[campaign-evidence] validated phase skipped", String(e?.message || e));
      productsMeta.validated = Array.from(new Set([...(productsMeta.declared || []), ...(productsMeta.observed || [])])).slice(0, 24);
      productsMeta.notes = productsMeta.notes || {};
      productsMeta.notes.validation = "fallback_on_error";
    }

    // Chosen products (deterministic, buyer-led)
    try {
      const declared = productsMeta.declared || [];
      const observed = productsMeta.observed || [];
      const validated = productsMeta.validated || [];

      const dedupe = (arr) => Array.from(new Set(arr.map(s => String(s).trim()).filter(Boolean)));

      let chosen = dedupe([
        ...declared.filter(x => validated.includes(x)),
        ...observed.filter(x => validated.includes(x))
      ]).slice(0, 12);

      if (chosen.length === 0 && declared.length > 0) {
        chosen = dedupe(declared).slice(0, 12);
        productsMeta.notes.reason = "no_validation_hit; using declared for continuity";
      }
      productsMeta.notes = productsMeta.notes || {};

      if (!Array.isArray(chosen)) chosen = [];

      if (chosen.length === 0) {
        const declaredArr = Array.isArray(productsMeta.declared)
          ? productsMeta.declared.map(s => String(s).trim()).filter(Boolean)
          : [];

        if (declaredArr.length) {
          chosen = [...new Set(declaredArr)].slice(0, 6);
          productsMeta.notes.validation = "fallback_declared_no_validated";
        } else {
          chosen = [];
          productsMeta.notes.validation = "no_candidates";
        }
      }

      productsMeta.chosen = chosen;
      productsMeta.notes.capability_map_used = false;
      productsMeta.notes.csv_sources = ["top_purchases", "top_needs_supplier|top_needs"];
      productsMeta.notes.counts = {
        declared: (productsMeta.declared || []).length,
        observed: (productsMeta.observed || []).length,
        validated: (productsMeta.validated || []).length,
        chosen: chosen.length
      };

      await putJson(container, `${prefix}products.json`, { products: chosen });
      await putJson(container, `${prefix}products_meta.json`, productsMeta);
    } catch (e) {
      context.log.error("[campaign-evidence] choosing products failed", String(e?.message || e));
      throw e;
    }

    // LINKEDIN (reference only)
    const li = supplierLinkedIn
      ? [{
        url: supplierLinkedIn,
        host: hostnameOf(supplierLinkedIn),
        fetchedAt: nowIso(),
        type: "link",
        note: "LinkedIn metadata not scraped; stored as reference only."
      }]
      : [];
    await putJson(container, `${prefix}linkedin.json`, li);

    // Case study discovery (HTML & PDF on same host)
    let pdfExtracts = [];
    if (siteGraph.pages.length) {
      try {
        const host = siteGraph.pages[0].host || hostnameOf(supplierWebsite);
        const candidateLinks = await collectCaseStudyLinks(siteGraph.pages, siteGraph.links, host);
        pdfExtracts = await fetchCaseStudySummaries(candidateLinks);
      } catch (e) {
        context.log.warn("[campaign-evidence] case study discovery failed", String(e?.message || e));
      }
    }
    await putJson(container, `${prefix}pdf_extracts.json`, pdfExtracts);

    // Directories placeholder
    await putJson(container, `${prefix}directories.json`, []);

    // Site/products map inputs for downstream
    const siteObjForMap = await getJson(container, `${prefix}site.json`);
    const productsObjForMap = await getJson(container, `${prefix}products.json`);
    const uspsList = Array.isArray(input?.supplier_usps)
      ? input.supplier_usps.map(s => String(s).trim()).filter(Boolean)
      : [];
    const productNames = collectProductNames(productsObjForMap, siteObjForMap, uspsList);

    // Pull buyer needs from csv_normalized (specific signals preferred; fallback to global)
    const needsList =
      (csvNormalizedCanonical?.signals?.top_needs_supplier && csvNormalizedCanonical.signals.top_needs_supplier.length
        ? csvNormalizedCanonical.signals.top_needs_supplier
        : (csvNormalizedCanonical?.global_signals?.top_needs_supplier || []))
        .map(s => String(s).trim()).filter(Boolean);

    // Build needs map & persist as needs_map.json
    const needsMap = needsList.map(n => matchNeedToCapabilities(n, productNames, uspsList));
    const coverage = summariseCoverage(needsMap);
    await putJson(container, `${prefix}needs_map.json`, {
      supplier_company: (input.supplier_company || input.company_name || input.prospect_company || "").trim(),
      selected_industry: csvNormalizedCanonical?.selected_industry || (input?.selected_industry || input?.campaign_industry || ""),
      coverage,
      items: needsMap
    });

    // 5c) PACKS (added): load packs + run evidence.buildEvidence for industry sources & more
    let packEvidence = [];
    try {
      const loadPacks = await loadPacksFn();
      const loaded = await loadPacks();
      const packs = loaded?.packs ?? loaded ?? {};
      const { buildEvidence } = await loadEvidenceLib();

      // Allow the builder to use input + packs to fetch curated industry-sources, regulator links, etc.
      const built = await buildEvidence({
        input,
        packs,
        runId,
        prefix,
        correlationId: `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`
      });

      // Normalize & filter to https
      if (Array.isArray(built)) {
        packEvidence = built
          .map(it => ({
            url: toHttps(it?.url || it?.link || ""),
            title: String(it?.title || "").trim(),
            summary: String(it?.summary || it?.quote || "").trim(),
            source_type: String(it?.source_type || it?.type || "source").trim(),
            quote: it?.quote ? String(it.quote).trim() : undefined
          }))
          .filter(it => it.url && it.title && it.summary && it.url.startsWith("https://"));
      }

      // Additionally, project explicit "industry-sources" lists from packs (defensive)
      const packIndustrySources = (() => {
        const srcs =
          packs?.industry_sources?.sources ||
          packs?.industry?.sources ||
          packs?.industry_sources ||
          [];
        const arr = Array.isArray(srcs) ? srcs : [];
        return arr.map((s) => {
          const url = toHttps(s?.url || s?.link || "");
          const title = String(s?.title || s?.name || "").trim();
          const summary = String(s?.summary || s?.desc || s?.description || "").trim();
          if (!url || !title) return null;
          return {
            url, title,
            summary,
            source_type: classifySourceType(url) // uses your existing helper
          };
        }).filter(Boolean);
      })();

      // Merge projected industry sources too
      packEvidence = [...packEvidence, ...packIndustrySources];
      // 5c.1) Customer profile Markdown → evidence (subscriber intelligence)
      try {
        const companyName = (input?.supplier_company || input?.company_name || "").trim();
        const slug = toSlug(companyName);
        if (slug) {
          // Try run-scoped profile first, then global packs/ as fallback
          const runScopedPath = `${prefix}packs/${slug}/profile.md`;
          const globalPath = `packs/${slug}/profile.md`;

          let mdText = await getText(container, runScopedPath);
          if (!mdText) mdText = await getText(container, globalPath);
          if (mdText && mdText.length) {
            const refs = extractMdRefs(mdText);

            const push = (title, summary, url, tagTitle) => {
              const t = String(title || "").trim();
              const s = String(summary || "").trim();
              if (!t && !s) return;
              const item = {
                claim_id: nextClaimId(),
                source_type: "Customer profile",
                title: (tagTitle ? `${tagTitle}: ` : "") + (t || s).slice(0, 240),
                summary: addCitation(s || t, "Customer profile")
              };
              if (url) item.url = toHttps(url);
              packEvidence.push(item);
            };

            // One-paragraph summary
            const sec1 = mdSection(mdText, "One-paragraph summary");
            const para = firstParagraph(sec1);
            if (para) push("Company analysis summary", para);

            // --- What <Company> does (offer & delivery) ---
            // Try several generic H2 variants; take the first that exists
            const sec2 =
              mdSection(mdText, "What .* does \\(offer & delivery\\)") || // regex-like title support
              mdSection(mdText, "What .* does") ||
              mdSection(mdText, "What we do") ||
              mdSection(mdText, "Offer & delivery") ||
              "";

            for (const b of bullets(sec2)) {
              const titleBold = firstBold(b);
              const refNum = (/\[(\d+)\]\s*$/.exec(b) || [, ""])[1];
              const url = refs[refNum] || undefined;
              const title = titleBold || b.split(/[.–—:]/)[0].trim();
              push(title, b, url, "Service");
            }

            // Where it plays (priority segments & use cases)
            const sec3 = mdSection(mdText, "Where it plays (priority segments & use cases)");
            for (const b of bullets(sec3)) {
              const refNum = (/\[(\d+)\]\s*$/.exec(b) || [, ""])[1];
              const url = refs[refNum] || undefined;
              push("Segment/Use case", b, url);
            }

            // Where it wins (proof points)
            const sec4 = mdSection(mdText, "Where it wins (proof points)");
            for (const b of bullets(sec4)) {
              const refNum = (/\[(\d+)\]\s*$/.exec(b) || [, ""])[1];
              const url = refs[refNum] || undefined;
              const titleBold = firstBold(b) || "Proof point";
              push(titleBold, b, url, "Proof");
            }

            // Differentiators to note
            const sec5 = mdSection(mdText, "Differentiators to note");
            for (const b of bullets(sec5)) {
              const refNum = (/\[(\d+)\]\s*$/.exec(b) || [, ""])[1];
              const url = refs[refNum] || undefined;
              const titleBold = firstBold(b) || "Differentiator";
              push(titleBold, b, url, "Differentiator");
            }

            // Competitive position
            const sec6 = mdSection(mdText, "Competitive position (UK B2B connectivity)");
            for (const b of bullets(sec6)) {
              const refNum = (/\[(\d+)\]\s*$/.exec(b) || [, ""])[1];
              const url = refs[refNum] || undefined;
              push("Competitive position", b, url);
            }

            // Practical takeaways
            const sec7 = mdSection(mdText, "Practical takeaways for sales/partnership conversations");
            for (const b of bullets(sec7)) {
              const refNum = (/\[(\d+)\]\s*$/.exec(b) || [, ""])[1];
              const url = refs[refNum] || undefined;
              push("Practical takeaway", b, url);
            }

            // Questions to validate (discovery)
            const sec8 = mdSection(mdText, "Questions to validate (discovery)");
            for (const b of bullets(sec8)) {
              const refNum = (/\[(\d+)\]\s*$/.exec(b) || [, ""])[1];
              const url = refs[refNum] || undefined;
              push("Discovery question", b, url);
            }
          }
        }
      } catch (e) {
        context.log.warn("[campaign-evidence] profile.md parse failed", String(e?.message || e));
      }

      // Prioritise industry/regulator sources when an industry is selected
      const selInd = String(
        csvNormalizedCanonical?.selected_industry ||
        input?.selected_industry ||
        input?.campaign_industry ||
        ""
      ).toLowerCase();

      function industryScore(pe) {
        const t = `${(pe.title || "")} ${(pe.summary || "")}`.toLowerCase();
        const src = String(pe.source_type || "").toLowerCase();
        const ind = String(csvNormalizedCanonical?.selected_industry || "").toLowerCase();

        let s = 0;

        // Supplier/profile top-tier
        if (src.includes("customer profile")) s += 50;
        if (src.includes("company site") || src.includes("website")) s += 35;

        // Regulator / official statistics
        if (/(ofcom|ons|dsit|gov\.uk)/.test(t) || /(ofcom|ons|dsit|gov\.uk)/.test(src)) s += 30;

        // Exact industry match in title/summary
        if (ind && t.includes(ind)) s += 20;

        // HTTPS hygiene
        if (pe.url && /^https:\/\//.test(pe.url)) s += 2;

        // Non-generic source_type preferred
        if (src && src !== "source") s += 1;

        return s;
      }

      // Sort in-place so Construction/regulator facts lead the evidence log
      packEvidence.sort((a, b) => industryScore(b) - industryScore(a));

    } catch (packErr) {
      context.log.warn("[campaign-evidence] packs/buildEvidence failed", String(packErr?.message || packErr));
      packEvidence = [];
    }

    // 6) evidence_log.json (CSV-first + supplier artefacts + PACKS merged)
    let evidenceLog = [];

    // PATCH EV-SITE-SEEDS-1: deterministic supplier-site claims (idempotent)
    (function seedSupplierSiteClaims() {
      try {
        const siteUrl =
          String(input?.supplier_website || input?.company_website || input?.prospect_website || "").trim();
        if (siteUrl) {
          const homepageClaim = {
            source_type: "website",
            url: siteUrl,
            title: "Supplier homepage",
            summary: "Primary company site used for proposition, offers, and trust signals.",
            topic: "site_overview",
            weight: 1
          };
          safePushIfRoom(evidenceLog, homepageClaim, MAX_EVIDENCE_ITEMS);

          const salesModelClaim = {
            source_type: "website",
            url: siteUrl,
            title: "Sales model context",
            summary: "Route-to-market signals (direct/partner/reseller pages, contact/sales pages).",
            topic: "sales_model",
            weight: 1
          };
          safePushIfRoom(evidenceLog, salesModelClaim, MAX_EVIDENCE_ITEMS);
        }
      } catch { /* no-op */ }
    })();

    // 6.0 PACK evidence (added first so CSV summary appears near the top but we still include industry sources)
    for (const pe of packEvidence) {
      safePushIfRoom(evidenceLog, {
        claim_id: nextClaimId(),
        source_type: pe.source_type || "source",
        title: pe.title,
        url: pe.url,
        summary: addCitation(pe.summary, pe.source_type || "source"),
        quote: pe.quote || ""
      }, MAX_EVIDENCE_ITEMS);
      if (evidenceLog.length >= MAX_EVIDENCE_ITEMS) break;
    }

    // 6.1 CSV summary first (if any)
    const industryName =
      csvNormalizedCanonical?.selected_industry ||
      input?.selected_industry ||
      input?.campaign_industry ||
      "";

    const csvSummaryItem = csvSummaryEvidence(
      csvNormalizedCanonical,
      input,
      prefix,
      container.url,
      csvFocusInsight,
      industryName
    );
    if (csvSummaryItem) {
      // Ensure the CSV summary sits at index 0 and survives any later trimming
      evidenceLog.unshift(csvSummaryItem);
    }

    // 6.1b CSV buyer signals (blockers/needs/purchases)
    if (evidenceLog.length < MAX_EVIDENCE_ITEMS) {
      const _csvSignalsItem = csvSignalsEvidence(csvNormalizedCanonical, prefix, container.url);
      if (_csvSignalsItem) safePushIfRoom(evidenceLog, _csvSignalsItem, MAX_EVIDENCE_ITEMS);
    }
    // 6.1c Capability coverage summary (from needs_map.json)
    try {
      const nm = await getJson(container, `${prefix}needs_map.json`);
      if (nm && nm.coverage) {
        const cov = nm.coverage;
        const lines = Array.isArray(nm.items)
          ? nm.items.slice(0, 12).map(m => `${m.need} → ${m.status}${m.hits?.length ? ` (${m.hits.map(h => h.name).join(", ")})` : ""}`)
          : [];
        safePushIfRoom(
          evidenceLog,
          {
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "Supplier coverage of buyer needs",
            url: `${container.url}/${prefix}needs_map.json`,
            summary: addCitation(`Matched: ${cov.matched}, Partial: ${cov.partial}, Gaps: ${cov.gap} (coverage ${cov.coverage}%)`, "Directory"),
            quote: lines.join("\n")
          },
          MAX_EVIDENCE_ITEMS
        );
      }
    } catch { /* non-fatal */ }

    // 6.2 Supplier identity + user notes + competitors (core context)
    {
      const _supplierItem = supplierIdentityEvidence(
        supplierCompany,
        supplierWebsite,
        `${container.url}/${prefix}site.json`
      );
      if (_supplierItem) safePushIfRoom(evidenceLog, _supplierItem, MAX_EVIDENCE_ITEMS);

      // Persisted user notes & competitors (write side artifacts)
      const notesRel = `${prefix}notes.txt`;
      const notesVal = (input?.notes || "").trim();
      if (notesVal) {
        try { await putText(container, notesRel, notesVal); } catch { /* ignore */ }
        safePushIfRoom(
          evidenceLog,
          {
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "User notes (must integrate)",
            url: `${container.url}/${notesRel}`,
            summary: addCitation(notesVal.length > 240 ? `${notesVal.slice(0, 240)}…` : notesVal, "Directory"),
            quote: ""
          },
          MAX_EVIDENCE_ITEMS
        );
      }

      const competitors = Array.isArray(input?.relevant_competitors)
        ? input.relevant_competitors.map(s => String(s || "").trim()).filter(Boolean).slice(0, 8)
        : [];
      if (competitors.length) {
        const rel = `${prefix}competitors.json`;
        try { await putJson(container, rel, { competitors }); } catch { /* ignore */ }
        safePushIfRoom(
          evidenceLog,
          {
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "Competitors (user-supplied)",
            url: `${container.url}/${rel}`,
            summary: addCitation(`${competitors.length} competitor(s): ${competitors.join(", ")}`, "Directory"),
            quote: ""
          },
          MAX_EVIDENCE_ITEMS
        );
      }
    }

    // 6.3a Case studies / PDF extracts
    {
      let pdfs = [];
      try {
        const v = await getJson(container, `${prefix}pdf_extracts.json`);
        if (Array.isArray(v)) pdfs = v;
      } catch { /* ignore */ }

      if (pdfs.length) {
        for (const ex of pdfs.slice(0, MAX_CASESTUDIES)) {
          const url = ex?.url ? String(ex.url).trim() : "";
          if (!/^https:\/\//i.test(url)) continue; // enforce https only

          const title = (ex?.title && String(ex.title).trim()) || "Case study";
          const quote = (ex?.quote && String(ex.quote).trim()) || "";
          const summaryText =
            (ex?.summary && String(ex.summary).trim()) ||
            (ex?.snippet && String(ex.snippet).trim()) ||
            `${title} — key points extracted.`;

          safePushIfRoom(
            evidenceLog,
            {
              claim_id: nextClaimId(),
              source_type: ex?.type === "pdf" ? "PDF extract" : "Company site",
              title,
              url,
              summary: addCitation(summaryText, ex?.type === "pdf" ? "PDF extract" : "Company site"),
              quote
            },
            MAX_EVIDENCE_ITEMS
          );
        }
      }
    }
    // 6.3b Product pages (claims) — merge after case studies
    if (Array.isArray(productPageEvidence) && productPageEvidence.length) {
      for (const ev of productPageEvidence) {
        safePushIfRoom(evidenceLog, ev, MAX_EVIDENCE_ITEMS);
        if (evidenceLog.length >= MAX_EVIDENCE_ITEMS) break;
      }
    }

    // 6.4 Deterministic industry sources (existing md-based)
    {
      try {
        const { general, sector } = loadIndustrySources(
          input?.campaign_industry || input?.company_industry || ""
        );
        const generalItems = mdToSourceItems(general).slice(0, 2);
        const sectorItems = mdToSourceItems(sector).slice(0, 2);

        // push SECTOR items first
        for (const it of sectorItems) {
          const t = classifySourceType(it.url);
          safePushIfRoom(
            evidenceLog,
            {
              claim_id: nextClaimId(),
              source_type: t,
              title: it.title,
              url: it.url,
              summary: addCitation(`${it.title} — sector-relevant evidence.`, t),
              quote: ""
            },
            MAX_EVIDENCE_ITEMS
          );
        }

        // then GENERAL items
        for (const it of generalItems) {
          const t = classifySourceType(it.url);
          safePushIfRoom(
            evidenceLog,
            {
              claim_id: nextClaimId(),
              source_type: t,
              title: it.title,
              url: it.url,
              summary: addCitation(`${it.title} — reputable general source for stats/trends.`, t),
              quote: ""
            },
            MAX_EVIDENCE_ITEMS
          );
        }
      } catch { /* non-fatal */ }
    }

    // 6.5 Supplier products (from products.json; never invent)
    {
      try {
        const productsObj = await getJson(container, `${prefix}products.json`);
        const prodItem = siteProductsEvidence(productsObj?.products || [], supplierWebsite, container.url, prefix);
        if (prodItem) safePushIfRoom(evidenceLog, prodItem, MAX_EVIDENCE_ITEMS);
      } catch { /* no-op */ }
    }

    // 6.6 LinkedIn reference
    if (supplierLinkedIn) {
      safePushIfRoom(evidenceLog, linkedinEvidence(supplierLinkedIn), MAX_EVIDENCE_ITEMS);
    }

    // 6.7 LinkedIn relevance hooks
    {
      try {
        const liHooks = [];

        // Supplier/company posts
        const liCompanyUrl = liCompanySearch(supplierCompany || hostnameOf(supplierWebsite || ""));
        if (liCompanyUrl) {
          liHooks.push(
            linkedinSearchEvidence({ url: liCompanyUrl, title: "LinkedIn — supplier/company posts" })
          );
        }

        // Product/service mentions (from detected products list if any)
        let productNames = [];
        try {
          const productsObj = await getJson(container, `${prefix}products.json`);
          productNames = Array.isArray(productsObj?.products) ? productsObj.products.slice(0, 5) : [];
        } catch { /* no-op */ }

        for (const pName of productNames) {
          const u = liProductSearch(pName, supplierCompany);
          safePushIfRoom(
            liHooks,
            linkedinSearchEvidence({ url: u, title: `LinkedIn — "${pName}" mentions` }),
            MAX_EVIDENCE_ITEMS
          );
        }

        // Competitor content (only if provided by user)
        const competitors = Array.isArray(input?.relevant_competitors) ? input.relevant_competitors : [];
        for (const cName of competitors.slice(0, 6)) {
          const u = liCompetitorSearch(cName);
          safePushIfRoom(
            liHooks,
            linkedinSearchEvidence({ url: u, title: `LinkedIn — competitor: ${cName}` }),
            MAX_EVIDENCE_ITEMS
          );
        }

        for (const item of liHooks) if (item) safePushIfRoom(evidenceLog, item, MAX_EVIDENCE_ITEMS);
      } catch { /* ignore LI hook failures */ }
    }

    // 6.8 Homepage evidence (explicit)
    if (supplierWebsite) {
      const host = hostnameOf(supplierWebsite) || supplierWebsite;
      safePushIfRoom(
        evidenceLog,
        {
          claim_id: nextClaimId(),
          source_type: "Company site",
          title: `${host} — homepage`,
          url: supplierWebsite,
          summary: addCitation(`Homepage snapshot captured for ${host}.`, "Company site"),
          quote: ""
        },
        MAX_EVIDENCE_ITEMS
      );
    }

    // Sales-model context item (deterministic)
    {
      const salesModel = String(input?.sales_model || input?.salesModel || input?.call_type || "").toLowerCase();
      if (salesModel === "direct" || salesModel === "partner") {
        const relSm = `${prefix}sales-model.txt`;
        const smNote = `Sales model selected: ${salesModel}. Evidence context recorded (no filtering applied).`;
        try { await putText(container, relSm, smNote); } catch { /* ignore */ }
        safePushIfRoom(
          evidenceLog,
          {
            claim_id: nextClaimId(),
            source_type: "Directory",
            title: "Sales model context",
            url: `${container.url}/${relSm}`,
            summary: addCitation(smNote, "Directory"),
            quote: "Model discipline noted; evidence left intact."
          },
          MAX_EVIDENCE_ITEMS
        );
      }
    }

    // Final tidy: remove placeholders, de-dupe, renumber claim_ids (deterministic)
    evidenceLog = evidenceLog.filter(it => !isPlaceholderEvidence(it));
    evidenceLog = dedupeEvidence(evidenceLog);
    // Capacity trim with priority: CSV summary → CSV insights → supplier/site → LinkedIn → everything else (original order)
    const EFFECTIVE_CAP = Math.max(12, MAX_EVIDENCE_ITEMS);
    if (evidenceLog.length > EFFECTIVE_CAP) {
      const keep = [];
      const seen = new Set();

      const pushIf = (pred) => {
        for (let i = 0; i < evidenceLog.length && keep.length < EFFECTIVE_CAP; i++) {
          const it = evidenceLog[i];
          if (!seen.has(it) && pred(it, i)) { keep.push(it); seen.add(it); }
        }
      };

      // 0) CSV summary at index 0 if present
      pushIf((it, i) => i === 0);

      // 1) CSV buyer signals
      pushIf((it) => /csv/i.test(String(it?.source_type || "")) && /signals|population|csv/i.test(String(it?.title || "")));

      // 2) Supplier/site items
      pushIf((it) => /company site|website/i.test(String(it?.source_type || "")));

      // 3) LinkedIn
      pushIf((it) => /linkedin/i.test(String(it?.source_type || "")));

      // 4) Fill rest
      for (const it of evidenceLog) {
        if (keep.length >= EFFECTIVE_CAP) break;
        if (!seen.has(it)) { keep.push(it); seen.add(it); }
      }

      evidenceLog = keep;
    }
    // Deterministic renumbering after final order is set
    evidenceLog.forEach((it, i) => { it.claim_id = `CLM-${String(i + 1).padStart(3, "0")}`; });

    context.log("[evidence] writing evidence_log.json", {
      count: evidenceLog.length,
      titles: evidenceLog.map(x => x.title).slice(0, 12)
    });

    // PATCH EV-RANK-1: competitor relevance (product overlap) + industry precedence + source scoring
    try {
      // 1) Load supplier products for overlap check
      const pm = (await getJson(container, `${prefix}products_meta.json`)) || {};
      const declared = Array.isArray(pm?.declared) ? pm.declared : [];
      const validated = Array.isArray(pm?.validated) ? pm.validated.map(x => x?.name).filter(Boolean) : [];
      const chosen = Array.isArray(pm?.chosen) ? pm.chosen : [];
      const productBasis = (validated.length ? validated : (chosen.length ? chosen : declared))
        .map(x => String(x || "").toLowerCase());

      // 2) Get industry key (for precedence & matching)
      const industry = String(
        input?.selected_industry ?? input?.campaign_industry ?? input?.buyer_industry ?? ""
      ).toLowerCase();

      // 3) Filter competitor claims by product overlap (keep only relevant competitors)
      const hasOverlap = (txt) => {
        const s = String(txt || "").toLowerCase();
        return productBasis.some(p => p && s.includes(p));
      };
      const filtered = [];
      for (const it of evidenceLog) {
        if (it?.source_type === "profile_competitor" || it?.source_type === "competitor") {
          const basis = `${it.title || ""} ${it.summary || ""}`;
          if (!hasOverlap(basis)) continue; // drop irrelevant competitor claim
        }
        filtered.push(it);
      }

      // 4) Score claims for stable precedence
      function score(it) {
        const st = String(it?.source_type || "").toLowerCase();
        const text = `${it.title || ""} ${it.summary || ""}`.toLowerCase();
        let sc = 0;
        if (st === "profile") sc += 100;
        if (st === "profile_competitor") sc += 90;
        if (industry && text.includes(industry)) sc += 80;            // industry boost
        if (st.includes("website")) sc += 40;
        if (st.includes("csv")) sc += 30;
        if (st.includes("linkedin")) sc += 20;
        if (st.includes("pdf")) sc += 15;
        if (it.weight) sc += (Number(it.weight) || 0);                // respect explicit weights
        return sc;
      }

      filtered.sort((a, b) => score(b) - score(a)); // high → low
      const cap =
        Number.isFinite(MAX_EVIDENCE_ITEMS) && MAX_EVIDENCE_ITEMS > 0
          ? MAX_EVIDENCE_ITEMS
          : 24; // sane fallback
      const finalLog = filtered.length > cap ? filtered.slice(0, cap) : filtered;
      // Replace in-place for downstream write
      evidenceLog = finalLog;
    } catch {
      if (Array.isArray(evidenceLog)) {
        const cap =
          Number.isFinite(MAX_EVIDENCE_ITEMS) && MAX_EVIDENCE_ITEMS > 0
            ? MAX_EVIDENCE_ITEMS
            : 24;
        if (evidenceLog.length > cap) evidenceLog = evidenceLog.slice(0, cap);
      }/* non-fatal; keep original order */
    }

    // Write evidence (evidence.json)
    try {
      // Ensure array form
      const list = Array.isArray(evidenceLog) ? evidenceLog : [];

      // Normalise & de-duplicate claims (idempotent, safe for re-runs)
      const seen = new Set();
      const claims = [];
      for (const raw of list) {
        const c = raw || {};

        // Build a stable identity
        const id = String(c.claim_id || c.id || c.url || c.title || "").trim();
        const key = (String(c.title || "").trim() + "||" + String(c.url || "").trim()).toLowerCase();

        if (id) {
          if (seen.has(id) || (key && seen.has(key))) continue;
          seen.add(id);
          if (key) seen.add(key);
        } else if (key) {
          if (seen.has(key)) continue;
          seen.add(key);
        }

        claims.push({
          claim_id: id || undefined,
          title: String(c.title || "").trim(),
          summary: String(c.summary || c.quote || "").trim(),
          source_type: String(c.source_type || c.source || "").trim(),
          url: String(c.url || "").trim(),
          tag: String(c.tag || c.source_tag || "").trim(),
          date: c.date || c.published_at || null
        });
      }

      // Persist canonical log (normalised)
      await putJson(container, `${prefix}evidence_log.json`, claims);

      // Persist canonical bundle with counts summary
      const evidenceBundle = {
        claims,
        counts: summarizeClaims(claims)
      };
      await putJson(container, `${prefix}evidence.json`, evidenceBundle);
    } catch (e) {
      context.log.warn("[evidence] failed to write evidence bundle", String(e?.message || e));
    }

    // Hand off to router exactly once → afterevidence
    try {
      const st0 = (await getJson(container, `${prefix}status.json`)) || { markers: {} };
      const already = !!st0?.markers?.afterevidenceSent;
      if (!already) {
        const { QueueServiceClient } = require("@azure/storage-queue");
        const qs = QueueServiceClient.fromConnectionString(process.env.AzureWebJobsStorage);
        const mainQ = qs.getQueueClient(process.env.CAMPAIGN_QUEUE_NAME || "campaign");
        await mainQ.createIfNotExists();
        await mainQ.sendMessage(JSON.stringify({ op: "afterevidence", runId, page: "campaign", prefix }));

        // mark sent (idempotent)
        st0.markers = { ...(st0.markers || {}), afterevidenceSent: true };
        await putJson(container, `${prefix}status.json`, st0);
      }
    } catch (e) {
      context.log.warn("[evidence] afterevidence enqueue failed", String(e?.message || e));
    }

    // 7) Finalise phase
    try {
      const statusPath = `${prefix}status.json`;
      const cur = (await getJson(container, statusPath)) || { runId, history: [], markers: {} };
      cur.markers = cur.markers || {};
      cur.markers.evidenceDigestCompleted = true;
      await putJson(container, statusPath, cur);
    } catch { /* non-fatal */ }
    await updateStatus(
      container,
      prefix,
      {
        state: "EvidenceDigest",
        phase: "completed",
        updatedAt: nowIso(),
        artifacts: {
          site: "site.json",
          products: "products.json",
          linkedin: "linkedin.json",
          pdf_extracts: "pdf_extracts.json",
          directories: "directories.json",
          csv: "csv_normalized.json",
          evidence_log: "evidence_log.json"
        },
        evidence_counts: {
          csv_rows: Number(csvNormalizedCanonical?.meta?.rows || 0),
          items_total: evidenceLog.length
        }
      },
      { phase: "EvidenceDigest", note: "completed", count: evidenceLog.length }
    );

    // Notify orchestrator (afterevidence)
    try {
      const qc = new QueueClient(process.env.AzureWebJobsStorage, START_QUEUE_NAME);
      await qc.createIfNotExists();
      const msgOut = { op: "afterevidence", runId, page: (input.page || "campaign"), prefix };
      await qc.sendMessage(JSON.stringify(msgOut));
    } catch (notifyErr) {
      context.log.warn("[campaign-evidence] notify orchestrator failed", String(notifyErr?.message || notifyErr));
    }

    context.log("[campaign-evidence] completed", { runId, prefix });
  } catch (e) {
    const message = String(e?.message || e);
    context.log.error("[campaign-evidence] failed", message);
    try {
      await updateStatus(
        container,
        prefix,
        {
          state: "Failed",
          error: { code: "evidence_error", message },
          failedAt: nowIso()
        },
        { phase: "EvidenceDigest", note: "failed", error: message }
      );
    } catch { /* no-op */ }
  }
};
