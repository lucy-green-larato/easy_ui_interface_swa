// /api/campaign-evidence/index.js — Split campaign build. 05-11-2025 — v7
// Node 20, Azure Functions v4 (CommonJS)
// Purpose: build the canonical evidence set for a run.
// Artifacts written under: <prefix>/
//   - site.json               (homepage snapshot + lightweight link graph — array, legacy compatible)
//   - products.json           (very light product/solution names from homepage)
//   - linkedin.json           (reference link only; no scraping)
//   - pdf_extracts.json       (links discovered incl. PDFs/case studies; shallow metadata)
//   - directories.json        (placeholder; empty array)
//   - csv_normalized.json     (canonical: industry_mode + signals + meta)
//   - evidence_log.json       (ARRAY; CSV summary + supplier artefacts + regulator/sector seeds + LI hooks)
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
const MAX_EVIDENCE_ITEMS = parseInt(process.env.MAX_EVIDENCE_ITEMS || "24", 10);

// ---------- Azure helpers ----------
function getContainerClient() {
  const conn = process.env.AzureWebJobsStorage;
  if (!conn) throw new Error("AzureWebJobsStorage not configured");
  const svc = BlobServiceClient.fromConnectionString(conn);
  return svc.getContainerClient(RESULTS_CONTAINER);
}

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
  const header = rows[0].map(h => h.trim());
  const idx = (name) => header.findIndex(h => h.toLowerCase() === name.toLowerCase());

  // Flexible header keys
  const iIndustry = (() => {
    const keys = ["SimplifiedIndustry", "Industry", "industry", "buyer_industry", "vertical", "sector"];
    for (const k of keys) {
      const i = header.findIndex(h => h.toLowerCase() === k.toLowerCase());
      if (i >= 0) return i;
    }
    return -1;
  })();
  const iBlockers = idx("TopBlockers");
  const iPurchases = idx("TopPurchases");
  const iNeeds = idx("TopNeedsSupplier");
  const iSpend = idx("SpendDistribution");

  const agg = new Map(); // industry -> { TopBlockers:Set, TopPurchases:Set, TopNeedsSupplier:Set, SpendDistribution:Object, SampleSize:Number }

  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const industry = (iIndustry >= 0 ? (row[iIndustry] || "") : "").trim() || "Unknown";
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
      const items = (val || "")
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
      const raw = row[iSpend].trim();
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
    const toArr = (set) => Array.from(set);
    const pack = {
      TopBlockers: toArr(b.TopBlockers),
      TopPurchases: toArr(b.TopPurchases),
      TopNeedsSupplier: toArr(b.TopNeedsSupplier),
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

function resolveIndustry({ userIndustry, csvIndustry, csvHasMultipleSectors }) {
  const u = (userIndustry || "").trim().toLowerCase();
  if (u) return u;
  const c = (csvIndustry || "").trim().toLowerCase();
  if (c && !csvHasMultipleSectors) return c;
  return null; // agnostic
}

// --- Product extraction (lightweight heuristic, homepage) ---
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

async function crawlSiteGraph(rootUrl, budget = MAX_SITE_PAGES, timeout = FETCH_TIMEOUT_MS) {
  if (!rootUrl) return { pages: [], links: [] };
  const start = toHttps(rootUrl);
  const visited = new Set();
  const queue = [start];
  const pages = [];
  const links = new Set();

  while (queue.length && pages.length < budget) {
    const url = queue.shift();
    if (!url || visited.has(url)) continue;
    visited.add(url);

    const res = await httpGet(url, timeout);
    if (!res.ok) continue;
    const html = (res.text || "").slice(0, 150_000);
    pages.push({
      url,
      host: hostnameOf(url),
      fetchedAt: nowIso(),
      type: "html",
      bytes: Buffer.byteLength(html, "utf8"),
      sha1: sha1(html),
      snippet: html.slice(0, 6000)
    });

    const hrefs = extractLinks(html, url);
    for (const h of hrefs) {
      links.add(h);
      if (sameHost(h, start) && !visited.has(h) && queue.length + pages.length < budget) {
        queue.push(h);
      }
    }
  }
  return { pages, links: Array.from(links) };
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
    const m = line.match(/^\s*-\s*([^:—]+?)(?:\s*[—-]\s*[^:]+?)?\s*:\s*(https?:\/\/\S+)/i);
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
function isPlaceholderEvidence(it) {
  if (!it) return true;
  const url = (it.url || "").trim();
  const title = (it.title || "").trim();
  const summary = (it.summary || "").trim();

  // 1) No substance
  if (!url && !title && !summary) return true;

  // 2) Weak scaffold titles
  if (/^(?:products|product|placeholder|lorem ipsum)$/i.test(title)) return true;

  // 3) Non-https or junk hostnames
  if (!/^https:\/\//i.test(url)) return true;            // enforce https only
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

function csvSummaryEvidence(csvCanonical, input, prefix, containerUrl) {
  const rows = Number(csvCanonical?.meta?.rows || input?.rowCount || 0);
  const fileName = csvCanonical?.meta?.source || input?.csvFilename || "inline";
  const root = String(containerUrl).replace(/\/+$/, "");
  const pfx = String(prefix).replace(/^\/+/, "");
  const cleanName = String(fileName || "").replace(/^\/+/, "");
  const csvUrl = (cleanName && cleanName !== "inline")
    ? `${root}/${pfx}${cleanName}`
    : `${root}/${pfx}csv_normalized.json`;

  let focusNote = "";
  if (rows >= 180 && rows <= 320) focusNote = "Good focus range for a single campaign (≈200–300).";
  else if (rows > 320) focusNote = "Large set; consider segmenting into waves for focus.";
  else if (rows > 0) focusNote = "Very narrow set; consider broadening if volume is required.";

  const base = `Addressable market: ${rows} companies in ${fileName}. ${focusNote}`.trim();
  return {
    claim_id: nextClaimId(),
    source_type: "CSV",
    title: "CSV population",
    url: csvUrl,
    summary: addCitation(base, "CSV"),
    quote: "CSV-derived population size and focus guidance."
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
  let prefix = (msg.prefix && String(msg.prefix).trim()) || `runs/${runId}/`;
  // Normalise to container-relative (strip container name, strip leading slash, ensure trailing slash)
  if (prefix.startsWith(`${RESULTS_CONTAINER}/`)) prefix = prefix.slice(`${RESULTS_CONTAINER}/`.length);
  if (prefix.startsWith("/")) prefix = prefix.replace(/^\/+/, "");
  if (!prefix.endsWith("/")) prefix = `${prefix}/`;

  // ---- Enter EvidenceDigest  ----
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

    // 1a) Products (from homepage snippet)
    try {
      const html = siteGraph.pages?.[0]?.snippet || "";
      const products = await extractProductsFromSite(html);
      await putJson(container, `${prefix}products.json`, { products });
    } catch (e) {
      context.log.warn("[campaign-evidence] product extraction skipped", String(e?.message || e));
    }

    // 2) LINKEDIN (reference only)
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

    // 3) Case study discovery (HTML & PDF on same host)
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

    // 4) Directories placeholder
    await putJson(container, `${prefix}directories.json`, []);

    // 5) CSV → csv_normalized.json (canonical)
    let csvText = null;
    let chosenCsvName = null;

    // 5a) Inline CSV (highest priority)
    if (typeof input.csvText === "string" && input.csvText.trim()) {
      csvText = input.csvText;
      chosenCsvName = (typeof input.csvFilename === "string" && input.csvFilename.trim()) ? input.csvFilename.trim() : "inline";
    }

    // 5b) Blob CSV (fallback)
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
      const nonEmptyRows = rows.filter(r => Array.isArray(r) && r.some(c => String(c || "").trim().length));
      const agg = normalizeCsv(nonEmptyRows);

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
          rows: Math.max(0, rows.length - 1),
          source: chosenCsvName || "inline",
          csv_has_multiple_sectors: !!csvHasMultipleSectors
        }
      };
    }

    await putJson(container, `${prefix}csv_normalized.json`, csvNormalizedCanonical);

    // 6) evidence_log.json (CSV-first + supplier artefacts)
    let evidenceLog = [];

    // 6.1 CSV summary first (if any)
    if (Number(csvNormalizedCanonical?.meta?.rows || 0) > 0) {
      safePushIfRoom(
        evidenceLog,
        csvSummaryEvidence(csvNormalizedCanonical, input, prefix, container.url),
        MAX_EVIDENCE_ITEMS
      );
    }

    // 6.1b CSV buyer signals (blockers/needs/purchases)
    {
      const _csvSignalsItem = csvSignalsEvidence(csvNormalizedCanonical, prefix, container.url);
      if (_csvSignalsItem) safePushIfRoom(evidenceLog, _csvSignalsItem, MAX_EVIDENCE_ITEMS);
    }

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

    // 6.3 Case studies / PDF extracts
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

    // 6.4 Deterministic industry sources
    {
      try {
        const { general, sector } = loadIndustrySources(
          input?.campaign_industry || input?.company_industry || ""
        );
        const generalItems = mdToSourceItems(general).slice(0, 2);
        const sectorItems = mdToSourceItems(sector).slice(0, 2);

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
    evidenceLog.forEach((it, i) => { it.claim_id = `CLM-${String(i + 1).padStart(3, "0")}`; });

    context.log("[evidence] writing evidence_log.json", {
      count: evidenceLog.length,
      titles: evidenceLog.map(x => x.title).slice(0, 12)
    });
    await putJson(container, `${prefix}evidence_log.json`, evidenceLog);

    // 7) Finalise phase
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
      const msg = { op: "afterevidence", runId, page: (input.page || "campaign"), prefix };
      await qc.sendMessage(Buffer.from(JSON.stringify(msg), "utf8").toString("base64"));
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
