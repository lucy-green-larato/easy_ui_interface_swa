// /api/campaign-evidence/index.js 07-11-2025 v12 (Option B – Canonical Evidence Builder)
// Node 20, Azure Functions v4 (CommonJS)

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const crypto = require("node:crypto");
const fs = require("fs");
const path = require("path");

// ---------- Env ----------
const STORAGE_CONN        = process.env.AzureWebJobsStorage;
const RESULTS_CONTAINER   = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const MAIN_QUEUE          = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
const FETCH_TIMEOUT_MS    = Number(process.env.HTTP_FETCH_TIMEOUT_MS || 8000);
const MAX_SITE_PAGES      = 8;   // homepage + up to 7 internal pages
const MAX_CASESTUDIES     = 8;   // cap case study items
const MAX_EVIDENCE_ITEMS  = parseInt(process.env.MAX_EVIDENCE_ITEMS || "24", 10);

// ---------- Azure helpers ----------
function blobSvc() {
  if (!STORAGE_CONN) throw new Error("AzureWebJobsStorage not configured");
  return BlobServiceClient.fromConnectionString(STORAGE_CONN);
}
async function ensureContainer(container) { try { await container.createIfNotExists(); } catch {} }
async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const ch of readable) chunks.push(ch);
  return Buffer.concat(chunks).toString("utf8");
}
async function getJson(container, rel) {
  const bc = container.getBlockBlobClient(rel);
  if (!(await bc.exists())) return null;
  const dl = await bc.download();
  const txt = await streamToString(dl.readableStreamBody);
  try { return JSON.parse(txt); } catch { return null; }
}
async function putJson(container, rel, obj) {
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await container.getBlockBlobClient(rel)
    .uploadData(body, { blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" } });
}
async function putText(container, rel, text) {
  const body = Buffer.from(String(text ?? ""), "utf8");
  await container.getBlockBlobClient(rel)
    .uploadData(body, { blobHTTPHeaders: { blobContentType: "text/plain; charset=utf-8" } });
}
async function listCsvUnderPrefix(container, prefix) {
  const out = [];
  for await (const it of container.listBlobsFlat({ prefix })) {
    if (it.name.toLowerCase().endsWith(".csv")) out.push(it.name);
  }
  return out;
}
async function downloadText(container, rel) {
  const bc = container.getBlockBlobClient(rel);
  if (!(await bc.exists())) return null;
  const dl = await bc.download();
  return streamToString(dl.readableStreamBody);
}

// ---------- Small utils ----------
const nowISO = () => new Date().toISOString();
const sha1 = (s) => crypto.createHash("sha1").update(String(s || "")).digest("hex");
const hostnameOf = (u) => { try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return null; } };
const toAbsUrl = (base, href) => { try { return new URL(href, base).toString(); } catch { return null; } };
function normalizePrefix(p) {
  let x = String(p || "").trim();
  if (!x) return null;
  if (x.startsWith(`${RESULTS_CONTAINER}/`)) x = x.slice(`${RESULTS_CONTAINER}/`.length);
  x = x.replace(/^\/+/, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}
function toHttps(u) {
  if (!u) return u;
  const s = String(u).trim();
  if (!s) return s;
  if (/^https:\/\//i.test(s)) return s;
  if (/^http:\/\//i.test(s)) return s.replace(/^http:\/\//i, "https://");
  return `https://${s}`;
}

// ---------- Status (append-only history; single write to state="EvidenceDigest") ----------
async function readStatus(container, prefix) {
  return (await getJson(container, `${prefix}status.json`)) || {};
}
async function patchStatus(container, prefix, patch, historyNode) {
  const cur = (await readStatus(container, prefix)) || {};
  const next = { ...cur };
  if (!Array.isArray(next.history)) next.history = [];
  if (historyNode) next.history.push({ at: nowISO(), ...historyNode });
  // Note: we DO NOT set state here unless caller passes it at the end of phase.
  for (const [k, v] of Object.entries(patch || {})) next[k] = v;
  await putJson(container, `${prefix}status.json`, next);
  return next;
}

// ---------- HTTP (resilient GET) ----------
async function httpGet(url, timeoutMs = FETCH_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort("timeout"), timeoutMs);
  try {
    const res = await fetch(url, { method: "GET", signal: ctrl.signal, redirect: "follow",
      headers: { "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36" } });
    const text = await res.text();
    return { ok: res.ok, status: res.status, text };
  } catch (e) {
    return { ok: false, status: 0, text: String(e?.message || e) };
  } finally { clearTimeout(t); }
}

// ---------- CSV (loose parser + aggregator) ----------
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
      if (ch === '"') { if (s[i + 1] === '"') { cur += '"'; i += 2; continue; } inQ = false; i++; continue; }
      cur += ch; i++; continue;
    } else {
      if (ch === '"') { inQ = true; i++; continue; }
      if (ch === "," || ch === ";") { pushCell(); i++; continue; }
      if (ch === "\n") { pushCell(); pushRow(); i++; continue; }
      cur += ch; i++; continue;
    }
  }
  pushCell(); pushRow();
  return rows;
}
function normalizeCsv(rows) {
  if (!rows.length) return { industries: [], dominant_industry: null, by_industry: {} };
  const header = rows[0].map(h => h.trim());
  const idx = (name) => header.findIndex(h => h.toLowerCase() === name.toLowerCase());
  const iIndustry = (() => {
    const keys = ["SimplifiedIndustry","Industry","industry","buyer_industry","vertical","sector"];
    for (const k of keys) { const i = header.findIndex(h => h.toLowerCase() === k.toLowerCase()); if (i >= 0) return i; }
    return -1;
  })();
  const iBlockers = idx("TopBlockers");
  const iPurchases = idx("TopPurchases");
  const iNeeds = idx("TopNeedsSupplier");
  const iSpend = idx("SpendDistribution");

  const agg = new Map();
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const industry = (iIndustry >= 0 ? (row[iIndustry] || "") : "").trim() || "Unknown";
    if (!agg.has(industry)) agg.set(industry, { TopBlockers:new Set(), TopPurchases:new Set(), TopNeedsSupplier:new Set(), SpendDistribution:{}, SampleSize:0 });
    const b = agg.get(industry);
    const addList = (val, set) => {
      const items = (val || "").split(/\r?\n|;|,|\|/).map(s => s.trim()).filter(Boolean);
      items.forEach(x => set.add(x));
    };
    if (iBlockers >= 0) addList(row[iBlockers], b.TopBlockers);
    if (iPurchases >= 0) addList(row[iPurchases], b.TopPurchases);
    if (iNeeds >= 0) addList(row[iNeeds], b.TopNeedsSupplier);

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
      for (const [k, v] of Object.entries(obj)) b.SpendDistribution[k] = (b.SpendDistribution[k] || 0) + Number(v || 0);
    }
    b.SampleSize++;
  }
  const by_industry = {};
  let dominant = null, bestN = -1;
  for (const [ind, pack] of agg.entries()) {
    by_industry[ind] = {
      TopBlockers: Array.from(pack.TopBlockers),
      TopPurchases: Array.from(pack.TopPurchases),
      TopNeedsSupplier: Array.from(pack.TopNeedsSupplier),
      SpendDistribution: pack.SpendDistribution,
      SampleSize: pack.SampleSize
    };
    if (pack.SampleSize > bestN) { bestN = pack.SampleSize; dominant = ind; }
  }
  return { industries: Object.keys(by_industry), dominant_industry: dominant, by_industry };
}
const topByFrequency = (arr, limit=8) => {
  const freq = new Map();
  for (const s of Array.isArray(arr) ? arr : []) {
    const t = String(s || "").trim(); if (!t) continue;
    freq.set(t, (freq.get(t) || 0) + 1);
  }
  return [...freq.entries()].sort((a,b)=>b[1]-a[1]).map(([k])=>k).slice(0, limit);
};
function resolveIndustry({ userIndustry, csvIndustry, csvHasMultipleSectors }) {
  const u = (userIndustry || "").trim().toLowerCase();
  if (u) return u;
  const c = (csvIndustry || "").trim().toLowerCase();
  if (c && !csvHasMultipleSectors) return c;
  return null; // agnostic
}

// ---------- Lightweight product extraction (homepage snippet only) ----------
async function extractProductsFromSite(html) {
  if (!html || typeof html !== "string") return [];
  const lines = html.split(/\r?\n/).slice(0, 2000);
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
        ) out.add(val);
      }
    }
  }
  return Array.from(out);
}

// ---------- Link extraction & crawl ----------
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
const sameHost = (a, b) => { try { return new URL(a).hostname === new URL(b).hostname; } catch { return false; } };
const isCaseStudyUrl = (u) => {
  const low = String(u || "").toLowerCase();
  return (
    low.includes("case-study") || low.includes("case-studies") ||
    low.includes("/customers/") || low.includes("/customer/") ||
    low.includes("/success/") || low.includes("/stories/") ||
    low.endsWith(".pdf") || /\b(success|story|customer|deployment|implementation|results|case)\b/.test(low)
  );
};
async function crawlSiteGraph(rootUrl, budget = MAX_SITE_PAGES, timeout = FETCH_TIMEOUT_MS) {
  if (!rootUrl) return { pages: [], links: [] };
  const start = toHttps(rootUrl);
  const visited = new Set();
  const q = [start];
  const pages = [];
  const links = new Set();

  while (q.length && pages.length < budget) {
    const url = q.shift();
    if (!url || visited.has(url)) continue;
    visited.add(url);

    const res = await httpGet(url, timeout);
    if (!res.ok) continue;
    const html = (res.text || "").slice(0, 150_000);
    pages.push({
      url, host: hostnameOf(url), fetchedAt: nowISO(), type: "html",
      bytes: Buffer.byteLength(html, "utf8"),
      sha1: sha1(html),
      snippet: html.slice(0, 6000)
    });

    const hrefs = extractLinks(html, url);
    for (const h of hrefs) {
      links.add(h);
      if (sameHost(h, start) && !visited.has(h) && q.length + pages.length < budget) q.push(h);
    }
  }
  return { pages, links: Array.from(links) };
}
async function collectCaseStudyLinks(pages, allLinks, rootHost) {
  const candidates = new Set();
  for (const p of pages) {
    if (isCaseStudyUrl(p.url)) candidates.add(p.url);
    const pageLinks = extractLinks(p.snippet || "", p.url);
    for (const l of pageLinks) if (sameHost(l, `https://${rootHost}`) && isCaseStudyUrl(l)) candidates.add(l);
  }
  for (const l of allLinks) if (sameHost(l, `https://${rootHost}`) && isCaseStudyUrl(l)) candidates.add(l);
  return Array.from(candidates).slice(0, MAX_CASESTUDIES);
}
async function fetchCaseStudySummaries(urls) {
  const out = [];
  for (const raw of urls) {
    const url = toHttps(raw);
    const isPdf = url.toLowerCase().endsWith(".pdf");
    if (isPdf) {
      out.push({
        url, host: hostnameOf(url), fetchedAt: nowISO(), type: "pdf",
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
    out.push({ url, host: hostnameOf(url), fetchedAt: nowISO(), type: "html", title, quote: bodySnippet });
  }
  return out;
}

// ---------- Evidence composition ----------
function claimIdFactory() { let n = 0; return () => `CLM-${String((n = Math.min(999, n + 1))).padStart(3, "0")}`; }
const nextClaimId = claimIdFactory();
const addCitation = (text, tag) => {
  const t = String(text || "").trim();
  return t ? (/\([^()]{2,}\)\.?$/.test(t) ? t : `${t} (${tag})`) : `(${tag})`;
};
const classifySourceType = (url) => {
  const u = String(url || "").toLowerCase();
  if (u.includes("linkedin.com")) return "LinkedIn";
  if (u.includes("ofcom")) return "Ofcom";
  if (u.includes("ons.gov")) return "ONS";
  if (u.includes("dsit.gov") || (u.includes("gov.uk") && u.includes("dsit"))) return "DSIT";
  if (u.endsWith(".pdf")) return "PDF extract";
  if (u.includes("google.")) return "Directory";
  return "Company site";
};
function isPlaceholderEvidence(it) {
  if (!it) return true;
  const url = (it.url || "").trim();
  const title = (it.title || "").trim();
  const summary = (it.summary || "").trim();
  if (!url && !title && !summary) return true;
  if (/^(?:products|product|placeholder|lorem ipsum)$/i.test(title)) return true;
  if (!/^https:\/\//i.test(url)) return true;
  const u = url.toLowerCase();
  if (u.startsWith("about:")) return true;
  if (u.includes("example.com")) return true;
  if (/\b(companywebsite\.com|companyx\.com|competitor\d+\.com|vendora\.com|vendorb\.com|vendorc\.com|vendord\.com|vendore\.com|test\.(com|net)|localhost|127\.0\.0\.1)\b/i.test(u)) return true;
  return false;
}
function dedupeEvidence(list) {
  const seen = new Set(); const out = [];
  for (const it of list) {
    const key = `${(it.title || "").toLowerCase()}|${(it.url || "").toLowerCase()}|${(it.source_type || "")}`;
    if (seen.has(key)) continue; seen.add(key); out.push(it);
  }
  return out;
}

// CSV-derived evidence items
function csvSummaryEvidence(csvCanonical, input, prefix, containerUrl) {
  const rows = Number(csvCanonical?.meta?.rows || input?.rowCount || 0);
  const fileName = csvCanonical?.meta?.source || input?.csvFilename || "inline";
  const root = String(containerUrl).replace(/\/+$/, "");
  const pfx = String(prefix).replace(/^\/+/, "");
  const cleanName = String(fileName || "").replace(/^\/+/, "");
  const csvUrl = (cleanName && cleanName !== "inline")
    ? `${root}/${pfx}${cleanName}`
    : `${root}/${pfx}csv_normalized.json`;

  const focusNote =
    rows >= 180 && rows <= 320 ? "Good focus range for a single campaign (≈200–300)." :
    rows > 320 ? "Large set; consider segmenting into waves for focus." :
    rows > 0 ? "Very narrow set; consider broadening if volume is required." : "";

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
  const topNeeds    = (Array.isArray(sig.top_needs_supplier) && sig.top_needs_supplier.length ? sig.top_needs_supplier : gsig.top_needs_supplier) || [];
  const topPurch    = (Array.isArray(sig.top_purchases) && sig.top_purchases.length ? sig.top_purchases : gsig.top_purchases) || [];

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

// Supplier/site-derived items
const supplierIdentityEvidence = (company, website, fallbackUrl) => {
  const host = hostnameOf(website || "");
  const name = (company || "").trim();
  if (!name && !host) return null;
  const summary = [name ? `Supplier: ${name}` : null, host ? `Website host: ${host}` : null].filter(Boolean).join(" — ");
  return {
    claim_id: nextClaimId(),
    source_type: "Company site",
    title: "Supplier identity",
    url: toHttps(website || fallbackUrl || "https://inside-track.local/site"),
    summary: addCitation(summary, "Company site"),
    quote: ""
  };
};
function siteProductsEvidence(products, website, containerUrl, prefix) {
  if (!Array.isArray(products) || !products.length) return null;
  const base = website ? toHttps(website).replace(/\/+$/, "") : null;
  const items = products.slice(0, 12).map(p => (typeof p === "string" ? p : (p?.name || p?.title || ""))).map(s => s.trim()).filter(Boolean);
  if (!items.length) return null;
  return {
    claim_id: nextClaimId(),
    source_type: "Company site",
    title: "Products (homepage-derived)",
    url: base || (containerUrl && prefix ? `${containerUrl.replace(/\/+$/, "")}/${prefix}products.json` : undefined),
    summary: addCitation(items.slice(0, 8).join(", "), "Company site"),
    quote: ""
  };
}
const linkedinEvidence = (linkedin) => {
  if (!linkedin) return null;
  return {
    claim_id: nextClaimId(),
    source_type: "LinkedIn",
    title: "Supplier LinkedIn (reference)",
    url: toHttps(linkedin),
    summary: addCitation("Company LinkedIn profile reference for employer facts and recent posts.", "LinkedIn"),
    quote: ""
  };
};

// LinkedIn search hooks (reference links only)
const liCompanySearch = (company) => company ? `https://www.linkedin.com/search/results/content/?keywords=${encodeURIComponent(company)}&origin=GLOBAL_SEARCH_HEADER` : null;
const liProductSearch = (name, company) => `https://www.linkedin.com/search/results/content/?keywords=${encodeURIComponent([company || "", name || ""].filter(Boolean).join(" "))}&origin=GLOBAL_SEARCH_HEADER`;
const liCompetitorSearch = (name) => name ? `https://www.linkedin.com/search/results/content/?keywords=${encodeURIComponent(name)}&origin=GLOBAL_SEARCH_HEADER` : null;
const linkedinSearchEvidence = ({ url, title }) => url ? ({
  claim_id: nextClaimId(),
  source_type: "LinkedIn",
  title,
  url: toHttps(url),
  summary: addCitation(`${title} — relevance scan link.`, "LinkedIn"),
  quote: ""
}) : null;

// ---------- Handler ----------
module.exports = async function (context, job) {
  const svc = blobSvc();
  const container = svc.getContainerClient(RESULTS_CONTAINER);
  await ensureContainer(container);

  // Parse incoming
  const msg = (typeof job === "string") ? (() => { try { return JSON.parse(job); } catch { return {}; } })() : (job || {});
  const runId = String(msg.runId || (crypto.randomUUID ? crypto.randomUUID() : Date.now().toString(36)));
  let prefix = normalizePrefix(msg.prefix) || normalizePrefix(`runs/${runId}/`);
  if (!prefix) { context.log.error("[evidence] unable to resolve prefix"); return; }

  // Read current status/markers (idempotence)
  const status0 = await readStatus(container, prefix);
  const markers = status0.markers || {};
  if (markers.evidenceDigestCompleted) {
    context.log("[evidence] already completed; skipping", { runId, prefix });
    // Ensure we don't resend afterevidence if it was sent
    if (!markers.afterevidenceSent) {
      try {
        const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
        const qc = qs.getQueueClient(MAIN_QUEUE);
        await qc.createIfNotExists();
        await qc.sendMessage(JSON.stringify({ op: "afterevidence", runId, page: (msg?.input?.page || "campaign"), prefix }));
        const st = await readStatus(container, prefix);
        st.markers = { ...(st.markers || {}), afterevidenceSent: true };
        await putJson(container, `${prefix}status.json`, st);
      } catch (err) { context.log.warn("[evidence] resend afterevidence failed", String(err?.message || err)); }
    }
    return;
  }

  // Phase start (history only; DO NOT set state yet to avoid duplicate writes)
  await patchStatus(container, prefix, { runId, markers: { ...(status0.markers || {}) } }, { phase: "EvidenceDigest", note: "start" });

  try {
    const input = msg.input || msg.inputs || msg || {};
    const supplierWebsiteRaw   = (input.supplier_website || input.company_website || input.prospect_website || "").trim();
    const supplierLinkedInRaw  = (input.supplier_linkedin || input.company_linkedin || input.prospect_linkedin || "").trim();
    const supplierCompany      = (input.supplier_company || input.company_name || input.prospect_company || "").trim();
    const supplierWebsite      = toHttps(supplierWebsiteRaw);
    const supplierLinkedIn     = supplierLinkedInRaw ? toHttps(supplierLinkedInRaw) : "";

    // 1) Site crawl (bounded)
    let siteGraph = { pages: [], links: [] };
    if (supplierWebsite) {
      try { siteGraph = await crawlSiteGraph(supplierWebsite, MAX_SITE_PAGES, FETCH_TIMEOUT_MS); }
      catch (e) { context.log.warn("[evidence] crawlSiteGraph failed", String(e?.message || e)); }
    }
    const siteJson = siteGraph.pages.length ? [siteGraph.pages[0]] : [];
    await putJson(container, `${prefix}site.json`, siteJson);

    // 1a) Products (homepage snippet)
    try {
      const html = siteGraph.pages?.[0]?.snippet || "";
      const products = await extractProductsFromSite(html);
      await putJson(container, `${prefix}products.json`, { products });
    } catch (e) { context.log.warn("[evidence] product extraction skipped", String(e?.message || e)); }

    // 2) LinkedIn reference
    const li = supplierLinkedIn
      ? [{ url: supplierLinkedIn, host: hostnameOf(supplierLinkedIn), fetchedAt: nowISO(), type: "link", note: "LinkedIn stored as reference only." }]
      : [];
    await putJson(container, `${prefix}linkedin.json`, li);

    // 3) Case study discovery (same host)
    let pdfExtracts = [];
    if (siteGraph.pages.length) {
      try {
        const host = siteGraph.pages[0].host || hostnameOf(supplierWebsite);
        const candidateLinks = await collectCaseStudyLinks(siteGraph.pages, siteGraph.links, host);
        pdfExtracts = await fetchCaseStudySummaries(candidateLinks);
      } catch (e) { context.log.warn("[evidence] case study discovery failed", String(e?.message || e)); }
    }
    await putJson(container, `${prefix}pdf_extracts.json`, pdfExtracts);

    // 4) Directories placeholder
    await putJson(container, `${prefix}directories.json`, []);

    // 5) CSV → csv_normalized.json
    let csvText = null;
    let chosenCsvName = null;

    // Inline CSV (highest priority)
    if (typeof input.csvText === "string" && input.csvText.trim()) {
      csvText = input.csvText;
      chosenCsvName = (typeof input.csvFilename === "string" && input.csvFilename.trim()) ? input.csvFilename.trim() : "inline";
    }
    // Blob CSV under prefix
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

    let csvCanonical = {
      selected_industry: null,
      industry_mode: "agnostic",
      signals: { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] },
      global_signals: { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] },
      meta: { rows: 0, source: null, csv_has_multiple_sectors: false }
    };

    if (csvText && csvText.trim()) {
      const rows = parseCsvLoose(csvText);
      const agg = normalizeCsv(rows);

      const industries = Array.isArray(agg.industries) ? agg.industries : [];
      const csvHasMultipleSectors = industries.filter(x => x && x !== "Unknown").length > 1;
      const userSelectedIndustry = (input.selected_industry || input.campaign_industry || input.company_industry || input.industry || "").trim().toLowerCase();
      const csvIndustry = (agg.dominant_industry || "").trim().toLowerCase();
      const selectedIndustry = resolveIndustry({ userIndustry: userSelectedIndustry, csvIndustry, csvHasMultipleSectors });

      const allBlockers = [], allNeeds = [], allPurchases = [];
      for (const ind of industries) {
        const pack = agg.by_industry[ind] || {};
        allBlockers.push(...(Array.isArray(pack.TopBlockers) ? pack.TopBlockers : []));
        allNeeds.push(...(Array.isArray(pack.TopNeedsSupplier) ? pack.TopNeedsSupplier : []));
        allPurchases.push(...(Array.isArray(pack.TopPurchases) ? pack.TopPurchases : []));
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

      csvCanonical = {
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

    await putJson(container, `${prefix}csv_normalized.json`, csvCanonical);

    // 6) Compose evidence_log.json
    let evidenceLog = [];

    if (Number(csvCanonical?.meta?.rows || 0) > 0) {
      evidenceLog.push(csvSummaryEvidence(csvCanonical, input, prefix, container.url));
    }
    {
      const item = csvSignalsEvidence(csvCanonical, prefix, container.url);
      if (item) evidenceLog.push(item);
    }
    {
      const ident = supplierIdentityEvidence(
        supplierCompany,
        supplierWebsite,
        `${container.url}/${prefix}site.json`
      );
      if (ident) evidenceLog.push(ident);

      const notes = (input?.notes || "").trim();
      if (notes) {
        const rel = `${prefix}notes.txt`;
        try { await putText(container, rel, notes); } catch {}
        evidenceLog.push({
          claim_id: "TEMP", source_type: "Directory", title: "User notes (must integrate)",
          url: `${container.url}/${rel}`, summary: addCitation(notes.length > 240 ? `${notes.slice(0,240)}…` : notes, "Directory"), quote: ""
        });
      }

      const competitors = Array.isArray(input?.relevant_competitors)
        ? input.relevant_competitors.map(s => String(s || "").trim()).filter(Boolean).slice(0, 8)
        : [];
      if (competitors.length) {
        const rel = `${prefix}competitors.json`;
        try { await putJson(container, rel, { competitors }); } catch {}
        evidenceLog.push({
          claim_id: "TEMP", source_type: "Directory", title: "Competitors (user-supplied)",
          url: `${container.url}/${rel}`, summary: addCitation(`${competitors.length} competitor(s): ${competitors.join(", ")}`, "Directory"), quote: ""
        });
      }
    }
    {
      // Case studies / pdf extracts
      let pdfs = [];
      try { const v = await getJson(container, `${prefix}pdf_extracts.json`); if (Array.isArray(v)) pdfs = v; } catch {}
      for (const ex of (pdfs || []).slice(0, MAX_CASESTUDIES)) {
        const url = ex?.url ? String(ex.url).trim() : "";
        if (!/^https:\/\//i.test(url)) continue;
        const title = (ex?.title && String(ex.title).trim()) || "Case study";
        const quote = (ex?.quote && String(ex.quote).trim()) || "";
        const summaryText =
          (ex?.summary && String(ex.summary).trim()) ||
          (ex?.snippet && String(ex.snippet).trim()) ||
          `${title} — key points extracted.`;
        evidenceLog.push({
          claim_id: "TEMP",
          source_type: ex?.type === "pdf" ? "PDF extract" : "Company site",
          title, url,
          summary: addCitation(summaryText, ex?.type === "pdf" ? "PDF extract" : "Company site"),
          quote
        });
      }
    }
    {
      const productsObj = await getJson(container, `${prefix}products.json`);
      const prodItem = siteProductsEvidence(productsObj?.products || [], supplierWebsite, container.url, prefix);
      if (prodItem) evidenceLog.push(prodItem);
    }
    if (supplierLinkedIn) evidenceLog.push(linkedinEvidence(supplierLinkedIn));
    {
      // LinkedIn search hooks
      const liHooks = [];
      const liCompanyUrl = liCompanySearch(supplierCompany || hostnameOf(supplierWebsite || ""));
      if (liCompanyUrl) liHooks.push(linkedinSearchEvidence({ url: liCompanyUrl, title: "LinkedIn — supplier/company posts" }));

      let productNames = [];
      try {
        const productsObj = await getJson(container, `${prefix}products.json`);
        productNames = Array.isArray(productsObj?.products) ? productsObj.products.slice(0, 5) : [];
      } catch {}
      for (const pName of productNames) {
        liHooks.push(linkedinSearchEvidence({ url: liProductSearch(pName, supplierCompany), title: `LinkedIn — "${pName}" mentions` }));
      }

      const competitors = Array.isArray(input?.relevant_competitors) ? input.relevant_competitors : [];
      for (const cName of competitors.slice(0, 6)) {
        liHooks.push(linkedinSearchEvidence({ url: liCompetitorSearch(cName), title: `LinkedIn — competitor: ${cName}` }));
      }
      for (const it of liHooks) if (it) evidenceLog.push(it);
    }
    if (supplierWebsite) {
      const host = hostnameOf(supplierWebsite) || supplierWebsite;
      evidenceLog.push({
        claim_id: "TEMP",
        source_type: "Company site",
        title: `${host} — homepage`,
        url: supplierWebsite,
        summary: addCitation(`Homepage snapshot captured for ${host}.`, "Company site"),
        quote: ""
      });
    }
    {
      const salesModel = String(input?.sales_model || input?.salesModel || input?.call_type || "").toLowerCase();
      if (salesModel === "direct" || salesModel === "partner") {
        const relSm = `${prefix}sales-model.txt`;
        const smNote = `Sales model selected: ${salesModel}. Evidence context recorded (no filtering applied).`;
        try { await putText(container, relSm, smNote); } catch {}
        evidenceLog.push({
          claim_id: "TEMP", source_type: "Directory", title: "Sales model context",
          url: `${container.url}/${relSm}`, summary: addCitation(smNote, "Directory"), quote: "Model discipline noted; evidence left intact."
        });
      }
    }

    // Final tidy → filter, dedupe, deterministic claim_ids
    evidenceLog = evidenceLog.filter(it => !isPlaceholderEvidence(it));
    evidenceLog = dedupeEvidence(evidenceLog);
    evidenceLog.forEach((it, i) => { it.claim_id = `CLM-${String(i + 1).padStart(3, "0")}`; });

    await putJson(container, `${prefix}evidence_log.json`, evidenceLog);

    // Single WRITE to state="EvidenceDigest" (phase completed) with markers
    const statusDone = await readStatus(container, prefix);
    statusDone.state   = "EvidenceDigest";
    statusDone.updatedAt = nowISO();
    statusDone.artifacts = {
      site: "site.json",
      products: "products.json",
      linkedin: "linkedin.json",
      pdf_extracts: "pdf_extracts.json",
      directories: "directories.json",
      csv: "csv_normalized.json",
      evidence_log: "evidence_log.json"
    };
    statusDone.evidence_counts = {
      csv_rows: Number(csvCanonical?.meta?.rows || 0),
      items_total: evidenceLog.length
    };
    statusDone.history = Array.isArray(statusDone.history) ? statusDone.history : [];
    statusDone.history.push({ at: nowISO(), phase: "EvidenceDigest", note: "completed", count: evidenceLog.length });
    statusDone.markers = { ...(statusDone.markers || {}), evidenceDigestCompleted: true };
    await putJson(container, `${prefix}status.json`, statusDone);

    // Single-shot notify (afterevidence) with marker
    if (!statusDone.markers.afterevidenceSent) {
      try {
        const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
        const qc = qs.getQueueClient(MAIN_QUEUE);
        await qc.createIfNotExists();
        const page = (input.page || "campaign");
        await qc.sendMessage(JSON.stringify({ op: "afterevidence", runId, page, prefix }));

        const st2 = await readStatus(container, prefix);
        st2.markers = { ...(st2.markers || {}), afterevidenceSent: true };
        await putJson(container, `${prefix}status.json`, st2);
      } catch (notifyErr) {
        context.log.warn("[evidence] notify afterevidence failed", String(notifyErr?.message || notifyErr));
      }
    }

    context.log("[evidence] completed", { runId, prefix, count: evidenceLog.length });
  } catch (err) {
    context.log.error("[evidence] failure", String(err?.message || err));
    try {
      const cur = (await readStatus(container, prefix)) || {};
      cur.state = "Failed";
      cur.failedAt = nowISO();
      cur.error = { code: "evidence_error", message: String(err?.message || err) };
      cur.history = Array.isArray(cur.history) ? cur.history : [];
      cur.history.push({ at: nowISO(), phase: "EvidenceDigest", note: "failed", error: String(err?.message || err) });
      await putJson(container, `${prefix}status.json`, cur);
    } catch { /* ignore */ }
  }
};
