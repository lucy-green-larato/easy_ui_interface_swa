/** api/lib/chdi.js — CH + Document Intelligence utilities 01-10-2025 
 *  - Normalizes Companies House numbers
 *  - Finds latest Accounts PDF via CH APIs (auth using CH_API_KEY)
 *  - OCRs PDF with Azure Document Intelligence (buffer-first for CH)
 *  - Optional Reader endpoint, with fallbacks
 *  - Extracts “Strategic Report” section and builds sentence snippets
 */
"use strict";
// --- tracing (no-op by default) ---
let __trace = () => {};
function setTrace(fn) { __trace = typeof fn === "function" ? fn : () => {}; }
function trace(event) {
  try { __trace({ at: new Date().toISOString(), ...event }); } catch { /* ignore */ }
}

/* ------------------------- ENV + CONSTANTS ------------------------- */

// Endpoint may be given with or without /formrecognizer; normalize once
const DI_ENDPOINT_RAW =
  (process.env.DI_ENDPOINT ||
   process.env.AZ_DI_ENDPOINT ||
   process.env.DOCUMENTINTELLIGENCE_ENDPOINT ||
   "").trim();

const DI_ENDPOINT = DI_ENDPOINT_RAW.replace(/\/+$/, "").replace(/\/formrecognizer$/i, "");
const NEEDS_FR_SEGMENT = !/\/formrecognizer(\/|$)/i.test(DI_ENDPOINT);
const DI_BASE = NEEDS_FR_SEGMENT ? `${DI_ENDPOINT}/formrecognizer` : DI_ENDPOINT;

// One canonical analyze URL (NO pages here)
const DI_ANALYZE_URL =
  `${DI_BASE}/documentModels/prebuilt-read:analyze?api-version=2023-07-31`;

// Page range config (e.g. "4-12", "4-8,10-12"); convert to array for DI body
const SR_PAGES_SPEC = (process.env.CH_SR_PAGES || process.env.CH_SR_PAGE_RANGE || "").trim();
const SR_PAGES = SR_PAGES_SPEC ? SR_PAGES_SPEC.split(",").map(s => s.trim()).filter(Boolean) : null;

const DI_KEY =
  process.env.DI_KEY ||
  process.env.AZ_DI_KEY ||
  process.env.DOCUMENTINTELLIGENCE_KEY || "";

const CH_API_BASE = (process.env.CH_API_BASE || "https://api.company-information.service.gov.uk").replace(/\/+$/, "");
const CH_DOC_API_BASE = (process.env.CH_DOC_API_BASE || "https://document-api.company-information.service.gov.uk").replace(/\/+$/, "");
const CH_API_KEY = process.env.CH_API_KEY || "";
const READER_BASE = (process.env.CH_STRATEGIC_TEXT_URL || process.env.CH_READER_ENDPOINT || "").replace(/\/+$/, "");

const USER_AGENT = `chdi/1.0 (+larato; node${process.version})`;

/* ------------------------- UTILS ------------------------- */

function debugLog(stage, data) {
  if (process.env.CH_DEBUG !== "1") return;
  try { console.log(`CH_PROBE ${JSON.stringify({ stage, ...(data || {}) })}`); } catch {}
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function fetchWithTimeout(url, opts = {}, timeoutMs = 15000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const headers = { "User-Agent": USER_AGENT, ...(opts?.headers || {}) };
    return await fetch(url, { ...(opts || {}), headers, signal: ctrl.signal });
  } catch (e) {
    try { debugLog("fetch.error", { url, timeoutMs, error: String(e?.message || e) }); } catch {}
    return {
      ok: false, status: 0, headers: new Map(),
      async json(){ return {}; }, async text(){ return ""; }, async arrayBuffer(){ return new ArrayBuffer(0); }
    };
  } finally { clearTimeout(timer); }
}
async function fetchJsonWithTimeout(url, headers, timeoutMs = 15000) {
  const res = await fetchWithTimeout(url, { headers: { ...(headers || {}), Accept: "application/json" } }, timeoutMs);
  if (!res.ok) return null;
  try { return await res.json(); } catch { return null; }
}

/* ------------------------- CH HELPERS ------------------------- */

function normalizeCompanyNumber(raw) {
  let s = String(raw || "").toUpperCase().trim().replace(/\s+/g, "");
  if (!s) return "";
  if (/^\d+$/.test(s)) return s.padStart(8, "0");
  const m = s.match(/^([A-Z]{1,3})(\d{1,7})$/);
  if (m) {
    const prefix = m[1]; let tail = m[2];
    if (tail.length < 6) tail = tail.padStart(6, "0");
    return prefix + tail;
  }
  return s;
}
function chAuthHeader() {
  if (!CH_API_KEY) return null;
  const token = Buffer.from(`${CH_API_KEY}:`).toString("base64");
  return { Authorization: `Basic ${token}` };
}
function ensureChContentUrl(href) {
  if (!href) return "";
  const abs = /^https?:\/\//i.test(href) ? href : `${CH_DOC_API_BASE}${href}`;
  if (/\/content(?:\/|$)/i.test(abs)) return abs.replace(/[?#].*$/, "");
  if (/\/document\/[^/]+/i.test(abs)) return abs.replace(/\/document\/([^/?#]+).*/, "/document/$1/content");
  return abs;
}
async function chFetchJson(url) {
  const hdr = chAuthHeader(); if (!hdr) return null;
  return fetchJsonWithTimeout(url, hdr, 15000);
}
async function chFindAccountsPdfUrl(companyNumber) {
  const num = encodeURIComponent(normalizeCompanyNumber(companyNumber));
  const fh = await chFetchJson(`${CH_API_BASE}/company/${num}/filing-history?items_per_page=100`);
  if (!fh || !Array.isArray(fh.items)) return null;
  const cand = fh.items.find(it => it && (it.category === "accounts" || /accounts/i.test(it.category || "")) && it.links?.document_metadata);
  if (!cand) return null;

  const hdr = chAuthHeader(); if (!hdr) return null;
  const docMeta = String(cand.links.document_metadata || "");
  const metaUrl = /^https?:\/\//i.test(docMeta) ? docMeta : `${CH_DOC_API_BASE}${docMeta}`;
  const meta = await fetchJsonWithTimeout(metaUrl, { ...(hdr || {}) }, 15000);
  if (!meta) return null;

  const resPdf = meta?.resources && (meta.resources["application/pdf"] || meta.resources["application/pdf; charset=utf-8"]);
  const href = (resPdf && (resPdf.url || resPdf.href)) || meta?.links?.document || meta?.links?.self;
  if (!href) return null;
  return ensureChContentUrl(href);
}
async function chDownloadPdfBuffer(pdfUrl) {
  const url = ensureChContentUrl(pdfUrl); if (!url) return null;
  const hdr = chAuthHeader(); if (!hdr) return null;
  const res = await fetchWithTimeout(url, { headers: { ...hdr, Accept: "application/pdf" } }, 25000);
  if (!res.ok) return null;
  const ab = await res.arrayBuffer();
  return Buffer.from(ab);
}

/* ------------------------- DI READ HELPERS ------------------------- */

async function diPoll(opLoc, headers, { timeoutMs = 60000, pollMs = 1200 } = {}) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    await sleep(pollMs);
    const res = await fetchWithTimeout(opLoc, { headers }, Math.max(5000, pollMs * 3));
    if (!res.ok) continue;
    const j = await res.json().catch(() => null);
    if (!j) continue;
    const status = String(j.status || "").toLowerCase();
    if (status === "succeeded") return j.analyzeResult || j;
    if (status === "failed" || status === "error") return null;
    if (res.status === 429) pollMs = Math.min(pollMs * 2, 5000);
  }
  return null;
}
function extractContentFromAnalyzeResult(result, maxLen) {
  let txt = result?.content || (result?.pages || []).map(p => p.content || "").join("\n");
  txt = (txt || "").replace(/^\uFEFF/, "").trim();
  if (typeof maxLen === "number" && txt.length > maxLen) txt = txt.slice(0, maxLen);
  return txt;
}

// URL-source (only for public files; CH PDFs usually need auth)
async function diReadTextFromUrl(pdfUrl, { timeoutMs = 45000, pollMs = 1200, maxLen = 2_000_000 } = {}) {
  if (!DI_ENDPOINT || !DI_KEY || !pdfUrl) return "";
  const headers = { "Ocp-Apim-Subscription-Key": DI_KEY, "Content-Type": "application/json" };
  const body = SR_PAGES ? { urlSource: pdfUrl, pages: SR_PAGES } : { urlSource: pdfUrl };

  const res = await fetchWithTimeout(DI_ANALYZE_URL, { method: "POST", headers, body: JSON.stringify(body) }, 30000);
  if (!res.ok) { debugLog("di.analyze.url.fail", { status: res.status }); return ""; }

  const opLoc = res.headers.get("operation-location") || res.headers.get("Operation-Location");
  if (!opLoc) return "";
  const result = await diPoll(opLoc, { "Ocp-Apim-Subscription-Key": DI_KEY }, { timeoutMs, pollMs });
  if (!result) return "";
  return extractContentFromAnalyzeResult(result, maxLen);
}

// Buffer path (preferred for CH PDFs which require auth)
async function diReadTextFromBuffer(pdfBuffer, { timeoutMs = 45000, pollMs = 1200, maxLen = 2_000_000 } = {}) {
  if (!DI_ENDPOINT || !DI_KEY || !pdfBuffer?.length) return "";
  const headers = { "Ocp-Apim-Subscription-Key": DI_KEY, "Content-Type": "application/pdf" };

  const res = await fetchWithTimeout(DI_ANALYZE_URL, { method: "POST", headers, body: pdfBuffer }, 30000);
  if (!res.ok) { debugLog("di.analyze.buf.fail", { status: res.status }); return ""; }

  const opLoc = res.headers.get("operation-location") || res.headers.get("Operation-Location");
  if (!opLoc) return "";
  const result = await diPoll(opLoc, { "Ocp-Apim-Subscription-Key": DI_KEY }, { timeoutMs, pollMs });
  if (!result) return "";
  return extractContentFromAnalyzeResult(result, maxLen);
}

/* ------------------------- SR EXTRACT + SNIPPET ------------------------- */

function extractStrategicReport(fullText) {
  if (!fullText) return "";
  const text = String(fullText).replace(/\r/g, "\n");
  const lc = text.toLowerCase();
  const startPatterns = [/\bthe?\s*strategic[\s\-–—]*report\b/i, /\bstrategic[\s\-–—]*report\b/i];
  const endPatterns = [
    /\bdirectors[\s’']?\s*report\b/i, /\bindependent\s+auditor/i, /\bcorporate\s+governance/i,
    /\bgovernance\s+statement\b/i, /\bremuneration\s+report\b/i, /\bstatement\s+of\s+directors/i,
    /\bprofit\s+and\s+loss\b/i, /\bbalance\s+sheet\b/i, /\bconsolidated\s+statement\b/i
  ];
  let start = -1;
  for (const re of startPatterns) { const m = lc.match(re); if (m && (start === -1 || m.index < start)) start = m.index; }
  if (start === -1) return "";
  let end = text.length;
  for (const re of endPatterns) {
    const m = lc.slice(start + 16).match(re);
    if (m) { const idx = (start + 16) + m.index; if (idx < end) end = idx; }
  }
  let section = text.slice(start, end);
  section = section.replace(/^\s*the?\s*strategic[\s\-–—]*report[^\n]*\n?/i, "");
  return section.replace(/\s+/g, " ").trim();
}
function sentenceSnippet(text, term, maxChars = 250) {
  if (!text || !term) return "";
  const hay = text;
  const idx = hay.toLowerCase().indexOf(String(term).toLowerCase());
  if (idx < 0) return "";
  const win = 600;
  const from = Math.max(0, idx - win);
  const to = Math.min(hay.length, idx + term.length + win);
  const windowText = hay.slice(from, to);
  const rel = idx - from;
  const left = windowText.slice(0, rel).lastIndexOf(". ");
  const right = windowText.slice(rel).indexOf(". ");
  let snip = windowText.slice(left >= 0 ? left + 2 : 0, right >= 0 ? rel + right + 2 : windowText.length).replace(/\s+/g, " ").trim();
  if (snip.length > maxChars) {
    const half = Math.floor(maxChars / 2);
    const cutFrom = Math.max(0, rel - half);
    const cutTo = Math.min(windowText.length, rel + (maxChars - (rel - cutFrom)));
    snip = windowText.slice(cutFrom, cutTo).replace(/\s+/g, " ").trim();
    if (cutFrom > 0) snip = "… " + snip;
    if (cutTo < windowText.length) snip = snip + " …";
  }
  return snip;
}

/* ------------------------- READER-FIRST STRATEGY ------------------------- */

function pickPdfUrlFromReaderPayload(j, companyNumber) {
  const enc = encodeURIComponent(companyNumber);
  const templ = process.env.CH_PDF_URL_TEMPLATE ? String(process.env.CH_PDF_URL_TEMPLATE).replace("{companyNumber}", enc) : "";
  const cand = [j?.pdfUrl, j?.url, j?.pdf?.url, templ].filter(Boolean);
  return cand.find(u => /\.pdf(\?|#|$)/i.test(String(u))) || "";
}

/*---------with debug statements------------*/
// Drop-in replacement with tracing (safe, no secrets logged)
async function tryGetReportText(companyNumber, { timeoutMs = 8000, maxLen = 200000 } = {}) {
  // --- tiny no-op tracer that can forward to a global hook if present ---
  const trace = (event) => {
    try {
      const fn = (globalThis && globalThis.__chdi_trace) || null;
      if (typeof fn === "function") fn({ at: new Date().toISOString(), ...event });
    } catch { /* ignore */ }
  };
  const redactUrl = (u) => {
    try {
      const url = new URL(u);
      ["key","code","sig","token","signature"].forEach(k => {
        if (url.searchParams.has(k)) url.searchParams.set(k, "REDACTED");
      });
      return url.toString();
    } catch { return u; }
  };
  const trimLen = (s) => (s && s.length > maxLen ? s.slice(0, maxLen) : s || "");

  const num = normalizeCompanyNumber(companyNumber);
  if (!num) return "";

  trace({
    stage: "reader.start",
    companyNumber: num,
    features: {
      READER_BASE: Boolean(READER_BASE),
      DI: Boolean(DI_ENDPOINT && DI_KEY),
      CH: Boolean(CH_API_KEY)
    }
  });

  // (1) Reader service (if configured)
  if (READER_BASE) {
    const cand = [
      `${READER_BASE}/${encodeURIComponent(num)}`,
      `${READER_BASE}?id=${encodeURIComponent(num)}`,
      `${READER_BASE}?companyNumber=${encodeURIComponent(num)}`
    ];
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      for (const rawUrl of cand) {
        const url = rawUrl;
        try {
          trace({ stage: "reader:req", url: redactUrl(url), method: "GET" });
          const res = await fetch(url, {
            headers: {
              Accept: "application/json, text/plain, application/pdf",
              "User-Agent": USER_AGENT
            },
            signal: ctrl.signal
          });
          const ctype = (res.headers.get("content-type") || "").toLowerCase();
          trace({ stage: "reader:resp", url: redactUrl(url), status: res.status, contentType: ctype });

          if (!res.ok) {
            // Only sample body on errors, small slice, no secrets
            let sample = "";
            try { sample = (await res.text()).slice(0, 500); } catch {}
            trace({ stage: "reader:respBodySample", url: redactUrl(url), status: res.status, sample });
            if (res.status >= 500) break; // try next path only if not server error
            continue;
          }

          if (ctype.includes("application/json")) {
            const j = await res.json().catch(() => ({}));
            let txt =
              j?.text || j?.body || j?.content ||
              j?.data?.text || j?.data?.body || j?.data?.content || "";
            txt = (txt || "").replace(/^\uFEFF/, "").trim();

            if (txt && !/<!doctype html>|<html[\s>]/i.test(txt)) {
              trace({ stage: "reader:text", bytes: txt.length });
              return trimLen(txt);
            }

            const rawPdfUrl = pickPdfUrlFromReaderPayload(j, num);
            const pdfUrl = ensureChContentUrl(rawPdfUrl);
            trace({ stage: "reader:pdfPicked", url: pdfUrl ? redactUrl(pdfUrl) : null, hadPdf: Boolean(pdfUrl) });

            if (pdfUrl) {
              // CH buffer-first
              trace({ stage: "ch:file:req", url: redactUrl(pdfUrl), via: "buffer" });
              const buf = await chDownloadPdfBuffer(pdfUrl);
              trace({ stage: "ch:file:resp", url: redactUrl(pdfUrl), ok: Boolean(buf && buf.length), bytes: buf?.length || 0 });
              if (buf?.length) {
                trace({ stage: "di:begin", via: "buffer" });
                const byBuf = await diReadTextFromBuffer(buf);
                trace({ stage: "di:done", via: "buffer", hasText: Boolean(byBuf), bytes: byBuf?.length || 0 });
                if (byBuf) return trimLen(byBuf);
              }
              // DI by URL (only if public)
              trace({ stage: "di:begin", via: "url", url: redactUrl(pdfUrl) });
              const byUrl = await diReadTextFromUrl(pdfUrl);
              trace({ stage: "di:done", via: "url", hasText: Boolean(byUrl), bytes: byUrl?.length || 0 });
              if (byUrl) return trimLen(byUrl);
            }
            continue;
          }

          if (ctype.includes("application/pdf")) {
            const srcUrl = ensureChContentUrl(url);
            trace({ stage: "reader:pdfDirect", url: redactUrl(srcUrl) });

            // CH buffer-first
            trace({ stage: "ch:file:req", url: redactUrl(srcUrl), via: "buffer" });
            const buf = await chDownloadPdfBuffer(srcUrl);
            trace({ stage: "ch:file:resp", url: redactUrl(srcUrl), ok: Boolean(buf && buf.length), bytes: buf?.length || 0 });
            if (buf?.length) {
              trace({ stage: "di:begin", via: "buffer" });
              const byBuf = await diReadTextFromBuffer(buf);
              trace({ stage: "di:done", via: "buffer", hasText: Boolean(byBuf), bytes: byBuf?.length || 0 });
              if (byBuf) return trimLen(byBuf);
            }
            // DI by URL (only if public)
            trace({ stage: "di:begin", via: "url", url: redactUrl(srcUrl) });
            const byUrl = await diReadTextFromUrl(srcUrl);
            trace({ stage: "di:done", via: "url", hasText: Boolean(byUrl), bytes: byUrl?.length || 0 });
            if (byUrl) return trimLen(byUrl);

            continue;
          }

          // Fallback: treat as text
          const text = (await res.text().catch(() => "")).replace(/^\uFEFF/, "").trim();
          if (text && !/<!doctype html>|<html[\s>]/i.test(text)) {
            trace({ stage: "reader:text", bytes: text.length });
            return trimLen(text);
          }
        } catch (e) {
          trace({ stage: "reader:error", url: redactUrl(url), message: String(e && e.message || e) });
          // try next candidate
        }
      }
    } finally { clearTimeout(t); }
  }

  // (2) CH fallback → PDF → DI (buffer-first)
  trace({ stage: "ch:findPdf:req", companyNumber: num });
  const raw = await chFindAccountsPdfUrl(num);
  const pdfUrl = ensureChContentUrl(raw);
  trace({ stage: "ch:findPdf:resp", companyNumber: num, url: pdfUrl ? redactUrl(pdfUrl) : null, hadPdf: Boolean(pdfUrl) });

  if (pdfUrl) {
    // buffer-first
    trace({ stage: "ch:file:req", url: redactUrl(pdfUrl), via: "buffer" });
    const buf = await chDownloadPdfBuffer(pdfUrl);
    trace({ stage: "ch:file:resp", url: redactUrl(pdfUrl), ok: Boolean(buf && buf.length), bytes: buf?.length || 0 });
    if (buf?.length) {
      trace({ stage: "di:begin", via: "buffer" });
      const byBuf = await diReadTextFromBuffer(buf);
      trace({ stage: "di:done", via: "buffer", hasText: Boolean(byBuf), bytes: byBuf?.length || 0 });
      if (byBuf) return trimLen(byBuf);
    }
    // DI by URL (only if public)
    trace({ stage: "di:begin", via: "url", url: redactUrl(pdfUrl) });
    const byUrl = await diReadTextFromUrl(pdfUrl);
    trace({ stage: "di:done", via: "url", hasText: Boolean(byUrl), bytes: byUrl?.length || 0 });
    if (byUrl) return trimLen(byUrl);
  }

  trace({ stage: "reader.none", companyNumber: num });
  return "";
}
/* ------------------------- EXPORTS ------------------------- */

module.exports = {
  tryGetReportText,
  extractStrategicReport,
  sentenceSnippet,
  normalizeCompanyNumber,
  chFindAccountsPdfUrl,
  chDownloadPdfBuffer,
  diReadTextFromUrl,
  diReadTextFromBuffer,
};
