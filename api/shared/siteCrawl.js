// /api/shared/siteCrawl.js 17-11-2025 v3
//
// Responsibilities:
//  - Resilient HTTP GET wrapper
//  - Minimal site "crawl" (homepage-only graph for now)
//  - Link extraction helpers
//  - Case study discovery (HTML + PDF)
//  - Product / claim extraction from HTML
//
// No Azure SDK usage here. Pure JS + built-in fetch.
//
// Exports:
//  - httpGet(url, timeoutMs?)
//  - crawlSiteGraph(rootUrl, maxPages?, timeoutMs?)
//  - extractLinks(html, baseUrl)
//  - sameHost(a, b)
//  - isCaseStudyUrl(url)
//  - collectCaseStudyLinks(pages, allLinks, rootHost)
//  - fetchCaseStudySummaries(urls)
//  - parseProductClaims(html)
//  - extractProductsFromSite(html)
//  - hostnameOf(url)
//  - toHttps(url)

// ---------- Config ----------

const RAW_FETCH_TIMEOUT_MS = Number(process.env.HTTP_FETCH_TIMEOUT_MS);
const FETCH_TIMEOUT_MS =
  Number.isFinite(RAW_FETCH_TIMEOUT_MS) &&
    RAW_FETCH_TIMEOUT_MS >= 1000 &&
    RAW_FETCH_TIMEOUT_MS <= 60000
    ? RAW_FETCH_TIMEOUT_MS
    : 8000;

const RAW_MAX_CASESTUDIES = Number(process.env.MAX_CASESTUDIES);
const MAX_CASESTUDIES =
  Number.isFinite(RAW_MAX_CASESTUDIES) &&
    RAW_MAX_CASESTUDIES >= 1 &&
    RAW_MAX_CASESTUDIES <= 32
    ? RAW_MAX_CASESTUDIES
    : 8;

// ---------- Small utils ----------

function nowIso() {
  return new Date().toISOString();
}

function hostnameOf(u) {
  try {
    return new URL(u).hostname.replace(/^www\./, "");
  } catch {
    return null;
  }
}

function toAbsUrl(base, href) {
  try {
    return new URL(href, base).toString();
  } catch {
    return null;
  }
}

// Enforce https scheme for all evidence URLs
function toHttps(u) {
  if (!u) return u;
  const s = String(u).trim();
  if (!s) return s;

  // Already https
  if (/^https:\/\//i.test(s)) return s;

  // http → https
  if (/^http:\/\//i.test(s)) return s.replace(/^http:\/\//i, "https://");

  // Protocol-relative //example.com
  if (/^\/\//.test(s)) return `https:${s}`;

  return `https://${s}`;
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
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
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

// ---------- Simple link extraction ----------

function extractLinks(html, baseUrl) {
  if (!html || !baseUrl) return [];
  const hrefs = new Set();

  // Support href="...", href='...', and href=bare
  const re = /href\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const rawHref = m[1] || m[2] || m[3] || "";
    if (!rawHref) continue;
    const abs = toAbsUrl(baseUrl, rawHref);
    if (!abs) continue;
    const noFragment = abs.split("#")[0];
    const httpsUrl = toHttps(noFragment);
    if (httpsUrl) hrefs.add(httpsUrl);
  }

  return Array.from(hrefs);
}

function sameHost(a, b) {
  const ha = hostnameOf(a);
  const hb = hostnameOf(b);
  return !!ha && ha === hb;
}

// ---------- Case study URL heuristics ----------

function isCaseStudyUrl(u) {
  const low = String(u || "").toLowerCase();

  // Strong path-based signals (high precision)
  if (low.includes("/case-studies")) return true;
  if (low.includes("/case-study")) return true;
  if (low.includes("/customers/")) return true;
  if (low.includes("/customer/")) return true;
  if (low.includes("/success-stories")) return true;
  if (low.includes("/success-story")) return true;
  if (low.includes("/stories/")) return true;

  // PDFs are still candidates, but not everything with "results" etc.
  if (low.endsWith(".pdf")) return true;

  return false;
}


// Collect candidate case-study URLs from pages + global link set
function collectCaseStudyLinks(pages, allLinks, rootHost) {
  const candidates = new Set();
  const pagesArr = Array.isArray(pages) ? pages : [];
  const linksArr = Array.isArray(allLinks) ? allLinks : [];

  const rootUrl = rootHost ? `https://${rootHost}` : null;

  // ---------------------------------------------------------------------------
  // Deterministic "known entrypoints"
  // These massively improve recall when:
  //  - homepage HTML is truncated
  //  - links are JS-rendered
  //  - nav links are not in the first 120k of HTML
  // ---------------------------------------------------------------------------
  if (rootUrl) {
    const known = [
      `${rootUrl}/case-studies`,
      `${rootUrl}/case-studies/`,
      `${rootUrl}/case-study`,
      `${rootUrl}/case-study/`,
      `${rootUrl}/customers`,
      `${rootUrl}/customers/`,
      `${rootUrl}/customer-stories`,
      `${rootUrl}/customer-stories/`,
      `${rootUrl}/success-stories`,
      `${rootUrl}/success-stories/`,
      `${rootUrl}/stories`,
      `${rootUrl}/stories/`,
      `${rootUrl}/resources/case-studies`,
      `${rootUrl}/resources/case-studies/`
    ];

    for (const k of known) candidates.add(toHttps(k));
  }

  // ---------------------------------------------------------------------------
  // Pull candidates from crawled pages (including non-homepage pages if available)
  // ---------------------------------------------------------------------------
  for (const p of pagesArr) {
    if (!p || !p.url) continue;

    const pageUrl = toHttps(p.url);
    if (pageUrl && (!rootUrl || sameHost(pageUrl, rootUrl))) {
      // Page itself may be a case study landing page
      if (isCaseStudyUrl(pageUrl)) candidates.add(pageUrl);

      // Links inside page snippet
      const pageLinks = extractLinks(p.snippet || "", pageUrl);
      for (const l of pageLinks) {
        const u = toHttps(l);
        if (!u) continue;
        if ((!rootUrl || sameHost(u, rootUrl)) && isCaseStudyUrl(u)) {
          candidates.add(u);
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Pull candidates from the global link set (usually from homepage extraction)
  // ---------------------------------------------------------------------------
  for (const l of linksArr) {
    const url = toHttps(l);
    if (!url) continue;
    if ((!rootUrl || sameHost(url, rootUrl)) && isCaseStudyUrl(url)) {
      candidates.add(url);
    }
  }

  // ---------------------------------------------------------------------------
  // Deterministic ordering:
  //  - prefer likely case-study hubs first (case-studies > customers > stories > pdf)
  //  - then stable lexicographic ordering
  // This improves hit-rate under MAX_CASESTUDIES caps.
  // ---------------------------------------------------------------------------
  const score = (u) => {
    const low = String(u || "").toLowerCase();
    if (low.includes("/case-studies")) return 0;
    if (low.includes("/case-study")) return 1;
    if (low.includes("/customers")) return 2;
    if (low.includes("/customer-stories")) return 3;
    if (low.includes("/success-stories")) return 4;
    if (low.includes("/stories")) return 5;
    if (low.endsWith(".pdf")) return 9;
    return 50;
  };

  return Array.from(candidates)
    .filter(Boolean)
    .filter((u) => (!rootUrl || sameHost(u, rootUrl)))
    .sort((a, b) => {
      const sa = score(a);
      const sb = score(b);
      if (sa !== sb) return sa - sb;
      return a.localeCompare(b);
    })
    .slice(0, MAX_CASESTUDIES);
}

// Fetch a small summary for each candidate case-study URL
async function fetchCaseStudySummaries(urls) {
  const out = [];
  const list = Array.isArray(urls)
    ? urls.slice(0, MAX_CASESTUDIES)
    : [];

  // deterministic failure capture (for auditability)
  const failures = [];

  for (const raw of list) {
    const url = toHttps(raw);
    if (!url) continue;

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

    if (!res.ok) {
      // DO NOT skip silently: capture the failure deterministically
      failures.push({
        url,
        host: hostnameOf(url),
        fetchedAt: nowIso(),
        type: "fetch_error",
        title: "Case study fetch failed",
        status: Number(res.status || 0),
        ok: false,
        quote: String(res.text || "").slice(0, 240) // keep short; deterministic
      });
      continue;
    }

    const html = String(res.text || "").slice(0, 120_000);
    const titleMatch = html.match(/<title[^>]*>([^<]{3,200})<\/title>/i);
    const title = titleMatch
      ? titleMatch[1].trim()
      : (url.split("/").slice(-1)[0] || "Case study");

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

  // If we found at least one case study, return only successes (no noise).
  if (out.length > 0) return out;

  // If we found none, return the failures so downstream can see WHY.
  // This is the "not skip silently" guarantee.
  if (failures.length > 0) return failures;

  // If we had no candidates, we genuinely have nothing to report.
  return out;
}

// ---------- Product / claim extraction ----------

// Parse product/solution claims from HTML (H1/H2/LI → short bullet texts)
function parseProductClaims(html) {
  if (!html || typeof html !== "string") return [];
  const claims = [];

  const h1 = (html.match(/<h1[^>]*>(.*?)<\/h1>/i) || [, ""])[1];
  if (h1) claims.push(String(h1).replace(/<[^>]+>/g, "").trim());

  const h2s = [...html.matchAll(/<h2[^>]*>(.*?)<\/h2>/gim)].map((m) =>
    String(m[1] || "").replace(/<[^>]+>/g, "").trim()
  );
  for (const h of h2s) if (h) claims.push(h);

  const lis = [...html.matchAll(/<li[^>]*>(.*?)<\/li>/gim)].map((m) =>
    String(m[1] || "").replace(/<[^>]+>/g, "").trim()
  );
  for (const li of lis) if (li && li.length <= 200) claims.push(li);

  // De-dupe + trim + cap
  const seen = new Set();
  const out = [];
  for (const c of claims) {
    const t = String(c || "").replace(/\s+/g, " ").trim();
    if (!t) continue;
    const key = t.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(t);
    if (out.length >= 12) break; // safety cap per page
  }
  return out;
}

// Lightweight product-name candidates from homepage HTML
function extractProductsFromSite(html) {
  if (!html || typeof html !== "string") return [];
  const lines = html.split(/\r?\n/).slice(0, 2000); // safety cap
  const out = new Set();

  for (const rawLine of lines) {
    const line = String(rawLine || "");
    if (/(<h1|<h2|<h3|<li|product|solutions|services)/i.test(line)) {
      const m = line.match(/>([^<>]{3,120})</);
      if (m) {
        const val = String(m[1] || "").trim();
        if (
          val &&
          !/^(home|about|contact|login|log in|sign in|support|learn|blog|cookie|privacy|terms|partners|resources)$/i.test(
            val
          ) &&
          !/^(read more|learn more)$/i.test(val)
        ) {
          out.add(val);
        }
      }
    }
  }

  return Array.from(out);
}

// ---------- Minimal crawlSiteGraph stub (Phase 1) ----------
//
// For now we only fetch the homepage and treat it as a single-page graph.
// maxPages is accepted for future compatibility but not used yet.

async function crawlSiteGraph(rootUrl, maxPages = 1, timeoutMs = FETCH_TIMEOUT_MS) {
  const root = toHttps(rootUrl);
  if (!root) return { pages: [], links: [] };

  const rootHost = hostnameOf(root);
  if (!rootHost) return { pages: [], links: [] };

  // Deterministic queue
  const queue = [root];
  const visited = new Set();
  const pages = [];
  const allLinks = new Set();

  // Helper: only crawl same-host https pages, no fragments, no mailto/tel
  const normaliseInternal = (u) => {
    try {
      const url = new URL(toHttps(u));
      if (url.protocol !== "https:") return null;
      if (hostnameOf(url.toString()) !== rootHost) return null;
      if (/^(mailto:|tel:)/i.test(url.toString())) return null;
      url.hash = "";
      return url.toString();
    } catch {
      return null;
    }
  };

  // Deterministic priority score: crawl likely “case study / customer / resource” pages first
  const priority = (u) => {
    const low = String(u).toLowerCase();
    if (low.includes("/case-studies")) return 0;
    if (low.includes("/case-study")) return 1;
    if (low.includes("/customers")) return 2;
    if (low.includes("/customer")) return 3;
    if (low.includes("/success")) return 4;
    if (low.includes("/stories")) return 5;
    if (low.includes("/resources")) return 6;
    if (low.includes("/insights")) return 7;
    if (low.includes("/blog")) return 8;
    return 50;
  };

  // Always enforce at least 1 page (homepage)
  const CAP = Math.max(1, Math.min(Number(maxPages) || 1, 12));

  while (queue.length && pages.length < CAP) {
    const url = queue.shift();
    const norm = normaliseInternal(url);
    if (!norm) continue;
    if (visited.has(norm)) continue;
    visited.add(norm);

    const res = await httpGet(norm, timeoutMs);
    if (!res.ok) continue;

    const html = String(res.text || "").slice(0, 120_000);

    const titleMatch = html.match(/<title[^>]*>([^<]{3,200})<\/title>/i);
    const title = titleMatch ? titleMatch[1].trim() : norm;

    const links = extractLinks(html, norm);

    // Collect global link set
    for (const l of links) {
      const internal = normaliseInternal(l);
      if (internal) allLinks.add(internal);
    }

    pages.push({
      url: norm,
      title,
      snippet: html,
      host: rootHost
    });

    // After fetching homepage, seed queue with high-value internal pages
    // Deterministic: stable sort by priority then lexicographic.
    if (pages.length === 1) {
      const seed = Array.from(allLinks)
        .filter((u) => {
          const low = u.toLowerCase();
          // Avoid wasting crawl budget on obvious low-value pages
          if (/\/(privacy|terms|cookie|login|sign-in|sign_in|wp-admin)\b/i.test(low)) return false;
          return true;
        })
        .sort((a, b) => {
          const pa = priority(a);
          const pb = priority(b);
          if (pa !== pb) return pa - pb;
          return a.localeCompare(b);
        });

      // Add top candidates to queue up to CAP
      for (const u of seed) {
        if (queue.length + pages.length >= CAP) break;
        if (!visited.has(u)) queue.push(u);
      }
    }
  }

  return {
    pages,
    links: Array.from(allLinks)
  };
}

module.exports = {
  httpGet,
  crawlSiteGraph,
  extractLinks,
  sameHost,
  isCaseStudyUrl,
  collectCaseStudyLinks,
  fetchCaseStudySummaries,
  parseProductClaims,
  extractProductsFromSite,
  hostnameOf,
  toHttps
};
