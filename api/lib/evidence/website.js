// api/lib/evidence/website.js 24-10-2025 v2
// Fetches the homepage + a few obvious child pages and extracts readable text.
// Uses Node 18/20 global fetch (no undici import), resilient timeouts and headers.

const cheerio = require("cheerio");

const MAX_PAGES = 4;                 // homepage + up to 3 obvious links
const MAX_TEXT_PER_PAGE = 12000;
const FETCH_TIMEOUT_MS = 15000;      // per request
const DEFAULT_HEADERS = {
  "User-Agent": "LaratoEvidenceBot/1.0 (+https://larato.co.uk)",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
};

function absolute(base, href) {
  try { return new URL(href, base).toString(); } catch { return null; }
}

function extractReadable(html, baseUrl) {
  const $ = cheerio.load(html);

  // Remove boilerplate/noise
  $("nav, header, footer, script, style, noscript, svg, form, iframe").remove();

  // Prefer <main>, fallback to body
  const root = $("main").length ? $("main") : $("body");

  const title = ($("title").first().text() || $("h1").first().text() || "").trim();

  // Gather headings & body text
  const parts = [];
  root.find("h1,h2,h3,p,li").each((_, el) => {
    const t = $(el).text().replace(/\s+/g, " ").trim();
    if (t) parts.push(t);
  });

  const text = parts.join("\n").slice(0, MAX_TEXT_PER_PAGE);
  return { url: baseUrl, title, text };
}

async function fetchWithTimeout(u, opts = {}) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(new Error("fetch_timeout")), opts.timeoutMs ?? FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(u, {
      redirect: "follow",
      headers: { ...DEFAULT_HEADERS, ...(opts.headers || {}) },
      signal: controller.signal
    });
    return res;
  } finally {
    clearTimeout(t);
  }
}

function pickChildLinks(html, baseUrl) {
  const $ = cheerio.load(html);
  const candidates = new Set();

  $("a[href]").each((_, a) => {
    const href = ($(a).attr("href") || "").trim();
    if (!href) return;
    const abs = absolute(baseUrl, href);
    if (!abs) return;

    // stay on the same origin
    let sameOrigin = false;
    try { sameOrigin = new URL(abs).origin === new URL(baseUrl).origin; } catch { /* ignore */ }
    if (!sameOrigin) return;

    // obvious sections only
    if (/(about|company|leadership|team|services|solutions|products|industr(y|ies)|news|insights|events)/i.test(abs)) {
      candidates.add(abs.split("#")[0]);
    }
  });

  return Array.from(candidates).slice(0, MAX_PAGES - 1);
}

async function buildWebsitePack(rootUrl) {
  try {
    // HOME
    const homeRes = await fetchWithTimeout(rootUrl);
    if (!homeRes.ok) return [];
    const homeCt = homeRes.headers.get("content-type") || "";
    if (!/text\/html/i.test(homeCt)) return [];
    const homeHtml = await homeRes.text();
    const pack = [];
    const home = extractReadable(homeHtml, homeRes.url || rootUrl);
    pack.push(home);

    // CHILDREN
    const children = pickChildLinks(homeHtml, homeRes.url || rootUrl);
    for (const u of children) {
      try {
        const r = await fetchWithTimeout(u);
        if (!r.ok) continue;
        const ct = r.headers.get("content-type") || "";
        if (!/text\/html/i.test(ct)) continue;
        const html = await r.text();
        pack.push(extractReadable(html, r.url || u));
        if (pack.length >= MAX_PAGES) break;
      } catch { /* skip bad child */ }
    }
    return pack;
  } catch {
    return [];
  }
}

module.exports = { buildWebsitePack };
