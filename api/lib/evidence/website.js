// api/lib/evidence/website.js 24-10-2025 v4
// Fetches the homepage + a few obvious child pages and extracts readable text.
// Uses Node 18/20 global fetch (no undici import), resilient timeouts and headers.
// Reads runtime config from Function App environment variables with sensible defaults.
//
// Config (all optional):
//   WEBSITE_MAX_PAGES                (int, default 4)          // homepage + N-1 child pages
//   WEBSITE_MAX_TEXT_PER_PAGE       (int, default 12000)      // max chars kept per page
//   WEBSITE_FETCH_TIMEOUT_MS        (int, default 15000)      // per-request timeout
//   WEBSITE_SAME_ORIGIN_ONLY        (bool "1"/"true", default true)
//   WEBSITE_SECTION_REGEX           (regex source, default /(about|company|leadership|team|services|solutions|products|industr(y|ies)|news|insights|events)/i)
//   WEBSITE_USER_AGENT              (string, default "LaratoEvidenceBot/1.0 (+https://larato.co.uk)")
//   WEBSITE_ACCEPT_HEADER           (string, default "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
//   WEBSITE_EXTRA_HEADERS_JSON      (JSON object string, merged into headers)
//   WEBSITE_CHILD_CONCURRENCY       (int, default = WEBSITE_MAX_PAGES-1, >=1)  // cap parallel child fetches

const cheerio = require("cheerio");

/* -------------------- env + defaults -------------------- */
function envInt(name, def) {
  const v = process.env[name];
  if (v == null || v === "") return def;
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : def;
}
function envBool(name, def) {
  const v = (process.env[name] || "").toString().trim().toLowerCase();
  if (!v) return def;
  return v === "1" || v === "true" || v === "yes";
}
function envStr(name, def) {
  const v = process.env[name];
  return (v == null || v === "") ? def : String(v);
}
function envJson(name, def) {
  const raw = process.env[name];
  if (!raw) return def;
  try { const obj = JSON.parse(raw); return (obj && typeof obj === "object") ? obj : def; }
  catch { return def; }
}
function safeRegex(source, def) {
  try {
    // If the env contains slashes or flags (e.g. "/foo/i"), strip them safely:
    const m = String(source).match(/^\/(.*)\/([gimsuy]*)$/);
    if (m) return new RegExp(m[1], m[2]);
    return new RegExp(String(source), "i");
  } catch {
    return def;
  }
}

const CONFIG = {
  MAX_PAGES: envInt("WEBSITE_MAX_PAGES", 4),
  MAX_TEXT_PER_PAGE: envInt("WEBSITE_MAX_TEXT_PER_PAGE", 12000),
  FETCH_TIMEOUT_MS: envInt("WEBSITE_FETCH_TIMEOUT_MS", 15000),
  SAME_ORIGIN_ONLY: envBool("WEBSITE_SAME_ORIGIN_ONLY", true),
  SECTION_RE: safeRegex(
    envStr(
      "WEBSITE_SECTION_REGEX",
      "(about|company|leadership|team|services|solutions|products|industr(y|ies)|news|insights|events)"
    ),
    /(about|company|leadership|team|services|solutions|products|industr(y|ies)|news|insights|events)/i
  ),
  HEADERS: {
    "User-Agent": envStr("WEBSITE_USER_AGENT", "LaratoEvidenceBot/1.0 (+https://larato.co.uk)"),
    "Accept": envStr("WEBSITE_ACCEPT_HEADER", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    ...envJson("WEBSITE_EXTRA_HEADERS_JSON", {})
  }
};
// concurrency defaults to number of children we might fetch
const CHILD_CONCURRENCY = Math.max(1, envInt("WEBSITE_CHILD_CONCURRENCY", Math.max(0, CONFIG.MAX_PAGES - 1)));

/* -------------------- helpers -------------------- */
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

  const text = parts.join("\n").slice(0, CONFIG.MAX_TEXT_PER_PAGE);
  return { url: baseUrl, title, text };
}

async function fetchWithTimeout(u, opts = {}) {
  const controller = new AbortController();
  const timeoutMs = Number.isFinite(opts.timeoutMs) && opts.timeoutMs > 0 ? opts.timeoutMs : CONFIG.FETCH_TIMEOUT_MS;
  const t = setTimeout(() => controller.abort(new Error("fetch_timeout")), timeoutMs);
  try {
    const res = await fetch(u, {
      redirect: "follow",
      headers: { ...CONFIG.HEADERS, ...(opts.headers || {}) },
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

    // same-origin restriction if configured
    if (CONFIG.SAME_ORIGIN_ONLY) {
      try {
        if (new URL(abs).origin !== new URL(baseUrl).origin) return;
      } catch {
        return;
      }
    }

    // obvious sections only (configurable)
    if (CONFIG.SECTION_RE.test(abs)) {
      candidates.add(abs.split("#")[0]);
    }
  });

  // We only need up to MAX_PAGES - 1 children (homepage already included)
  return Array.from(candidates).slice(0, Math.max(0, CONFIG.MAX_PAGES - 1));
}

// Simple concurrency limiter for promises
async function allWithConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let i = 0;
  const runners = new Array(Math.min(limit, items.length)).fill(null).map(async () => {
    while (i < items.length) {
      const idx = i++;
      try {
        results[idx] = await worker(items[idx], idx);
      } catch (e) {
        results[idx] = null;
      }
    }
  });
  await Promise.all(runners);
  return results;
}

/* -------------------- main -------------------- */
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

    // CHILDREN (parallel with cap, de-dupe, obey concurrency)
    const children = pickChildLinks(homeHtml, homeRes.url || rootUrl);
    const need = Math.max(0, CONFIG.MAX_PAGES - pack.length);
    const targets = Array.from(new Set(children)).slice(0, need);

    const results = await allWithConcurrency(targets, CHILD_CONCURRENCY, async (u) => {
      try {
        const r = await fetchWithTimeout(u);
        if (!r || !r.ok) return null;
        const ct = r.headers.get("content-type") || "";
        if (!/text\/html/i.test(ct)) return null;
        const html = await r.text();
        return extractReadable(html, r.url || u);
      } catch {
        return null;
      }
    });

    for (const v of results) {
      if (v) {
        pack.push(v);
        if (pack.length >= CONFIG.MAX_PAGES) break;
      }
    }

    return pack;
  } catch {
    return [];
  }
}

module.exports = { buildWebsitePack };
