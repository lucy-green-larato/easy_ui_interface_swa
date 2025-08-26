// Fetches the homepage + a few obvious child pages and extracts readable text.
const { fetch } = require("undici");
const cheerio = require("cheerio");
const URL = require("url").URL;

const MAX_PAGES = 4;           // homepage + up to 3 obvious links
const MAX_TEXT_PER_PAGE = 12000;

function absolute(base, href) {
  try { return new URL(href, base).toString(); } catch { return null; }
}

function extractReadable(html, baseUrl) {
  const $ = cheerio.load(html);

  // Kill nav/footers/menus to reduce noise
  $("nav, header, footer, script, style, noscript, svg, form, iframe").remove();

  // Prefer main content
  const root = $("main").length ? $("main") : $("body");

  const title = ($("title").first().text() || $("h1").first().text() || "").trim();

  // Keep headings + paras
  const parts = [];
  root.find("h1,h2,h3,p,li").each((_, el) => {
    const t = $(el).text().replace(/\s+/g, " ").trim();
    if (t) parts.push(t);
  });

  const text = parts.join("\n").slice(0, MAX_TEXT_PER_PAGE);
  return { url: baseUrl, title, text };
}

async function fetchPage(u) {
  const r = await fetch(u, { redirect: "follow" });
  const ct = r.headers.get("content-type") || "";
  if (!ct.includes("text/html")) return null;
  const html = await r.text();
  return extractReadable(html, u);
}

function pickChildLinks(html, baseUrl) {
  const $ = cheerio.load(html);
  const candidates = new Set();
  $("a[href]").each((_, a) => {
    const href = ($(a).attr("href") || "").trim();
    if (!href) return;
    const abs = absolute(baseUrl, href);
    if (!abs) return;
    if (!abs.startsWith(new URL(baseUrl).origin)) return; // same site only
    // obvious sections
    if (/(about|company|leadership|team|services|solutions|products|industr(y|ies)|news|insights|events)/i.test(abs)) {
      candidates.add(abs.split("#")[0]);
    }
  });
  return Array.from(candidates).slice(0, MAX_PAGES - 1);
}

async function buildWebsitePack(rootUrl) {
  try {
    const homeRes = await fetch(rootUrl, { redirect: "follow" });
    const homeHtml = await homeRes.text();
    const pack = [];
    const home = extractReadable(homeHtml, homeRes.url || rootUrl);
    pack.push(home);

    const children = pickChildLinks(homeHtml, homeRes.url || rootUrl);
    for (const u of children) {
      try {
        const r = await fetch(u, { redirect: "follow" });
        const ct = r.headers.get("content-type") || "";
        if (!ct.includes("text/html")) continue;
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
