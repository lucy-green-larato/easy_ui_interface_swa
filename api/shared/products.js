// /api/shared/products.js 17-11-2025 v4 
//
// Responsibilities:
//  - Tokenise product/need strings and measure similarity
//  - Build problem/need signals from CSV-like data, packs, and site snippets
//  - Extract product-ish names from supplier HTML (including JSON-LD)
//  - Map buyer needs to supplier capabilities (products + USPs)
//  - Summarise coverage
//
// This module is PURE: no Azure, no storage, no logging.
//

// ---------- Tokenisation & similarity ----------

function tokenize(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s\-\+\/]/g, " ")
    .split(/\s+/)
    .filter(Boolean);
}

function jaccard(aTokens, bTokens) {
  const a = new Set(aTokens || []);
  const b = new Set(bTokens || []);
  let inter = 0;
  for (const t of a) if (b.has(t)) inter++;
  const union = a.size + b.size - inter;
  return union ? inter / union : 0;
}

/**
 * Score a product candidate against problem/need signals.
 * Returns { score, matches[] } where score ∈ [0,1].
 */
function scoreProductAgainstSignals(name, signals) {
  const nt = tokenize(name);
  let best = { score: 0, match: "" };
  for (const s of signals || []) {
    const st = tokenize(s);
    const sc = jaccard(nt, st);
    if (sc > best.score) best = { score: sc, match: s };
  }
  return { score: best.score, matches: best.score ? [best.match] : [] };
}

// ---------- Problem / need signal builder ----------

function _normaliseSignalBucket(val, out) {
  if (!out) return;
  if (Array.isArray(val)) {
    for (const x of val) {
      const s = String(x || "").trim();
      if (s) out.push(s);
    }
  } else if (typeof val === "string") {
    for (const part of val.split(/[;,\n]/)) {
      const s = String(part || "").trim();
      if (s) out.push(s);
    }
  }
}

/**
 * Build problem/need signals from:
 *  - CSV-like signals:
 *      - flat: { top_needs_supplier, top_needs, top_purchases }
 *      - canonical: { signals: {...}, global_signals: {...} }
 *  - Packs: { industry, generic, company } each may contain
 *      problems / pains / needs as string[] or delimited string
 *  - Site: { home_text, titles[] }
 */
function buildProblemSignals({ csvSignals, packs, site }) {
  const sigs = [];

  // CSV signals (support both flat & canonical csv_normalized shapes)
  const csv = csvSignals && typeof csvSignals === "object" ? csvSignals : {};

  const sigObj =
    csv.signals && typeof csv.signals === "object" ? csv.signals : csv;
  const gSigObj =
    csv.global_signals && typeof csv.global_signals === "object"
      ? csv.global_signals
      : null;

  for (const key of ["top_needs_supplier", "top_needs", "top_purchases"]) {
    _normaliseSignalBucket(sigObj[key], sigs);
    if (gSigObj) _normaliseSignalBucket(gSigObj[key], sigs);
  }

  // Packs: industry / generic / company → problems, pains, needs
  const packSets = [packs?.industry, packs?.generic, packs?.company].filter(
    (p) => p && typeof p === "object"
  );

  for (const p of packSets) {
    for (const k of ["problems", "pains", "needs"]) {
      _normaliseSignalBucket(p[k], sigs);
    }
  }

  // Site: simple heuristics over homepage text + titles
  const homeText = String(site?.home_text || "");
  const titlesText = Array.isArray(site?.titles)
    ? site.titles.map((t) => String(t || "")).join(" ")
    : "";
  const home = `${homeText} ${titlesText}`.trim();

  if (home) {
    home.split(/[.\n]/).forEach((s) => {
      const t = String(s || "").trim();
      if (!t) return;
      if (
        /\b(latency|outage|resilien|failover|security|compliance|sla|uptime|coverage|support|integration|cost|efficien|visibility|monitor|redundan)/i.test(
          t
        )
      ) {
        sigs.push(t);
      }
    });
  }

  // Deduplicate & normalise
  return [...new Set(sigs.map((s) => String(s || "").trim()).filter(Boolean))].slice(
    0,
    200
  );
}

// ---------- JSON-LD Products ----------

// Extract product objects from JSON-LD <script type="application/ld+json"> blocks
// Returns an array of { topic, title, summary, weight }.
//
// - topic:  "product_detail"
// - title:  product name (or "Product")
// - summary: description (or a generic label)
// - weight: hint for downstream ranking; 2 is "strong but not dominant".
function extractProductsFromLdJson(html) {
  const out = [];
  const text = String(html || "");

  // Find all JSON-LD script blocks
  const matches = text.matchAll(
    /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  );

  const pushProductNode = (node) => {
    if (!node || typeof node !== "object") return;
    const type = node["@type"];
    const types = Array.isArray(type) ? type : [type].filter(Boolean);
    const isProductLike = types
      .map((t) => String(t || "").toLowerCase())
      .some((t) => t === "product" || t === "service");
    if (!isProductLike) return;

    const title = String(node.name || node.alternateName || "").trim();
    const summary = String(node.description || "").trim();
    if (!title && !summary) return;

    out.push({
      topic: "product_detail",
      title: title || "Product",
      summary: summary || "Product (JSON-LD)",
      weight: 2
    });
  };

  for (const m of matches) {
    const raw = m[1];
    if (!raw) continue;

    try {
      const parsed = JSON.parse(raw);

      // Handle @graph containers
      if (parsed && Array.isArray(parsed["@graph"])) {
        for (const node of parsed["@graph"]) pushProductNode(node);
        continue;
      }

      // Handle itemListElement with potential product entries
      if (parsed && Array.isArray(parsed.itemListElement)) {
        for (const el of parsed.itemListElement) {
          if (el && typeof el === "object") {
            const item = el.item || el;
            pushProductNode(item);
          }
        }
      }

      const nodes = Array.isArray(parsed) ? parsed : [parsed];
      for (const n of nodes) pushProductNode(n);
    } catch {
      // Ignore broken JSON-LD blocks and keep going
      continue;
    }
  }

  return out;
}

// ---------- Product extraction from HTML / site ----------

/**
 * Lightweight product name extraction from first-page HTML.
 * Looks at H1/H2/H3/LI that appear product-y, filters out nav rubbish.
 */
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

/**
 * Parse short product / capability claims from HTML.
 * Pulls H1, all H2, and LI items as concise bullets.
 */
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

/**
 * Collect product/capability names from:
 *  - products.json ({ products: [...] })
 *  - site.json (legacy array form; uses page 0 snippet)
 *  - supplier_usps (array of strings)
 */
function collectProductNames(productsObj, siteJson, usps) {
  const out = new Set();

  // products.json (v7.1 stores { products: [...] })
  if (Array.isArray(productsObj?.products)) {
    for (const p of productsObj.products) {
      const name =
        typeof p === "string" ? p : p?.name || p?.title || "";
      const trimmed = String(name || "").trim();
      if (trimmed) out.add(trimmed);
    }
  }

  // site.json (array, legacy compatible) — harvest obvious headings/titles from page 0 snippet
  const page0 = Array.isArray(siteJson) ? siteJson[0] : null;
  const html = page0 && typeof page0.snippet === "string" ? page0.snippet : "";
  if (html) {
    const matches =
      html.match(/<(h1|h2|h3|li)[^>]*>([^<]{3,120})</gi) || [];
    for (const tag of matches) {
      const t = String(tag).replace(/<[^>]+>/g, "").trim();
      if (
        t &&
        !/^(home|about|contact|support|blog|learn|login|log in|privacy|terms)$/i.test(
          t
        )
      ) {
        out.add(t);
      }
    }
  }

  // supplier_usps hint at capabilities
  if (Array.isArray(usps)) {
    for (const u of usps) {
      const s = String(u || "").trim();
      if (s) out.add(s);
    }
  }

  return Array.from(out);
}

// ---------- Needs → capabilities mapping ----------

/**
 * Match a single buyer need string to product names + USPs.
 *
 * Returns:
 *  {
 *    need,
 *    status: "matched" | "partial" | "gap",
 *    hits: [ { type: "product" | "usp", name }, ... ]
 *  }
 */
function matchNeedToCapabilities(need, productNames, usps) {
  const tokens = String(need || "")
    .toLowerCase()
    .split(/\W+/)
    .filter(Boolean);
  const hits = [];

  for (const name of productNames || []) {
    const nlow = String(name || "").toLowerCase();
    if (tokens.some((tok) => nlow.includes(tok))) {
      hits.push({ type: "product", name });
    }
  }
  for (const u of usps || []) {
    const ulow = String(u || "").toLowerCase();
    if (tokens.some((tok) => ulow.includes(tok))) {
      hits.push({ type: "usp", name: u });
    }
  }

  let status = "gap";
  if (hits.length >= 2) status = "matched";
  else if (hits.length === 1) status = "partial";

  return { need, status, hits };
}

/**
 * Summarise a list of need→capability maps.
 *
 * Input: array of { status: "matched"|"partial"|"gap", ... }
 * Returns: { total, matched, partial, gap, coverage }
 */
function summariseCoverage(map) {
  const list = Array.isArray(map) ? map : [];
  const total = list.length;
  const matched = list.filter((x) => x.status === "matched").length;
  const partial = list.filter((x) => x.status === "partial").length;
  const gap = list.filter((x) => x.status === "gap").length;
  const coverage = total
    ? Math.round(((matched + 0.5 * partial) / total) * 100)
    : 0;
  return { total, matched, partial, gap, coverage };
}

/**
 * Build a full needs coverage structure:
 *  {
 *    coverage: { total, matched, partial, gap, coverage },
 *    items: [ { need, status, hits[] }, ... ]
 *  }
 */
function buildNeedsCoverage(needsList, productNames, usps) {
  const needs = (needsList || [])
    .map((s) => String(s || "").trim())
    .filter(Boolean);

  const prods = (productNames || [])
    .map((s) => String(s || "").trim())
    .filter(Boolean);

  const uspList = (usps || [])
    .map((s) => String(s || "").trim())
    .filter(Boolean);

  const items = needs.map((n) =>
    matchNeedToCapabilities(n, prods, uspList)
  );
  const coverage = summariseCoverage(items);
  return { coverage, items };
}

module.exports = {
  // primitives
  tokenize,
  jaccard,
  scoreProductAgainstSignals,

  // problem signals
  buildProblemSignals,

  // product extraction
  extractProductsFromSite,
  parseProductClaims,
  collectProductNames,
  extractProductsFromLdJson,

  // needs mapping
  matchNeedToCapabilities,
  summariseCoverage,
  buildNeedsCoverage
};
