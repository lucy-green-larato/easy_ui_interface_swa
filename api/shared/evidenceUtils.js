// /api/shared/evidenceUtils.js
// Canonical evidence utilities for the campaign engine
// Consolidated 2025-12-09 v4
"use strict";

const path = require("path");

/* ============================================================
   CLAIM ID GENERATOR
   ============================================================ */

function makeClaimIdFactory(prefix = "CLM") {
  let n = 1;
  return function nextClaimId() {
    const id = `${prefix}-${String(n).padStart(3, "0")}`;
    n += 1;
    return id;
  };
}

/* ============================================================
   CITATIONS
   ============================================================ */

function addCitation(text, sourceLabel) {
  const s = String(text || "").trim();
  const label = String(sourceLabel || "").trim();

  if (!s) return "";
  if (!label) return s;

  const lower = s.toLowerCase();
  const marker = `[${label.toLowerCase()}]`;

  // Avoid double-tagging
  if (lower.includes(marker) || lower.includes("(source:")) {
    return s;
  }

  return `${s} [${label}]`;
}

/* ============================================================
   ARRAY PUSH + PLACEHOLDER FILTERING
   ============================================================ */

function safePushIfRoom(list, item, max) {
  if (!Array.isArray(list)) return;
  if (!item || typeof item !== "object") return;

  const cap = Number.isFinite(max) && max > 0 ? max : 24;
  if (list.length >= cap) return;

  list.push(item);
}

function isPlaceholderEvidence(ev) {
  if (!ev || typeof ev !== "object") return true;

  const title = String(ev.title || "").trim().toLowerCase();
  const summary = String(ev.summary || "").trim().toLowerCase();
  const quote = String(ev.quote || "").trim().toLowerCase();

  const body = `${title} ${summary} ${quote}`;

  if (!body.trim()) return true;
  if (body.includes("lorem ipsum")) return true;
  if (body.includes("placeholder")) return true;

  // Very short = junk
  if (body.length < 12) return true;

  return false;
}

/* ============================================================
   DEDUPLICATION + SUMMARIES
   ============================================================ */

function _norm(v) {
  return String(v || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .slice(0, 160);
}

function dedupeEvidence(list) {
  if (!Array.isArray(list) || list.length === 0) return [];

  const seen = new Set();
  const out = [];

  for (const raw of list) {
    const ev = raw || {};
    const k = `${_norm(ev.title)}||${_norm(ev.url)}||${_norm(ev.summary || ev.quote)}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(ev);
  }

  return out;
}

function summarizeClaims(claims) {
  if (!Array.isArray(claims) || claims.length === 0) {
    return { total: 0, by_source_type: {}, by_tag: {} };
  }

  const bySource = {};
  const byTag = {};

  for (const c of claims) {
    const st = String(c.source_type || c.source || "Unknown").trim() || "Unknown";
    const tag = String(c.tag || c.source_tag || "untagged").trim() || "untagged";

    bySource[st] = (bySource[st] || 0) + 1;
    byTag[tag] = (byTag[tag] || 0) + 1;
  }

  return {
    total: claims.length,
    by_source_type: bySource,
    by_tag: byTag
  };
}

/* ============================================================
   SOURCE TYPE CLASSIFICATION
   ============================================================ */

function classifySourceType(url) {
  const raw = String(url || "").trim().toLowerCase();
  if (!raw) return "Unknown";

  const ext = path.extname(raw);
  if (ext === ".pdf") return "PDF extract";

  if (raw.includes("linkedin.com")) return "LinkedIn";
  if (/\.gov\.uk\b/.test(raw)) return "Official";
  if (raw.includes("ofcom")) return "Official";
  if (raw.includes("ons.gov")) return "Official";

  if (raw.includes("commsbusiness") || raw.includes("techradar") || raw.includes("computerweekly")) {
    return "Press";
  }

  return "Company site";
}

/* ============================================================
   INDUSTRY RELEVANCE SCORING
   ============================================================ */

function scoreIndustryEvidence(ev, industry) {
  if (!ev || typeof ev !== "object") return 0;

  const text = `${ev.title || ""} ${ev.summary || ""} ${ev.quote || ""}`.toLowerCase();
  const src = String(ev.source_type || "").toLowerCase();
  const tag = String(ev.tag || "").toLowerCase();
  const ind = String(industry || "").trim().toLowerCase();

  let score = 1.0;

  // Source-type weights
  if (src.includes("pdf")) score += 0.5;
  if (src.includes("linkedin")) score += 0.3;
  if (src.includes("official")) score += 0.4;

  // Tag-based weights
  if (tag.includes("buyer_blocker")) score += 0.4;
  if (tag.includes("differentiator")) score += 0.4;
  if (tag.includes("capability")) score += 0.3;

  // Industry keyword hits
  if (ind) {
    const toks = ind.split(/[^a-z0-9]+/).filter(Boolean);
    for (const t of toks) {
      if (t.length >= 3 && text.includes(t)) score += 0.35;
    }
  }

  return score;
}

/* ============================================================
   MICROCLAIM EXTRACTION (formerly in /lib/evidence-utils.js)
   ============================================================ */

function cleanText(html = "") {
  return String(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractSentences(text = "") {
  const s = String(text).trim();
  if (!s) return [];

  return s
    .split(/(?<=[.!?])\s+/)
    .map(v => v.trim())
    .filter(v => v.length > 20 && v.length < 320);
}

function tagSentence(sentence) {
  const s = sentence.toLowerCase();

  if (/customers?|clients?/.test(s)) return "customer_value";
  if (/partner|reseller|distributor/.test(s)) return "route_to_market";
  if (/ai|machine learning|automation/.test(s)) return "technology";
  if (/cost|save|roi/.test(s)) return "benefit_financial";

  return "general";
}

function extractStructuredClaims({ html, url, sourceType, addCitation }) {
  const text = cleanText(html);
  const sents = extractSentences(text);

  return sents.map(sent => ({
    title: sent.slice(0, 60),
    summary: addCitation(sent, sourceType),
    url,
    quote: sent,
    tag: tagSentence(sent),
    source_type: sourceType
  }));
}

/* ============================================================
   SITE + LINKEDIN EVIDENCE HELPERS
   (Moved from campaign-evidence/index.js to central utilities)
   ============================================================ */

function siteProductsEvidence(products, website, containerUrl, prefix, nextClaimId) {
  const arr = Array.isArray(products) ? products : [];
  const names = Array.from(
    new Set(
      arr
        .slice(0, 24)
        .map(p => (typeof p === "string" ? p : p?.name || p?.title || ""))
        .map(s => String(s || "").trim())
        .filter(Boolean)
    )
  );

  if (!names.length) return null;

  let rawUrl = (website || "").trim();
  if (!rawUrl && containerUrl && prefix) {
    const root = String(containerUrl).replace(/\/+$/, "");
    const pfx = String(prefix).replace(/^\/+/, "");
    rawUrl = `${root}/${pfx}products.json`;
  }

  if (!rawUrl) return null;
  if (!/^https:\/\//i.test(rawUrl)) return null;

  const summaryList = names.slice(0, 8).join(", ");

  return {
    claim_id: nextClaimId(),
    source_type: "Company site",
    title: "Products",
    url: rawUrl,
    summary: addCitation(summaryList, "Company site"),
    quote: ""
  };
}

function linkedinEvidence(linkedin, nextClaimId) {
  if (!linkedin) return null;
  const raw = String(linkedin).trim();
  if (!raw) return null;
  if (!/^https:\/\//i.test(raw)) return null;

  return {
    claim_id: nextClaimId(),
    source_type: "LinkedIn",
    title: "Supplier LinkedIn (reference)",
    url: raw,
    summary: addCitation(
      "Company LinkedIn profile reference for employer facts and recent posts.",
      "LinkedIn"
    ),
    quote: ""
  };
}

function liCompanySearch(company) {
  const n = String(company || "").trim();
  if (!n) return null;
  return `https://www.linkedin.com/search/results/content/?keywords=${encodeURIComponent(n)}&origin=GLOBAL_SEARCH_HEADER`;
}

function liProductSearch(name, company) {
  const n = String(name || "").trim();
  const c = String(company || "").trim();
  const parts = [c, n].filter(Boolean);
  if (!parts.length) return null;

  return `https://www.linkedin.com/search/results/content/?keywords=${encodeURIComponent(
    parts.join(" ")
  )}&origin=GLOBAL_SEARCH_HEADER`;
}

function liCompetitorSearch(name) {
  const n = String(name || "").trim();
  if (!n) return null;

  return `https://www.linkedin.com/search/results/content/?keywords=${encodeURIComponent(
    n
  )}&origin=GLOBAL_SEARCH_HEADER`;
}

function linkedinSearchEvidence({ url, title }, nextClaimId) {
  const u = String(url || "").trim();
  const t = String(title || "").trim();
  if (!u || !t) return null;
  if (!/^https:\/\//i.test(u)) return null;

  return {
    claim_id: nextClaimId(),
    source_type: "LinkedIn",
    title: t,
    url: u,
    summary: addCitation(`${t} — relevance scan link.`, "LinkedIn"),
    quote: ""
  };
}

// ============================================================
// Dynamic load of lib/evidence (CJS → ESM fallback)
// ============================================================

let _evidenceLibCache = null;

async function loadEvidenceLib() {
  if (_evidenceLibCache) return _evidenceLibCache;

  let firstError;

  // Try CommonJS (lib/evidence.js exporting buildEvidence)
  try {
    // eslint-disable-next-line global-require
    const mod = require("../lib/evidence");
    const buildEvidence = mod.buildEvidence ?? mod.default ?? mod;

    if (typeof buildEvidence !== "function") {
      throw new Error("lib/evidence: buildEvidence missing (CJS)");
    }

    _evidenceLibCache = { buildEvidence };
    return _evidenceLibCache;
  } catch (e1) {
    firstError = e1;
  }

  // Try ESM fallback
  try {
    const { pathToFileURL } = require("url");
    const esm = await import(pathToFileURL(require.resolve("../lib/evidence.js")).href);

    const buildEvidence = esm.buildEvidence ?? esm.default ?? esm;
    if (typeof buildEvidence !== "function") {
      throw new Error("lib/evidence: buildEvidence missing (ESM)");
    }

    _evidenceLibCache = { buildEvidence };
    return _evidenceLibCache;
  } catch (e2) {
    throw new Error(
      `evidence lib load failed: ${firstError?.message || firstError} | ${e2?.message || e2}`
    );
  }
}



/* ============================================================
   EXPORTS
   ============================================================ */

module.exports = {
  makeClaimIdFactory,
  addCitation,
  safePushIfRoom,
  isPlaceholderEvidence,
  dedupeEvidence,
  classifySourceType,
  summarizeClaims,
  scoreIndustryEvidence,

  extractStructuredClaims,
  cleanText,
  extractSentences,
  tagSentence,

  siteProductsEvidence,
  linkedinEvidence,
  liCompanySearch,
  liProductSearch,
  liCompetitorSearch,
  linkedinSearchEvidence,
  loadEvidenceLib
};
