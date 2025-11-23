// /api/shared/evidenceUtils.js 23-11-2025 v3
//
// Responsibilities:
//  - Claim ID generation (CLM-001 style)
//  - Evidence array helpers (push with caps, de-duplication)
//  - Placeholder / junk evidence detection
//  - Simple citation formatting
//  - Source-type classification
//  - Summary counts for evidence bundles
//
// This module is PURE: no Azure, no storage, no logging.
//

// ---------- Claim ID helpers ----------

/**
 * Sequential CLM-### generator (001..999).
 *
 * Usage:
 *   const nextClaimId = makeClaimIdFactory();
 *   const id = nextClaimId(); // "CLM-001"
 */

const { nowIso } = require("./utils");

function makeClaimIdFactory(startAt = 0) {
  let n = Number.isFinite(startAt) && startAt >= 0 ? startAt : 0;
  return function nextId() {
    n = Math.min(999, n + 1);
    return `CLM-${String(n).padStart(3, "0")}`;
  };
}

// ---------- Evidence array helpers ----------

/**
 * Push an item into an array if there is room.
 * Does NOT do any validation.
 */
function pushIfRoom(arr, item, cap) {
  if (!Array.isArray(arr) || !item) return;
  const limit = Number.isFinite(cap) && cap > 0 ? cap : Infinity;
  if (arr.length < limit) arr.push(item);
}

/**
 * Central placeholder filter: returns true if an evidence item
 * is too weak / junk to be useful.
 *
 * Rules:
 *  1) Reject items with no title, no summary, and no URL.
 *  2) Allow "Customer profile" / "profile_competitor" / "competitor profile"
 *     to be URL-less IF they have some text.
 *  3) Reject weak scaffold titles (e.g. "Product", "Products", "Placeholder").
 *  4) For everything else, URL must be https:// and not obviously fake/test.
 */
function isPlaceholderEvidence(it) {
  if (!it) return true;

  const src = String(it.source_type || "").trim();
  const url = String(it.url || "").trim();
  const title = String(it.title || "").trim();
  const summary = String(it.summary || "").trim();
  const srcLower = src.toLowerCase();

  // 1) No substance at all
  if (!title && !summary && !url) return true;

  // 2) Special allowance: Customer / competitor profile items may omit URL
  if (
    srcLower === "customer profile" ||
    srcLower === "profile_competitor" ||
    srcLower === "competitor profile" ||
    srcLower === "competitor_profile"
  ) {
    // Require at least some meaningful text
    return !(title || summary);
  }

  // 3) Weak scaffold titles
  if (/^(?:products|product|placeholder|lorem ipsum)$/i.test(title)) return true;

  // 4) URL hygiene for everything else: must be https and non-junk hostnames
  if (!/^https:\/\//i.test(url)) return true;

  const u = url.toLowerCase();
  if (u.startsWith("about:")) return true;
  if (u.includes("example.com")) return true;
  if (
    /\b(companywebsite\.com|companyx\.com|competitor\d+\.com|vendora\.com|vendorb\.com|vendorc\.com|vendord\.com|vendore\.com|test\.(com|net)|localhost|127\.0\.0\.1)\b/i.test(
      u
    )
  ) {
    return true;
  }

  return false;
}

/**
 * Push an evidence item into an array if:
 *  - The item is non-null
 *  - It is NOT a placeholder/junk item
 *  - There is room under the cap
 */
function safePushIfRoom(arr, item, cap) {
  if (!item) return;
  if (isPlaceholderEvidence(item)) return;
  pushIfRoom(arr, item, cap);
}

/**
 * Deduplicate evidence by (title|url|source_type).
 * Preserves first occurrence; stable.
 */
function dedupeEvidence(list) {
  if (!Array.isArray(list)) return [];
  const seen = new Set();
  const out = [];
  for (const it of list) {
    if (!it) continue;
    const title = String(it.title || "").toLowerCase();
    const url = String(it.url || "").toLowerCase();
    const src = String(it.source_type || "").toLowerCase();
    const key = `${title}|${url}|${src}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(it);
  }
  return out;
}

// ---------- Citations & source type ----------

/**
 * Add a trailing citation tag in brackets unless one already exists.
 *
 * Example:
 *   addCitation("Some stat about SMEs.", "Ofcom")
 *   → "Some stat about SMEs. (Ofcom)"
 */
function addCitation(text, tag) {
  const t = String(text || "").trim();
  const label = String(tag || "").trim() || "source";
  if (!t) return `(${label})`;

  // If there is already a "(...)" at the end, leave it alone
  return /\([^()]{2,}\)\.?$/.test(t) ? t : `${t} (${label})`;
}

/**
 * Coarse source-type classification from URL.
 * Used to normalise e.g. Ofcom/ONS/DSIT, LinkedIn, PDFs, directories, etc.
 */
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

// ---------- Evidence bundle summary ----------

/**
 * Summarise evidence bundle counts by broad source_type family.
 *
 * Returns:
 *  {
 *    website: n,
 *    linkedin: n,
 *    pdf: n,
 *    directories: n,
 *    ixbrl: n,
 *    csv: n
 *  }
 */
function summarizeClaims(all) {
  const list = Array.isArray(all) ? all : [];
  const counts = {
    website: 0,
    linkedin: 0,
    pdf: 0,
    directories: 0,
    ixbrl: 0,
    csv: 0
  };

  for (const it of list) {
    const t = String(it?.source_type || "").toLowerCase();
    if (t.includes("site") || t.includes("website") || t.includes("company site")) {
      counts.website++;
    } else if (t.includes("linkedin")) {
      counts.linkedin++;
    } else if (t.includes("pdf")) {
      counts.pdf++;
    } else if (t.includes("directory")) {
      counts.directories++;
    } else if (t.includes("ixbrl")) {
      counts.ixbrl++;
    } else if (t.includes("csv")) {
      counts.csv++;
    }
  }

  return counts;
}

// Score a pack evidence item, favouring:
// - Customer/supplier and competitor profiles
// - Company site / website items
// - Regulator / official stats (Ofcom, ONS, DSIT, gov.uk)
// - Exact industry mentions
// - Clean https URLs
function scoreIndustryEvidence(pe, selectedIndustry) {
  if (!pe) return 0;

  const title = String(pe.title || "");
  const summary = String(pe.summary || "");
  const t = `${title} ${summary}`.toLowerCase();

  const src = String(pe.source_type || "").toLowerCase();
  const ind = String(selectedIndustry || "").toLowerCase();

  let s = 0;

  // Supplier/customer profile items are top-tier
  if (src.includes("customer profile")) s += 50;

  // Competitor profiles are also strong but slightly lower
  if (src.includes("competitor profile") || src.includes("profile_competitor")) {
    s += 40;
  }

  // Company site / website strong signal
  if (src.includes("company site") || src.includes("website")) s += 35;

  // Regulator / official sources (Ofcom, ONS, DSIT, gov.uk)
  if (/(ofcom|ons|dsit|gov\.uk)/.test(t) || /(ofcom|ons|dsit|gov\.uk)/.test(src)) s += 30;

  // Exact industry match in title/summary
  if (ind && t.includes(ind)) s += 20;

  // HTTPS hygiene
  if (pe.url && /^https:\/\//.test(String(pe.url))) s += 2;

  // Non-generic source_type preferred
  if (src && src !== "source") s += 1;

  return s;
}

module.exports = {
   // time helpers
  nowIso, 

  // IDs
  makeClaimIdFactory,

  // array helpers
  pushIfRoom,
  safePushIfRoom,
  isPlaceholderEvidence,
  dedupeEvidence,

  // text/source helpers
  addCitation,
  classifySourceType,

  // summaries
  summarizeClaims,
  scoreIndustryEvidence
};
