// FILE: /api/shared/evidenceTiers.js
// evidenceTiers.js 09-12-2025 v1
// Central tier model + metadata helpers for campaign evidence.
//
// Doctrine encoded here (Lucy):
//   Tier 0  → CSV summary (primary market reality; must be first)
//   Tier 1  → External strategic markdown
//              - industry_drivers
//              - industry_risks
//              - persona_pressures
//              - competitor_profiles
//   Tier 2a → Supplier markdown + content_pillars
//   Tier 2b → Industry_stats (context-only, narrative support)
//   Tier 3  → Case studies
//   Tier 4  → Microclaims (crawl, HTML, ixbrl micro-facts, etc.)
//   Tier 5  → LinkedIn hooks / low-signal auxiliary hooks
//
// This module does NOT change any behaviour by itself.
// It gives us a single source of truth for:
//   - tier codes ("0", "1", "2a", "2b", "3", "4", "5")
//   - tier groups (csv, markdown_t1, markdown_t2a, etc.)
//   - numeric rank for sorting (0..6)
//   - a tiny helper to stamp metadata onto evidence items.
//
// Later steps will:
//   - call classifyTierFromHint() in the evidence builder
//   - use getTierRank() when sorting evidence_log/evidence.json

"use strict";

// Canonical tier codes as strings (for storage / metadata).
const TIER_CODES = Object.freeze({
  CSV_SUMMARY: "0",
  MARKDOWN_T1: "1",
  MARKDOWN_T2A: "2a",
  MARKDOWN_T2B: "2b",
  CASE_STUDY: "3",
  MICROCLAIM: "4",
  LINKEDIN: "5"
});

// Tier groups: compact labels that explain *why* something has a tier.
const TIER_GROUPS = Object.freeze({
  CSV: "csv_summary",
  MARKDOWN_T1: "markdown_t1",          // drivers/risks/persona/competitors
  MARKDOWN_T2A: "markdown_t2a",        // supplier markdown + content_pillars
  MARKDOWN_T2B: "markdown_t2b",        // industry_stats
  CASE_STUDY: "case_study",
  MICROCLAIM: "microclaim",
  LINKEDIN: "linkedin_hook",
  OTHER: "other"
});

// Numeric rank used for strict global tier-first sorting.
// Lower = higher priority in the evidence log.
const TIER_RANK = Object.freeze({
  "0": 0,   // CSV summary
  "1": 1,   // Tier-1 markdown
  "2a": 2,  // Tier-2a markdown
  "2b": 3,  // Tier-2b markdown
  "3": 4,   // Case studies
  "4": 5,   // Microclaims
  "5": 6    // LinkedIn hooks / auxiliary
});

/**
 * Return a numeric rank for a tier code. Unknown tiers get a low priority
 * (sorted towards the bottom).
 *
 * @param {string} tierCode - e.g. "1", "2a", "3"
 * @returns {number} rank (0..6, or 999 for unknown)
 */
function getTierRank(tierCode) {
  const code = String(tierCode || "").toLowerCase();
  if (Object.prototype.hasOwnProperty.call(TIER_RANK, code)) {
    return TIER_RANK[code];
  }
  return 999; // unknown/legacy → always last
}

/**
 * Attach tier metadata to an evidence item without changing any of the core
 * fields. This is deliberately tiny and boring.
 *
 * @param {object} ev           Existing evidence object
 * @param {string} tierCode     One of TIER_CODES values (e.g. "1", "2a")
 * @param {string} tierGroup    One of TIER_GROUPS values (e.g. "markdown_t1")
 * @param {string} [reason]     Optional short explanation for debugging
 * @returns {object}            The same object (mutated) for convenience
 */
function stampTier(ev, tierCode, tierGroup, reason) {
  if (!ev || typeof ev !== "object") return ev;

  const code = String(tierCode || "").toLowerCase();
  const group = String(tierGroup || "").trim() || TIER_GROUPS.OTHER;

  ev.tier = code;
  ev.tier_group = group;

  // Optional, for debugging / audit; safe to ignore in Writer.
  if (reason) {
    ev.tier_reason = String(reason);
  }

  return ev;
}

/**
 * Very small helper that converts a "hint" into a tier assignment.
 * This helper does NOT look at the whole evidence object; it is meant
 * to be fed by the builder, e.g.:
 *
 *   classifyTierFromHint({ kind: "markdown", bucket: "industry_drivers" })
 *
 * so that the mapping logic lives here, not scattered across modules.
 *
 * It returns:
 *   { tier: <code>, tier_group: <group> }
 *
 * and does NOT mutate anything.
 */
function classifyTierFromHint(hint) {
  const h = hint || {};
  const kind = String(h.kind || "").toLowerCase();
  const bucket = String(h.bucket || "").toLowerCase();
  const src = String(h.source || "").toLowerCase();

  // --- CSV summary (Tier 0) ---
  if (kind === "csv_summary" || src === "csv_summary") {
    return { tier: TIER_CODES.CSV_SUMMARY, tier_group: TIER_GROUPS.CSV };
  }

  // --- Tier-1 markdown (external strategic) ---
  if (kind === "markdown") {
    // bucket describes which markdown category this came from
    if (
      bucket === "industry_drivers" ||
      bucket === "industry_risks" ||
      bucket === "persona_pressures" ||
      bucket === "competitor_profiles"
    ) {
      return {
        tier: TIER_CODES.MARKDOWN_T1,
        tier_group: TIER_GROUPS.MARKDOWN_T1
      };
    }

    // Tier-2a markdown: supplier markdown + content_pillars
    if (
      bucket === "content_pillars" ||
      bucket === "supplier_strengths" ||
      bucket === "supplier_capabilities" ||
      bucket === "supplier_differentiators" ||
      bucket === "supplier_value_proposition"
    ) {
      return {
        tier: TIER_CODES.MARKDOWN_T2A,
        tier_group: TIER_GROUPS.MARKDOWN_T2A
      };
    }

    // Tier-2b markdown: industry_stats (context only)
    if (bucket === "industry_stats") {
      return {
        tier: TIER_CODES.MARKDOWN_T2B,
        tier_group: TIER_GROUPS.MARKDOWN_T2B
      };
    }
  }

  // --- Case studies (Tier 3) ---
  if (kind === "case_study" || bucket === "case_study") {
    return {
      tier: TIER_CODES.CASE_STUDY,
      tier_group: TIER_GROUPS.CASE_STUDY
    };
  }

  // --- Microclaims (Tier 4) ---
  if (kind === "microclaim" || bucket === "microclaim" || kind === "ixbrl_micro") {
    return {
      tier: TIER_CODES.MICROCLAIM,
      tier_group: TIER_GROUPS.MICROCLAIM
    };
  }

  // --- LinkedIn hooks (Tier 5) ---
  if (kind === "linkedin" || bucket === "linkedin" || src === "linkedin") {
    return {
      tier: TIER_CODES.LINKEDIN,
      tier_group: TIER_GROUPS.LINKEDIN
    };
  }

  // Fallback — unknown, treated as lowest priority by getTierRank().
  return {
    tier: null,
    tier_group: TIER_GROUPS.OTHER
  };
}

module.exports = {
  TIER_CODES,
  TIER_GROUPS,
  getTierRank,
  stampTier,
  classifyTierFromHint
};
