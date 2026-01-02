// /api/shared/hash.js
// 02-01-2026 — Deterministic hashing helpers (Gold doctrine)
// - stableStringify: canonical JSON string (sorted keys) for deterministic SHA
// - sha1 / sha1OfJson: consistent hashing across pipeline
// - semanticClaimFingerprint: SHA over ONLY semantic fields of a claim
//
// Doctrine:
// - Evidence may evolve (superset/additive), but semantic meaning of existing claim_ids must not change.
// - We fingerprint semantic fields to detect drift even when claim_id exists.

"use strict";

const crypto = require("crypto");

// -----------------------------------------------------------------------------
// stable stringify (canonical JSON)
// -----------------------------------------------------------------------------
function stableStringify(obj) {
  if (obj === null || obj === undefined) return "null";

  const t = typeof obj;

  if (t === "number" || t === "boolean") return JSON.stringify(obj);
  if (t === "string") return JSON.stringify(obj);

  if (Array.isArray(obj)) {
    return "[" + obj.map((x) => stableStringify(x)).join(",") + "]";
  }

  if (t === "object") {
    const keys = Object.keys(obj).sort();
    return (
      "{" +
      keys
        .map((k) => JSON.stringify(k) + ":" + stableStringify(obj[k]))
        .join(",") +
      "}"
    );
  }

  // functions/symbols/etc (should not occur)
  return JSON.stringify(String(obj));
}

// -----------------------------------------------------------------------------
// sha1 helpers
// -----------------------------------------------------------------------------
function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function sha1OfJson(obj) {
  return sha1(stableStringify(obj ?? null));
}

// -----------------------------------------------------------------------------
// semantic claim fingerprint
// -----------------------------------------------------------------------------
function normaliseString(v, max = 4000) {
  if (v === null || v === undefined) return "";
  const s = String(v).trim();
  if (!s) return "";
  if (s.length <= max) return s;
  return s.slice(0, max);
}

function normaliseUrl(v) {
  const s = normaliseString(v, 2000);
  if (!s) return "";
  // keep as-is (no URL parsing) for stability across environments
  return s;
}

function semanticClaimCanonical(claim) {
  const c = claim && typeof claim === "object" ? claim : {};

  // claim_id is the identity anchor (required for immutability)
  const claim_id = normaliseString(c.claim_id || c.id, 200);

  // Meaning-bearing fields (doctrine)
  const title = normaliseString(c.title, 800);
  const summary = normaliseString(c.summary || c.quote || c.extract || "", 2400);
  const url = normaliseUrl(c.url);
  const source_type = normaliseString(c.source_type, 120);

  // These can affect interpretation; treat as semantic unless you decide otherwise
  const tier = c.tier === null || c.tier === undefined ? null : c.tier;
  const tier_group = normaliseString(c.tier_group || c.tag || "", 120);

  // If your evidence model carries numeric fields, include them if present
  const numeric_value =
    c.numeric_value === null || c.numeric_value === undefined ? null : c.numeric_value;
  const units = normaliseString(c.units, 80);

  return {
    claim_id,
    title,
    summary,
    url,
    source_type,
    tier,
    tier_group,
    numeric_value,
    units
  };
}

function semanticClaimFingerprint(claim) {
  const canon = semanticClaimCanonical(claim);
  // If claim_id is missing, fingerprint still works but is less useful.
  return "sha1:" + sha1OfJson(canon);
}

module.exports = {
  stableStringify,
  sha1,
  sha1OfJson,
  semanticClaimCanonical,
  semanticClaimFingerprint
};
