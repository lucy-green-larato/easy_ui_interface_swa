// /api/shared/status.js 02-01-2026 v6
// Shared status.json updater with optional history node.
// ✅ Fixes v5 regression: normalisePrefix must exist (ReferenceError broke EvidenceDigest).
// ✅ Fixes input clobber: patch.input never overwrites canonical non-empty values with null/empty.
// ✅ Prefix discipline: accepts a prefix string OR a queue message object (computePrefixFromMessage).
// ✅ Safe, idempotent writes with bounded retries.
//
// CONTRACT
// - updateStatus(containerClient, prefixOrMsg, patchObj?, historyNode?)
//   - prefixOrMsg: string prefix (container-relative) OR message object with { prefix/page/userId/runId/date... }
//   - patchObj: shallow patch; if patchObj.input is present, it merges safely
//   - historyNode: optional { phase, note, error, ... } appended with { at: nowIso() }
//
// NOTE
// - status.json is ALWAYS written at `${prefix}/status.json` (prefix must end with "/").
// - This module does NOT recompute prefix if a canonical prefix string is passed.
// - It only normalises representation (strip leading "/" and ensure trailing "/").
//

"use strict";

const { getJson, putJson } = require("../shared/storage");
const { nowIso } = require("./utils");
const { computePrefixFromMessage } = require("../lib/prefix");

// -----------------------------------------------------------------------------
// Prefix normalisation (representation only)
// -----------------------------------------------------------------------------
function normalisePrefix(prefix) {
  let p = String(prefix || "").trim();
  if (!p) return "";
  p = p.replace(/^\/+/, "");   // never absolute
  if (!p.endsWith("/")) p += "/";
  return p;
}

// -----------------------------------------------------------------------------
// Input merge helpers (prevent corruption)
// -----------------------------------------------------------------------------
function isNullishOrEmpty(v) {
  if (v === null || v === undefined) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  return false;
}

// Merge nested input without clobbering canonical values.
// Rule: do not overwrite a non-empty existing value with null/empty.
function mergeInputPreservingCanonical(curInput, patchInput) {
  const base = (curInput && typeof curInput === "object") ? curInput : {};
  const patch = (patchInput && typeof patchInput === "object") ? patchInput : {};

  const out = { ...base };

  for (const [k, v] of Object.entries(patch)) {
    const existing = out[k];

    // If patch provides null/empty, keep existing non-empty value.
    if (isNullishOrEmpty(v) && !isNullishOrEmpty(existing)) {
      continue;
    }

    out[k] = v;
  }

  return out;
}

// -----------------------------------------------------------------------------
// Status updater (bounded retry; optional history append)
// -----------------------------------------------------------------------------
async function updateStatus(containerClient, prefixOrMsg, patch = {}, historyNode) {
  if (!containerClient) {
    throw new Error("updateStatus: containerClient is required");
  }

  // Allow passing a queue message object (computePrefixFromMessage expects your canonical fields)
  let prefix = prefixOrMsg;
  if (prefix && typeof prefix === "object" && !Array.isArray(prefix)) {
    prefix = computePrefixFromMessage(prefix);
  }

  const safePrefix = normalisePrefix(prefix);

  if (!safePrefix) {
    throw new Error("updateStatus: missing/invalid prefix");
  }

  const statusPath = `${safePrefix}status.json`;
  const patchObj = (patch && typeof patch === "object") ? patch : {};
  const maxAttempts = 3;

  // Small helper: stable backoff
  const backoff = (attempt) => new Promise(resolve => setTimeout(resolve, 60 * (attempt + 1)));

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      let cur = await getJson(containerClient, statusPath);
      if (!cur || typeof cur !== "object") cur = {};

      // Ensure structural invariants
      if (!Array.isArray(cur.history)) cur.history = [];
      if (!cur.markers || typeof cur.markers !== "object") cur.markers = {};

      // Seed runId only if missing (never overwrite)
      if (!cur.runId && patchObj.runId) cur.runId = patchObj.runId;

      // Build next with shallow merge first (patch wins at top-level)
      const next = {
        ...cur,
        ...patchObj,
        history: Array.isArray(cur.history) ? cur.history.slice() : []
      };

      // ✅ Critical: merge input safely (no clobbering with null/empty)
      // Only do this if patchObj explicitly contains "input"
      if (Object.prototype.hasOwnProperty.call(patchObj, "input")) {
        next.input = mergeInputPreservingCanonical(cur.input, patchObj.input);
      } else if (cur.input && typeof cur.input === "object") {
        // Ensure input remains preserved even if patch omits it
        next.input = cur.input;
      }

      // Append history node if provided
      if (historyNode && typeof historyNode === "object") {
        // Never allow a caller to override "at"
        const { at, ...rest } = historyNode;
        next.history.push({
          at: nowIso(),
          ...rest
        });
      }

      await putJson(containerClient, statusPath, next);
      return;
    } catch (err) {
      if (attempt === maxAttempts - 1) throw err;
      await backoff(attempt);
    }
  }
}

module.exports = { updateStatus };
