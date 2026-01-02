// /api/shared/status.js 02-01-2026 v5
// Shared status.json updater with optional history node.

const { getJson, putJson } = require("../shared/storage");
const { nowIso } = require("./utils");
const { canonicalPrefix, computePrefixFromMessage } = require("../lib/prefix");

// ... existing normalisePrefix ...

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

async function updateStatus(containerClient, prefix, patch = {}, historyNode) {
  if (typeof prefix === "object" && !Array.isArray(prefix) && prefix !== null) {
    prefix = computePrefixFromMessage(prefix);
  }
  const safePrefix = normalisePrefix(prefix);
  const statusPath = `${safePrefix}status.json`;
  const patchObj = (patch && typeof patch === "object") ? patch : {};
  const maxAttempts = 3;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      let cur = await getJson(containerClient, statusPath);
      if (!cur || typeof cur !== "object") cur = {};
      if (!Array.isArray(cur.history)) cur.history = [];
      if (!cur.runId && patchObj.runId) cur.runId = patchObj.runId;

      // Build next with shallow merge first
      const next = {
        ...cur,
        ...patchObj,
        history: Array.isArray(cur.history) ? cur.history.slice() : []
      };

      // ✅ Critical fix: merge `input` safely (no clobbering with null/empty)
      if ("input" in patchObj) {
        next.input = mergeInputPreservingCanonical(cur.input, patchObj.input);
      } else if (cur.input && typeof cur.input === "object") {
        // Ensure input remains preserved even if patch omits it
        next.input = cur.input;
      }

      // Append history node if provided
      if (historyNode && typeof historyNode === "object") {
        next.history.push({
          at: nowIso(),
          ...historyNode
        });
      }

      await putJson(containerClient, statusPath, next);
      break;

    } catch (err) {
      if (attempt === maxAttempts - 1) throw err;
      await new Promise(resolve => setTimeout(resolve, 50 * (attempt + 1)));
    }
  }
}

module.exports = { updateStatus };
