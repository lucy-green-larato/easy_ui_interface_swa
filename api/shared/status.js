// /api/shared/status.js 01-12-2025 v3
// / Shared status.json updater with optional history node.

const { getJson, putJson } = require("./storage");
const { nowIso } = require("./utils");
const { canonicalPrefix, computePrefixFromMessage } = require("../lib/prefix");

/**
 * Normalise a prefix for status.json paths:
 *  - null/undefined → ""
 *  - strip leading slashes
 *  - ensure exactly one trailing slash when non-empty
 */
function normalisePrefix(prefix) {
  let raw = String(prefix || "").trim();
  if (!raw) return "";
  const m = raw.match(/^([a-z0-9-]+)\/(.+)$/i);
  if (m && m[1].toLowerCase() === process.env.RESULTS_CONTAINER?.toLowerCase()) {
    raw = m[2];   // discard container segment
  }

  const stripped = raw.replace(/^\/+/, "").replace(/\/+$/, "");
  return stripped ? `${stripped}/` : "";
}

/**
 * Update status.json under a given prefix, merging in `patch`
 * and optionally appending a history node.
 *
 * @param {import("@azure/storage-blob").ContainerClient} containerClient
 * @param {string} prefix            e.g. "runs/<runId>/" (can be sloppy; normalised internally)
 * @param {object} patch             partial fields to merge into status
 * @param {object} [historyNode]     optional history entry { ... }
 */
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
      if (!cur || typeof cur !== "object") {
        cur = {};
      }
      if (!Array.isArray(cur.history)) {
        cur.history = [];
      }
      if (!cur.runId && patchObj.runId) {
        cur.runId = patchObj.runId;
      }
      const next = {
        ...cur,
        ...patchObj,
        history: Array.isArray(cur.history) ? cur.history.slice() : []
      };

      // Append history node if provided
      if (historyNode && typeof historyNode === "object") {
        next.history.push({
          at: nowIso(),
          ...historyNode
        });
      }

      await putJson(containerClient, statusPath, next);
      // ✅ success, exit retry loop
      break;

    } catch (err) {
      // Last attempt → rethrow
      if (attempt === maxAttempts - 1) {
        throw err;
      }
      // Backoff a bit before retrying
      await new Promise(resolve => setTimeout(resolve, 50 * (attempt + 1)));
    }
  }
}
module.exports = {
  updateStatus
};
