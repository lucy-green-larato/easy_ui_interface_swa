// **** /api/shared/status.js 14-11-2025 v2 ****
// Shared status.json updater with optional history node.

const { getJson, putJson } = require("./storage");
const { nowIso } = require("./utils");

/**
 * Normalise a prefix for status.json paths:
 *  - null/undefined → ""
 *  - strip leading slashes
 *  - ensure exactly one trailing slash when non-empty
 */
function normalisePrefix(prefix) {
  const raw = String(prefix || "").trim();
  if (!raw) return "";
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
  const safePrefix = normalisePrefix(prefix);
  const statusPath = `${safePrefix}status.json`;

  // Ensure patch is an object; ignore non-object primitives defensively
  const patchObj = (patch && typeof patch === "object") ? patch : {};

  // Load current status, tolerating missing/invalid JSON
  let cur = await getJson(containerClient, statusPath);
  if (!cur || typeof cur !== "object") {
    cur = {};
  }

  // Normalise existing history if present
  if (!Array.isArray(cur.history)) {
    cur.history = [];
  }

  // Prefer an existing runId, otherwise seed from patch if provided
  if (!cur.runId && patchObj.runId) {
    cur.runId = patchObj.runId;
  }

  // Merge patch over current status, but keep history array instance
  const next = {
    ...cur,
    ...patchObj,
    history: Array.isArray(cur.history) ? cur.history.slice() : []
  };

  if (historyNode && typeof historyNode === "object") {
    next.history.push({
      at: nowIso(),
      ...historyNode
    });
  }

  await putJson(containerClient, statusPath, next);
}

module.exports = {
  updateStatus
};
