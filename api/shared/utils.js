//**** */ /api/shared/utils.js 17-11-2025 v4 ****//
const crypto = require("node:crypto");

/**
 * Stable SHA-1 hash helper.
 */
function sha1(s) {
  return crypto
    .createHash("sha1")
    .update(String(s ?? ""), "utf8")
    .digest("hex");
}

/**
 * ISO timestamp helper.
 */
function nowIso() {
  return new Date().toISOString();
}

/**
 * Return the most frequent items (as strings) in an array.
 *
 * @param {any[]} arr
 * @param {number} [limit=8]
 * @returns {string[]} Top items by frequency, descending
 */
function topByFrequency(arr, limit = 8) {
  const freq = new Map();
  for (const s of (Array.isArray(arr) ? arr : [])) {
    const t = String(s || "").trim();
    if (!t) continue;
    freq.set(t, (freq.get(t) || 0) + 1);
  }
  return [...freq.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([k]) => k)
    .slice(0, limit);
}

module.exports = {
  sha1,
  nowIso,
  topByFrequency
};
