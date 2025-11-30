// /api/lib/prefix.js 29-11-2025 v8
// Single source of truth for results blob prefixes
// Layout (container-relative):
//   runs/<page>/<userId>/<YYYY>/<MM>/<DD>/<runId>/
//
//  - page   → logical tool or flow name ("campaign", etc.)
//  - userId → user or "anonymous"
//  - runId  → required; any unique string/UUID
//  - date   → optional Date, defaults to now (UTC)

"use strict";

/**
 * Normalise a path segment into a safe, compact string.
 * - lowercases
 * - replaces non [a-z0-9_-] with "-"
 * - collapses multiple "-" into one
 * - uses fallback if empty
 */
function sanitizeSegment(value, fallbackIfEmpty) {
  let s = value == null ? "" : String(value).trim();
  if (!s && fallbackIfEmpty) s = String(fallbackIfEmpty);
  if (!s) s = "unknown";

  s = s
    .toLowerCase()
    .replace(/[^a-z0-9_\-]/g, "-")
    .replace(/-+/g, "-");

  return s;
}

/**
 * Compute the canonical results prefix.
 *
 * @param {Object} params
 * @param {string} params.userId  - logical user id (e.g. "anonymous" in tests)
 * @param {string} params.page    - logical page/flow key (e.g. "campaign")
 * @param {string} params.runId   - unique run identifier (UUID, etc.)
 * @param {Date}   [params.date]  - optional Date; defaults to now (UTC)
 *
 * @returns {string} container-relative prefix:
 *   "runs/<page>/<userId>/<YYYY>/<MM>/<DD>/<runId>/"
 */
function canonicalPrefix({ userId, page, runId, date } = {}) {
  const d = date instanceof Date ? date : new Date();

  const year  = d.getUTCFullYear();
  const month = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day   = String(d.getUTCDate()).padStart(2, "0");

  const segUser = sanitizeSegment(userId, "anonymous");
  const segPage = sanitizeSegment(page, "campaign");
  const segRun  = sanitizeSegment(runId, "run");

  return `runs/${segPage}/${segUser}/${year}/${month}/${day}/${segRun}/`;
}

/**
 * Backwards-compatible helper for call-sites that pass the
 * whole queue message instead of explicit fields.
 *
 * This is a THIN WRAPPER over canonicalPrefix and MUST NOT
 * introduce any different folder scheme.
 *
 * @param {Object} msg - queue message or similar
 * @returns {string} canonical prefix
 */
function computePrefixFromMessage(msg) {
  const m = msg || {};

  const runId =
    m.runId   ||
    m.run_id  ||
    m.id      ||
    m.fileId  ||
    m.file_id ||
    "unknown";

  const userId =
    m.userId  ||
    m.user    ||
    m.ownerId ||
    "anonymous";

  const page =
    m.page ||
    m.tool ||
    m.flow ||
    "campaign";

  let date;
  if (m.date instanceof Date) {
    date = m.date;
  } else if (typeof m.date === "string") {
    const parsed = new Date(m.date);
    if (!Number.isNaN(parsed.getTime())) {
      date = parsed;
    }
  }

  return canonicalPrefix({ userId, page, runId, date });
}

// Alias for legacy imports: const { computePrefix } = require("../lib/prefix")
const computePrefix = computePrefixFromMessage;

module.exports = {
  canonicalPrefix,
  computePrefixFromMessage,
  computePrefix
};
