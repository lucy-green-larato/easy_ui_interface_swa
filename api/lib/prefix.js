// /api/lib/prefix.js — Unified Prefix Engine (v10, 29-11-2025)
// EXACTLY matches campaign-start prefix logic.
// Layout (container-relative):
//   runs/<page>/<userId>/<YYYY>/<MM>/<DD>/<runId>/
//
// - page sanitisation matches sanitizePage() from campaign-start
// - userId sanitisation matches campaign-start (allows . and @)
// - runId is NOT sanitised (campaign-start behaviour)
// - date handled in UTC

"use strict";

/**
 * Sanitize page segment.
 * Matches campaign-start sanitizePage:
 *   - lowercases
 *   - keeps a-z 0-9 . _ -
 *   - replaces everything else with "-"
 *   - collapses multiple "-" into one
 */
function sanitizePageSegment(value) {
  let s = value == null ? "" : String(value).trim();
  if (!s) s = "campaign";
  return s
    .toLowerCase()
    .replace(/[^a-z0-9._-]/g, "-")
    .replace(/-+/g, "-");
}

/**
 * Sanitize userId segment.
 * MUST MATCH campaign-start userId sanitiser:
 *   /[^a-z0-9_.@-]/g
 * This allows dots and "@", which are legitimate Azure AD identifiers.
 */
function sanitizeUserSegment(value) {
  let s = value == null ? "" : String(value).trim();
  if (!s) s = "anonymous";
  return s
    .toLowerCase()
    .replace(/[^a-z0-9_.@-]/g, "-")
    .replace(/-+/g, "-");
}

/**
 * Sanitize runId segment.
 * campaign-start does NOT sanitize runId — it is used verbatim.
 * We only trim whitespace and default to "run" if missing.
 */
function sanitizeRunId(value) {
  return value ? String(value).trim() : "run";
}

/**
 * Compute canonical prefix — MUST MATCH campaign-start output EXACTLY.
 *
 * @param {Object} params
 * @param {string} params.userId
 * @param {string} params.page
 * @param {string} params.runId
 * @param {Date}   [params.date]
 *
 * @returns {string} "runs/<page>/<userId>/<YYYY>/<MM>/<DD>/<runId>/"
 */
function canonicalPrefix({ userId, page, runId, date } = {}) {
  const d = date instanceof Date ? date : new Date();

  const year  = d.getUTCFullYear();
  const month = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day   = String(d.getUTCDate()).padStart(2, "0");

  const segPage = sanitizePageSegment(page);
  const segUser = sanitizeUserSegment(userId);
  const segRun  = sanitizeRunId(runId);

  return `runs/${segPage}/${segUser}/${year}/${month}/${day}/${segRun}/`;
}

/**
 * Compute prefix directly from a queue message or similar object.
 * This MUST NOT introduce any new layout — it is a thin wrapper
 * for backwards-compatibility.
 */
function computePrefixFromMessage(msg = {}) {
  const runId =
    msg.runId   ||
    msg.run_id  ||
    msg.id      ||
    msg.fileId  ||
    msg.file_id ||
    "unknown";

  const userId =
    msg.userId  ||
    msg.user    ||
    msg.ownerId ||
    "anonymous";

  const page =
    msg.page ||
    msg.tool ||
    msg.flow ||
    "campaign";

  let date;
  if (msg.date instanceof Date) {
    date = msg.date;
  } else if (typeof msg.date === "string") {
    const parsed = new Date(msg.date);
    if (!Number.isNaN(parsed.getTime())) {
      date = parsed;
    }
  }

  return canonicalPrefix({ userId, page, runId, date });
}

// Legacy alias
const computePrefix = computePrefixFromMessage;

module.exports = {
  canonicalPrefix,
  computePrefixFromMessage,
  computePrefix
};
