// api/ch-strategic/config.js 19-10-2025 v9 (master debug flag + page-window sanitize)

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueClient } = require("@azure/storage-queue");

/* -------------------- helpers -------------------- */

function getEnv(name, aliases = [], def) {
  for (const k of [name, ...aliases]) {
    const v = process.env[k];
    if (v !== undefined && v !== null) {
      const s = String(v).trim();
      if (s !== "") return s;
    }
  }
  return def;
}

function toInt(v, def) {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

function safeJsonArray(str, fallbackJson) {
  try {
    const val = JSON.parse(str ?? fallbackJson);
    return Array.isArray(val) ? val : JSON.parse(fallbackJson);
  } catch {
    return JSON.parse(fallbackJson);
  }
}

function asBool(v, def = false) {
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return def;
  return ["1", "true", "yes", "on", "y"].includes(s);
}

/* -------------------- auth / roles -------------------- */

const ALLOWED_ROLES = safeJsonArray(
  getEnv("ALLOWED_ROLES_CHS", [], undefined),
  '["campaign","campaign-admin","sales-admin"]'
);

/* -------------------- upload limits -------------------- */

const MAX_UPLOAD_BYTES = toInt(
  getEnv("MAX_UPLOAD_BYTES", [], 10 * 1024 * 1024),
  10 * 1024 * 1024
);

const DEFAULT_ALLOWED_UPLOAD_MIME = (getEnv(
  "DEFAULT_ALLOWED_UPLOAD_MIME",
  [],
  "text/csv,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
) || "")
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

/* -------------------- business tuning -------------------- */

const CH_STRATEGIC_MAX_ROWS = toInt(
  getEnv("CH_STRATEGIC_MAX_ROWS", ["CHS_MAX_ROWS"], 5000),
  5000
);

const CH_STRATEGIC_CHUNK_SIZE = toInt(
  getEnv("CH_STRATEGIC_CHUNK_SIZE", ["CHS_CHUNK_SIZE"], 100),
  100
);

const CH_STRATEGIC_FEEDBACK_RPM = toInt(
  getEnv("CH_STRATEGIC_FEEDBACK_RPM", ["CHS_FEEDBACK_RPM"], 60),
  60
);

const CH_STRATEGIC_FEEDBACK_TTL_DAYS = toInt(
  getEnv(
    "CH_STRATEGIC_FEEDBACK_TTL_DAYS",
    ["CH_STRATEGIC_TTL_DAYS", "CHS_FEEDBACK_TTL_DAYS"],
    30
  ),
  30
);

// Standard Track caps (single-run, no fan-out)
const STANDARD_MAX_ITEMS = toInt(
  getEnv("CH_STRATEGIC_STANDARD_MAX_ITEMS", ["CHS_STANDARD_MAX_ITEMS"], 200),
  200
);

// Per-record safety (soft timeout + retries) used by Standard Track
const RECORD_SOFT_TIMEOUT_MS = toInt(
  getEnv("CH_STRATEGIC_RECORD_SOFT_TIMEOUT_MS", ["CHS_RECORD_SOFT_TIMEOUT_MS"], 15000),
  15000
);

const RECORD_MAX_ATTEMPTS = toInt(
  getEnv("CH_STRATEGIC_RECORD_MAX_ATTEMPTS", ["CHS_RECORD_MAX_ATTEMPTS"], 1),
  3
);

// Page-window for SR scanning (sanitize)
const RAW_PAGE_FIRST = toInt(getEnv("CH_SR_PAGES_FIRST", ["CH_SR_PAGES_PRIMARY"], 6), 6);
const RAW_PAGE_FALLBACK = toInt(getEnv("CH_SR_PAGES_FALLBACK", ["CH_SR_PAGES_SECONDARY"], 12), 12);
const _PAGE_FIRST = Math.max(1, RAW_PAGE_FIRST | 0);
const _PAGE_FALLBACK = Math.max(_PAGE_FIRST, RAW_PAGE_FALLBACK | 0);
// Public, sanitized page-window values for importers
const CH_SR_PAGES_FIRST = _PAGE_FIRST;
const CH_SR_PAGES_FALLBACK = _PAGE_FALLBACK;
const MAX_TEXT_CHARS = toInt(
  getEnv("CH_SR_MAX_TEXT_CHARS", ["CHS_MAX_TEXT_CHARS", "MAX_TEXT_CHARS"], 200000),
  200000
);

/* -------------------- debug (master switch + per-flag) -------------------- */

// Master switch: if false, all debug features are off regardless of per-flags.
const DEBUG_ENABLED = asBool(getEnv("CH_STRATEGIC_DEBUG", [], "0"), false);

// Per-flags only apply when DEBUG_ENABLED is true
const DEBUG_SAVE_TEXT  = DEBUG_ENABLED && asBool(getEnv("CHS_DEBUG_SAVE_TEXT", [], "false"), false);
const DEBUG_FORCE_TEXT = DEBUG_ENABLED ? String(getEnv("CHS_DEBUG_FORCE_TEXT", [], "") || "") : "";

/* -------------------- storage / containers / queue -------------------- */

const AZURE_STORAGE =
  getEnv("AzureWebJobsStorage", ["AZUREWEBJOBSSTORAGE", "AZURE_WEBJOBS_STORAGE"]) ||
  getEnv("AZURE_STORAGE_CONNECTION_STRING", ["AZURE_STORAGE"]);

const blobSvc = AZURE_STORAGE
  ? BlobServiceClient.fromConnectionString(AZURE_STORAGE)
  : null;

const CHS_OUT_CONTAINER = getEnv(
  "CH_STRATEGIC_OUT_CONTAINER",
  ["CHS_OUT_CONTAINER"],
  "ch-strategic-out"
);

const CHS_STATUS_CONTAINER = getEnv(
  "CH_STRATEGIC_STATUS_CONTAINER",
  ["CHS_STATUS_CONTAINER"],
  "ch-strategic-status"
);

const CHS_CACHE_CONTAINER = getEnv(
  "CH_STRATEGIC_CACHE_CONTAINER",
  ["CHS_CACHE_CONTAINER"],
  "ch-strategic-cache"
);

const CHS_FEEDBACK_CONTAINER = getEnv(
  "CH_STRATEGIC_FEEDBACK_CONTAINER",
  ["CHS_FEEDBACK_CONTAINER"],
  "ch-strategic-feedback"
);

// Queue name (raw, unsanitized; binding expands env token)
const CH_STRATEGIC_JOBS_QUEUE =
  (getEnv("CH_STRATEGIC_JOBS_QUEUE", ["CHS_JOBS_QUEUE", "CH_JOBS_QUEUE", "JOBS_QUEUE", "QUEUE_NAME"]) ||
    "ch-strategic-jobs").trim();

function resolveStorageConnectionString() {
  return (
    getEnv("AzureWebJobsStorage", [
      "AZUREWEBJOBSSTORAGE",
      "AZURE_WEBJOBS_STORAGE",
      "AZURE_STORAGE_CONNECTION_STRING",
      "AZURE_STORAGE",
    ]) || ""
  );
}

function getQueueClient() {
  const conn = resolveStorageConnectionString();
  const q = CH_STRATEGIC_JOBS_QUEUE;
  if (!conn || !q) return null;
  try {
    return QueueClient.fromConnectionString(conn, q);
  } catch {
    return null;
  }
}

// Eager client (optional)
const queueClient = (() => {
  const conn = resolveStorageConnectionString();
  const q = CH_STRATEGIC_JOBS_QUEUE;
  try {
    return conn && q ? QueueClient.fromConnectionString(conn, q) : null;
  } catch {
    return null;
  }
})();

/**
 * Ensure containers and the jobs queue exist (idempotent, NON-THROWING).
 */
async function ensureInfrastructure() {
  if (blobSvc) {
    await Promise.allSettled([
      blobSvc.getContainerClient(CHS_OUT_CONTAINER).createIfNotExists(),
      blobSvc.getContainerClient(CHS_STATUS_CONTAINER).createIfNotExists(),
      blobSvc.getContainerClient(CHS_CACHE_CONTAINER).createIfNotExists(),
      blobSvc.getContainerClient(CHS_FEEDBACK_CONTAINER).createIfNotExists(),
    ]);
  }
  try {
    const qc = queueClient || getQueueClient();
    if (qc) {
      try { await qc.createIfNotExists(); } catch {}
    }
  } catch { /* best-effort */ }
}

module.exports = {
  // env + limits
  MAX_UPLOAD_BYTES,
  DEFAULT_ALLOWED_UPLOAD_MIME,

  // business tuning
  CH_STRATEGIC_MAX_ROWS,
  CH_STRATEGIC_CHUNK_SIZE,
  CH_STRATEGIC_FEEDBACK_RPM,
  CH_STRATEGIC_FEEDBACK_TTL_DAYS,

  // Standard Track knobs
  STANDARD_MAX_ITEMS,
  RECORD_SOFT_TIMEOUT_MS,
  RECORD_MAX_ATTEMPTS,

  // SR scanning window + text guard
  CH_SR_PAGES_FIRST,
  CH_SR_PAGES_FALLBACK,
  MAX_TEXT_CHARS,

  // debug flags
  DEBUG_ENABLED,
  DEBUG_SAVE_TEXT,
  DEBUG_FORCE_TEXT,

  // storage (containers)
  AZURE_STORAGE,
  CHS_OUT_CONTAINER,
  CHS_STATUS_CONTAINER,
  CHS_CACHE_CONTAINER,
  CHS_FEEDBACK_CONTAINER,

  // queue
  CH_STRATEGIC_JOBS_QUEUE,

  // clients / helpers
  blobSvc,
  queueClient,
  getQueueClient,
  ensureInfrastructure,
};

