// /api/lib/campaign-queue.js 17-12-2025 v8
// Canonical queue helper for Campaign apps.
// - No manual Base64 encoding (SDK + Functions runtime handle that)
// - Strict queue name validation
// - JSON-safe message serialization
// - Deterministic startup failure on misconfig
// - Single source of truth for ALL pipeline queues

"use strict";

const { QueueServiceClient } = require("@azure/storage-queue");

// ---- ENV ----
const STORAGE_CONN = process.env.AzureWebJobsStorage || "";

// Main orchestration / router queue
const DEFAULT_QUEUE_CANDIDATE =
  process.env.CAMPAIGN_QUEUE_NAME ||
  process.env.CAMPAIGN_QUEUE ||
  "campaign";

// Pipeline stage queues
const PACKSLOAD_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_PACKSLOAD || "campaign-packsload";

const MARKDOWN_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_MARKDOWN || "campaign-markdown-pack";

const LINKEDIN_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_LINKEDIN || "campaign-linkedin";

const EVIDENCE_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_EVIDENCE || "campaign-evidence-jobs";

const OUTLINE_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_OUTLINE || "campaign-outline";

const WORKER_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_WORKER || "campaign-worker-jobs";

const WRITE_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_WRITE || "campaign-write";

// NEW: Phase 3/4 competitor queues
const COMPETITOR_ENRICH_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_COMPETITOR_ENRICH || "campaign-competitor-enrich-jobs";

const COMPETITOR_SCORE_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_COMPETITOR_SCORE || "campaign-competitor-score-jobs";

// --------------------------------------------------
// Queue name normalisation + validation
// --------------------------------------------------
function normaliseQueueName(rawName) {
  const name = String(rawName || "").trim().toLowerCase();

  if (!name) {
    throw new Error("Queue name is empty or not set.");
  }

  if (name.length < 3 || name.length > 63) {
    throw new Error(
      `Invalid Azure Storage queue name "${name}": length must be between 3 and 63 characters.`
    );
  }

  if (!/^[a-z0-9][a-z0-9-]*[a-z0-9]$/.test(name)) {
    throw new Error(
      `Invalid Azure Storage queue name "${name}": only lowercase letters, numbers, and hyphens are allowed, and it must start/end with a letter or number.`
    );
  }

  if (name.includes("--")) {
    throw new Error(
      `Invalid Azure Storage queue name "${name}": consecutive hyphens ("--") are not allowed.`
    );
  }

  return name;
}

// --------------------------------------------------
// Validated queue constants (FAIL FAST on startup)
// --------------------------------------------------
const DEFAULT_QUEUE_NAME = normaliseQueueName(DEFAULT_QUEUE_CANDIDATE);
const PACKSLOAD_QUEUE_NAME = normaliseQueueName(PACKSLOAD_QUEUE_CANDIDATE);
const MARKDOWN_QUEUE_NAME = normaliseQueueName(MARKDOWN_QUEUE_CANDIDATE);
const LINKEDIN_QUEUE_NAME = normaliseQueueName(LINKEDIN_QUEUE_CANDIDATE);
const EVIDENCE_QUEUE_NAME = normaliseQueueName(EVIDENCE_QUEUE_CANDIDATE);
const OUTLINE_QUEUE_NAME = normaliseQueueName(OUTLINE_QUEUE_CANDIDATE);
const WORKER_QUEUE_NAME = normaliseQueueName(WORKER_QUEUE_CANDIDATE);
const WRITE_QUEUE_NAME = normaliseQueueName(WRITE_QUEUE_CANDIDATE);

// NEW
const COMPETITOR_ENRICH_QUEUE_NAME = normaliseQueueName(COMPETITOR_ENRICH_QUEUE_CANDIDATE);
const COMPETITOR_SCORE_QUEUE_NAME = normaliseQueueName(COMPETITOR_SCORE_QUEUE_CANDIDATE);

// All validated constants (used to skip re-validation)
const VALIDATED_QUEUES = new Set([
  DEFAULT_QUEUE_NAME,
  PACKSLOAD_QUEUE_NAME,
  MARKDOWN_QUEUE_NAME,
  LINKEDIN_QUEUE_NAME,
  EVIDENCE_QUEUE_NAME,
  OUTLINE_QUEUE_NAME,
  WORKER_QUEUE_NAME,
  WRITE_QUEUE_NAME,
  COMPETITOR_ENRICH_QUEUE_NAME,
  COMPETITOR_SCORE_QUEUE_NAME
]);

let _queueService;

// --------------------------------------------------
// Core helpers
// --------------------------------------------------
function assertStorageConfigured() {
  if (!STORAGE_CONN) {
    throw new Error(
      "AzureWebJobsStorage not configured: queue operations require a valid storage connection string."
    );
  }
}

function getQueueService() {
  assertStorageConfigured();
  if (!_queueService) {
    _queueService = QueueServiceClient.fromConnectionString(STORAGE_CONN);
  }
  return _queueService;
}

function getQueueClient(queueName = DEFAULT_QUEUE_NAME) {
  const name = normaliseQueueName(queueName || DEFAULT_QUEUE_NAME);
  return getQueueService().getQueueClient(name);
}

const _createdQueues = new Set();

async function ensureQueue(queueName = DEFAULT_QUEUE_NAME) {
  const name = normaliseQueueName(queueName || DEFAULT_QUEUE_NAME);
  const q = getQueueClient(name);
  if (!_createdQueues.has(name)) {
    await q.createIfNotExists();
    _createdQueues.add(name);
  }
  return q;
}

function serialiseMessage(message) {
  if (typeof message === "string") return message;
  if (message === null || message === undefined) return "null";
  return JSON.stringify(message);
}

async function send(queueName, message, options = {}) {
  const q = await ensureQueue(queueName);
  const text = serialiseMessage(message);

  const size = Buffer.byteLength(text, "utf8");
  const MAX = 62 * 1024;
  if (size > MAX) {
    throw new Error(
      `Queue message exceeds safe limit (${size} bytes). Reduce payload before enqueue.`
    );
  }

  await q.sendMessage(text, options);
}

// --------------------------------------------------
// Public API
// --------------------------------------------------
async function enqueueStart(message, options = {}) {
  return send(DEFAULT_QUEUE_NAME, message, options);
}

async function enqueueTo(queueName, message, options = {}) {
  const name = VALIDATED_QUEUES.has(queueName)
    ? queueName
    : normaliseQueueName(queueName);

  return send(name, message, options);
}

async function enqueueEvidence(message, options = {}) {
  return send(EVIDENCE_QUEUE_NAME, message, options);
}

async function enqueueOutline(message, options = {}) {
  return send(OUTLINE_QUEUE_NAME, message, options);
}

async function enqueueWorker(message, options = {}) {
  return send(WORKER_QUEUE_NAME, message, options);
}

async function enqueueWrite(message, options = {}) {
  return send(WRITE_QUEUE_NAME, message, options);
}

module.exports = {
  // queue name constants
  DEFAULT_QUEUE_NAME,
  PACKSLOAD_QUEUE_NAME,
  MARKDOWN_QUEUE_NAME,
  LINKEDIN_QUEUE_NAME,
  EVIDENCE_QUEUE_NAME,
  OUTLINE_QUEUE_NAME,
  WORKER_QUEUE_NAME,
  WRITE_QUEUE_NAME,

  // NEW: competitor queues
  COMPETITOR_ENRICH_QUEUE_NAME,
  COMPETITOR_SCORE_QUEUE_NAME,

  // low-level
  getQueueClient,
  ensureQueue,
  enqueueTo,

  // high-level convenience
  enqueueStart,
  enqueueEvidence,
  enqueueOutline,
  enqueueWorker,
  enqueueWrite
};
