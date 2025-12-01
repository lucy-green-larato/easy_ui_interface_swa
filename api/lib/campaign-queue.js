//  /api/lib/campaign-queue.js 01-12-2025 v2
// Canonical queue helper for Campaign apps.
// - No manual Base64 encoding (SDK + Functions runtime handle that)
// - Strict queue name validation
// - JSON-safe message serialization
// - Convenience helpers for main / evidence / outline / write queues

const { QueueServiceClient } = require("@azure/storage-queue");

// ---- ENV ----
const STORAGE_CONN = process.env.AzureWebJobsStorage || "";

// Main “router” / orchestration queue
const DEFAULT_QUEUE_CANDIDATE =
  process.env.CAMPAIGN_QUEUE_NAME ||
  process.env.CAMPAIGN_QUEUE ||
  "campaign";

// Stage-specific queues (used by worker/evidence/outline/write)
const EVIDENCE_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_EVIDENCE || "campaign-evidence-jobs";

const OUTLINE_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_OUTLINE || "campaign-outline";

const WRITE_QUEUE_CANDIDATE =
  process.env.Q_CAMPAIGN_WRITE || "campaign-write";

/**
 * Normalise and validate an Azure Storage queue name.
 *
 * Rules (per Azure docs, simplified):
 *  - 3–63 characters
 *  - lowercase letters, numbers, and hyphen only
 *  - must start and end with letter/number
 *  - no consecutive hyphens
 */
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

// Validated queue names (throws on misconfig at app start)
const DEFAULT_QUEUE_NAME = normaliseQueueName(DEFAULT_QUEUE_CANDIDATE);
const EVIDENCE_QUEUE_NAME = normaliseQueueName(EVIDENCE_QUEUE_CANDIDATE);
const OUTLINE_QUEUE_NAME = normaliseQueueName(OUTLINE_QUEUE_CANDIDATE);
const WRITE_QUEUE_NAME = normaliseQueueName(WRITE_QUEUE_CANDIDATE);

let _queueService;

// ---- core helpers ----

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

/**
 * Get a QueueClient for a given queue.
 */
function getQueueClient(queueName = DEFAULT_QUEUE_NAME) {
  const name = normaliseQueueName(queueName || DEFAULT_QUEUE_NAME);
  return getQueueService().getQueueClient(name);
}

/**
 * Ensure the queue exists and return its client.
 */
const _createdQueues = new Set();

async function ensureQueue(queueName = DEFAULT_QUEUE_NAME) {
  const q = getQueueClient(queueName);

  if (!_createdQueues.has(queueName)) {
    await q.createIfNotExists();
    _createdQueues.add(queueName);
  }

  return q;
}

/**
 * Serialise a message to a string suitable for sendMessage().
 * - Strings are sent as-is
 * - Objects/arrays are JSON.stringified
 * - null / undefined are encoded as "null"
 */
function serialiseMessage(message) {
  if (typeof message === "string") {
    return message;
  }
  if (message === null || message === undefined) {
    return "null";
  }
  return JSON.stringify(message);
}

/**
 * Low-level send: ensures queue exists, then sends the serialised message.
 * No manual Base64: SDK + Functions runtime handle encoding/decoding.
 */
async function send(queueName, message, options = {}) {
  const q = await ensureQueue(queueName);
  const text = serialiseMessage(message);

  const size = Buffer.byteLength(text, "utf8");
  const MAX = 62 * 1024; // safety margin

  if (size > MAX) {
    throw new Error(
      `Queue message exceeds safe limit (${size} bytes). Reduce payload before enqueue.`
    );
  }

  await q.sendMessage(text, options);
}

// ---- Public API ----

/**
 * Enqueue to the main campaign queue (router / orchestration).
 */
async function enqueueStart(message, options = {}) {
  return send(DEFAULT_QUEUE_NAME, message, options);
}

/**
 * Generic enqueue to any named queue.
 */
async function enqueueTo(queueName, message, options = {}) {
  // If caller passes a validated name constant, avoid re-validating.
  const name = (queueName === DEFAULT_QUEUE_NAME ||
    queueName === EVIDENCE_QUEUE_NAME ||
    queueName === OUTLINE_QUEUE_NAME ||
    queueName === WRITE_QUEUE_NAME)
    ? queueName
    : normaliseQueueName(queueName);
  return send(name, message, options);
}

/**
 * Convenience helpers for stage-specific queues.
 */
async function enqueueEvidence(message, options = {}) {
  return send(EVIDENCE_QUEUE_NAME, message, options);
}

async function enqueueOutline(message, options = {}) {
  return send(OUTLINE_QUEUE_NAME, message, options);
}

async function enqueueWrite(message, options = {}) {
  return send(WRITE_QUEUE_NAME, message, options);
}

module.exports = {
  // queue names (useful for logging/tests)
  DEFAULT_QUEUE_NAME,
  EVIDENCE_QUEUE_NAME,
  OUTLINE_QUEUE_NAME,
  WRITE_QUEUE_NAME,

  // low-level
  getQueueClient,
  ensureQueue,
  enqueueTo,

  // high-level convenience
  enqueueStart,
  enqueueEvidence,
  enqueueOutline,
  enqueueWrite
};
