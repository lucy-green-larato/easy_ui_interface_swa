// **** /api/lib/queue.js 17-11-2025 v4 (with base64 + validation) ****
const { QueueServiceClient } = require("@azure/storage-queue");

// Connection string (may be unset; we validate lazily on first use)
const STORAGE_CONN = process.env.AzureWebJobsStorage || "";

// Prefer CAMPAIGN_QUEUE_NAME but fall back gracefully
const DEFAULT_QUEUE_CANDIDATE =
  process.env.CAMPAIGN_QUEUE_NAME ||
  process.env.CAMPAIGN_QUEUE ||
  "campaign";

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

// Validate the default queue name once at module load so misconfig is obvious.
const DEFAULT_QUEUE_NAME = normaliseQueueName(DEFAULT_QUEUE_CANDIDATE);

let _queueService;

/**
 * Ensure the storage connection string is configured before queue operations.
 */
function assertStorageConfigured() {
  if (!STORAGE_CONN) {
    throw new Error(
      "AzureWebJobsStorage not configured: queue operations require a valid storage connection string."
    );
  }
}

/**
 * Get a singleton QueueServiceClient.
 */
function getQueueService() {
  assertStorageConfigured();
  if (!_queueService) {
    _queueService = QueueServiceClient.fromConnectionString(STORAGE_CONN);
  }
  return _queueService;
}

/**
 * Get a QueueClient for a given queue (or the validated default campaign queue).
 */
function getQueueClient(queueName = DEFAULT_QUEUE_NAME) {
  const name = normaliseQueueName(queueName || DEFAULT_QUEUE_NAME);
  return getQueueService().getQueueClient(name);
}

/**
 * Ensure the queue exists and return the client.
 */
async function ensureQueue(queueName = DEFAULT_QUEUE_NAME) {
  const q = getQueueClient(queueName);
  await q.createIfNotExists();
  return q;
}

/**
 * Serialise a message to a JSON-ready string.
 *  - Strings are sent as-is
 *  - Objects/arrays are JSON.stringified
 *  - null / undefined are encoded as "null" so consumers can
 *    explicitly distinguish "no payload" from "{}" (empty object)
 */
function serialiseMessage(message) {
  if (typeof message === "string") {
    return message;
  }

  if (message === null || message === undefined) {
    return "null";
  }

  // For objects/arrays/primitives, JSON encoding preserves shape
  return JSON.stringify(message);
}

/**
 * Encode message text as Base64.
 * This matches consumers that expect base64 payloads.
 */
function encodeBase64(text) {
  return Buffer.from(String(text || ""), "utf8").toString("base64");
}

/**
 * Enqueue to the default campaign queue.
 *
 * @param {object|string|null|undefined} message  Payload (JSON-serialised then base64-encoded)
 * @param {object} [options]       Optional send options (visibilityTimeout, timeToLive, etc.)
 */
async function enqueue(message, options = {}) {
  const q = await ensureQueue(DEFAULT_QUEUE_NAME);
  const text = serialiseMessage(message);
  const body = encodeBase64(text);
  await q.sendMessage(body, options);
}

/**
 * Enqueue to a named queue.
 *
 * @param {string} queueName
 * @param {object|string|null|undefined} message
 * @param {object} [options]
 */
async function enqueueTo(queueName, message, options = {}) {
  const q = await ensureQueue(queueName);
  const text = serialiseMessage(message);
  const body = encodeBase64(text);
  await q.sendMessage(body, options);
}

module.exports = {
  getQueueClient,
  enqueue,
  enqueueTo
};
