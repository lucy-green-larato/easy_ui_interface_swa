// /api/shared/campaignConfig.js 26-11-2025 v4
//
// Centralised configuration for the Campaign pipeline.
// Keeps Azure + tuning settings out of each function.
//
// Env vars used:
//   AzureWebJobsStorage              (required at use-time)
//   CAMPAIGN_RESULTS_CONTAINER       (optional, default "results")
//   RESULTS_CONTAINER                (fallback for above)
//   CAMPAIGN_INPUT_CONTAINER         (optional, default "input")   // NEW
//   INPUT_CONTAINER                  (fallback for above)          // NEW
//   CAMPAIGN_QUEUE_NAME              (optional, default "campaign")
//   HTTP_FETCH_TIMEOUT_MS            (optional, default 8000ms; must be >0)
//   MAX_EVIDENCE_ITEMS               (optional, default 24, clamped [1, 128])

/**
 * Ensure an env var is present and non-empty.
 * Evaluated lazily when the config property is accessed.
 */
function requireEnv(name) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === "") {
    throw new Error(`Missing env var: ${name}`);
  }
  return String(raw).trim();
}

/**
 * Validate an Azure Storage name (container/queue) against platform rules.
 *  - 3 to 63 characters
 *  - Only lowercase letters, numbers, and dashes
 *  - Must start and end with a letter or number
 *  - No consecutive dashes
 */
function validateStorageName(name, kind = "Azure Storage name") {
  if (typeof name !== "string") {
    throw new Error(`${kind} must be a string.`);
  }

  const trimmed = name.trim();
  if (!trimmed) {
    throw new Error(`${kind} is required but was empty.`);
  }

  // Basic shape + length
  if (!/^[a-z0-9-]{3,63}$/.test(trimmed)) {
    throw new Error(
      `Invalid ${kind} "${name}". ` +
      "Names must be 3–63 characters and use only lowercase letters, numbers, and dashes."
    );
  }

  // Start/end must be alphanumeric
  if (!/^[a-z0-9].*[a-z0-9]$/.test(trimmed)) {
    throw new Error(
      `Invalid ${kind} "${name}". ` +
      "Names must start and end with a letter or number."
    );
  }

  // No consecutive dashes
  if (/--/.test(trimmed)) {
    throw new Error(
      `Invalid ${kind} "${name}". ` +
      "Names cannot contain consecutive dashes."
    );
  }

  return trimmed;
}

/**
 * Resolve and validate the results container name.
 */
function resolveResultsContainer() {
  const raw =
    process.env.CAMPAIGN_RESULTS_CONTAINER ||
    process.env.RESULTS_CONTAINER ||
    "results";

  return validateStorageName(raw, "results container name");
}

/**
 * Resolve and validate the input container name (for static packs, etc.).
 */
function resolveInputContainer() {
  const raw =
    process.env.CAMPAIGN_INPUT_CONTAINER ||
    process.env.INPUT_CONTAINER ||
    "input"; // <- your `input` container default

  return validateStorageName(raw, "input container name");
}

/**
 * Resolve and validate the campaign queue name.
 */
function resolveCampaignQueueName() {
  const raw = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
  return validateStorageName(raw, "campaign queue name");
}

/**
 * Resolve HTTP fetch timeout (ms), with sane fallback.
 */
function resolveHttpFetchTimeout() {
  const raw = process.env.HTTP_FETCH_TIMEOUT_MS;
  if (raw === undefined || raw === null || String(raw).trim() === "") {
    return 8000;
  }

  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    return 8000;
  }
  return n;
}

/**
 * Resolve max evidence items with clamp [1, 128] and default 24.
 */
function resolveMaxEvidenceItems() {
  const raw = process.env.MAX_EVIDENCE_ITEMS;
  const n = raw !== undefined && raw !== null ? parseInt(String(raw).trim(), 10) : 24;

  let v = Number.isFinite(n) ? n : 24;
  if (v <= 0) v = 24;
  if (v > 128) v = 128;
  return v;
}

const config = {
  // Core Azure storage connection (validated lazily)
  get STORAGE_CONN() {
    // Will throw with a clear message if not set
    return requireEnv("AzureWebJobsStorage");
  },

  // Where campaign artefacts are written (site.json, csv_normalized.json, evidence_log.json, etc.)
  get RESULTS_CONTAINER() {
    return resolveResultsContainer();
  },

  // NEW: Where static/shared packs live (e.g. input/packs/…)
  get INPUT_CONTAINER() {
    return resolveInputContainer();
  },

  // Primary campaign queue name (validated for Azure Storage semantics)
  get CAMPAIGN_QUEUE_NAME() {
    return resolveCampaignQueueName();
  },

  // Network tuning for site fetches (ms)
  get HTTP_FETCH_TIMEOUT_MS() {
    return resolveHttpFetchTimeout();
  },

  // Evidence cap (per run), with safety clamp
  get MAX_EVIDENCE_ITEMS() {
    return resolveMaxEvidenceItems();
  }
};

module.exports = config;
