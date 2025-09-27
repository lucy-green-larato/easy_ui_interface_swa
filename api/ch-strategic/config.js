// api/ch-strategic/config.js
// Centralised env/config for ch-strategic (router + worker)

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueClient } = require("@azure/storage-queue");

// Read env with optional aliases + default (works in Azure & Codespaces)
function getEnv(name, aliases = [], def = undefined) {
  for (const key of [name, ...aliases]) {
    const v = process.env[key];
    if (v !== undefined && v !== "") return v;
  }
  return def;
}

// ---------- Auth / roles ----------
const ALLOWED_ROLES = JSON.parse(
  getEnv("ALLOWED_ROLES_CHS", [], '["campaign","campaign-admin","sales-admin"]')
);

// ---------- Upload limits ----------
const MAX_UPLOAD_BYTES = Number(getEnv("MAX_UPLOAD_BYTES", [], 10485760));
const DEFAULT_ALLOWED_UPLOAD_MIME = getEnv(
  "DEFAULT_ALLOWED_UPLOAD_MIME",
  [],
  "text/csv,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
).split(",").map(s => s.trim().toLowerCase());

// ---------- Business tuning ----------
const CH_STRATEGIC_MAX_ROWS   = Number(getEnv("CH_STRATEGIC_MAX_ROWS", [], 5000));
const CH_STRATEGIC_CHUNK_SIZE = Number(getEnv("CH_STRATEGIC_CHUNK_SIZE", [], 100));
const CH_STRATEGIC_FEEDBACK_RPM      = Number(getEnv("CH_STRATEGIC_FEEDBACK_RPM", [], 60));
const CH_STRATEGIC_FEEDBACK_TTL_DAYS = Number(getEnv("CH_STRATEGIC_FEEDBACK_TTL_DAYS", ["CH_STRATEGIC_TTL_DAYS"], 30));

// Optional: Key Vault or local
const DOMAIN_HASH_SALT = getEnv("DOMAIN_HASH_SALT", ["DOMAINHASHSALT"]);

// ---------- Storage / containers / queue ----------
// IMPORTANT: the Functions host needs AzureWebJobsStorage (exact casing)
const AZURE_STORAGE = getEnv("AzureWebJobsStorage", ["AZUREWEBJOBSSTORAGE"]);

const CHS_OUT_CONTAINER      = getEnv("CH_STRATEGIC_OUT_CONTAINER", [], "ch-strategic-out");
const CHS_STATUS_CONTAINER   = getEnv("CH_STRATEGIC_STATUS_CONTAINER", [], "ch-strategic-status");
const CHS_CACHE_CONTAINER    = getEnv("CH_STRATEGIC_CACHE_CONTAINER", [], "ch-strategic-cache");
const CHS_FEEDBACK_CONTAINER = getEnv("CH_STRATEGIC_FEEDBACK_CONTAINER", [], "ch-strategic-feedback");

// REQUIRED for the queue trigger binding (no default here)
const CHS_JOBS_QUEUE         = getEnv("CH_STRATEGIC_JOBS_QUEUE", [], undefined);

// Clients (safe if storage is missing)
const blobSvc = AZURE_STORAGE ? BlobServiceClient.fromConnectionString(AZURE_STORAGE) : null;
const queueClient = (AZURE_STORAGE && CHS_JOBS_QUEUE) ? new QueueClient(AZURE_STORAGE, CHS_JOBS_QUEUE) : null;

module.exports = {
  // auth
  ALLOWED_ROLES,

  // upload
  MAX_UPLOAD_BYTES,
  DEFAULT_ALLOWED_UPLOAD_MIME,

  // business
  CH_STRATEGIC_MAX_ROWS,
  CH_STRATEGIC_CHUNK_SIZE,
  CH_STRATEGIC_FEEDBACK_RPM,
  CH_STRATEGIC_FEEDBACK_TTL_DAYS,
  DOMAIN_HASH_SALT,

  // storage
  AZURE_STORAGE,
  CHS_OUT_CONTAINER,
  CHS_STATUS_CONTAINER,
  CHS_CACHE_CONTAINER,
  CHS_FEEDBACK_CONTAINER,
  CHS_JOBS_QUEUE,

  // clients
  blobSvc,
  queueClient,
};
