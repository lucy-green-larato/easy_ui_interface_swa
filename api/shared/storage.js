//  /api/shared/storage.js 17-11-2025 v4
//
// Shared Azure Storage helpers for the campaign pipeline.
// Centralises container + connection config so functions stay lean.

const { BlobServiceClient } = require("@azure/storage-blob");
const {
  STORAGE_CONN,
  RESULTS_CONTAINER
} = require("./campaignConfig");

// ---------- Small helpers ----------

/**
 * Normalise a blob path:
 *  - null/undefined → ""
 *  - strip leading slashes
 */
function normaliseBlobPath(p) {
  return String(p || "").replace(/^\/+/, "");
}

/**
 * Normalise a prefix for listing:
 *  - null/undefined → ""
 *  - strip leading slashes
 */
function normalisePrefix(prefix) {
  return String(prefix || "").replace(/^\/+/, "");
}

// --- Core clients ---

function getBlobServiceClient() {
  // STORAGE_CONN is already validated in campaignConfig
  return BlobServiceClient.fromConnectionString(STORAGE_CONN);
}

function getContainerClient(containerName = RESULTS_CONTAINER) {
  const name = String(containerName || RESULTS_CONTAINER).trim() || RESULTS_CONTAINER;
  const svc = getBlobServiceClient();
  return svc.getContainerClient(name);
}

// --- Stream/text helpers ---

async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  // readable is an async iterable stream in Azure SDK v12
  for await (const chunk of readable) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

// --- Blob read helpers ---

async function getText(containerClient, blobPath) {
  const path = normaliseBlobPath(blobPath);
  if (!path) return null;

  try {
    const blob = containerClient.getBlobClient(path);
    if (!(await blob.exists())) return null;
    const resp = await blob.download();
    return streamToString(resp.readableStreamBody);
  } catch {
    // Best-effort: treat read failures as "no content" at this layer.
    return null;
  }
}

async function getJson(containerClient, blobPath) {
  try {
    const text = await getText(containerClient, blobPath);
    if (!text) return null;
    return JSON.parse(text);
  } catch {
    return null;
  }
}

// --- Blob write helpers ---

async function putText(containerClient, blobPath, text) {
  const path = normaliseBlobPath(blobPath);
  const b = containerClient.getBlockBlobClient(path);
  const body = Buffer.from(String(text ?? ""), "utf8");
  await b.uploadData(body, {
    blobHTTPHeaders: { blobContentType: "text/plain; charset=utf-8" }
  });
}

async function putJson(containerClient, blobPath, obj) {
  const path = normaliseBlobPath(blobPath);
  const b = containerClient.getBlockBlobClient(path);
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await b.uploadData(body, {
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }
  });
}

// --- Listing helpers ---

async function listBlobsUnderPrefix(containerClient, prefix) {
  const pfx = normalisePrefix(prefix);
  const out = [];
  for await (const item of containerClient.listBlobsFlat({ prefix: pfx })) {
    out.push(item.name);
  }
  return out;
}

async function listCsvUnderPrefix(containerClient, prefix) {
  const all = await listBlobsUnderPrefix(containerClient, prefix);
  return all.filter((name) => name.toLowerCase().endsWith(".csv"));
}

// --- Exports ---

module.exports = {
  getBlobServiceClient,
  getContainerClient,
  streamToString,
  getText,
  putText,
  getJson,
  putJson,
  listBlobsUnderPrefix,
  listCsvUnderPrefix,
  RESULTS_CONTAINER
};
