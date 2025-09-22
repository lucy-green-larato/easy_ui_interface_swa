// api/worker/ch-strategic-cleanup/index.js
// Timer-triggered cleanup for ch-strategic artifacts.
// Deletes stale status, cache, and output blobs older than TTL_DAYS.
// Azure Functions v4 programming model.

import { app } from "@azure/functions";
import { BlobServiceClient } from "@azure/storage-blob";

const DAY_MS = 24 * 60 * 60 * 1000;

const CFG = (() => {
  const required = (k) => {
    const v = process.env[k];
    if (!v) throw new Error(`Missing required env ${k}`);
    return v;
  };
  return {
    AZURE_STORAGE_CONNECTION_STRING: required("AZURE_STORAGE_CONNECTION_STRING"),
    CONTAINER_STATUS: process.env.CH_STRATEGIC_STATUS_CONTAINER || "ch-strategic-status",
    CONTAINER_CACHE: process.env.CH_STRATEGIC_CACHE_CONTAINER || "ch-strategic-cache",
    CONTAINER_OUT: process.env.CH_STRATEGIC_OUT_CONTAINER || "ch-strategic-out",
    TTL_DAYS: Number(process.env.CH_STRATEGIC_TTL_DAYS || 7),
  };
})();

const blobSvc = BlobServiceClient.fromConnectionString(CFG.AZURE_STORAGE_CONNECTION_STRING);
const statusContainer = blobSvc.getContainerClient(CFG.CONTAINER_STATUS);
const cacheContainer = blobSvc.getContainerClient(CFG.CONTAINER_CACHE);
const outContainer = blobSvc.getContainerClient(CFG.CONTAINER_OUT);

async function ensureContainers() {
  await Promise.all([
    statusContainer.createIfNotExists(),
    cacheContainer.createIfNotExists(),
    outContainer.createIfNotExists(),
  ]);
}

function cutoffDate() {
  return new Date(Date.now() - CFG.TTL_DAYS * DAY_MS);
}

async function deleteIfExists(container, name) {
  try { await container.deleteBlob(name); } catch { /* ignore */ }
}

async function deletePrefix(container, prefix, ctx) {
  const it = container.listBlobsFlat({ prefix });
  for await (const item of it) {
    try { await container.deleteBlob(item.name); }
    catch (e) { ctx?.error?.(`delete ${container.containerName}/${item.name} failed: ${e?.message || e}`); }
  }
}

async function cleanupStatusAndArtifacts(ctx) {
  const cut = cutoffDate();
  const toDeleteJobs = new Set();

  // Sweep status blobs (*.json) and cancel markers (*.cancel)
  for await (const item of statusContainer.listBlobsFlat()) {
    const last = item.properties.lastModified || new Date(0);
    if (last <= cut) {
      const name = item.name;
      if (name.endsWith(".json")) {
        // Try to parse and only clean finished or very old entries
        try {
          const b = await statusContainer.getBlockBlobClient(name).download();
          const text = await streamToString(b.readableStreamBody);
          const s = JSON.parse(text);
          const state = (s?.state || "").toLowerCase();
          const finished = ["done", "cancelled", "failed"].includes(state);
          if (finished || last <= cut) {
            const jobId = name.replace(/\.json$/, "");
            toDeleteJobs.add(jobId);
          }
        } catch {
          // corrupt or unreadable → delete defensively
          const jobId = name.replace(/\.json$/, "");
          toDeleteJobs.add(jobId);
        }
      } else if (name.endsWith(".cancel")) {
        const jobId = name.replace(/\.cancel$/, "");
        toDeleteJobs.add(jobId);
      }
    }
  }

  // Execute deletions
  for (const jobId of toDeleteJobs) {
    try {
      await deleteIfExists(statusContainer, `${jobId}.json`);
      await deleteIfExists(statusContainer, `${jobId}.cancel`);
      // Remove cache folder
      await deletePrefix(cacheContainer, `jobs/${jobId}/`, ctx);
      // Remove output CSV
      await deleteIfExists(outContainer, `${jobId}.csv`);
      ctx?.log?.(`Cleaned job ${jobId}`);
    } catch (e) {
      ctx?.error?.(`Cleanup for job ${jobId} failed: ${e?.message || e}`);
    }
  }
}

async function cleanupOrphanCache(ctx) {
  // Delete cache jobs that have no corresponding status blob and are older than cutoff
  const cut = cutoffDate();
  const seen = new Set();
  for await (const item of statusContainer.listBlobsFlat()) {
    const name = item.name;
    if (name.endsWith(".json")) seen.add(name.replace(/\.json$/, ""));
  }
  // Walk cache/jobs/
  const prefix = "jobs/";
  const folderSeen = new Set();
  for await (const item of cacheContainer.listBlobsByHierarchy("/", { prefix })) {
    // Collect job folders
    if (item.kind === "prefix") {
      const jobFolder = item.name; // e.g., jobs/<jobId>/
      const jobId = jobFolder.split("/")[1];
      if (!seen.has(jobId)) {
        // Check newest blob in this prefix; if all older than cutoff, delete prefix
        let newest = new Date(0);
        for await (const f of cacheContainer.listBlobsFlat({ prefix: jobFolder })) {
          const lm = f.properties.lastModified || new Date(0);
          if (lm > newest) newest = lm;
        }
        if (newest <= cut) {
          await deletePrefix(cacheContainer, jobFolder, ctx);
          ctx?.log?.(`Cleaned orphan cache ${jobFolder}`);
        }
      }
      folderSeen.add(jobFolder);
    }
  }
}

async function streamToString(rs) { const chunks = []; for await (const c of rs) chunks.push(Buffer.from(c)); return Buffer.concat(chunks).toString("utf8"); }

app.timer("chStrategicCleanup", {
  // Default: run every day at 02:40 UTC. Override with env CH_STRATEGIC_CLEANUP_CRON
  schedule: { cron: process.env.CH_STRATEGIC_CLEANUP_CRON || "0 40 2 * * *" },
  handler: async (timer, ctx) => {
    await ensureContainers();
    try {
      await cleanupStatusAndArtifacts(ctx);
      await cleanupOrphanCache(ctx);
    } catch (e) {
      ctx.error?.(e);
    }
  },
});
