'use strict';
const { BlobServiceClient } = require('@azure/storage-blob');

const AZURE_STORAGE = process.env.AzureWebJobsStorage;
const STATUS_CONTAINER = 'ch-strategic-status';
const CACHE_CONTAINER  = 'ch-strategic-cache';
const OUT_CONTAINER    = 'ch-strategic-out';
const TTL_DAYS = parseInt(process.env.CH_STRATEGIC_TTL_DAYS || '7', 10);

function olderThan(date, days) {
  const cutoff = Date.now() - days * 86400 * 1000;
  return new Date(date).getTime() < cutoff;
}

module.exports = async function (context, myTimer) {
  const blob = BlobServiceClient.fromConnectionString(AZURE_STORAGE);

  async function purgeStatus() {
    const c = blob.getContainerClient(STATUS_CONTAINER);
    await c.createIfNotExists();
    for await (const b of c.listBlobsFlat()) {
      if (!b.properties.lastModified) continue;
      if (olderThan(b.properties.lastModified, TTL_DAYS)) {
        await c.deleteBlob(b.name).catch(() => {});
      }
    }
  }

  async function purgeOutputs() {
    const c = blob.getContainerClient(OUT_CONTAINER);
    await c.createIfNotExists();
    for await (const b of c.listBlobsFlat()) {
      if (!b.properties.lastModified) continue;
      if (olderThan(b.properties.lastModified, TTL_DAYS)) {
        await c.deleteBlob(b.name).catch(() => {});
      }
    }
  }

  async function purgeCache() {
    const c = blob.getContainerClient(CACHE_CONTAINER);
    await c.createIfNotExists();
    // We delete entire job folders when the manifest/input is old.
    for await (const b of c.listBlobsByHierarchy('/', { prefix: 'jobs/' })) {
      if (!('lastModified' in b.properties)) continue;
      if (olderThan(b.properties.lastModified, TTL_DAYS)) {
        if (b.kind === 'prefix') {
          // delete whole virtual folder
          for await (const inner of c.listBlobsFlat({ prefix: b.name })) {
            await c.deleteBlob(inner.name).catch(() => {});
          }
        } else {
          await c.deleteBlob(b.name).catch(() => {});
        }
      }
    }
  }

  await Promise.all([purgeStatus(), purgeOutputs(), purgeCache()]);
  context.log(`ch-strategic cleanup completed (TTL=${TTL_DAYS}d)`);
};
