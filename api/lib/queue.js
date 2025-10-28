// api/lib/queue.js
const { QueueServiceClient } = require("@azure/storage-queue");

const QUEUE_CONN = process.env.AzureWebJobsStorage;
const CAMPAIGN_QUEUE = process.env.CAMPAIGN_QUEUE || "campaign";

function getQueue() {
  if (!QUEUE_CONN) throw new Error("AzureWebJobsStorage not configured");
  const svc = QueueServiceClient.fromConnectionString(QUEUE_CONN);
  const q = svc.getQueueClient(CAMPAIGN_QUEUE);
  return q;
}

async function ensureQueue(q) {
  try { await q.createIfNotExists(); } catch {}
}

async function enqueue(message) {
  const q = getQueue();
  await ensureQueue(q);
  const body = Buffer.from(JSON.stringify(message), "utf8").toString("base64");
  await q.sendMessage(body);
}

module.exports = { enqueue };
