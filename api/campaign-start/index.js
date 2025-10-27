// /api/campaign-start/index.js 26-10-2025 v8
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// POST /api/campaign/start → enqueues job to campaign-jobs, writes initial status.json ("Queued"), returns 202 { runId }.

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const crypto = require("crypto");

// ---- Config ----
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const QUEUE_NAME = process.env.CAMPAIGN_JOBS_QUEUE || "campaign-jobs";
const MAX_BYTES_DEFAULT = 48 * 1024; // 49152
const MAX_BYTES_ENV = Number.parseInt(process.env.CAMPAIGN_MAX_MSG_BYTES, 10);
const MAX_BYTES =
  Number.isFinite(MAX_BYTES_ENV) && MAX_BYTES_ENV >= 1024 && MAX_BYTES_ENV <= 62 * 1024
    ? MAX_BYTES_ENV
    : MAX_BYTES_DEFAULT;

// ---- Small utils ----
function readHeader(req, name) {
  return req?.headers?.[name] || req?.headers?.[name.toLowerCase()] || null;
}

function getCorrelationId(req) {
  return (
    readHeader(req, "x-correlation-id") ||
    `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`
  );
}

function sanitizePage(page) {
  const s = String(page || "default").trim().toLowerCase();
  const cleaned = s.replace(/[^a-z0-9._-]/g, "-").replace(/-+/g, "-");
  return cleaned || "default";
}

// --- helper: enqueue a message to Azure Storage Queue ---
async function enqueueMessage(queueClient, jsonString) {
  // SDK base64-encodes for you; pass a plain string
  return queueClient.sendMessage(jsonString);
}

// IMPORTANT: container-relative prefix (NO container name here)
function computePrefix(page, runId, now = new Date()) {
  const yyyy = now.getUTCFullYear();
  const MM = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  const p = sanitizePage(page);
  return `campaign/${p}/${yyyy}/${MM}/${dd}/${runId}/`;
}

async function writeInitialStatus(containerClient, relPrefix, status) {
  // relPrefix is container-relative (e.g., "campaign/.../<runId>/")
  const client = containerClient.getBlockBlobClient(`${relPrefix}status.json`);
  const payload = JSON.stringify(status);
  await client.upload(payload, Buffer.byteLength(payload), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });
}

module.exports = async function (context, req) {
  const method = (req?.method || "GET").toUpperCase();
  const correlationId = getCorrelationId(req);

  if (method !== "POST") {
    context.res = {
      status: 405,
      headers: {
        "content-type": "application/json",
        "x-correlation-id": correlationId,
        allow: "POST",
      },
      body: { error: "method_not_allowed", message: "Only POST is supported" },
    };
    return;
  }

  try {
    const STORAGE_CONN = process.env.AzureWebJobsStorage;
    if (!STORAGE_CONN) {
      context.res = {
        status: 500,
        headers: {
          "content-type": "application/json",
          "x-correlation-id": correlationId,
        },
        body: { error: "config", message: "AzureWebJobsStorage app setting is missing" },
      };
      return;
    }
    if (!QUEUE_NAME) {
      context.res = {
        status: 500,
        headers: { "content-type": "application/json", "x-correlation-id": correlationId },
        body: { error: "config", message: "CAMPAIGN_JOBS_QUEUE app setting is missing" }
      };
      return;
    }
    // Parse/normalise input
    const body = (typeof req.body === "object" && req.body) || {};
    // --- Company inputs from the body (parse only; no validation here) ---
    const prospect_company = (body.prospect_company || "").toString().trim();
    const prospect_website = (body.prospect_website || "").toString().trim();
    const prospect_linkedin = (body.prospect_linkedin || "").toString().trim();
    const user_usps = Array.isArray(body.user_usps)
      ? body.user_usps.map(s => (s == null ? "" : String(s)).trim()).filter(Boolean)
      : [];
    const pageRaw = body.page || "campaign";
    const effectivePage = sanitizePage(pageRaw);

    let salesModel =
      body.salesModel ??
      body.sales_model ??
      body.filters?.salesModel ??
      body.filters?.sales_model ??
      null;

    let callType = body.call_type ?? body.callType ?? body.filters?.call_type ?? body.filters?.callType ?? null;

    if (salesModel != null) {
      const sm = String(salesModel).trim().toLowerCase();
      if (sm !== "direct" && sm !== "partner") {
        context.res = {
          status: 400,
          headers: {
            "content-type": "application/json",
            "x-correlation-id": correlationId,
          },
          body: { error: "bad_request", message: "salesModel must be 'direct' or 'partner' if provided" },
        };
        return;
      }
      salesModel = sm;
    }

    // numeric rowCount (optional)
    let rc = body.rowCount;
    if (rc != null) {
      const n = Number(rc);
      if (!Number.isFinite(n) || n < 0) {
        context.res = {
          status: 400,
          headers: {
            "content-type": "application/json",
            "x-correlation-id": correlationId,
          },
          body: { error: "bad_request", message: "rowCount must be a non-negative number" },
        };
        return;
      }
      rc = Math.floor(n);
    }

    // Idempotency support
    const clientRunKey = body.clientRunKey || readHeader(req, "x-idempotency-key") || null;
    const runId = clientRunKey
      ? crypto.createHash("sha1").update(String(clientRunKey)).digest("hex")
      : (crypto.randomUUID ? crypto.randomUUID() : `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`);

    const now = new Date();
    const relPrefix = computePrefix(effectivePage, runId, now); // container-relative (no 'results/' here)

    // Blob container client
    const blobService = BlobServiceClient.fromConnectionString(STORAGE_CONN);
    const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);
    await containerClient.createIfNotExists();

    // Initial status (Queued)
    const enqueuedAt = now.toISOString();
    const initialStatus = {
      runId,
      state: "Queued",
      input: {
        page: effectivePage,
        rowCount: rc ?? null,
        filters: body.filters ?? null,
        notes: body.notes ?? null,
        sales_model: salesModel ?? null,
        call_type: callType ?? null,

        // keep for traceability
        prospect_company,
        prospect_website,
        prospect_linkedin,
        user_usps
      },
      enqueuedAt,
      correlationId,
    };

    await writeInitialStatus(containerClient, relPrefix, initialStatus);

    // Build queue message (container-relative prefix)
    const msg = {
      runId,
      page: effectivePage,
      rowCount: rc ?? null,
      filters: body.filters ?? null,
      notes: body.notes ?? null,
      salesModel: salesModel ?? null,
      call_type: callType ?? null,
      enqueuedAt,
      prefix: relPrefix,
      correlationId,
      clientRunKey: clientRunKey ?? null,
      prospect_company,
      prospect_website,
      prospect_linkedin,
      user_usps
    };

    // Trim oversized payload (Azure Queue limit ~64KB post-base64)
    function safeStringify(obj) {
      try { return JSON.stringify(obj); } catch { return "{}"; }
    }
    let payload = safeStringify(msg);

    if (Buffer.byteLength(payload) > MAX_BYTES) {
      const slim = { ...msg, notes: null };
      let s = safeStringify(slim);
      if (Buffer.byteLength(s) > MAX_BYTES) {
        slim.filters = null;
        s = safeStringify(slim);
        if (Buffer.byteLength(s) > MAX_BYTES) {
          slim.rowCount = null;
          s = safeStringify(slim);
        }
      }
      payload = s;
    }
    // Enqueue (SDK handles base64 encoding; send plain JSON string)
    const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
    const q = qs.getQueueClient(QUEUE_NAME);
    await q.createIfNotExists();
    context.log({
      event: "campaign_start_storage_targets",
      blobContainerUrl: containerClient.url,
      queueUrl: q.url,
      queueName: QUEUE_NAME
    });

    try {
      context.log({
        event: "campaign_start_inputs",
        prospect_company,
        has_site: !!prospect_website,
        has_linkedin: !!prospect_linkedin,
        user_usps_count: user_usps.length
      });
      const resp = await enqueueMessage(q, payload);
      context.log({
        event: "campaign_start_enqueued_ok",
        runId,
        queue: QUEUE_NAME,
        messageId: resp.messageId,
        insertedOn: resp.insertedOn,
        correlationId,
      });
    } catch (e) {
      // Mark run as Failed if we couldn't enqueue
      await writeInitialStatus(containerClient, relPrefix, {
        ...initialStatus,
        state: "Failed",
        error: { code: "enqueue_error", message: String(e?.message || e) },
      });
      context.res = {
        status: 500,
        headers: { "content-type": "application/json", "x-correlation-id": correlationId },
        body: { error: "enqueue_error", message: String(e?.message || e) },
      };
      return;
    }

    context.res = {
      status: 202,
      headers: {
        "content-type": "application/json",
        "x-correlation-id": correlationId,
      },
      body: { runId },
    };
  } catch (e) {
    context.log.error("campaign_start_failed", e);
    context.res = {
      status: 500,
      headers: {
        "content-type": "application/json",
        "x-correlation-id": getCorrelationId(req),
      },
      body: { error: "server_error", message: String(e?.message || e) },
    };
  }
};
