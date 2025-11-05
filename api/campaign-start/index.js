// /api/campaign-start/index.js 05-11-2025 v12
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// POST /api/campaign-start → enqueues job to campaign queue, writes initial status.json ("Queued"), returns 202 { runId }.

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const crypto = require("crypto");

// ---- Config ----
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const QUEUE_NAME = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
const EVIDENCE_QUEUE = process.env.Q_CAMPAIGN_EVIDENCE || "campaign-evidence-jobs";

const MAX_BYTES_DEFAULT = 48 * 1024; // 49152
const MAX_BYTES_ENV = Number.parseInt(process.env.CAMPAIGN_MAX_MSG_BYTES, 10);
const MAX_BYTES =
  Number.isFinite(MAX_BYTES_ENV) && MAX_BYTES_ENV >= 1024 && MAX_BYTES_ENV <= 62 * 1024
    ? MAX_BYTES_ENV
    : MAX_BYTES_DEFAULT;

// ---- Small utils ----
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, X-Correlation-Id, Authorization",
};

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
  const s = String(page || "campaign").trim().toLowerCase();
  return s.replace(/[^a-z0-9._-]/g, "-").replace(/-+/g, "-") || "campaign";
}
async function enqueueMessage(queueClient, jsonString) {
  // SDK base64-encodes for you; pass a plain string
  return queueClient.sendMessage(jsonString);
}

// ---- Auth helpers (SWA) ----
function decodeClientPrincipal(req) {
  const b64 = readHeader(req, "x-ms-client-principal");
  if (!b64) return null;
  try {
    const json = Buffer.from(b64, "base64").toString("utf8");
    return JSON.parse(json);
  } catch {
    return null;
  }
}
function getUserIdFromReq(req) {
  const cp = decodeClientPrincipal(req) || {};
  const claims = cp?.claims || [];
  const byType = Object.create(null);
  for (const c of claims) byType[c.typ?.toLowerCase?.() || ""] = c.val;

  const oid = byType["http://schemas.microsoft.com/identity/claims/objectidentifier"]
    || byType["oid"] || null;
  const sub = byType["sub"] || null;
  const email = (byType["emails"] || byType["email"] || cp?.userDetails || "").toLowerCase();

  const chosen = oid || sub || email || "anonymous";
  return String(chosen).trim().toLowerCase().replace(/[^a-z0-9_.@-]/g, "-").replace(/-+/g, "-");
}

// IMPORTANT: container-relative prefix (user-scoped, date-bucketed)
function computePrefix({ page = "campaign", runId, userId, now = new Date() }) {
  if (!runId || typeof runId !== "string") {
    throw new Error("computePrefix: runId is required and must be a string");
  }
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  const d = String(now.getUTCDate()).padStart(2, "0");
  const segPage = sanitizePage(page);
  const segUser = String(userId || "anonymous").trim().toLowerCase().replace(/[^a-z0-9_.@-]/g, "-").replace(/-+/g, "-");
  return `runs/${segPage}/${segUser}/${y}/${m}/${d}/${runId}/`;
}

async function writeInitialStatus(containerClient, relPrefix, status) {
  const client = containerClient.getBlockBlobClient(`${relPrefix}status.json`);
  const payload = JSON.stringify(status);
  await client.upload(payload, Buffer.byteLength(payload), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });
}

function toHttps(u) {
  if (!u) return u;
  const s = String(u).trim();
  if (!s) return s;
  if (/^https:\/\//i.test(s)) return s;
  if (/^http:\/\//i.test(s)) return s.replace(/^http:\/\//i, "https://");
  return `https://${s}`;
}

// small helper to read blob stream
async function streamToBuffer(readable) {
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks);
}

module.exports = async function (context, req) {
  const method = (req?.method || "GET").toUpperCase();
  const correlationId = getCorrelationId(req);

  if (method === "OPTIONS") {
    context.res = { status: 204, headers: CORS };
    return;
  }

  if (method !== "POST") {
    context.res = {
      status: 405,
      headers: { ...CORS, "content-type": "application/json", "x-correlation-id": correlationId, allow: "POST" },
      body: { error: "method_not_allowed", message: "Only POST is supported" },
    };
    return;
  }

  try {
    const STORAGE_CONN = process.env.AzureWebJobsStorage;
    if (!STORAGE_CONN) {
      context.res = {
        status: 500,
        headers: { ...CORS, "content-type": "application/json", "x-correlation-id": correlationId },
        body: { error: "config", message: "AzureWebJobsStorage app setting is missing" },
      };
      return;
    }
    if (!QUEUE_NAME) {
      context.res = {
        status: 500,
        headers: { ...CORS, "content-type": "application/json", "x-correlation-id": correlationId },
        body: { error: "config", message: "CAMPAIGN_QUEUE_NAME app setting is missing" },
      };
      return;
    }

    // ---- Parse / normalise input (normalise BEFORE validation) ----
    const body = (typeof req.body === "object" && req.body) || {};
    const page = sanitizePage(body.page || "campaign");
    const userId = getUserIdFromReq(req);

    const csvText = (typeof body.csvText === "string" && body.csvText.trim()) || null;
    const csvFilename = (typeof body.csvFilename === "string" && body.csvFilename.trim()) || null;

    // Legacy → prospect_* (back-compat)
    if (body.company && !body.prospect_company) body.prospect_company = String(body.company).trim();
    if (body.company_name && !body.prospect_company) body.prospect_company = String(body.company_name).trim();
    if (body.website && !body.prospect_website) body.prospect_website = String(body.website).trim();
    if (body.company_website && !body.prospect_website) body.prospect_website = String(body.company_website).trim();
    if (body.linkedin && !body.prospect_linkedin) body.prospect_linkedin = String(body.linkedin).trim();
    if (body.company_linkedin && !body.prospect_linkedin) body.prospect_linkedin = String(body.company_linkedin).trim();

    // USPs: allow comma-separated string or array
    if (!Array.isArray(body.supplier_usps)) {
      if (typeof body.usps === "string") {
        body.supplier_usps = body.usps.split(/[,;\n]/).map(s => s.trim()).filter(Boolean);
      } else if (Array.isArray(body.usps)) {
        body.supplier_usps = body.usps.map(s => String(s ?? "").trim()).filter(Boolean);
      }
    }

    // Relevant competitors: array or delimited string; cap to 8 + dedupe
    let relevant_competitors = [];
    if (Array.isArray(body.relevant_competitors)) {
      relevant_competitors = body.relevant_competitors.map(s => (s == null ? "" : String(s))).filter(Boolean);
    } else if (typeof body.relevant_competitors === "string") {
      relevant_competitors = body.relevant_competitors.split(/[,;\n]/).map(s => s.trim()).filter(Boolean);
    }
    if (relevant_competitors.length) {
      const seen = new Set();
      relevant_competitors = relevant_competitors
        .map(s => s.trim())
        .filter(s => s && !seen.has(s.toLowerCase()) && (seen.add(s.toLowerCase()) || true))
        .slice(0, 8);
    }

    // sales model / call type normalisation
    if (!body.sales_model && body.salesModel) body.sales_model = String(body.salesModel).trim();
    if (!body.call_type && body.callType) body.call_type = String(body.callType).trim();

    // Campaign context
    const campaign_industry = (body.campaign_industry ?? body.companyIndustry ?? body.industry ?? "").trim();

    // Canonical supplier inputs (prefer supplier_*; fallback to legacy prospect_*)
    const supplier_company_raw = (body.supplier_company ?? body.prospect_company ?? "").trim();
    const supplier_website_raw = (body.supplier_website ?? body.prospect_website ?? "").trim();
    const supplier_linkedin_raw = (body.supplier_linkedin ?? body.prospect_linkedin ?? body.companyLinkedIn ?? "").trim();

    const supplier_company = supplier_company_raw;
    const supplier_website_https = supplier_website_raw ? toHttps(supplier_website_raw) : "";
    const supplier_linkedin_https = supplier_linkedin_raw ? toHttps(supplier_linkedin_raw) : "";

    // Supplier USPs: cap to 12
    const supplier_usps = Array.isArray(body.supplier_usps)
      ? body.supplier_usps.map(s => String(s || "").trim()).filter(Boolean).slice(0, 12)
      : (typeof body.supplier_usps === "string"
        ? body.supplier_usps.split(/[,;\n]/).map(s => s.trim()).filter(Boolean).slice(0, 12)
        : (typeof body.user_usps === "string"
          ? body.user_usps.split(/[,;\n]/).map(s => s.trim()).filter(Boolean).slice(0, 12)
          : Array.isArray(body.user_usps)
            ? body.user_usps.map(s => String(s || "").trim()).filter(Boolean).slice(0, 12)
            : []));

    // Page & totals
    const pageRaw = body.page || "campaign";
    const effectivePage = sanitizePage(pageRaw);

    // salesModel / callType canonicalisation
    const _salesCandidates = [
      body.sales_model,
      body.salesModel,
      body.filters?.sales_model,
      body.filters?.salesModel,
      body.call_type, body.callType,
      body.filters?.call_type, body.filters?.callType
    ].map(v => (v == null ? "" : String(v).trim().toLowerCase()));
    const salesModel = _salesCandidates.find(v => v === "direct" || v === "partner") || "direct";
    body.sales_model = salesModel;

    const callTypeRaw =
      body.call_type ?? body.callType ?? body.filters?.call_type ?? body.filters?.callType ?? null;
    const callType = (callTypeRaw == null) ? null : String(callTypeRaw).trim().toLowerCase();

    // numeric rowCount (optional)
    let rc = (body.rowCount !== undefined && body.rowCount !== null) ? body.rowCount : null;
    if (rc != null) {
      const n = Number(rc);
      if (!Number.isFinite(n) || n < 0) {
        context.res = {
          status: 400,
          headers: { ...CORS, "content-type": "application/json", "x-correlation-id": correlationId },
          body: { error: "bad_request", message: "rowCount must be a non-negative number" },
        };
        return;
      }
      rc = Math.floor(n);
    } else if (typeof csvText === "string" && csvText.trim()) {
      const lines = csvText.split(/\r?\n/).filter(l => l.trim().length > 0);
      rc = Math.max(0, lines.length - 1);
    } else {
      rc = null;
    }

    // Campaign requirement
    let campaign_requirement = null;
    if (typeof body.campaign_requirement === "string") {
      const v = body.campaign_requirement.trim().toLowerCase();
      campaign_requirement = ["upsell", "win-back", "growth"].includes(v) ? v : null;
    }
    const campaign_requirement_effective = campaign_requirement ?? "unspecified";

    // ---- Validate AFTER normalisation ----
    const missing = [];
    if (!supplier_company) missing.push("supplier_company");
    if (!supplier_website_https) missing.push("supplier_website");
    if (missing.length) {
      context.res = {
        status: 400,
        headers: { ...CORS, "content-type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: `Missing required field(s): ${missing.join(", ")}` }
      };
      return;
    }

    // ---- Storage + Queue clients ----
    const blobService = BlobServiceClient.fromConnectionString(STORAGE_CONN);
    const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);
    await containerClient.createIfNotExists();

    const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
    const qMain = qs.getQueueClient(QUEUE_NAME);
    const qEvidence = qs.getQueueClient(EVIDENCE_QUEUE);
    await qMain.createIfNotExists();
    await qEvidence.createIfNotExists();

    // ---- IDs / prefix ----
    const clientRunKey = body.clientRunKey || readHeader(req, "x-idempotency-key") || null;
    const runId = clientRunKey
      ? crypto.createHash("sha1").update(String(clientRunKey)).digest("hex")
      : (crypto.randomUUID ? crypto.randomUUID() : `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`);
    const now = new Date();
    const relPrefix = computePrefix({ page: effectivePage, runId, userId, now });

    // ---- Initial status ----
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
        supplier_company,
        supplier_website: supplier_website_https,
        supplier_linkedin: supplier_linkedin_https,
        supplier_usps,
        campaign_industry,
        campaign_requirement: campaign_requirement_effective,
        relevant_competitors,
        // Legacy mirrors
        prospect_company: supplier_company,
        prospect_website: supplier_website_https,
        prospect_linkedin: supplier_linkedin_https,
        user_usps: supplier_usps,
        company_industry: campaign_industry
      },
      enqueuedAt,
      correlationId,
    };
    await writeInitialStatus(containerClient, relPrefix, initialStatus);

    // ---- Write input.json ----
    const inputPayload = {
      page: effectivePage,
      rowCount: rc ?? null,
      filters: body.filters ?? null,
      notes: body.notes ?? null,
      sales_model: salesModel ?? null,
      call_type: callType ?? null,
      supplier_company,
      supplier_website: supplier_website_https,
      supplier_linkedin: supplier_linkedin_https,
      supplier_usps,
      campaign_industry,
      selected_industry: campaign_industry || undefined,
      campaign_requirement: campaign_requirement_effective,
      relevant_competitors: Array.isArray(relevant_competitors) ? relevant_competitors : [],
      csvText: csvText || null,
      csvFilename: csvFilename || null,
      csvSummary: body.csvSummary || null,
      // Legacy
      prospect_company: supplier_company,
      prospect_website: supplier_website_https,
      prospect_linkedin: supplier_linkedin_https,
      user_usps: supplier_usps,
      company_industry: campaign_industry
    };
    const inputStr = JSON.stringify(inputPayload, null, 2);
    await containerClient.getBlockBlobClient(`${relPrefix}input.json`)
      .upload(inputStr, Buffer.byteLength(inputStr), { blobHTTPHeaders: { blobContentType: "application/json" } });

    // ---- Update per-user recent runs index: users/<userId>/recent.json ----
    const userIdxPath = `users/${userId}/recent.json`;
    const userIdxClient = containerClient.getBlockBlobClient(userIdxPath);
    let idx = { userId, items: [] };
    try {
      const dl = await userIdxClient.download();
      const buf = await streamToBuffer(dl.readableStreamBody);
      idx = JSON.parse(buf.toString("utf8"));
    } catch { /* not found → create */ }

    idx.items = [
      {
        runId,
        page: effectivePage,
        when: now.toISOString(),
        prefix: relPrefix,
        summary: { supplier_company, campaign_industry, rowCount: rc ?? null }
      },
      ...(Array.isArray(idx.items) ? idx.items : [])
    ].slice(0, 50);

    const idxStr = JSON.stringify(idx, null, 2);
    await userIdxClient.upload(idxStr, Buffer.byteLength(idxStr), {
      blobHTTPHeaders: { blobContentType: "application/json" }
    });

    // ---- Build queue payload (canonical + back-compat mirrors) ----
    const input = inputPayload; // canonical
    const msg = {
      op: "kickoff",
      runId,
      userId,
      page: effectivePage,
      enqueuedAt,
      prefix: relPrefix,                // container-relative
      container: RESULTS_CONTAINER,     // convenience for workers
      correlationId,
      clientRunKey: clientRunKey ?? null,
      runConfig: {
        campaign_requirement: campaign_requirement_effective,
        relevant_competitors: Array.isArray(relevant_competitors) ? relevant_competitors : []
      },
      input,
      // mirrors for older workers
      page_mirror: input.page,
      rowCount: input.rowCount,
      filters: input.filters,
      notes: input.notes,
      sales_model: input.sales_model,
      call_type: input.call_type,
      supplier_company: input.supplier_company,
      supplier_website: input.supplier_website,
      supplier_linkedin: input.supplier_linkedin,
      supplier_usps: input.supplier_usps,
      campaign_industry: input.campaign_industry,
      selected_industry: input.selected_industry,
      campaign_requirement: input.campaign_requirement,
      relevant_competitors: input.relevant_competitors,
      csvText: input.csvText,
      csvFilename: input.csvFilename,
      csvSummary: input.csvSummary,
      prospect_company: input.prospect_company,
      prospect_website: input.prospect_website,
      prospect_linkedin: input.prospect_linkedin,
      user_usps: input.user_usps,
      company_industry: input.company_industry
    };

    // ---- Slim payload if needed (queue limit) ----
    const PROTECT = new Set([
      "notes", "rowCount", "campaign_industry", "company_industry",
      "relevant_competitors", "sales_model", "salesModel",
      "call_type", "callType", "supplier_company", "supplier_website",
      "supplier_linkedin", "prospect_company", "prospect_website",
      "prospect_linkedin", "csvFilename"
    ]);

    function safeStringify(obj) { try { return JSON.stringify(obj); } catch { return "{}"; } }
    function bytes(s) { return Buffer.byteLength(s, "utf8"); }

    let payload = safeStringify(msg);
    if (bytes(payload) > MAX_BYTES) {
      const slim = { ...msg, input: { ...(msg.input || {}) } };

      // drop biggest offenders in order
      if (slim.input && typeof slim.input.csvText === "string") slim.input.csvText = null;
      if (bytes(safeStringify(slim)) > MAX_BYTES && "csvSummary" in slim.input) slim.input.csvSummary = null;
      if (bytes(safeStringify(slim)) > MAX_BYTES && "filters" in slim.input) slim.input.filters = null;

      if (bytes(safeStringify(slim)) > MAX_BYTES) {
        for (const k of ["user_usps", "supplier_usps"]) {
          if (bytes(safeStringify(slim)) <= MAX_BYTES) break;
          if (!(k in slim.input)) continue;
          if (!PROTECT.has(k)) slim.input[k] = null;
        }
      }
      if (bytes(safeStringify(slim)) > MAX_BYTES && slim.input && typeof slim.input === "object") {
        for (const k of Object.keys(slim.input)) {
          if (bytes(safeStringify(slim)) <= MAX_BYTES) break;
          if (!PROTECT.has(k) && slim.input[k] != null) slim.input[k] = null;
        }
      }
      payload = safeStringify(slim);
      context.log.warn("campaign_start_payload_slimmed", { bytes: bytes(payload) });
    }

    // ---- Enqueue to main + evidence queues (once each) ----
    const r1 = await enqueueMessage(qMain, payload);
    const r2 = await enqueueMessage(qEvidence, payload);
    context.log({
      event: "campaign_start_enqueued",
      runId,
      mainQueue: QUEUE_NAME,
      evidenceQueue: EVIDENCE_QUEUE,
      mainMsgId: r1?.messageId,
      evidMsgId: r2?.messageId,
      correlationId
    });

    // ---- Response ----
    context.res = {
      status: 202,
      headers: { ...CORS, "content-type": "application/json", "x-correlation-id": correlationId },
      body: {
        runId,
        prefix: relPrefix,
        statusUrl: `${containerClient.url}/${relPrefix}status.json`,
        inputUrl: `${containerClient.url}/${relPrefix}input.json`
      }
    };
  } catch (e) {
    context.log.error("campaign_start_failed", e);
    context.res = {
      status: 500,
      headers: { ...CORS, "content-type": "application/json", "x-correlation-id": getCorrelationId(req) },
      body: { error: "server_error", message: String(e?.message || e) },
    };
  }
};
