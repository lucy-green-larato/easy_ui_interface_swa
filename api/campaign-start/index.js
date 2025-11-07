// /api/campaign-start/index.js 07-11-2025 v13
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
const MAX_BYTES = Number.isFinite(MAX_BYTES_ENV) && MAX_BYTES_ENV > 4096 ? MAX_BYTES_ENV : MAX_BYTES_DEFAULT;

const STORAGE_CONN = process.env.AzureWebJobsStorage;

// ---------- CORS ----------
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, X-Correlation-Id, Authorization, X-Idempotency-Key",
};

// ---------- utils ----------
function readHeader(req, name) {
  if (!req || !req.headers) return undefined;
  const h = req.headers;
  return h[name] ?? h[name.toLowerCase()] ?? h[name.toUpperCase()];
}
function getCorrelationId(req) {
  return readHeader(req, "x-correlation-id") || `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
}
function sanitizePage(p) {
  const s = String(p || "").trim().toLowerCase();
  return s || "campaign";
}
function normalizeWebsite(u) {
  if (!u) return null;
  try {
    const URLu = new URL(u.startsWith("http") ? u : `https://${u}`);
    URLu.protocol = "https:";
    URLu.hash = "";
    return URLu.toString();
  } catch { return null; }
}
function computePrefix({ page, runId, userId, now }) {
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  const bucket = `${yyyy}/${mm}/${dd}`;
  const safeUser = (String(userId || "anon").replace(/[^a-z0-9._-]/gi, "-").slice(0, 80) || "anon");
  return `runs/${runId}/${page}/${bucket}/${safeUser}/`;
}
function streamToBuffer(readable) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readable.on("data", (d) => chunks.push(d));
    readable.on("end", () => resolve(Buffer.concat(chunks)));
    readable.on("error", (e) => reject(e));
  });
}
function normalizeCompany(s) {
  if (!s) return null;
  const out = String(s).replace(/\s+/g, " ").trim();
  return out || null;
}
function normalizeArray(v, splitter = /[,;\n]/) {
  if (Array.isArray(v)) return v.map((s) => String(s ?? "").trim()).filter(Boolean);
  if (typeof v === "string") return v.split(splitter).map((s) => s.trim()).filter(Boolean);
  return [];
}
function sanitizeKey(s) {
  if (!s) return "campaign";
  return String(s).toLowerCase().replace(/[^a-z0-9._-]/g, "-").replace(/-+/g, "-");
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
  return sanitizeKey(chosen);
}

module.exports = async function (context, req) {
  if (req.method === "OPTIONS") {
    context.res = { status: 204, headers: CORS };
    return;
  }

  try {
    if (req.method !== "POST") {
      context.res = {
        status: 405,
        headers: CORS,
        body: { error: "method_not_allowed", message: "Use POST" }
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
    if (Array.isArray(body.relevant_competitors)) relevant_competitors = body.relevant_competitors;
    else if (typeof body.relevant_competitors === "string") relevant_competitors = normalizeArray(body.relevant_competitors);
    relevant_competitors = [...new Set(relevant_competitors.map(s => s.trim()).filter(Boolean))].slice(0, 8);

    // campaign_requirement constraint
    const rawReq = (body.campaign_requirement || body.campaignRequirement || "").toLowerCase();
    const campaign_requirement_effective = ["upsell", "win-back", "growth"].includes(rawReq) ? rawReq : null;

    // Normalized input payload (canonical)
    const inputPayload = {
      page,
      rowCount: Number.isFinite(Number(body.rowCount)) ? Number(body.rowCount) : undefined,
      filters: (body.filters && typeof body.filters === "object" && !Array.isArray(body.filters)) ? body.filters : undefined,
      notes: typeof body.notes === "string" ? body.notes : undefined,

      sales_model: (typeof body.sales_model === "string" ? body.sales_model
        : (typeof body.salesModel === "string" ? body.salesModel : undefined)),

      call_type: (typeof body.call_type === "string" ? body.call_type
        : (typeof body.callType === "string" ? body.callType : undefined)),

      supplier_company: normalizeCompany(body.supplier_company || body.company_name || body.company),
      supplier_website: normalizeWebsite(body.supplier_website || body.company_website || body.website),
      supplier_linkedin: body.supplier_linkedin || body.company_linkedin || body.linkedin || undefined,
      supplier_usps: Array.isArray(body.supplier_usps) ? body.supplier_usps.slice(0, 16) : undefined,

      campaign_industry: body.campaign_industry || body.company_industry || undefined,
      selected_industry: body.selected_industry || undefined,
      campaign_requirement: campaign_requirement_effective,
      relevant_competitors,

      prospect_company: body.prospect_company || undefined,
      prospect_website: normalizeWebsite(body.prospect_website || undefined),
      prospect_linkedin: body.prospect_linkedin || undefined,

      csvText,
      csvFilename,
      csvSummary: (body.csvSummary && typeof body.csvSummary === "object") ? body.csvSummary : undefined,
    };

    // ---- Validation (lightweight) ----
    const missing = [];
    if (!inputPayload.page) missing.push("page");
    if (!inputPayload.supplier_company && !inputPayload.prospect_company) missing.push("supplier_company|prospect_company");
    if (!inputPayload.supplier_website && !inputPayload.prospect_website) missing.push("supplier_website|prospect_website");
    if (missing.length) {
      context.res = {
        status: 400,
        headers: { ...CORS, "content-type": "application/json", "x-correlation-id": getCorrelationId(req) },
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
    const relPrefix = computePrefix({ page: sanitizePage(body.page || "campaign"), runId, userId, now });

    // ---- Initial status ----
    const enqueuedAt = now.toISOString();
    const initialStatus = {
      runId,
      state: "Queued",
      input: {
        ...inputPayload,
        csvText: undefined // avoid echoing raw CSV in status
      },
      enqueuedAt,
      links: {
        prefix: relPrefix,
        statusUrl: `${containerClient.url}/${relPrefix}status.json`,
        inputUrl: `${containerClient.url}/${relPrefix}input.json`
      }
    };

    // persist status + input
    const statusStr = JSON.stringify(initialStatus, null, 2);
    await containerClient.getBlockBlobClient(`${relPrefix}status.json`)
      .upload(statusStr, Buffer.byteLength(statusStr), { blobHTTPHeaders: { blobContentType: "application/json" } });

    const inputStr = JSON.stringify({ ...inputPayload }, null, 2);
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
        page: sanitizePage(body.page || "campaign"),
        when: now.toISOString(),
        prefix: relPrefix,
        summary: { supplier_company: inputPayload.supplier_company, campaign_industry: inputPayload.campaign_industry, rowCount: inputPayload.rowCount ?? null }
      },
      ...(Array.isArray(idx.items) ? idx.items : [])
    ].slice(0, 50);

    const idxStr = JSON.stringify(idx, null, 2);
    await userIdxClient.upload(idxStr, Buffer.byteLength(idxStr), {
      blobHTTPHeaders: { blobContentType: "application/json" }
    });

    // ---- Build queue payload (canonical + back-compat mirrors) ----
    const input = inputPayload; // canonical
    const correlationId = getCorrelationId(req);

    let msg = {
      op: "kickoff",
      runId,
      userId,
      page: sanitizePage(body.page || "campaign"),
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
      evidenceMsgId: r2?.messageId
    });

    context.res = {
      status: 202,
      headers: { ...CORS, "content-type": "application/json", "x-correlation-id": correlationId },
      body: {
        runId,
        container: RESULTS_CONTAINER,
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
