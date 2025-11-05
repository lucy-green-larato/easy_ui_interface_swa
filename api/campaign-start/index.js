// /api/campaign-start/index.js 04-11-2025 v10.1
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// POST /api/campaign-start → enqueues job to campaign queue, writes initial status.json ("Queued"), returns 202 { runId }.

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const crypto = require("crypto");

// ---- Config ----
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const QUEUE_NAME = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
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
  // Prefer AAD object id / sub; fall back to email; last resort: anonymous
  const cp = decodeClientPrincipal(req) || {};
  const claims = cp?.claims || [];
  const byType = Object.create(null);
  for (const c of claims) byType[c.typ?.toLowerCase?.() || ""] = c.val;

  const oid = byType["http://schemas.microsoft.com/identity/claims/objectidentifier"]
    || byType["oid"]
    || null;
  const sub = byType["sub"] || null;
  const email = (byType["emails"] || byType["email"] || cp?.userDetails || "").toLowerCase();

  const chosen = oid || sub || email || "anonymous";
  // Sanitize to a path segment
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
  const segPage = String(page).trim().toLowerCase().replace(/[^a-z0-9._-]/g, "-").replace(/-+/g, "-") || "campaign";
  const segUser = String(userId || "anonymous").trim().toLowerCase().replace(/[^a-z0-9_.@-]/g, "-").replace(/-+/g, "-");
  return `runs/${segPage}/${segUser}/${y}/${m}/${d}/${runId}/`;
}

async function writeInitialStatus(containerClient, relPrefix, status) {
  // relPrefix is container-relative (e.g., "runs/<runId>/")
  const client = containerClient.getBlockBlobClient(`${relPrefix}status.json`);
  const payload = JSON.stringify(status);
  await client.upload(payload, Buffer.byteLength(payload), {
    blobHTTPHeaders: { blobContentType: "application/json" },
  });
}

// Enforce https:// for downstream schema expectations
function toHttps(u) {
  if (!u) return u;
  const s = String(u).trim();
  if (!s) return s;
  if (/^https:\/\//i.test(s)) return s;
  if (/^http:\/\//i.test(s)) return s.replace(/^http:\/\//i, "https://");
  return `https://${s}`; // no scheme -> assume https
}

module.exports = async function (context, req) {
  context.log("campaign-start hit", {
    method: req?.method,
    url: req?.url,
    origin: req?.headers?.origin,
    acReqMethod: req?.headers?.['access-control-request-method']
  });
  const method = (req?.method || "GET").toUpperCase();
  const correlationId = getCorrelationId(req);

  if (method === "OPTIONS") {
    context.res = {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*", 
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Correlation-Id, Authorization"
      }
    };
    return;
  }

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
        body: { error: "config", message: "CAMPAIGN_QUEUE_NAME app setting is missing" }
      };
      return;
    }
    // Parse/normalise input
    const body = (typeof req.body === "object" && req.body) || {};
    const csvText = (typeof body.csvText === "string" && body.csvText.trim().length)
      ? body.csvText
      : null;
    const csvFilename = (typeof body.csvFilename === "string" && body.csvFilename.trim().length)
      ? body.csvFilename.trim()
      : null;

    // --- Normalise alternate client field names to the canonical ones ---
    if (body.company && !body.prospect_company) body.prospect_company = String(body.company).trim();
    if (body.company_name && !body.prospect_company) body.prospect_company = String(body.company_name).trim();

    if (body.website && !body.prospect_website) body.prospect_website = String(body.website).trim();
    if (body.company_website && !body.prospect_website) body.prospect_website = String(body.company_website).trim();

    if (body.linkedin && !body.prospect_linkedin) body.prospect_linkedin = String(body.linkedin).trim();
    if (body.company_linkedin && !body.prospect_linkedin) body.prospect_linkedin = String(body.company_linkedin).trim();

    // USPs: allow comma-separated string or array
    if (!Array.isArray(body.supplier_usps)) {
      if (typeof body.usps === "string") {
        body.supplier_usps = body.usps.split(",").map(s => s.trim()).filter(Boolean);
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
    if (Array.isArray(relevant_competitors) && relevant_competitors.length) {
      const seen = new Set();
      relevant_competitors = relevant_competitors
        .map(s => s.trim())
        .filter(s => s && !seen.has(s.toLowerCase()) && (seen.add(s.toLowerCase()) || true))
        .slice(0, 8);
    }

    // Campaign requirement: strict enum → default to "unspecified"
    let campaign_requirement = null;
    if (typeof body.campaign_requirement === "string") {
      const v = body.campaign_requirement.trim().toLowerCase();
      campaign_requirement = ["upsell", "win-back", "growth"].includes(v) ? v : null;
    }
    const campaign_requirement_effective = campaign_requirement ?? "unspecified";

    // sales model / call type normalisation
    if (!body.sales_model && body.salesModel) body.sales_model = String(body.salesModel).trim();
    if (!body.call_type && body.callType) body.call_type = String(body.callType).trim();

    // --- Normalise campaign context (industry, supplier inputs) ---
    const campaign_industry = (body.campaign_industry ?? body.companyIndustry ?? body.industry ?? "").trim();

    // Canonical supplier inputs (prefer supplier_*; fallback to legacy prospect_*)
    const supplier_company_raw = (body.supplier_company ?? body.prospect_company ?? "").trim();
    const supplier_website_raw = (body.supplier_website ?? body.prospect_website ?? "").trim();
    const supplier_linkedin_raw = (body.supplier_linkedin ?? body.prospect_linkedin ?? body.companyLinkedIn ?? "").trim();

    // HTTPS-normalised
    const supplier_company = supplier_company_raw;
    const supplier_website_https = toHttps(supplier_website_raw);
    const supplier_linkedin_https = supplier_linkedin_raw ? toHttps(supplier_linkedin_raw) : "";

    // Supplier USPs: array or delimited string; cap to 12 (re-checked here)
    const supplier_usps = Array.isArray(body.supplier_usps)
      ? body.supplier_usps.map(s => String(s || "").trim()).filter(Boolean).slice(0, 12)
      : (typeof body.supplier_usps === "string"
        ? body.supplier_usps.split(/[,;\n]/).map(s => s.trim()).filter(Boolean).slice(0, 12)
        : (typeof body.user_usps === "string"
          ? body.user_usps.split(/[,;\n]/).map(s => s.trim()).filter(Boolean).slice(0, 12)
          : Array.isArray(body.user_usps)
            ? body.user_usps.map(s => String(s || "").trim()).filter(Boolean).slice(0, 12)
            : []));

    // Page routing
    const pageRaw = body.page || "campaign";
    const effectivePage = sanitizePage(pageRaw);

    // salesModel / callType (canonical normalisation; default to 'direct')
    // Collect all possible inputs (including legacy & filter paths), coerce to lower-case strings
    const _salesCandidates = [
      body.sales_model,
      body.salesModel,
      body.filters?.sales_model,
      body.filters?.salesModel,
      body.call_type,          // legacy shim
      body.callType,           // legacy shim
      body.filters?.call_type, // legacy shim
      body.filters?.callType   // legacy shim
    ].map(v => (v == null ? "" : String(v).trim().toLowerCase()));

    // Pick first valid; default to 'direct'
    let salesModel = _salesCandidates.find(v => v === "direct" || v === "partner") || "direct";

    // Write back the canonical key so every downstream stage sees the same field
    body.sales_model = salesModel;

    // Keep (optional) legacy field as read-only shim for full traceability
    // (do NOT rely on it downstream)
    const callTypeRaw =
      body.call_type ?? body.callType ?? body.filters?.call_type ?? body.filters?.callType ?? null;
    const callType = (callTypeRaw == null) ? null : String(callTypeRaw).trim().toLowerCase();

    // numeric rowCount (optional) — canonical variable is `rc`
    let rc = (body.rowCount !== undefined && body.rowCount !== null) ? body.rowCount : null;
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
    } else {
      // Optional: if client didn't send rowCount but did send CSV text, estimate from CSV
      if (typeof csvText === "string" && csvText.trim()) {
        // count non-empty lines minus 1 header row
        const lines = csvText.split(/\r?\n/).filter(l => l.trim().length > 0);
        rc = Math.max(0, lines.length - 1);
      } else {
        rc = null;
      }
    }

    // Idempotency support
    const clientRunKey = body.clientRunKey || readHeader(req, "x-idempotency-key") || null;
    const runId = clientRunKey
      ? crypto.createHash("sha1").update(String(clientRunKey)).digest("hex")
      : (crypto.randomUUID ? crypto.randomUUID() : `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`);
    const now = new Date();
    const userId = getUserIdFromReq(req); // ← SWA-auth derived (safe fallback to 'anonymous')
    const relPrefix = computePrefix({ page: effectivePage, runId, userId, now }); // container-relative

    // --- Minimal validation (fail fast if truly empty) ---
    const missing = [];
    if (!supplier_company) missing.push("supplier_company");
    if (!supplier_website_https) missing.push("supplier_website");
    // LinkedIn optional; uncomment to enforce:
    // if (!supplier_linkedin_https) missing.push("supplier_linkedin");

    if (missing.length) {
      context.res = {
        status: 400,
        headers: { "content-type": "application/json", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: `Missing required field(s): ${missing.join(", ")}` }
      };
      return;
    }

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

        // Canonical (preferred)
        supplier_company,
        supplier_website: supplier_website_https,
        supplier_linkedin: supplier_linkedin_https,
        supplier_usps,
        campaign_industry,
        campaign_requirement: campaign_requirement_effective,
        relevant_competitors,

        // Legacy aliases (traceability/back-compat) — mirror HTTPS values
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
    // --- Persist canonical input.json at the run prefix ---
    const inputBlob = containerClient.getBlockBlobClient(`${relPrefix}input.json`);
    await inputBlob.upload(JSON.stringify({
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
      prospect_company: supplier_company,
      prospect_website: supplier_website_https,
      prospect_linkedin: supplier_linkedin_https,
      user_usps: supplier_usps,
      company_industry: campaign_industry
    }, null, 2), Buffer.byteLength(JSON.stringify({})), {
      blobHTTPHeaders: { blobContentType: "application/json" }
    });

    // --- Update per-user recent runs index: results/users/<userId>/recent.json ---
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
        summary: {
          supplier_company,
          campaign_industry,
          rowCount: rc ?? null
        }
      },
      ...(Array.isArray(idx.items) ? idx.items : [])
    ].slice(0, 50);

    await userIdxClient.upload(JSON.stringify(idx, null, 2), Buffer.byteLength(JSON.stringify(idx)), {
      blobHTTPHeaders: { blobContentType: "application/json" }
    });

    // small helper to read blob stream
    async function streamToBuffer(readable) {
      const chunks = [];
      for await (const chunk of readable) chunks.push(chunk);
      return Buffer.concat(chunks);
    }

    // ---- Build the INPUT payload workers ----
    const input = {
      page: effectivePage,
      rowCount: rc ?? null,
      filters: body.filters ?? null,
      notes: body.notes ?? null,
      sales_model: salesModel ?? null,
      call_type: callType ?? null,

      // canonical supplier/company fields
      supplier_company,
      supplier_website: supplier_website_https,
      supplier_linkedin: supplier_linkedin_https,
      supplier_usps,
      campaign_industry,
      selected_industry: campaign_industry || undefined,

      // commercial intent & competition
      campaign_requirement: campaign_requirement_effective,
      relevant_competitors: Array.isArray(relevant_competitors) ? relevant_competitors : [],

      // CSV from the browser (inline fallback used by campaign-evidence)
      csvText: csvText || null,
      csvFilename: csvFilename || null,
      csvSummary: body.csvSummary || null,

      // back-compat mirrors (several workers still read these)
      prospect_company: supplier_company,
      prospect_website: supplier_website_https,
      prospect_linkedin: supplier_linkedin_https,
      user_usps: supplier_usps,
      company_industry: campaign_industry
    };

    // ---- Queue message ---------//
    const msg = {
      op: "kickoff",
      runId,
      userId,
      page: effectivePage,
      enqueuedAt,
      prefix: relPrefix,
      correlationId,
      clientRunKey: clientRunKey ?? null,

      // Keep runConfig for writer/outline
      runConfig: {
        campaign_requirement: campaign_requirement_effective,
        relevant_competitors: Array.isArray(relevant_competitors) ? relevant_competitors : []
      },

      // ✅ Canonical input (single source of truth)
      input,

      // ✅ Back-compat mirrors (read by some stages that expect top-level fields)
      page: input?.page,
      rowCount: input?.rowCount,
      filters: input?.filters,
      notes: input?.notes,
      sales_model: input?.sales_model,
      call_type: input?.call_type,
      supplier_company: input?.supplier_company,
      supplier_website: input?.supplier_website,
      supplier_linkedin: input?.supplier_linkedin,
      supplier_usps: input?.supplier_usps,
      campaign_industry: input?.campaign_industry,
      selected_industry: input?.selected_industry,
      campaign_requirement: input?.campaign_requirement,
      relevant_competitors: Array.isArray(input?.relevant_competitors) ? input.relevant_competitors : [],
      csvText: input?.csvText,
      csvFilename: input?.csvFilename,
      csvSummary: input?.csvSummary,
      prospect_company: input?.prospect_company,
      prospect_website: input?.prospect_website,
      prospect_linkedin: input?.prospect_linkedin,
      user_usps: input?.user_usps,
      company_industry: input?.company_industry
    };

    // Trim oversized payload (Azure Queue limit ~64KB post-base64)
    function safeStringify(obj) {
      try { return JSON.stringify(obj); } catch { return "{}"; }
    }

    let payload = safeStringify(msg);
    if (Buffer.byteLength(payload) > MAX_BYTES) {
      const slim = { ...msg, input: { ...(msg.input || {}) } };
      let s = safeStringify(slim);

      // Drop biggest offenders first, in order
      const shrink = () => Buffer.byteLength(s) > MAX_BYTES;

      // Protect critical inputs from being dropped (worker + harness rely on these)
      const PROTECT = new Set([
        "notes",
        "rowCount",
        "campaign_industry",
        "company_industry",
        "relevant_competitors",
        "sales_model",
        "salesModel",
        "call_type",
        "callType",
        "supplier_company",
        "supplier_website",
        "supplier_linkedin",
        "prospect_company",
        "prospect_website",
        "prospect_linkedin",
        "csvFilename"
      ]);

      // helper: null a field if not protected
      function nullIfAllowed(obj, pathArr) {
        const last = pathArr[pathArr.length - 1];
        if (PROTECT.has(last)) return false;
        let ref = obj;
        for (let i = 0; i < pathArr.length - 1; i++) {
          if (!ref) return false;
          ref = ref[pathArr[i]];
        }
        const key = last;
        if (ref && Object.prototype.hasOwnProperty.call(ref, key)) {
          ref[key] = null;
          return true;
        }
        return false;
      }

      // 1) Inline CSV text (usually the largest)
      if (slim.input && typeof slim.input.csvText === "string") {
        slim.input.csvText = null;
        s = safeStringify(slim);
      }

      // 2) CSV summary (can be large if included)
      if (shrink()) nullIfAllowed(slim, ["input", "csvSummary"]);

      // 3) Filters (often chunky)
      if (shrink()) nullIfAllowed(slim, ["input", "filters"]);

      // 4) USPS arrays (can be verbose) — safe to drop if needed
      if (shrink()) nullIfAllowed(slim, ["input", "user_usps"]);
      if (shrink()) nullIfAllowed(slim, ["input", "supplier_usps"]);

      // 5) As a last resort, trim non-critical items under runConfig
      if (shrink()) nullIfAllowed(slim, ["runConfig", "relevant_competitors"]);

      // 6) Absolute last resort: null any remaining non-protected extras under input
      if (shrink() && slim.input && typeof slim.input === "object") {
        for (const k of Object.keys(slim.input)) {
          if (!shrink()) break;
          if (!PROTECT.has(k) && slim.input[k] != null) {
            slim.input[k] = null;
            s = safeStringify(slim);
          }
        }
      }
      payload = s;
      context.log.warn("campaign_start_payload_slimmed", { bytes: Buffer.byteLength(payload) });
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

    // --- ALSO enqueue to the evidence queue so evidence_log.json is built ---
    const EVIDENCE_QUEUE = process.env.Q_CAMPAIGN_EVIDENCE || "campaign-evidence-jobs";
    const eq = qs.getQueueClient(EVIDENCE_QUEUE);
    await eq.createIfNotExists();

    // Important: send the SAME payload (already includes prefix + userId)
    await enqueueMessage(eq, payload);
    context.log({
      event: "campaign_start_enqueued_evidence",
      runId,
      queue: EVIDENCE_QUEUE,
      correlationId
    });

    try {
      context.log({
        event: "campaign_start_inputs",
        supplier_company,
        has_site: !!supplier_website_https,
        has_linkedin: !!supplier_linkedin_https,
        supplier_usps_count: supplier_usps.length,
        campaign_industry,
        campaign_requirement: campaign_requirement_effective,
        relevant_competitors_count: Array.isArray(relevant_competitors) ? relevant_competitors.length : 0
      });
      const EVIDENCE_QUEUE = process.env.Q_CAMPAIGN_EVIDENCE || "campaign-evidence-jobs";
      const eq = qs.getQueueClient(EVIDENCE_QUEUE);
      await eq.createIfNotExists();
      await enqueueMessage(eq, payload);
      context.log({
        event: "campaign_start_enqueued_evidence",
        runId,
        queue: EVIDENCE_QUEUE,
        correlationId,
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
