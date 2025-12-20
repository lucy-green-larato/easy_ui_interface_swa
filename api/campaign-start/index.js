// /api/campaign-start/index.js 20-12-2025 v26
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// POST /api/campaign-start → writes status/input, seeds csv_normalized, updates per-user recent index,
// then enqueues router kickoff (afterstart). No other queue is enqueued here.

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { enqueueTo } = require("../lib/campaign-queue");
const crypto = require("crypto");
const { canonicalPrefix } = require("../lib/prefix");

// ---- Config ----
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const ROUTER_QUEUE = process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";
const STORAGE_CONN = process.env.AzureWebJobsStorage;

const MAX_BYTES_DEFAULT = 48 * 1024; // safe headroom under Azure Queue ~64KB limit
const MAX_BYTES_ENV = Number.parseInt(process.env.CAMPAIGN_MAX_MSG_BYTES, 10);
const MAX_BYTES =
  Number.isFinite(MAX_BYTES_ENV) && MAX_BYTES_ENV >= 4096 && MAX_BYTES_ENV <= 62 * 1024
    ? MAX_BYTES_ENV
    : MAX_BYTES_DEFAULT;

// ---- Small utils ----
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers":
    "Content-Type, X-Correlation-Id, X-Idempotency-Key, Authorization",
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
function normalizeWebsite(u) {
  if (!u) return null;
  const s = String(u).trim();
  if (!s) return null;
  try {
    const url = new URL(s.startsWith("http") ? s : `https://${s}`);
    url.protocol = "https:";
    url.hash = "";
    return url.toString();
  } catch {
    return null;
  }
}
function normalizeArray(v) {
  if (Array.isArray(v)) return v.map(x => String(x ?? "").trim()).filter(Boolean);
  if (typeof v === "string") return v.split(/[,;\n]/).map(s => s.trim()).filter(Boolean);
  return [];
}

function decodeClientPrincipal(req) {
  const b64 = readHeader(req, "x-ms-client-principal");
  if (!b64) return null;
  try {
    return JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
  } catch {
    return null;
  }
}

function getUserIdFromReq(req) {
  const cp = decodeClientPrincipal(req) || {};
  const claims = cp.claims || [];
  const byType = Object.create(null);
  for (const c of claims) byType[(c.typ || "").toLowerCase()] = c.val;

  const oid =
    byType["http://schemas.microsoft.com/identity/claims/objectidentifier"] ||
    byType["oid"] ||
    null;
  const sub = byType["sub"] || null;
  const email = (byType["emails"] || byType["email"] || cp.userDetails || "").toLowerCase();

  const chosen = oid || sub || email || "anonymous";
  return String(chosen)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_.@-]/g, "-")
    .replace(/-+/g, "-");
}

// Blob helpers
async function streamToBuffer(readable) {
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks);
}

async function writeJson(containerClient, relPath, obj) {
  const data = Buffer.from(JSON.stringify(obj, null, 2));
  await containerClient
    .getBlockBlobClient(relPath)
    .uploadData(data, {
      blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" },
    });
}

async function writeInitialStatus(containerClient, relPrefix, status) {
  await writeJson(containerClient, `${relPrefix}status.json`, status);
}

function pushHistory(status, phase, note) {
  if (!status || typeof status !== "object") return;
  if (!Array.isArray(status.history)) status.history = [];
  status.history.push({
    at: new Date().toISOString(),
    phase: String(phase || "status"),
    note: note ? String(note) : "",
  });
}

// Canonical seed for csv_normalized.json. Evidence phase may overwrite/extend this later,
// but all writers should preserve this shape.
async function normalizeCsvAndPersist(containerClient, prefix, input) {
  const csv = input?.csvSummary || {};
  const rows = Number.isFinite(Number(input?.rowCount))
    ? Number(input.rowCount)
    : Number(csv.rowCountScoped || 0);

  const selected_industry =
    (input?.selected_industry ?? input?.campaign_industry ?? input?.buyer_industry ?? null) ||
    null;

  const normalized = {
    selected_industry,
    industry_mode: selected_industry ? "specific" : "agnostic",
    signals: { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] },
    global_signals: { spend_band: null, top_blockers: [], top_needs_supplier: [], top_purchases: [] },
    meta: {
      rows: Math.max(0, rows || 0),
      source: csv?.source || (input?.csvFilename || null),
      csv_has_multiple_sectors: Boolean(csv?.csvHasMultipleSectors || csv?.hasMultipleSectors),
    },
    preview: {
      cohort: Array.isArray(csv.sampleRows) ? csv.sampleRows.slice(0, 5) : [],
    },
  };

  try {
    const sum = csv && typeof csv === "object" ? csv : null;
    if (sum) {
      const takeTop = (arr, key = "value", n = 8) =>
        Array.isArray(arr)
          ? arr.map(x => String(x?.[key] || "").trim()).filter(Boolean).slice(0, n)
          : [];

      const needs = takeTop(sum.needs);
      const blockers = takeTop(sum.blockers);
      const purchases = takeTop(sum.purchases);

      if (needs.length || blockers.length || purchases.length) {
        normalized.signals = {
          spend_band: null,
          top_blockers: blockers,
          top_needs_supplier: needs,
          top_purchases: purchases,
        };
        normalized.global_signals = {
          spend_band: null,
          top_blockers: blockers,
          top_needs_supplier: needs,
          top_purchases: purchases,
        };
      }
    }
  } catch {
    // safe best-effort; leave normalized with defaults
  }

  await writeJson(containerClient, `${prefix}csv_normalized.json`, normalized);
  return normalized;
}

// Queue helpers
function safeStringify(obj) {
  try {
    return JSON.stringify(obj);
  } catch {
    return "{}";
  }
}
function byteLen(s) {
  return Buffer.byteLength(s, "utf8");
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
      headers: {
        ...CORS,
        "content-type": "application/json; charset=utf-8",
        "x-correlation-id": correlationId,
        allow: "POST",
      },
      body: { error: "method_not_allowed", message: "Only POST is supported" },
    };
    return;
  }

  try {
    if (!STORAGE_CONN) {
      context.res = {
        status: 500,
        headers: { ...CORS, "content-type": "application/json; charset=utf-8", "x-correlation-id": correlationId },
        body: { error: "config", message: "AzureWebJobsStorage app setting is missing" },
      };
      return;
    }

    // ---- Parse / normalise input (normalize BEFORE validation) ----
    const body = (typeof req.body === "object" && req.body) || {};
    const page = sanitizePage(body.page || "campaign");
    const userId = getUserIdFromReq(req);
    const now = new Date();

    // Legacy → prospect_* mirrors (back-compat)
    if (body.company && !body.prospect_company) body.prospect_company = String(body.company).trim();
    if (body.company_name && !body.prospect_company) body.prospect_company = String(body.company_name).trim();
    if (body.website && !body.prospect_website) body.prospect_website = String(body.website).trim();
    if (body.company_website && !body.prospect_website) body.prospect_website = String(body.company_website).trim();
    if (body.linkedin && !body.prospect_linkedin) body.prospect_linkedin = String(body.linkedin).trim();
    if (body.company_linkedin && !body.prospect_linkedin) body.prospect_linkedin = String(body.company_linkedin).trim();

    // USPs: accept array or delimited string
    if (!Array.isArray(body.supplier_usps)) {
      if (typeof body.usps === "string") {
        body.supplier_usps = body.usps.split(/[,;\n]/).map(s => s.trim()).filter(Boolean);
      } else if (Array.isArray(body.usps)) {
        body.supplier_usps = body.usps.map(s => String(s ?? "").trim()).filter(Boolean);
      }
    }

    // Competitors (dedup, cap 8)
    let relevant_competitors = normalizeArray(body.relevant_competitors);
    if (relevant_competitors.length) {
      const seen = new Set();
      relevant_competitors = relevant_competitors
        .map(s => s.trim())
        .filter(s => s && !seen.has(s.toLowerCase()) && (seen.add(s.toLowerCase()) || true))
        .slice(0, 8);
    }

    // sales_model / call_type accept mirrors (including filters.*)
    const salesModelRaw = [
      body.sales_model,
      body.salesModel,
      body.filters?.sales_model,
      body.filters?.salesModel,
    ]
      .map(v => (v == null ? "" : String(v).trim().toLowerCase()))
      .find(v => v === "direct" || v === "partner");
    const sales_model = salesModelRaw || "direct";

    const callTypeRaw = [
      body.call_type,
      body.callType,
      body.filters?.call_type,
      body.filters?.callType,
    ]
      .map(v => (v == null ? "" : String(v).trim().toLowerCase()))
      .find(v => v === "direct" || v === "partner");
    const call_type = callTypeRaw || null;

    // Canonical supplier inputs (prefer supplier_*; fallback to prospect_*)
    const supplier_company = (body.supplier_company ?? body.prospect_company ?? "").toString().trim();
    const supplier_website = normalizeWebsite(body.supplier_website ?? body.prospect_website);
    const supplier_linkedin = normalizeWebsite(body.supplier_linkedin ?? body.prospect_linkedin ?? "");

    // Normalize USPs: cap to 12
    const supplier_usps = Array.isArray(body.supplier_usps)
      ? body.supplier_usps.map(s => String(s || "").trim()).filter(Boolean).slice(0, 12)
      : [];

    let supplier_products = [];
    if (Array.isArray(body.supplier_products)) {
      supplier_products = body.supplier_products.map(s => String(s || "").trim()).filter(Boolean).slice(0, 24);
    } else if (typeof body.supplier_products === "string") {
      supplier_products = body.supplier_products.split(/[,;\n]/).map(s => s.trim()).filter(Boolean).slice(0, 24);
    } else if (typeof body.products === "string" || Array.isArray(body.products)) {
      const arr = Array.isArray(body.products) ? body.products : body.products.split(/[,;\n]/);
      supplier_products = arr.map(s => String(s || "").trim()).filter(Boolean).slice(0, 24);
    }

    // Campaign context
    const campaign_industry =
      (body.campaign_industry ?? body.companyIndustry ?? body.industry ?? "").toString().trim() || null;

    // csv inputs
    const csvText = (typeof body.csvText === "string" && body.csvText.trim()) || null;
    const csvFilename = (typeof body.csvFilename === "string" && body.csvFilename.trim()) || null;
    const csvSummary = body.csvSummary && typeof body.csvSummary === "object" ? body.csvSummary : null;

    // rowCount (optional); infer from csvText if not supplied
    let rowCount = null;
    if (body.rowCount != null) {
      const n = Number(body.rowCount);
      if (!Number.isFinite(n) || n < 0) {
        context.res = {
          status: 400,
          headers: { ...CORS, "content-type": "application/json; charset=utf-8", "x-correlation-id": correlationId },
          body: { error: "bad_request", message: "rowCount must be a non-negative number" },
        };
        return;
      }
      rowCount = Math.floor(n);
    } else if (csvText) {
      const lines = csvText.split(/\r?\n/).filter(l => l.trim().length > 0);
      rowCount = Math.max(0, lines.length - 1);
    }

    // campaign requirement (enum) with default
    let campaign_requirement = null;
    if (typeof body.campaign_requirement === "string") {
      const v = body.campaign_requirement.trim().toLowerCase();
      campaign_requirement = ["upsell", "win-back", "growth"].includes(v) ? v : null;
    }
    const campaign_requirement_effective = campaign_requirement ?? "unspecified";

    // ---- Validate AFTER normalization ----
    const missing = [];
    if (!supplier_company) missing.push("supplier_company");
    if (!supplier_website) missing.push("supplier_website");
    if (missing.length) {
      context.res = {
        status: 400,
        headers: { ...CORS, "content-type": "application/json; charset=utf-8", "x-correlation-id": correlationId },
        body: { error: "bad_request", message: `Missing required field(s): ${missing.join(", ")}` },
      };
      return;
    }

    // ---- Storage ----
    const blobService = BlobServiceClient.fromConnectionString(STORAGE_CONN);
    const containerClient = blobService.getContainerClient(RESULTS_CONTAINER);
    await containerClient.createIfNotExists();

    // ---- runId / prefix / idempotency ----
    const clientRunKey = body.clientRunKey || readHeader(req, "x-idempotency-key") || null;
    const runId = clientRunKey
      ? crypto.createHash("sha1").update(String(clientRunKey)).digest("hex")
      : (crypto.randomUUID ? crypto.randomUUID() : `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`);

    const prefix = canonicalPrefix({ page, userId, runId, date: now });

    // ---- Build input payload (full) ----
    const inputPayload = {
      page,
      rowCount: rowCount ?? null,
      filters: body.filters ?? null,
      notes: body.notes ?? null,
      sales_model,
      call_type,
      supplier_company,
      supplier_website,
      supplier_linkedin,
      supplier_usps,
      supplier_products,
      campaign_industry,
      selected_industry: campaign_industry,
      campaign_requirement: campaign_requirement_effective,
      relevant_competitors,
      csvText,
      csvFilename,
      csvSummary,
      // Legacy mirrors
      prospect_company: supplier_company,
      prospect_website: supplier_website,
      prospect_linkedin: supplier_linkedin,
      user_usps: supplier_usps,
      company_industry: campaign_industry,
    };

    // ============================================================
    // ✅ CRITICAL FIX: write canonical status.json ONCE, EARLY
    // This prevents your router/status endpoint 404.
    // ============================================================
    const enqueuedAt = now.toISOString();
    const initialStatus = {
      runId,
      prefix,
      state: "Queued",
      input: {
        page,
        rowCount: rowCount ?? null,
        filters: body.filters ?? null,
        notes: body.notes ?? null,
        sales_model,
        call_type,
        supplier_company,
        supplier_website,
        supplier_linkedin,
        supplier_usps,
        supplier_products,
        campaign_industry,
        selected_industry: campaign_industry,
        campaign_requirement: campaign_requirement_effective,
        relevant_competitors,
        // Legacy mirrors (back-compat)
        prospect_company: supplier_company,
        prospect_website: supplier_website,
        prospect_linkedin: supplier_linkedin,
        user_usps: supplier_usps,
        company_industry: campaign_industry,
        flags: {
          use_new_evidence: false,
          use_new_insights: false,
          use_new_strategy: false,
          use_writer_assembler: false,
        },
      },
      markers: {},
      history: [],
      enqueuedAt,
      correlationId,
    };

    pushHistory(initialStatus, "campaign_start", "status_seeded");
    await writeInitialStatus(containerClient, prefix, initialStatus);

    // ---- input.json (full) ----
    await writeJson(containerClient, `${prefix}input.json`, inputPayload);

    // ---- csv_normalized.json seed ----
    await normalizeCsvAndPersist(containerClient, prefix, inputPayload);

    // ---- per-user recent index (bounded 50) ----
    const isAnonymous = userId === "anonymous";
    if (!isAnonymous) {
      const userIdxClient = containerClient.getBlockBlobClient(`users/${userId}/recent.json`);
      let idx = { userId, items: [] };
      try {
        const dl = await userIdxClient.download();
        idx = JSON.parse((await streamToBuffer(dl.readableStreamBody)).toString("utf8"));
        if (!idx || typeof idx !== "object") idx = { userId, items: [] };
      } catch {
        // create fresh
      }

      idx.items = [
        {
          runId,
          page,
          when: now.toISOString(),
          prefix,
          summary: { supplier_company, campaign_industry, rowCount: rowCount ?? null },
        },
        ...(Array.isArray(idx.items) ? idx.items : []),
      ].slice(0, 50);

      await writeJson(containerClient, `users/${userId}/recent.json`, idx);
    }

    // ---- Build queue payload (canonical + mirrors) ----
    const msg = {
      op: "kickoff",
      runId,
      userId,
      page,
      prefix,
      date: now.toISOString(),
      enqueuedAt,
      container: RESULTS_CONTAINER,
      correlationId,
      clientRunKey: clientRunKey ?? null,
      runConfig: {
        campaign_requirement: campaign_requirement_effective,
        relevant_competitors,
      },
      input: inputPayload,

      // mirrors for legacy workers
      rowCount: inputPayload.rowCount,
      filters: inputPayload.filters,
      notes: inputPayload.notes,
      sales_model: inputPayload.sales_model,
      call_type: inputPayload.call_type,
      supplier_company: inputPayload.supplier_company,
      supplier_website: inputPayload.supplier_website,
      supplier_linkedin: inputPayload.supplier_linkedin,
      supplier_usps: inputPayload.supplier_usps,
      campaign_industry: inputPayload.campaign_industry,
      selected_industry: inputPayload.selected_industry,
      campaign_requirement: inputPayload.campaign_requirement,
      relevant_competitors: inputPayload.relevant_competitors,
      csvText: inputPayload.csvText,
      csvFilename: inputPayload.csvFilename,
      csvSummary: inputPayload.csvSummary,
      prospect_company: inputPayload.prospect_company,
      prospect_website: inputPayload.prospect_website,
      prospect_linkedin: inputPayload.prospect_linkedin,
      user_usps: inputPayload.user_usps,
      company_industry: inputPayload.company_industry,
    };

    // ---- Payload slimming (preserve important fields) ----
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
      "csvFilename",
    ]);

    let payload = safeStringify(msg);
    if (byteLen(payload) > MAX_BYTES) {
      const slim = { ...msg, input: { ...(msg.input || {}) } };

      // Drop CSV text in both places
      if (typeof slim.input.csvText === "string") slim.input.csvText = null;
      if (typeof slim.csvText === "string") slim.csvText = null;

      if (byteLen(safeStringify(slim)) > MAX_BYTES) {
        slim.input.csvSummary = null;
        slim.csvSummary = null;
      }

      if (byteLen(safeStringify(slim)) > MAX_BYTES) {
        slim.input.filters = null;
        slim.filters = null;
      }

      if (byteLen(safeStringify(slim)) > MAX_BYTES) {
        for (const k of Object.keys(slim.input)) {
          if (byteLen(safeStringify(slim)) <= MAX_BYTES) break;
          if (!PROTECT.has(k) && slim.input[k] != null) slim.input[k] = null;
        }
      }

      payload = safeStringify(slim);

      if (byteLen(payload) > MAX_BYTES) {
        const { op, runId, userId, page, prefix, container, correlationId, clientRunKey, runConfig, input } = slim;
        payload = safeStringify({ op, runId, userId, page, prefix, container, correlationId, clientRunKey, runConfig, input });
      }

      context.log.warn("campaign_start_payload_slimmed", { bytes: byteLen(payload) });
    }

    // (We keep this parse step because you had it; it also validates JSON)
    const parsed = JSON.parse(payload);

    // ============================================================
    // Enqueue router continuation: afterstart
    // ============================================================
    await enqueueTo(ROUTER_QUEUE, {
      op: "afterstart",
      runId,
      prefix,
      page,
      correlationId,
      clientRunKey: clientRunKey ?? null,
    });

    pushHistory(initialStatus, "router_enqueued", "afterstart");
    await writeJson(containerClient, `${prefix}status.json`, initialStatus);

    // ---- Response ----
    context.res = {
      status: 202,
      headers: {
        ...CORS,
        "content-type": "application/json; charset=utf-8",
        "x-correlation-id": correlationId,
      },
      body: {
        runId,
        prefix,
        statusUrl: `${containerClient.url}/${prefix}status.json`,
        inputUrl: `${containerClient.url}/${prefix}input.json`,
        bytes: byteLen(safeStringify(parsed)),
      },
    };
  } catch (e) {
    context.log.error("campaign_start_failed", { error: String(e?.message || e), correlationId });
    context.res = {
      status: 500,
      headers: {
        ...CORS,
        "content-type": "application/json; charset=utf-8",
        "x-correlation-id": correlationId,
      },
      body: { error: "server_error", message: String(e?.message || e) },
    };
  }
};
