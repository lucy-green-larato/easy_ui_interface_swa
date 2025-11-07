// /api/campaign-worker/index.js 07-11-2025 v16
// Classic Azure Functions (function.json + scriptFile), CommonJS.
// NEW: Orchestrator for blob-based pipeline. Still supports legacy single-shot generation if op is missing.

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const path = require("path");

// ---- ENV ----
const RESULTS_CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CAMPAIGN_QUEUE = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
const OUTLINE_QUEUE = process.env.Q_CAMPAIGN_OUTLINE || "campaign-outline";
const WRITE_QUEUE = process.env.Q_CAMPAIGN_WRITE || "campaign-write";

// For legacy single-shot path (kept intact)
const schemaPath = path.join(__dirname, "../schemas/campaign.schema.json");

// ---------- Guarded, lazy loaders (legacy path only) ----------
let _promptHarness;
async function loadPromptHarness() {
  if (_promptHarness) return _promptHarness;
  try {
    // CJS fast path
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const mod = require("../lib/prompt-harness");
    const out = mod?.generate ? mod : { generate: mod?.default ?? mod };
    if (!out.generate || typeof out.generate !== "function") throw new Error("prompt-harness missing generate()");
    _promptHarness = out;
    return _promptHarness;
  } catch (e1) {
    try {
      const modUrl = new URL("../lib/prompt-harness.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      const out = esm?.generate ? esm : { generate: esm?.default ?? esm };
      if (!out.generate || typeof out.generate !== "function") throw new Error("prompt-harness missing generate()");
      _promptHarness = out;
      return _promptHarness;
    } catch (e2) {
      throw new Error(`prompt-harness load failed: ${e1?.message || e1} | ${e2?.message || e2}`);
    }
  }
}

let _pack;
async function loadPackModule() {
  try {
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const mod = require("../lib/pack");
    const out = typeof mod === "function" ? { pack: mod } : (mod.pack ? mod : { pack: mod?.default ?? mod });
    if (!out.pack || typeof out.pack !== "function") throw new Error("lib/pack missing pack()");
    _pack = out;
    return _pack;
  } catch (e1) {
    try {
      const modUrl = new URL("../lib/pack.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      const out = typeof esm.default === "function" ? { pack: esm.default } : (esm.pack ? esm : { pack: esm });
      if (!out.pack || typeof out.pack !== "function") throw new Error("lib/pack missing pack()");
      _pack = out;
      return _pack;
    } catch (e2) {
      throw new Error(`pack load failed: ${e1?.message || e1} | ${e2?.message || e2}`);
    }
  }
}

let _evidence;
async function loadEvidence() {
  if (_evidence) return _evidence;
  try {
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const mod = require("../lib/evidence");
    const buildEvidence = mod.buildEvidence ?? mod.default ?? mod;
    if (typeof buildEvidence !== "function") throw new Error("evidence module has no buildEvidence()");
    _evidence = { buildEvidence };
    return _evidence;
  } catch (e1) {
    try {
      const modUrl = new URL("../lib/evidence.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      const buildEvidence = esm.buildEvidence ?? esm.default ?? esm;
      if (typeof buildEvidence !== "function") throw new Error("evidence module has no buildEvidence()");
      _evidence = { buildEvidence };
      return _evidence;
    } catch (e2) {
      throw new Error(`evidence load failed: ${e1?.message || e1} | ${e2?.message || e2}`);
    }
  }
}

// ---- blob utils ----
function blobSvc() { return BlobServiceClient.fromConnectionString(STORAGE_CONN); }
async function getJson(containerClient, relPath) {
  const bc = containerClient.getBlobClient(relPath);
  if (!(await bc.exists())) return null;
  const dl = await bc.download();
  const chunks = []; for await (const ch of dl.readableStreamBody) chunks.push(ch);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}
async function putJson(containerClient, relPath, obj) {
  const bb = containerClient.getBlockBlobClient(relPath);
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await bb.uploadData(body, { blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }});
}
async function patchStatus(containerClient, prefix, state, extra = {}) {
  const p = `${prefix}status.json`;
  const cur = (await getJson(containerClient, p)) || {};
  const next = { ...cur, state, ...extra };
  await putJson(containerClient, p, next);
}

// ---- orchestrator helpers ----
function normalizePrefix(prefix) {
  let p = String(prefix || "").trim();
  if (!p) return null;
  if (p.startsWith(`${RESULTS_CONTAINER}/`)) p = p.slice(`${RESULTS_CONTAINER}/`.length);
  if (p.startsWith("/")) p = p.replace(/^\/+/, "");
  if (!p.endsWith("/")) p += "/";
  return p;
}
async function enqueue(queueName, msg) {
  const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
  const qc = qs.getQueueClient(queueName);
  await qc.createIfNotExists();
  await qc.sendMessage(JSON.stringify(msg)); // SDK base64-encodes
}

const OUTLINE_ORDER = ["exec", "positioning", "offer", "messaging", "channel", "strategy", "sales", "measurement", "risks", "compliance", "one"];
// Map mirrors write/index.js SECTION_MAP keys for safety
const SECTION_MAP = {
  exec: "executive_summary",
  positioning: "positioning_and_differentiation",
  messaging: "messaging_matrix",
  offer: "offer_strategy",
  channel: "channel_plan",
  risks: "risks_and_contingencies",
  compliance: "compliance_and_governance",
  strategy: "campaign_strategy",
  sales: "sales_enablement",
  measurement: "measurement_and_learning",
  one: "one_pager_summary"
};

module.exports = async function (context, queueItem) {
  context.log("campaign-worker TRIGGERED");

  // Parse queue message
  let msg = queueItem;
  if (typeof msg === "string") {
    try { msg = JSON.parse(msg); } catch { msg = { op: "", raw: queueItem }; }
  }
  const op = String(msg?.op || "").toLowerCase();
  const runId = msg?.runId || msg?.id || null;
  let prefix = normalizePrefix(msg?.prefix || "");
  const page = String(msg?.page || msg?.data?.page || "campaign");

  const svc = blobSvc();
  const container = svc.getContainerClient(RESULTS_CONTAINER);

  // Resolve prefix defensively if missing (bounded search)
  if (!prefix && runId) {
    for await (const it of container.listBlobsFlat({ prefix: `runs/${runId}/` })) {
      if (it.name.endsWith("/status.json")) { prefix = it.name.replace(/status\.json$/, ""); break; }
    }
  }
  if (!prefix) {
    // Last resort
    prefix = `runs/${runId || "unknown"}/`;
  }

  // ---- Orchestrated flow ----
  if (op === "kickoff") {
    await patchStatus(container, prefix, "Evidence", { orchestrator: "worker", kickedAt: new Date().toISOString() });
    context.log("worker: kickoff recorded", { runId, prefix });
    return;
  }

  if (op === "afterevidence") {
    await patchStatus(container, prefix, "Outline", { outlineQueuedAt: new Date().toISOString() });
    await enqueue(OUTLINE_QUEUE, { op: "outline", runId, page, prefix });
    context.log("worker: outline enqueued", { runId, prefix, queue: OUTLINE_QUEUE });
    return;
  }

  if (op === "afteroutline") {
    await patchStatus(container, prefix, "SectionWrites", { writeQueuedAt: new Date().toISOString() });

    // Enqueue all sections (including sales_enablement & strategy)
    for (const outlineKey of OUTLINE_ORDER) {
      if (!SECTION_MAP[outlineKey]) continue;
      await enqueue(WRITE_QUEUE, { op: "write_section", runId, page, prefix, section: outlineKey });
    }
    // Enqueue assemble (idempotent – assembles what exists)
    await enqueue(WRITE_QUEUE, { op: "assemble", runId, page, prefix });

    context.log("worker: write+assemble enqueued", { runId, prefix, writeQueue: WRITE_QUEUE });
    return;
  }

  if (op === "afterassemble") {
    await patchStatus(container, prefix, "Completed", { completedAt: new Date().toISOString() });
    context.log("worker: pipeline completed", { runId, prefix });
    return;
  }

  // ---- Legacy single-shot path (kept for back-compat) ----
  try {
    context.log("worker: legacy single-shot path (no op). Prefer 500-series pipeline.");
    const harness = await loadPromptHarness();
    const { pack } = await loadPackModule();
    const { buildEvidence } = await loadEvidence();

    await patchStatus(container, prefix, "Evidence", { legacySingleShot: true });

    const input = (await getJson(container, `${prefix}input.json`)) || {};
    const evidenceLog = (await getJson(container, `${prefix}evidence_log.json`)) || (await buildEvidence(input)) || [];
    await putJson(container, `${prefix}evidence_log.json`, evidenceLog);

    await patchStatus(container, prefix, "Completed", { legacySingleShot: true, completedAt: new Date().toISOString() });
    context.log("worker: legacy path finished", { runId, prefix });
  } catch (e) {
    context.log.error("campaign_worker_failed", e);
    try {
      await patchStatus(container, prefix, "Failed", {
        error: { code: "worker_error", message: String(e?.message || e) },
        failedAt: new Date().toISOString()
      });
    } catch { /* no-op */ }
  }
};
