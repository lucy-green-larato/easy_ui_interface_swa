// /api/campaign-worker/index.js 07-11-2025 v22 (Option B – Fast Path)
// Queue-triggered on %CAMPAIGN_QUEUE_NAME%. Generates a full campaign.json directly.
// Does NOT orchestrate sections/assemble (that belongs in a separate tiny router function).

const path = require("path");
const { BlobServiceClient } = require("@azure/storage-blob");

// ---- Env ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";

const AZO_ENDPOINT    = process.env.AZURE_OPENAI_ENDPOINT;
const AZO_API_KEY     = process.env.AZURE_OPENAI_API_KEY;
const AZO_API_VERSION = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";
const AZO_DEPLOYMENT  = process.env.AZURE_OPENAI_DEPLOYMENT;

const LLM_TIMEOUT_MS  = Number(process.env.LLM_TIMEOUT_MS  || 60000);
const LLM_ATTEMPTS    = Number(process.env.LLM_ATTEMPTS    || 1);
const LLM_BACKOFF_MS  = Number(process.env.LLM_BACKOFF_MS  || 500);
const LLM_TEMPERATURE = Number(process.env.LLM_TEMPERATURE ?? 0);

// ---- Lazy, guarded loaders (CJS/ESM) ----
let _harness;
async function loadPromptHarness() {
  if (_harness) return _harness;
  try {
    // CJS
    // eslint-disable-next-line global-require, import/no-dynamic-require
    const mod = require("../lib/prompt-harness");
    const generate = mod.generate ?? mod.default ?? mod;
    if (typeof generate !== "function") throw new Error("prompt-harness: generate() missing");
    _harness = { generate };
    return _harness;
  } catch (e1) {
    // ESM fallback
    const url = path.join(__dirname, "../lib/prompt-harness.mjs");
    const esm = await import(url);
    const generate = esm.generate ?? esm.default;
    if (typeof generate !== "function") throw new Error("prompt-harness.mjs: generate() missing");
    _harness = { generate };
    return _harness;
  }
}

let _evidence;
async function loadEvidenceLib() {
  if (_evidence) return _evidence;
  try {
    // CJS
    // eslint-disable-next-line global-require, import/no-dynamic-require
    const mod = require("../lib/evidence");
    const buildEvidence = mod.buildEvidence ?? mod.default ?? mod;
    if (typeof buildEvidence !== "function") throw new Error("lib/evidence: buildEvidence() missing");
    _evidence = { buildEvidence };
    return _evidence;
  } catch (e1) {
    // ESM fallback
    const url = path.join(__dirname, "../lib/evidence.mjs");
    const esm = await import(url);
    const buildEvidence = esm.buildEvidence ?? esm.default;
    if (typeof buildEvidence !== "function") throw new Error("evidence.mjs: buildEvidence() missing");
    _evidence = { buildEvidence };
    return _evidence;
  }
}

// ---- Blob helpers ----
function blobSvc() {
  if (!STORAGE_CONN) throw new Error("AzureWebJobsStorage not configured");
  return BlobServiceClient.fromConnectionString(STORAGE_CONN);
}
async function streamToString(readable) {
  const chunks = [];
  for await (const c of readable) chunks.push(c);
  return Buffer.concat(chunks).toString("utf8");
}
async function getJson(container, rel) {
  const bb = container.getBlockBlobClient(rel);
  if (!(await bb.exists())) return null;
  const dl = await bb.download();
  try { return JSON.parse(await streamToString(dl.readableStreamBody)); }
  catch { return null; }
}
async function putJson(container, rel, obj) {
  const data = Buffer.from(JSON.stringify(obj, null, 2));
  await container.getBlockBlobClient(rel)
    .uploadData(data, { blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" } });
}
function nowISO() { return new Date().toISOString(); }

function normalizePrefix(p) {
  let x = String(p || "").trim();
  if (!x) return null;
  if (x.startsWith(`${CONTAINER}/`)) x = x.slice(`${CONTAINER}/`.length);
  if (x.startsWith("/")) x = x.replace(/^\/+/, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}

// ---- Idempotence ----
async function tryLock(container, rel, payload) {
  const bb = container.getBlockBlobClient(rel);
  if (await bb.exists()) return false;
  await bb.uploadData(Buffer.from(JSON.stringify(payload || { at: nowISO() })), {
    blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" }
  });
  return true;
}

// ---- Status (append-only history; do not bloat input) ----
async function patchStatus(container, prefix, state, extra = {}) {
  const rel = `${prefix}status.json`;
  const cur = (await getJson(container, rel)) || {};
  const next = { ...cur, state, history: Array.isArray(cur.history) ? cur.history.slice() : [] };
  next.history.push({ state, at: nowISO(), ...(extra.op ? { op: extra.op } : {}) });
  for (const [k, v] of Object.entries(extra)) next[k] = v;
  await putJson(container, rel, next);
}

// ---- Evidence shaping ----
function onlyHttps(url) {
  try { const u = new URL(url); return u.protocol === "https:"; }
  catch { return false; }
}
function normalizeEvidenceArray(ev) {
  const arr = Array.isArray(ev) ? ev : [];
  let n = 1;
  return arr
    .filter(e => e && typeof e === "object")
    .map(e => {
      const url = e.url && onlyHttps(e.url) ? e.url : null;
      const claim_id = e.claim_id || `CLM-${String(n++).padStart(3, "0")}`;
      return { ...e, url, claim_id };
    });
}

// ---- Light contract normalisation for UI tabs (does not fight writer) ----
function normaliseForUI(draft, { evidence, csvCanon, runId }) {
  if (!draft || typeof draft !== "object") return draft;

  // executive_summary as array of strings
  const es = draft.executive_summary;
  if (!Array.isArray(es)) {
    if (typeof es === "string") draft.executive_summary = [es];
    else if (es && typeof es === "object") {
      const vals = Object.values(es).filter(v => typeof v === "string");
      draft.executive_summary = vals.length ? vals : [];
    } else draft.executive_summary = [];
  }

  // positioning value_prop string present
  const pos = draft.positioning_and_differentiation || {};
  if (typeof pos.value_prop !== "string") pos.value_prop = pos.value_prop ? String(pos.value_prop) : "";
  draft.positioning_and_differentiation = pos;

  // sales_enablement scaffolding
  const se = draft.sales_enablement || {};
  if (!Array.isArray(se.discovery_questions)) se.discovery_questions = se.discovery_questions ? [String(se.discovery_questions)] : [];
  if (!Array.isArray(se.objection_cards))    se.objection_cards = [];
  if (!Array.isArray(se.proof_pack_outline)) se.proof_pack_outline = [];
  draft.sales_enablement = se;

  // carry-through for fetch/evidence tab parity
  if (!Array.isArray(draft.evidence_log)) draft.evidence_log = evidence;
  if (!draft.csv_signals) draft.csv_signals = csvCanon || {};
  draft.meta = { ...(draft.meta || {}), run_id: runId, phase: "Completed", generatedBy: "worker" };
  return draft;
}

// ---- Main ----
module.exports = async function (context, job) {
  // parse message
  let msg = job;
  if (typeof msg === "string") { try { msg = JSON.parse(msg); } catch { msg = {}; } }

  const op     = String(msg.op || "").toLowerCase();           // kickoff | afterevidence | ...
  const runId  = msg.runId || msg.id || null;
  const prefix = normalizePrefix(msg.prefix) || (runId ? `runs/${runId}/` : null);
  if (!runId || !prefix) { context.log.warn("worker_skip", { op, runId, prefix }); return; }

  // Only act on kickoff or afterevidence (fast path)
  if (!(op === "kickoff" || op === "afterevidence")) {
    context.log("worker_ignored_op", { op, runId });
    return;
  }

  const blob = blobSvc();
  const container = blob.getContainerClient(CONTAINER);
  await container.createIfNotExists();

  // idempotence: run once per runId+prefix
  const locked = await tryLock(container, `${prefix}locks/worker.once.json`, { runId, op, at: nowISO() });
  if (!locked) { context.log("worker_lock_present_skip", { runId, prefix }); return; }

  await patchStatus(container, prefix, "EvidenceDigest", { op, workerStartedAt: nowISO() });

  try {
    // Load inputs
    const inputPayload = (await getJson(container, `${prefix}input.json`)) || {};
    const csvCanon     = (await getJson(container, `${prefix}csv_normalized.json`)) || { rows: 0 };

    // Prefer prebuilt evidence_log.json; fallback to builder
    let evidence = await getJson(container, `${prefix}evidence_log.json`);
    if (Array.isArray(evidence)) evidence = normalizeEvidenceArray(evidence);
    else {
      try {
        const { buildEvidence } = await loadEvidenceLib();
        const built = await buildEvidence({ input: inputPayload, prefix, container });
        evidence = normalizeEvidenceArray(built || []);
      } catch (err) {
        context.log.warn("worker_buildEvidence_failed", String(err?.message || err));
        evidence = [];
      }
    }

    // LLM config guard
    if (!AZO_ENDPOINT || !AZO_API_KEY || !AZO_DEPLOYMENT) {
      await patchStatus(container, prefix, "Failed", {
        error: { code: "config_missing", message: "Missing Azure OpenAI configuration (endpoint/apiKey/deployment)" },
        failedAt: nowISO(), op
      });
      return;
    }

    // Prompt harness call
    const { generate } = await loadPromptHarness();
    const schemaPath = path.join(__dirname, "../schemas/campaign.schema.json");

    const packs = {
      evidencePack: { items: evidence, csv: csvCanon,
        addressable_market: Number.isFinite(csvCanon?.rows) ? csvCanon.rows
          : (Array.isArray(csvCanon?.rows) ? csvCanon.rows.length : 0) },
      defaults: { moore_value_prop: true, discovery_questions_why_matters: true }
    };

    let draft = await generate({
      schemaPath,
      packs,
      input: {
        ...inputPayload,
        runId,
        addressable_market: packs.evidencePack.addressable_market
      },
      llm: {
        provider: "azure",
        endpoint: AZO_ENDPOINT,
        apiKey: AZO_API_KEY,
        apiVersion: AZO_API_VERSION,
        deployment: AZO_DEPLOYMENT,
        timeoutMs: LLM_TIMEOUT_MS,
        attempts: LLM_ATTEMPTS,
        backoffMs: LLM_BACKOFF_MS,
        temperature: LLM_TEMPERATURE
      }
    });
    if (typeof draft === "string") { try { draft = JSON.parse(draft); } catch { /* leave string */ } }

    // UI-shape normalisation (non-destructive)
    const finalDraft = normaliseForUI(draft, { evidence, csvCanon, runId });

    // Save artifact
    await putJson(container, `${prefix}campaign.json`, finalDraft);

    // Verify existence before completing
    const bb = container.getBlockBlobClient(`${prefix}campaign.json`);
    if (!(await bb.exists())) {
      await patchStatus(container, prefix, "Failed", {
        error: { code: "artifact_missing", message: "campaign.json not found after write" },
        failedAt: nowISO(), op
      });
      return;
    }

    await patchStatus(container, prefix, "Completed", { completedAt: nowISO(), op });
    context.log("worker_completed", { runId, prefix });

  } catch (err) {
    context.log.error("worker_exception", String(err?.message || err));
    try {
      await patchStatus(container, prefix, "Failed", {
        error: { code: "worker_error", message: String(err?.message || err) },
        failedAt: nowISO(), op
      });
    } catch { /* ignore secondary failure */ }
  }
};
