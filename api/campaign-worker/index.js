// /api/campaign-worker/index.js — Strategy Engine v18.4 (Option B, Router-aligned)
// Deterministic strategy_v2 builder with correct routing → afterworker

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { enqueueTo } = require("../lib/campaign-queue");

// ------------------- ENV -------------------
const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

const ROUTER_QUEUE = process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";

// ------------------- Storage -------------------
function getBlobServiceClient() {
  const conn =
    process.env.AzureWebJobsStorage ||
    process.env.AZURE_STORAGE_CONNECTION_STRING;

  if (!conn) {
    throw new Error("AzureWebJobsStorage not configured");
  }

  return BlobServiceClient.fromConnectionString(conn);
}

async function getResultsContainer() {
  const svc = getBlobServiceClient();
  const c = svc.getContainerClient(RESULTS_CONTAINER);
  await c.createIfNotExists();
  return c;
}

function streamToString(readable) {
  return new Promise((resolve, reject) => {
    if (!readable) return resolve("");
    const chunks = [];
    readable.on("data", d =>
      chunks.push(Buffer.isBuffer(d) ? d : Buffer.from(d))
    );
    readable.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    readable.on("error", reject);
  });
}

async function readJsonIfExists(container, blobPath) {
  try {
    const b = container.getBlobClient(blobPath);
    if (!(await b.exists())) return null;
    const dl = await b.download();
    const text = await streamToString(dl.readableStreamBody);
    return text ? JSON.parse(text) : null;
  } catch {
    return null;
  }
}

async function writeJson(container, blobPath, obj) {
  const bb = container.getBlockBlobClient(blobPath);
  const data = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await bb.upload(data, data.length, {
    blobHTTPHeaders: { blobContentType: "application/json" }
  });
}

// ------------------- Status -------------------
async function updateStatus(container, prefix, state, note, extra = {}) {
  const path = `${prefix}status.json`;
  let st =
    (await readJsonIfExists(container, path)) || {
      state: "pending",
      history: [],
      markers: {}
    };

  st.state = state;
  st.history.push({
    at: new Date().toISOString(),
    state,
    note,
    ...extra
  });

  await writeJson(container, path, st);
}

// ------------------- Utils -------------------
function uniqNonEmpty(arr) {
  const out = [];
  const seen = new Set();
  for (const v of arr || []) {
    const s = String(v || "").trim();
    if (s && !seen.has(s)) {
      seen.add(s);
      out.push(s);
    }
  }
  return out;
}

function safeGet(obj, path, def = undefined) {
  try {
    let cur = obj;
    for (const p of path.split(".")) {
      if (cur == null) return def;
      cur = cur[p];
    }
    return cur == null ? def : cur;
  } catch {
    return def;
  }
}

function indexClaimsByTag(ev) {
  const claims = ev && Array.isArray(ev.claims) ? ev.claims : [];
  const out = {};
  for (const c of claims) {
    const t = c.tag || "other";
    if (!out[t]) out[t] = [];
    out[t].push(c);
  }
  return out;
}

function bulletFromClaim(c) {
  if (!c) return "";
  const body = c.summary || c.title || "";
  const id = c.claim_id || "";
  if (!body) return "";
  return id ? `${body.trim()} [${id}]` : body.trim();
}

// ------------------ Strategy Builders (unchanged) ------------------
/* All buildStorySpine, buildValueProposition, buildCompetitiveStrategy,
   buildBuyerStrategy, buildGtmStrategy, buildProofPoints, buildRightToPlay,
   buildStrategyV2 remain exactly as in your v18 code.
   I am not repeating them here to keep the answer readable.
   Use **your existing v18 builder code with zero changes**. */
//
//  🔥 IMPORTANT: Do NOT change any builder functions.
//               Only routing + prefix + enqueue logic changed.
//

// ------------------ Main Function ------------------

module.exports = async function (context, job) {
  const log = context.log;

  // --- Parse the queue message safely ---
  let msg;
  if (typeof job === "string") {
    try {
      msg = JSON.parse(job);
    } catch {
      msg = {};
    }
  } else if (job && typeof job === "object") {
    msg = job;
  } else {
    msg = {};
  }

  const op = msg.op || "";

  // Worker only runs for explicit operations
  if (op !== "run_strategy" && op !== "kickoff") {
    log("[worker] ignoring op:", op);
    return;
  }

  // --- Require prefix (router must provide it) ---
  if (!msg.prefix) {
    throw new Error("Worker invoked without prefix – router must supply prefix");
  }

  let prefix = String(msg.prefix).replace(/^\/+/, "");
  if (!prefix.endsWith("/")) prefix += "/";

  // --- Resolve runId ---
  const runId =
    msg.runId ||
    msg.run_id ||
    prefix.split("/").filter(Boolean).pop();

  log("[worker] starting", { op, runId, prefix });

  const page = msg.page || "campaign";

  log("[worker] starting", { op, runId, prefix });

  const container = await getResultsContainer();

  // ---- Load Inputs ----
  const evidence = await readJsonIfExists(container, `${prefix}evidence.json`);
  const evidenceLog = await readJsonIfExists(container, `${prefix}evidence_log.json`);
  const insights =
    (await readJsonIfExists(container, `${prefix}insights_v1/insights.json`)) ||
    (await readJsonIfExists(container, `${prefix}insights.json`)) ||
    {};
  const buyerLogic =
    (await readJsonIfExists(container, `${prefix}insights_v1/buyer_logic.json`)) ||
    (await readJsonIfExists(container, `${prefix}buyer_logic.json`)) ||
    {};
  const markdownPack =
    (await readJsonIfExists(container, `${prefix}evidence_v2/markdown_pack.json`)) ||
    (await readJsonIfExists(container, `${prefix}markdown_pack.json`)) ||
    {};
  const csvNormalized =
    (await readJsonIfExists(container, `${prefix}csv_normalized.json`)) || {};
  const mergedInput =
    (await readJsonIfExists(container, `${prefix}input.json`)) || {};

  const combinedEvidence = {
    claims:
      (evidence && Array.isArray(evidence.claims) && evidence.claims) ||
      (Array.isArray(evidenceLog) && evidenceLog) ||
      []
  };

  // ---- Build strategy_v2 ----
  const strategy_v2 = buildStrategyV2({
    evidence: combinedEvidence,
    insights,
    buyerLogic,
    markdownPack,
    csvNormalized,
    mergedInput
  });

  // ---- Write output ----
  const outPath = `${prefix}strategy_v2/campaign_strategy.json`;
  await writeJson(container, outPath, { strategy_v2 });

  await updateStatus(
    container,
    prefix,
    "strategy_ready",
    "Strategy Engine completed successfully"
  );

  log("[worker] wrote strategy_v2", { outPath });

  // ---- NOW NOTIFY ROUTER ----
  await enqueueTo(ROUTER_QUEUE, {
    op: "afterworker",
    runId,
    page,
    prefix
  });

  log("[worker] enqueued afterworker", { runId, prefix });

  log("[worker] completed");
};

// -------------------------------
// End of worker
// -------------------------------
