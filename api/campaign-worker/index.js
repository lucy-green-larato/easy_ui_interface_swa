// /api/campaign-worker/index.js — Strategy Engine v18 (Option B)
// Deterministic strategy_v2 builder (no viability, no routing, no LLM)

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { canonicalPrefix } = require("../lib/prefix");

// ------------------- ENV -------------------
const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

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
  const service = getBlobServiceClient();
  const container = service.getContainerClient(RESULTS_CONTAINER);
  await container.createIfNotExists();
  return container;
}

// -------------- Storage helpers --------------
function streamToString(readable) {
  return new Promise((resolve, reject) => {
    if (!readable) return resolve("");
    const chunks = [];
    readable.on("data", (d) =>
      chunks.push(Buffer.isBuffer(d) ? d : Buffer.from(d))
    );
    readable.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    readable.on("error", reject);
  });
}

async function readJsonIfExists(container, blobPath) {
  try {
    const blob = container.getBlobClient(blobPath);
    if (!(await blob.exists())) return null;
    const dl = await blob.download();
    const text = await streamToString(dl.readableStreamBody);
    return text ? JSON.parse(text) : null;
  } catch {
    return null;
  }
}

async function writeJson(container, blobPath, obj) {
  const block = container.getBlockBlobClient(blobPath);
  const data = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await block.upload(data, data.length, {
    blobHTTPHeaders: { blobContentType: "application/json" }
  });
}

// -------------- Status helper ----------------
async function updateStatus(container, prefix, state, note, extra = {}) {
  const path = `${prefix}status.json`;
  let st =
    (await readJsonIfExists(container, path)) || { state: "pending", history: [] };

  st.state = state;
  st.history.push({ at: new Date().toISOString(), state, note, ...extra });

  await writeJson(container, path, st);
}

// -------------- Utility helpers ----------------
function uniqNonEmpty(arr) {
  const s = new Set();
  const out = [];
  for (const v of arr || []) {
    const t = (v || "").toString().trim();
    if (t && !s.has(t)) {
      s.add(t);
      out.push(t);
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

function indexClaimsByTag(evidence) {
  const claims =
    evidence && Array.isArray(evidence.claims) ? evidence.claims : [];
  const out = {};
  for (const c of claims) {
    const t = c.tag || "other";
    if (!out[t]) out[t] = [];
    out[t].push(c);
  }
  return out;
}

function bulletFromClaim(claim) {
  if (!claim) return "";
  const body = claim.summary || claim.title || "";
  const id = claim.claim_id || "";
  if (!body) return "";
  if (!id) return body.trim();
  return `${body.trim()} [${id}]`;
}

function withEvidenceTag(text, ids) {
  if (!text) return "";
  const s = text.toString().trim();
  if (!ids || !ids.length) return s;
  return `${s} [${ids[0]}]`;
}

// -------------- Deterministic strategy builders ----------------
// (unchanged from your v17 except viability removed)

function buildStorySpine({ evidence, insights, buyerLogic, markdownPack, csvNormalized, mergedInput }) {
  const byTag = indexClaimsByTag(evidence);

  const environment = uniqNonEmpty(
    (byTag.environment || []).map(bulletFromClaim)
      .concat((byTag.buyer_priority || []).map(bulletFromClaim))
      .concat((safeGet(markdownPack, "industry_drivers", []) || []).map(d => d.text || ""))
  ).slice(0, 4);

  const case_for_action = uniqNonEmpty(
    (safeGet(insights, "adoption_barriers", []) || []).map(x => x.label)
      .concat((safeGet(insights, "risk_landscape", []) || []).map(x => withEvidenceTag(x.text, x.claim_id ? [x.claim_id] : [])))
      .concat((safeGet(insights, "timing_drivers", []) || []).map(x => x.text))
      .concat((safeGet(buyerLogic, "commercial_impacts", []) || []).map(ci => withEvidenceTag(ci.label, safeGet(ci, "origin.related_claim_ids", []))))
  ).slice(0, 6);

  const how_we_win = uniqNonEmpty(
    (byTag.supplier_capability || []).map(bulletFromClaim)
      .concat((byTag.differentiator || []).map(bulletFromClaim))
      .concat((safeGet(markdownPack, "content_pillars", []) || []).map(p => p.text || ""))
  ).slice(0, 6);

  const n = safeGet(csvNormalized, "meta.rows", 0);
  const routeHint = mergedInput.sales_model || mergedInput.call_type || "";

  const deriveOutcome = (rows, route) => {
    const n = Number(rows) || 0;
    let bucket = "none";
    if (n > 0 && n < 50) bucket = "very_small";
    else if (n >= 50 && n < 400) bucket = "small_mid";
    else if (n >= 400) bucket = "mid_large";
    let r = "unspecified";
    const lower = (route || "").toLowerCase();
    if (lower.includes("partner")) r = "partner";
    else if (lower.includes("direct")) r = "direct";
    return `TAM_BUCKET=${bucket}; COHORT_SIZE=${n}; ROUTE_MODEL=${r}`;
  };

  const success = uniqNonEmpty([
    deriveOutcome(n, routeHint),
    ...(safeGet(insights, "success_signals", []) || []).map(x => x.text || "")
  ]).slice(0, 4);

  const next_steps = uniqNonEmpty([
    n ? `NEXT_STEP:target_cohort_size=${n}` : "",
    environment.length ? `NEXT_STEP:align_to_environment_signals=${environment.length}` : "",
    case_for_action.length ? `NEXT_STEP:prioritise_case_for_action=${case_for_action.length}` : "",
    how_we_win.length ? `NEXT_STEP:validate_how_we_win=${how_we_win.length}` : ""
  ]).slice(0, 4);

  return { environment, case_for_action, how_we_win, success, next_steps };
}

function buildValueProposition({ evidence, insights, buyerLogic, markdownPack, csvNormalized, mergedInput }) {
  const byTag = indexClaimsByTag(evidence);
  const industry = mergedInput.selected_industry || "input_group";
  const buyers = mergedInput.buyer_type || "personas";

  const topProblem =
    safeGet(buyerLogic, "problems.0.label") ||
    safeGet(insights, "buyer_pressures.0.text") ||
    "";

  const capClaim =
    (byTag.supplier_capability || [])[0] ||
    (byTag.right_to_play || [])[0];

  const ourSolutionCore = capClaim
    ? bulletFromClaim(capClaim).replace(/\s*\[[^\]]+\]\s*$/, "")
    : "";

  const outcomeCore = safeGet(buyerLogic, "commercial_impacts.0.label") || "";
  const unlikeCore =
    safeGet(markdownPack, "competitor_profiles.0.summary") || "";

  return {
    moore_chain: {
      for_who: `For ${industry} ${buyers}`,
      problem: `who struggle with ${topProblem}`,
      our_solution: `we provide ${ourSolutionCore}`,
      outcome: `so that ${outcomeCore}`,
      unlike: `unlike ${unlikeCore}`
    }
  };
}

function buildCompetitiveStrategy({ evidence, markdownPack }) {
  const byTag = indexClaimsByTag(evidence);

  const competitor_map = uniqNonEmpty(
    (safeGet(markdownPack, "competitor_profiles", []) || []).map(c => c.summary || "")
  );

  const diffs = []
    .concat(byTag.differentiator || [])
    .concat(byTag.right_to_play || [])
    .map(bulletFromClaim);

  const angles = (evidence.claims || [])
    .filter(c => c.tag === "buyer_blocker" || c.tag === "timing")
    .map(bulletFromClaim);

  const vulnerability_map =
    uniqNonEmpty(
      (evidence.claims || [])
        .filter(c => c.tag === "risk" || c.tag === "buyer_blocker")
        .map(bulletFromClaim)
    ).slice(0, 6) || ["VULNERABILITY_SIGNALS=INSUFFICIENT"];

  return {
    competitor_map,
    our_advantage: uniqNonEmpty(diffs).slice(0, 6),
    angles_of_attack: uniqNonEmpty(angles).slice(0, 6),
    defensible_differentiators: uniqNonEmpty(diffs).slice(0, 6),
    vulnerability_map
  };
}

function buildBuyerStrategy({ buyerLogic, insights, evidence }) {
  return {
    problems: uniqNonEmpty(
      (safeGet(buyerLogic, "problems", []) || []).map(p =>
        withEvidenceTag(p.label, safeGet(p, "origin.related_claim_ids", []))
      )
    ).slice(0, 8),

    barriers: uniqNonEmpty(
      (safeGet(insights, "adoption_barriers", []) || []).map(x => x.label || "")
        .concat(
          (safeGet(buyerLogic, "risk_tolerances", []) || []).map(r =>
            withEvidenceTag(r.label, safeGet(r, "origin.related_claim_ids", []))
          )
        )
    ).slice(0, 8),

    urgency: uniqNonEmpty(
      (safeGet(insights, "timing_drivers", []) || []).map(x => x.text || "")
        .concat(
          (safeGet(buyerLogic, "urgency_factors", []) || []).map(u =>
            withEvidenceTag(u.label, safeGet(u, "origin.related_claim_ids", []))
          )
        )
    ).slice(0, 6)
  };
}

function buildGtmStrategy({ csvNormalized, mergedInput }) {
  const routeRaw = mergedInput.sales_model || mergedInput.call_type || "";
  const lower = routeRaw.toLowerCase();

  let route = "mixed";
  if (lower.includes("partner")) route = "partner";
  if (lower.includes("direct")) route = "direct";

  return {
    route_implications: [`ROUTE_MODEL=${route}`]
  };
}

function buildProofPoints({ evidence }) {
  const byTag = indexClaimsByTag(evidence);
  return uniqNonEmpty(
    []
      .concat(byTag.supplier_capability || [])
      .concat(byTag.right_to_play || [])
      .map(bulletFromClaim)
  ).slice(0, 10);
}

function buildRightToPlay({ evidence }) {
  const byTag = indexClaimsByTag(evidence);
  return uniqNonEmpty(
    []
      .concat(byTag.right_to_play || [])
      .concat(byTag.supplier_overview || [])
      .map(bulletFromClaim)
  ).slice(0, 6);
}

function buildStrategyV2(args) {
  return {
    story_spine: buildStorySpine(args),
    value_proposition: buildValueProposition(args),
    competitive_strategy: buildCompetitiveStrategy(args),
    buyer_strategy: buildBuyerStrategy(args),
    gtm_strategy: buildGtmStrategy(args),
    proof_points: buildProofPoints(args),
    right_to_play: buildRightToPlay(args)
  };
}

// ---------------------- MAIN FUNCTION ---------------------- //

module.exports = async function (context, queueItem) {
  const log = context.log;

  // The worker should only run when explicitly fired for debugging or heavy lane.
  // In Option B, router never calls worker, so ignore unexpected ops.
  const msg = parseQueueItem(queueItem);
  if (msg.op !== "run_strategy" && msg.op !== "kickoff") {
    log("[worker] ignoring message", msg.op);
    return;
  }

  const container = await getResultsContainer();

  // Resolve prefix/runId
  const p = canonicalPrefix({
    userId: msg.user || "anonymous",
    page: msg.page || "campaign",
    runId: msg.runId,
    date: msg.date ? new Date(msg.date) : undefined
  });
  let prefix = p.endsWith("/") ? p : p + "/";
  const runId = msg.runId || prefix.split("/").filter(Boolean).pop();

  log("[worker] starting", { runId, prefix });

  // Load Phase 1 artefacts
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

  // Build strategy_v2
  const strategy_v2 = buildStrategyV2({
    evidence: combinedEvidence,
    insights,
    buyerLogic,
    markdownPack,
    csvNormalized,
    mergedInput
  });

  const outPath = `${prefix}strategy_v2/campaign_strategy.json`;
  await writeJson(container, outPath, { strategy_v2 });

  await updateStatus(
    container,
    prefix,
    "strategy_ready",
    "Strategy Engine completed successfully"
  );

  log("[worker] completed", { runId, outPath });
};
