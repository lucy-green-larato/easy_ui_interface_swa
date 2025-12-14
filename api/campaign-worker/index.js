// /api/campaign-worker/index.js 06-12-2025
// Strategy Engine v20 — Fully deterministic, router-driven (Option B)
// ---------------------------------------------------------------
// Responsibilities:
//   • Listen ONLY for queue op: "run_strategy"
//   • Load evidence, buyer_logic, insights, csv_normalized, markdown_pack, input.json
//   • Generate strategy_v2 (deterministic, no AI)
//   • Write results/<prefix>/strategy_v2/campaign_strategy.json
//   • Update status.json → state="strategy_ready"
//   • Notify router via afterworker event
//
// No viability. No routing logic inside worker. No LLM.
// ---------------------------------------------------------------

"use strict";

const { enqueueTo } = require("../lib/campaign-queue");
const {
  getResultsContainerClient,
  getJson,
  putJson
} = require("../shared/storage");

const ROUTER_QUEUE =
  process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";

// ---------------------------------------------------------------
// UTILITIES
// ---------------------------------------------------------------

function parseQueueItem(queueItem) {
  if (!queueItem) return {};
  if (typeof queueItem === "string") {
    try {
      return JSON.parse(queueItem);
    } catch {
      return {};
    }
  }
  return typeof queueItem === "object" ? queueItem : {};
}

function uniqNonEmpty(arr) {
  const out = [];
  const seen = new Set();
  for (const v of arr || []) {
    const s = (v || "").toString().trim();
    if (s && !seen.has(s)) {
      seen.add(s);
      out.push(s);
    }
  }
  return out;
}

function safeGet(obj, path, def) {
  try {
    const parts = path.split(".");
    let cur = obj;
    for (const p of parts) {
      if (cur == null) return def;
      cur = cur[p];
    }
    return cur == null ? def : cur;
  } catch {
    return def;
  }
}

// ---------------------------------------------------------------
// Basic text helpers
// ---------------------------------------------------------------

function bulletFromClaim(c) {
  if (!c) return "";
  const body = c.summary || c.title || "";
  const id = c.claim_id || "";
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

function indexClaimsByTag(evidence) {
  const claims = Array.isArray(evidence?.claims) ? evidence.claims : [];
  const out = {};
  for (const c of claims) {
    const tag = c.tag || "other";
    if (!out[tag]) out[tag] = [];
    out[tag].push(c);
  }
  return out;
}

// ---------------------------------------------------------------
// STRATEGY BUILDERS (v18 logic preserved, viability removed)
// ---------------------------------------------------------------

function buildStorySpine({
  evidence,
  insights,
  buyerLogic,
  markdownPack,
  csvNormalized,
  mergedInput
}) {
  const byTag = indexClaimsByTag(evidence);

  const environment = uniqNonEmpty(
    (byTag.environment || []).map(bulletFromClaim)
      .concat((byTag.buyer_priority || []).map(bulletFromClaim))
      // IMPORTANT: use markdownPack.industry_drivers (as in your original v18)
      .concat(
        (safeGet(markdownPack, "industry_drivers", []) || []).map(
          (d) => d.text || ""
        )
      )
  ).slice(0, 4);

  const case_for_action = uniqNonEmpty(
    (safeGet(insights, "adoption_barriers", []) || []).map(
      (x) => x.label || ""
    )
      .concat(
        (safeGet(insights, "risk_landscape", []) || []).map((x) =>
          withEvidenceTag(x.text, x.claim_id ? [x.claim_id] : [])
        )
      )
      .concat(
        (safeGet(insights, "timing_drivers", []) || []).map(
          (x) => x.text || ""
        )
      )
      .concat(
        (safeGet(buyerLogic, "commercial_impacts", []) || []).map((ci) =>
          withEvidenceTag(
            ci.label,
            safeGet(ci, "origin.related_claim_ids", [])
          )
        )
      )
  ).slice(0, 6);

  const how_we_win = uniqNonEmpty(
    (byTag.supplier_capability || []).map(bulletFromClaim)
      .concat((byTag.differentiator || []).map(bulletFromClaim))
      .concat(
        (safeGet(markdownPack, "content_pillars", []) || []).map(
          (p) => p.text || ""
        )
      )
  ).slice(0, 6);

  const n = safeGet(csvNormalized, "meta.rows", 0);
  const routeHint = mergedInput.sales_model || mergedInput.call_type || "";

  function deriveOutcome(cohortRows, routeRaw) {
    const num = Number(cohortRows) || 0;
    let bucket = "none";
    if (num > 0 && num < 50) bucket = "very_small";
    else if (num >= 50 && num < 400) bucket = "small_mid";
    else if (num >= 400) bucket = "mid_large";

    let route = "unspecified";
    const lower = (routeRaw || "").toLowerCase();
    if (lower.includes("partner")) route = "partner";
    else if (lower.includes("direct")) route = "direct";

    return `TAM_BUCKET=${bucket}; COHORT_SIZE=${num}; ROUTE_MODEL=${route}`;
  }

  const success = uniqNonEmpty([
    deriveOutcome(n, routeHint),
    ...(safeGet(insights, "success_signals", []) || []).map(
      (x) => x.text || ""
    )
  ]).slice(0, 4);

  const next_steps = uniqNonEmpty([
    n ? `NEXT_STEP:target_cohort_size=${n}` : "",
    environment.length
      ? `NEXT_STEP:align_to_environment_signals=${environment.length}`
      : "",
    case_for_action.length
      ? `NEXT_STEP:prioritise_case_for_action=${case_for_action.length}`
      : "",
    how_we_win.length
      ? `NEXT_STEP:validate_how_we_win=${how_we_win.length}`
      : ""
  ]).slice(0, 4);

  return { environment, case_for_action, how_we_win, success, next_steps };
}

function buildValueProposition({
  evidence,
  insights,
  buyerLogic,
  markdownPack,
  csvNormalized,
  mergedInput
}) {
  const industry = mergedInput.selected_industry || "input_group";
  const buyers = mergedInput.buyer_type || "personas";

  const topProblem =
    safeGet(buyerLogic, "problems.0.label") ||
    safeGet(insights, "buyer_pressures.0.text") ||
    "";

  const byTag = indexClaimsByTag(evidence);
  const capClaim =
    (byTag.supplier_capability || [])[0] ||
    (byTag.right_to_play || [])[0];

  const ourSolutionCore = capClaim
    ? bulletFromClaim(capClaim).replace(/\s*\[[^\]]+\]\s*$/, "")
    : "";

  const outcomeCore =
    safeGet(buyerLogic, "commercial_impacts.0.label") || "";
  const unlikeCore =
    safeGet(markdownPack, "competitor_profiles.0.summary") || "";

  return {
    value_proposition: {
      moore_chain: {
        for_who: `For ${industry} ${buyers}`,
        problem: `who struggle with ${topProblem}`,
        our_solution: `we provide ${ourSolutionCore}`,
        outcome: `so that ${outcomeCore}`,
        unlike: `unlike ${unlikeCore}`
      }
    }
  };
}

function buildCompetitiveStrategy({ evidence, markdownPack }) {
  const byTag = indexClaimsByTag(evidence);

  const competitor_map = uniqNonEmpty(
    (safeGet(markdownPack, "competitor_profiles", []) || []).map(
      (c) => c.summary || ""
    )
  );

  const diffs = []
    .concat(byTag.differentiator || [])
    .concat(byTag.right_to_play || [])
    .map(bulletFromClaim);

  const angles = (evidence.claims || [])
    .filter((c) => c.tag === "buyer_blocker" || c.tag === "timing")
    .map(bulletFromClaim);

  const vulnerability_map = uniqNonEmpty(
    (evidence.claims || [])
      .filter((c) => c.tag === "risk" || c.tag === "buyer_blocker")
      .map(bulletFromClaim)
  ).slice(0, 6);

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
      (safeGet(buyerLogic, "problems", []) || []).map((p) =>
        withEvidenceTag(
          p.label,
          safeGet(p, "origin.related_claim_ids", [])
        )
      )
    ).slice(0, 8),

    barriers: uniqNonEmpty(
      (safeGet(insights, "adoption_barriers", []) || []).map(
        (x) => x.label || ""
      )
        .concat(
          (safeGet(buyerLogic, "risk_tolerances", []) || []).map((r) =>
            withEvidenceTag(
              r.label,
              safeGet(r, "origin.related_claim_ids", [])
            )
          )
        )
    ).slice(0, 8),

    urgency: uniqNonEmpty(
      (safeGet(insights, "timing_drivers", []) || []).map(
        (x) => x.text || ""
      )
        .concat(
          (safeGet(buyerLogic, "urgency_factors", []) || []).map((u) =>
            withEvidenceTag(
              u.label,
              safeGet(u, "origin.related_claim_ids", [])
            )
          )
        )
    ).slice(0, 6)
  };
}

function buildGtmStrategy({ csvNormalized, mergedInput }) {
  const routeRaw = mergedInput.sales_model || mergedInput.call_type || "";
  const lower = (routeRaw || "").toLowerCase();

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

function buildStrategyV2({
  evidence,
  insights,
  buyerLogic,
  markdownPack,
  csvNormalized,
  mergedInput
}) {
  return {
    story_spine: buildStorySpine({
      evidence,
      insights,
      buyerLogic,
      markdownPack,
      csvNormalized,
      mergedInput
    }),
    value_proposition: buildValueProposition({
      evidence,
      insights,
      buyerLogic,
      markdownPack,
      csvNormalized,
      mergedInput
    }).value_proposition,
    competitive_strategy: buildCompetitiveStrategy({ evidence, markdownPack }),
    buyer_strategy: buildBuyerStrategy({ buyerLogic, insights, evidence }),
    gtm_strategy: buildGtmStrategy({ csvNormalized, mergedInput }),
    proof_points: buildProofPoints({ evidence }),
    right_to_play: buildRightToPlay({ evidence })
  };
}

// ---------------------------------------------------------------
// MAIN WORKER FUNCTION
// ---------------------------------------------------------------

module.exports = async function (context, queueItem) {
  const log = context.log;

  const msg = parseQueueItem(queueItem);
  const op = msg.op || "";

  if (op !== "run_strategy") {
    log("[worker] ignoring message", op);
    return;
  }

  if (!msg.prefix) {
    log("[worker] ERROR: No prefix supplied");
    return;
  }

  let prefix = String(msg.prefix).replace(/^\/+/, "");
  if (!prefix.endsWith("/")) prefix += "/";

  const container = await getResultsContainerClient();

  const runId =
    msg.runId ||
    prefix.split("/").filter(Boolean).pop() ||
    "unknown";

  log("[worker] starting", { runId, prefix });

  // Load Phase 1 artefacts
  const evidence = await getJson(container, `${prefix}evidence.json`);
  const evidenceLog = await getJson(container, `${prefix}evidence_log.json`);
  const combinedEvidence = {
    claims:
      (Array.isArray(evidence?.claims) && evidence.claims) ||
      (Array.isArray(evidenceLog) && evidenceLog) ||
      []
  };

  const insights =
    (await getJson(container, `${prefix}insights_v1/insights.json`)) ||
    (await getJson(container, `${prefix}insights.json`)) ||
    {};

  const buyerLogic =
    (await getJson(container, `${prefix}insights_v1/buyer_logic.json`)) ||
    (await getJson(container, `${prefix}buyer_logic.json`)) ||
    {};

  const markdownPack =
    (await getJson(container, `${prefix}evidence_v2/markdown_pack.json`)) ||
    (await getJson(container, `${prefix}markdown_pack.json`)) ||
    {};

  const csvNormalized =
    (await getJson(container, `${prefix}csv_normalized.json`)) || {};

  const mergedInput =
    (await getJson(container, `${prefix}input.json`)) || {};

  // Build deterministic strategy_v2
  const strategy_v2 = buildStrategyV2({
    evidence: combinedEvidence,
    insights,
    buyerLogic,
    markdownPack,
    csvNormalized,
    mergedInput
  });

  // Write output
  const outPath = `${prefix}strategy_v2/campaign_strategy.json`;
  await putJson(container, outPath, { strategy_v2 });

  // Update status
  const status = (await getJson(container, `${prefix}status.json`)) || {};
  status.state = "strategy_ready";
  status.markers = status.markers || {};
  status.markers.strategyCompleted = true;
  await putJson(container, `${prefix}status.json`, status);

  // Notify router
  await enqueueTo(ROUTER_QUEUE, {
    op: "afterworker",
    runId,
    prefix,
    userId: msg.userId || "anonymous",
    page: msg.page || "campaign"
  });
  log("[worker] completed", { runId, outPath });
};
