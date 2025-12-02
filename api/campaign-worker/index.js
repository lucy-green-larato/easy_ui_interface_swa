// /api/campaign-worker/index.js 02-12-2025 Strategy Engine v11
// 
// Responsibility:
//   - Read Phase 1 outputs (evidence, insights, buyer_logic, markdown_pack, csv_normalized, etc.).
//   - Build a structured strategy_v2 object (story_spine, value_proposition, competitive_strategy,
//     buyer_strategy, gtm_strategy, proof_points, right_to_play).
//   - Write: results/runs/<runId>/strategy_v2/campaign_strategy.json
//   - Update: results/runs/<runId>/status.json.state = "strategy_working" / "strategy_ready" / "strategy_error"
//   - No calls to prompt-harness, no packloader, no LLM – fully deterministic.

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { canonicalPrefix } = require("../lib/prefix");

// Viability engine (strategy_v2 quality + TAM/problem/diff/urgency warnings)
let computeViability = null;
try {
  // /api/lib/strategy-viability.js
  const mod = require("../lib/strategy-viability");

  // Support any of:
  //   module.exports = { computeViability }
  //   module.exports = computeViability
  //   module.exports.default = computeViability
  computeViability =
    mod.computeViability ||
    mod.default ||
    (typeof mod === "function" ? mod : null);
} catch (e) {
  // If the module isn’t available for some reason, worker still runs.
  computeViability = null;
}

// ---------------------- Environment / blob helpers ---------------------- //

const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

function getBlobServiceClient() {
  const conn =
    process.env.AzureWebJobsStorage ||
    process.env.AZURE_STORAGE_CONNECTION_STRING;
  if (!conn) {
    throw new Error(
      "AzureWebJobsStorage (or AZURE_STORAGE_CONNECTION_STRING) not configured"
    );
  }
  return BlobServiceClient.fromConnectionString(conn);
}

async function getResultsContainer() {
  const service = getBlobServiceClient();
  const container = service.getContainerClient(RESULTS_CONTAINER);
  await container.createIfNotExists();
  return container;
}

function streamToString(readable) {
  return new Promise((resolve, reject) => {
    if (!readable) return resolve("");

    const chunks = [];
    readable.on("data", (d) => {
      try {
        chunks.push(Buffer.isBuffer(d) ? d : Buffer.from(d));
      } catch (err) {
        return reject(err);
      }
    });
    readable.on("end", () => {
      try {
        resolve(Buffer.concat(chunks).toString("utf8"));
      } catch (err) {
        reject(err);
      }
    });
    readable.on("error", reject);
  });
}

async function readJsonIfExists(container, blobPath) {
  try {
    const blob = container.getBlobClient(blobPath);
    const ok = await blob.exists();
    if (!ok) return null;
    const dl = await blob.download();
    const text = await streamToString(dl.readableStreamBody);
    if (!text) return null;
    return JSON.parse(text);
  } catch (e) {
    // Fails closed but non-fatal; caller can handle null
    return null;
  }
}

async function writeJson(container, blobPath, obj) {
  const block = container.getBlockBlobClient(blobPath);
  const json = JSON.stringify(obj, null, 2);
  const data = Buffer.from(json, "utf8");
  await block.upload(data, data.length, {
    blobHTTPHeaders: { blobContentType: "application/json" }
  });
}

// ---------------------- Queue + status helpers ---------------------- //

function parseQueueItem(raw) {
  if (!raw) return {};
  if (typeof raw === "string") {
    try {
      return JSON.parse(raw);
    } catch {
      return { raw };
    }
  }
  if (typeof raw === "object") return raw;
  return { raw };
}

async function updateStatus(container, prefix, state, note, extra = {}) {
  const statusPath = `${prefix}status.json`;
  let status = (await readJsonIfExists(container, statusPath)) || {
    state: "pending",
    history: []
  };

  const entry = {
    at: new Date().toISOString(),
    state,
    note,
    ...extra
  };

  status.state = state;
  status.history = Array.isArray(status.history)
    ? [...status.history, entry]
    : [entry];

  await writeJson(container, statusPath, status);
}

// ---------------------- Small generic helpers ---------------------- //

function uniqNonEmpty(arr) {
  const seen = new Set();
  const out = [];
  for (const v of arr || []) {
    if (!v) continue;
    const s = String(v).trim();
    if (!s || seen.has(s)) continue;
    seen.add(s);
    out.push(s);
  }
  return out;
}

function safeGet(obj, path, def = undefined) {
  try {
    const parts = Array.isArray(path) ? path : String(path || "").split(".");
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

// ---------------------- Evidence helpers ---------------------- //

function indexClaimsByTag(evidence) {
  const claims =
    evidence && Array.isArray(evidence.claims) ? evidence.claims : [];
  const byTag = {};
  for (const c of claims) {
    const tag = c.tag || "other";
    if (!byTag[tag]) byTag[tag] = [];
    byTag[tag].push(c);
  }
  return byTag;
}

function bulletFromClaim(claim) {
  if (!claim) return "";
  const body = claim.summary || claim.title || "";
  const id = claim.claim_id || "";
  if (!body) return "";
  if (!id) return String(body).trim();
  // Avoid double tagging if already present
  if (/\[[A-Z0-9_:-]{4,}\]/.test(body)) return String(body).trim();
  return `${String(body).trim()} [${id}]`;
}

function withEvidenceTag(text, claimIds) {
  if (!text) return "";
  let s = String(text).trim();
  if (!s) return "";
  if (/\[[A-Z0-9_:-]{4,}\]/.test(s)) return s; // already tagged
  const ids = (claimIds || [])
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  if (!ids.length) return s;
  return `${s} [${ids[0]}]`;
}

function deriveOutcomeByTam(rowCount, routeHint) {
  const n = Number.isFinite(Number(rowCount)) ? Number(rowCount) : 0;

  let bucket = "none";
  if (n > 0 && n < 50) bucket = "very_small";
  else if (n >= 50 && n < 400) bucket = "small_mid";
  else if (n >= 400) bucket = "mid_large";

  const raw = (routeHint || "").toString().toLowerCase();
  let route = "unspecified";
  if (raw.includes("partner") || raw.includes("channel")) route = "partner";
  else if (raw.includes("direct") || raw.includes("field")) route = "direct";
  else if (raw) route = "mixed";

  // Deterministic, non-narrative signal string for the writer to interpret
  return `TAM_BUCKET=${bucket}; COHORT_SIZE=${n}; ROUTE_MODEL=${route}`;
}

// ---------------------- Strategy builders ---------------------- //

function buildStorySpine({
  evidence,
  insights,
  buyerLogic,
  markdownPack,
  csvNormalized,
  mergedInput
}) {
  const byTag = indexClaimsByTag(evidence);

  // environment: environment claims + buyer priority + industry_drivers
  const envBullets = [];

  (byTag.environment || []).forEach((c) => {
    const b = bulletFromClaim(c);
    if (b) envBullets.push(b);
  });

  const buyerPri = (byTag.buyer_priority || [])[0];
  if (buyerPri) {
    const b = bulletFromClaim(buyerPri);
    if (b) envBullets.push(b);
  }

  (safeGet(markdownPack, "industry_drivers", []) || []).forEach((d) => {
    if (!d || !d.text) return;
    envBullets.push(String(d.text).trim());
  });

  const environment = uniqNonEmpty(envBullets).slice(0, 4);

  // case_for_action: adoption barriers, risk/urgency & buyer problems
  const cfaBullets = [];

  (safeGet(insights, "adoption_barriers", []) || []).forEach((b) => {
    if (!b || !b.label) return;
    cfaBullets.push(String(b.label).trim());
  });

  (safeGet(insights, "risk_landscape", []) || []).forEach((r) => {
    if (!r || !r.text) return;
    cfaBullets.push(
      withEvidenceTag(r.text, r.claim_id ? [r.claim_id] : [])
    );
  });

  (safeGet(insights, "timing_drivers", []) || []).forEach((t) => {
    if (!t || !t.text) return;
    cfaBullets.push(String(t.text).trim());
  });

  (safeGet(buyerLogic, "commercial_impacts", []) || []).forEach((ci) => {
    if (!ci || !ci.label) return;
    const ids = safeGet(ci, "origin.related_claim_ids", []);
    cfaBullets.push(withEvidenceTag(ci.label, ids));
  });

  const case_for_action = uniqNonEmpty(cfaBullets).slice(0, 6);

  // how_we_win: supplier_capability + differentiator + content pillars
  const hwwBullets = [];

  (byTag.supplier_capability || []).forEach((c) => {
    const b = bulletFromClaim(c);
    if (b) hwwBullets.push(b);
  });

  (byTag.differentiator || []).forEach((c) => {
    const b = bulletFromClaim(c);
    if (b) hwwBullets.push(b);
  });

  (safeGet(markdownPack, "content_pillars", []) || []).forEach((p) => {
    if (!p || !p.text) return;
    hwwBullets.push(String(p.text).trim());
  });

  const how_we_win = uniqNonEmpty(hwwBullets).slice(0, 6);

  // success: TAM-based outcome + any explicit success criteria from insights
  const rowCount = safeGet(csvNormalized, "meta.rows", 0);
  const routeHint =
    mergedInput.sales_model ||
    mergedInput.salesModel ||
    mergedInput.call_type ||
    "";

  const successBullets = [];
  successBullets.push(deriveOutcomeByTam(rowCount, routeHint));

  (safeGet(insights, "success_signals", []) || []).forEach((s) => {
    if (!s || !s.text) return;
    successBullets.push(String(s.text).trim());
  });

  const success = uniqNonEmpty(successBullets).slice(0, 4);

  // next_steps: meta signals derived from inputs & other spine parts
  const next_steps = uniqNonEmpty([
    rowCount
      ? `NEXT_STEP:target_cohort_size=${rowCount}`
      : "",
    environment.length
      ? `NEXT_STEP:align_to_environment_signals=${environment.length}`
      : "",
    case_for_action.length
      ? `NEXT_STEP:prioritise_case_for_action_top=${Math.min(
        case_for_action.length,
        3
      )}`
      : "",
    how_we_win.length
      ? `NEXT_STEP:validate_how_we_win_points=${Math.min(
        how_we_win.length,
        3
      )}`
      : ""
  ]).slice(0, 4);

  return {
    environment,
    case_for_action,
    how_we_win,
    success,
    next_steps
  };
}

function buildValueProposition({
  evidence,
  insights,
  buyerLogic,
  markdownPack,
  csvNormalized,
  mergedInput
}) {
  const byTag = indexClaimsByTag(evidence);

  // Moore-style chain (deterministic templates)
  const industry =
    mergedInput.selected_industry ||
    mergedInput.industry ||
    safeGet(csvNormalized, "meta.industry") ||
    "input_cohort";

  const buyers =
    mergedInput.buyer_type ||
    "buyer_personas_from_persona_pack_or_input";

  const topProblem =
    safeGet(buyerLogic, "problems.0.label") ||
    safeGet(insights, "buyer_pressures.0.text") ||
    "";

  const capClaim =
    (byTag.supplier_capability || [])[0] ||
    (byTag.right_to_play || [])[0] ||
    (byTag.supplier_overview || [])[0];

  const ourSolutionCore = capClaim
    ? bulletFromClaim(capClaim).replace(/\s*\[[A-Z0-9_:-]+\]\s*$/, "")
    : "";

  const outcomeCore =
    safeGet(buyerLogic, "commercial_impacts.0.label") ||
    "";

  const unlikeCore =
    safeGet(markdownPack, "competitor_profiles.0.summary") ||
    "";

  const moore_chain = {
    for_who: `For ${industry} ${buyers}`,
    problem: `who struggle with ${topProblem}`,
    our_solution: `we provide ${ourSolutionCore}`,
    outcome: `so that ${outcomeCore}`,
    unlike: `unlike ${unlikeCore}`
  };

  // Pillar outcomes from opportunity_map
  const pillar_outcomes = uniqNonEmpty(
    (safeGet(insights, "opportunity_map", []) || []).map((o) => {
      const need = (o && o.need) || "";
      const fit = (o && o.fit_reason) || "";
      const v =
        need && fit
          ? `When buyers need ${need}, we can deliver ${fit}.`
          : (o && o.summary) || "";
      return v;
    })
  ).slice(0, 6);

  // Business value from commercial_impacts
  const business_value = uniqNonEmpty(
    (safeGet(buyerLogic, "commercial_impacts", []) || []).map((ci) => {
      const label = ci && ci.label;
      if (!label) return "";
      const ids = safeGet(ci, "origin.related_claim_ids", []);
      return withEvidenceTag(label, ids);
    })
  ).slice(0, 6);

  // Persona value from emotional_drivers
  const persona_value = uniqNonEmpty(
    (safeGet(buyerLogic, "emotional_drivers", []) || []).map((ed) => {
      const label = ed && ed.label;
      if (!label) return "";
      const ids = safeGet(ed, "origin.related_claim_ids", []);
      return withEvidenceTag(label, ids);
    })
  ).slice(0, 6);

  // Product fit from right_to_play + supplier_capability
  const product_fit = uniqNonEmpty(
    []
      .concat(byTag.right_to_play || [])
      .concat(byTag.supplier_capability || [])
      .map((c) => bulletFromClaim(c))
  ).slice(0, 6);

  return {
    moore_chain,
    pillar_outcomes,
    business_value,
    persona_value,
    product_fit
  };
}

function buildCompetitiveStrategy({ evidence, markdownPack }) {
  const byTag = indexClaimsByTag(evidence);

  // competitor_map from markdown_pack
  const competitor_map = uniqNonEmpty(
    (safeGet(markdownPack, "competitor_profiles", []) || []).map((c) => {
      if (!c) return "";
      if (c.summary) return String(c.summary).trim();
      if (c.name && c.positioning) {
        return `${c.name}: ${c.positioning}`;
      }
      return "";
    })
  );

  // our_advantage and defensible_differentiators from differentiator + right_to_play
  const diffClaims = []
    .concat(byTag.differentiator || [])
    .concat(byTag.right_to_play || []);

  const our_advantage = uniqNonEmpty(
    diffClaims.map((c) => bulletFromClaim(c))
  ).slice(0, 6);

  const defensible_differentiators = our_advantage.slice(0);

  // angles_of_attack from buyer problems + timing
  const angles_of_attack = [];

  (safeGet(evidence, "claims", []) || []).forEach((c) => {
    if (!c || !c.tag) return;
    if (c.tag === "buyer_blocker" || c.tag === "timing") {
      const b = bulletFromClaim(c);
      if (b) angles_of_attack.push(b);
    }
  });

  // vulnerability_map: evidence-led where possible; neutral fallback otherwise
  const vulnBullets = [];
  (safeGet(evidence, "claims", []) || []).forEach((c) => {
    if (!c || !c.tag) return;
    if (c.tag === "risk" || c.tag === "buyer_blocker") {
      const b = bulletFromClaim(c);
      if (b) vulnBullets.push(b);
    }
  });

  let vulnerability_map = uniqNonEmpty(vulnBullets).slice(0, 6);
  if (!vulnerability_map.length) {
    vulnerability_map = [
      "VULNERABILITY_SIGNALS=INSUFFICIENT"
    ];
  }

  return {
    competitor_map,
    our_advantage,
    angles_of_attack: uniqNonEmpty(angles_of_attack).slice(0, 6),
    defensible_differentiators,
    vulnerability_map
  };
}

function buildBuyerStrategy({ buyerLogic, insights, evidence }) {
  const problems = uniqNonEmpty(
    (safeGet(buyerLogic, "problems", []) || []).map((p) => {
      const label = p && p.label;
      const ids = safeGet(p, "origin.related_claim_ids", []);
      return withEvidenceTag(label, ids);
    })
  ).slice(0, 8);

  const barriers = uniqNonEmpty([
    ...(safeGet(insights, "adoption_barriers", []) || []).map((b) =>
      b && b.label ? String(b.label).trim() : ""
    ),
    ...(safeGet(buyerLogic, "risk_tolerances", []) || []).map((r) => {
      const label = r && r.label;
      const ids = safeGet(r, "origin.related_claim_ids", []);
      return withEvidenceTag(label, ids);
    })
  ]).slice(0, 8);

  const urgency = uniqNonEmpty([
    ...(safeGet(insights, "timing_drivers", []) || []).map((t) =>
      t && t.text ? String(t.text).trim() : ""
    ),
    ...(safeGet(buyerLogic, "urgency_factors", []) || []).map((u) => {
      const label = u && u.label;
      const ids = safeGet(u, "origin.related_claim_ids", []);
      return withEvidenceTag(label, ids);
    })
  ]).slice(0, 6);

  const decision_drivers = uniqNonEmpty([
    ...(safeGet(buyerLogic, "decision_criteria", []) || []).map((d) => {
      const label = d && d.label;
      const ids = safeGet(d, "origin.related_claim_ids", []);
      return withEvidenceTag(label, ids);
    }),
    ...(safeGet(evidence, "claims", []) || [])
      .filter((c) => c && c.tag === "buyer_priority")
      .map((c) => bulletFromClaim(c))
  ]).slice(0, 8);

  return {
    problems,
    barriers,
    urgency,
    decision_drivers
  };
}

function buildGtmStrategy({ csvNormalized, mergedInput }) {
  const routeRaw =
    mergedInput.sales_model ||
    mergedInput.salesModel ||
    mergedInput.call_type ||
    "";
  const routeLower = routeRaw.toString().toLowerCase();

  let routeCode = "mixed";
  if (routeLower.includes("partner") || routeLower.includes("channel")) {
    routeCode = "partner";
  } else if (routeLower.includes("direct") || routeLower.includes("field")) {
    routeCode = "direct";
  }

  const route_implications = [
    `ROUTE_MODEL=${routeCode}`
  ];

  const rowCount = safeGet(csvNormalized, "meta.rows", 0);
  const successNarrative = deriveOutcomeByTam(rowCount, routeRaw);

  const success_target = {
    narrative: successNarrative,
    commercial_focus: "",
    leading_indicators: [
      `LEADING_INDICATOR:cohort_size=${rowCount}`,
      `LEADING_INDICATOR:route_model=${routeCode}`
    ]
  };

  const pipeline_model = {
    tiers: [
      `PIPELINE_TIER_MODEL=3`,
      `PIPELINE_TIER_CRITERIA=urgency_and_fit`
    ],
    motions: []
  };

  return {
    route_implications,
    success_target,
    pipeline_model
  };
}

function buildProofPoints({ evidence }) {
  const byTag = indexClaimsByTag(evidence);
  const proofClaims = []
    .concat(byTag.supplier_capability || [])
    .concat(byTag.right_to_play || [])
    .concat(byTag.supplier_overview || []);

  const proof_points = uniqNonEmpty(
    proofClaims.map((c) => bulletFromClaim(c))
  ).slice(0, 10);

  return proof_points;
}

function buildRightToPlay({ evidence }) {
  const byTag = indexClaimsByTag(evidence);
  const rtpClaims = []
    .concat(byTag.right_to_play || [])
    .concat(byTag.supplier_overview || []);

  const right_to_play = uniqNonEmpty(
    rtpClaims.map((c) => bulletFromClaim(c))
  ).slice(0, 6);

  return right_to_play;
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
    }),
    competitive_strategy: buildCompetitiveStrategy({ evidence, markdownPack }),
    buyer_strategy: buildBuyerStrategy({ buyerLogic, insights, evidence }),
    gtm_strategy: buildGtmStrategy({ csvNormalized, mergedInput }),
    proof_points: buildProofPoints({ evidence }),
    right_to_play: buildRightToPlay({ evidence })
  };
}

// ---------------------- Main Azure Function ---------------------- //

module.exports = async function (context, queueItem) {
  const log = context.log;
  const msg = parseQueueItem(queueItem);
  if (msg.op && msg.op !== "kickoff") {
    context.log(`[*] campaign-worker: ignoring non-kickoff op=${msg.op}`);
    return;
  }
  const explicitRunId = msg.runId || msg.run_id || null;
  const userId = msg.userId || msg.user || "anonymous";
  const page = msg.page || "campaign";

  // Prefer prefix from message (authoritative); fall back to canonical if missing
  let prefix = msg.prefix || null;
  if (!prefix) {
    prefix = canonicalPrefix({
      userId,
      page,
      runId: explicitRunId || "run"
    });
    log.warn("campaign_worker_missing_prefix_msg_using_canonical", {
      userId,
      page,
      explicitRunId
    });
  }

  // Recover runId from prefix if it was slimmed away
  let runId = explicitRunId;
  if (!runId && typeof prefix === "string") {
    const parts = prefix.split("/").filter(Boolean);
    // expected layout: runs/<page>/<userId>/<YYYY>/<MM>/<DD>/<runId>
    if (parts.length >= 7) {
      runId = parts[parts.length - 1];
    }
  }
  if (!runId) runId = "unknown";

  log(`[*] Strategy Engine starting for runId=${runId}, prefix=${prefix}`);

  const container = await getResultsContainer();

  await updateStatus(
    container,
    prefix,
    "strategy_working",
    "Strategy Engine started"
  );

  try {
    // -------- Load Phase 1 artefacts (missing files are tolerated) -------- //
    const evidence = await readJsonIfExists(container, `${prefix}evidence.json`);
    const evidenceLog = await readJsonIfExists(
      container,
      `${prefix}evidence_log.json`
    );

    let insights =
      (await readJsonIfExists(
        container,
        `${prefix}insights_v1/insights.json`
      )) || (await readJsonIfExists(container, `${prefix}insights.json`));

    let buyerLogic =
      (await readJsonIfExists(
        container,
        `${prefix}insights_v1/buyer_logic.json`
      )) || (await readJsonIfExists(container, `${prefix}buyer_logic.json`));

    let markdownPack =
      (await readJsonIfExists(
        container,
        `${prefix}evidence_v2/markdown_pack.json`
      )) || (await readJsonIfExists(container, `${prefix}markdown_pack.json`));

    const csvNormalized = await readJsonIfExists(
      container,
      `${prefix}csv_normalized.json`
    );
    const outline = await readJsonIfExists(container, `${prefix}outline.json`);
    const baseInput = await readJsonIfExists(container, `${prefix}input.json`);

    log("[*] Loaded Phase 1 artefacts", {
      hasEvidence: !!evidence,
      hasEvidenceLog: !!evidenceLog,
      hasInsights: !!insights,
      hasBuyerLogic: !!buyerLogic,
      hasMarkdownPack: !!markdownPack,
      hasCsvNormalized: !!csvNormalized,
      hasOutline: !!outline,
      hasInput: !!baseInput
    });

    insights = insights || {};
    buyerLogic = buyerLogic || {};
    markdownPack = markdownPack || {};

    const mergedInput = {
      ...(baseInput || {}),
      ...(msg.input || {}),
      ...msg
    };

    // Build a canonical evidence object: prefer evidence.claims; fallback to evidence array or evidenceLog array.
    if (evidence && typeof evidence === "object" && !Array.isArray(evidence.claims)) {
      log.warn("evidence_object_missing_claims_array", {
        keys: Object.keys(evidence || {}),
        prefix
      });
    }

    let claims = [];
    if (evidence && Array.isArray(evidence.claims)) {
      claims = evidence.claims;
    } else if (Array.isArray(evidence)) {
      claims = evidence;
    } else if (Array.isArray(evidenceLog)) {
      claims = evidenceLog;
    }

    const combinedEvidence = { claims };

    //--------------------------------------------------------------------------//
    //  Build strategy_v2 (deterministic, evidence-only)
    //--------------------------------------------------------------------------//
    const strategy_v2 = buildStrategyV2({
      evidence: combinedEvidence,
      insights,
      buyerLogic,
      markdownPack,
      csvNormalized: csvNormalized || {},
      mergedInput
    });

    //--------------------------------------------------------------------------//
    //  Compute strategy_v2 viability (Strategy Engine v2) – optional, safe, non-blocking
    //--------------------------------------------------------------------------//
    let strategyV2Viability = null;

    try {
      if (computeViability) {
        const viabilityMode =
          (process.env.STRATEGY_VIABILITY_MODE || "conservative").toLowerCase();

        strategyV2Viability = computeViability({
          evidence: combinedEvidence,
          insights,
          buyerLogic,
          markdownPack,
          csvNormalized: csvNormalized || {},
          input: mergedInput,
          strategy_v2,
          mode: viabilityMode
        });

        log("[*] strategy_v2 viability computed successfully", {
          runId,
          viabilityMode,
          viabilityIncluded: strategyV2Viability ? true : false
        });
      } else {
        log("[*] computeViability not available – strategy_v2 viability skipped");
      }
    } catch (v2err) {
      log.warn(
        "[!] strategy_v2 viability computation failed (non-fatal)",
        {
          runId,
          error: String(v2err && v2err.message ? v2err.message : v2err)
        }
      );
    }

    //--------------------------------------------------------------------------//
    //  Compute strategy_v3 viability (stronger evaluator) – safe, non-blocking
    //--------------------------------------------------------------------------//
    try {
      const evaluateStrategyViability = require("../lib/evaluateStrategyViability");

      // ---- Canonicalise prefix (ensure it ends with "/") ----
      let v3prefix = prefix || "";
      if (!v3prefix.endsWith("/")) v3prefix = v3prefix + "/";

      // ---- Extract evidence for viability scoring ----
      const claimsForViability =
        Array.isArray(evidence?.claims)
          ? evidence.claims
          : Array.isArray(evidenceLog)
            ? evidenceLog
            : [];

      const evidenceClaimsCount = claimsForViability.length;
      const cohortSize =
        (csvNormalized &&
          csvNormalized.meta &&
          Number(csvNormalized.meta.rows)) ||
        null;

      const viabilityMode =
        (process.env.STRATEGY_VIABILITY_MODE || "conservative").toLowerCase();

      // ---- Run evaluator ----
      const strategyV3Viability = evaluateStrategyViability({
        strategyV2: strategy_v2,
        buyerLogic: buyerLogic || {},
        csvCanon: csvNormalized || {},
        cohortSize,
        evidenceClaimsCount,
        mode: viabilityMode
      });

      // ---- Build full path ----
      const v3Path = `${v3prefix}strategy_v3/viability.json`;
      const v3Blob = container.getBlockBlobClient(v3Path);

      // ---- Ensure Azure can write nested folder paths safely ----
      const viabilityJson = Buffer.from(
        JSON.stringify(strategyV3Viability, null, 2),
        "utf8"
      );

      await v3Blob.uploadData(viabilityJson, {
        blobHTTPHeaders: { blobContentType: "application/json" }
      });

      log("[*] strategy_v3 viability written successfully", {
        runId,
        v3Path
      });

    } catch (v3err) {
      log.warn(
        "[!] strategy_v3 viability generation failed (non-fatal)",
        String(v3err && v3err.message ? v3err.message : v3err)
      );
    }

    //--------------------------------------------------------------------------//
    //  Persist the strategy_v2/campaign_strategy.json (primary writer input)
    //--------------------------------------------------------------------------//
    const combinedOut = strategyV2Viability
      ? { strategy_v2, viability: strategyV2Viability }
      : { strategy_v2 };

    const strategyPath = `${prefix}strategy_v2/campaign_strategy.json`;
    await writeJson(container, strategyPath, combinedOut);

    log("[*] strategy_v2 written successfully", {
      runId,
      strategyPath,
      viabilityAttached: !!strategyV2Viability
    });

    //--------------------------------------------------------------------------//
    //  Mark state = strategy_ready
    //--------------------------------------------------------------------------//
    await updateStatus(
      container,
      prefix,
      "strategy_ready",
      "Strategy Engine completed successfully",
      {
        strategy_path: strategyPath,
        viability_mode: process.env.STRATEGY_VIABILITY_MODE || "conservative"
      }
    );

    const { enqueueTo } = require("../lib/campaign-queue");

    await enqueueTo(process.env.Q_CAMPAIGN_WRITE, {
      op: "afterstrategy",
      runId,
      prefix,
      page
    });

    log("[*] Strategy Engine completed", {
      runId,
      strategyPath
    });

  } catch (err) {
    log.error("[!] Strategy Engine failed", {
      runId,
      prefix,
      error: String(err && err.message ? err.message : err)
    });

    try {
      const errorPath = `${prefix}strategy_v2/error.json`;
      await writeJson(container, errorPath, {
        message: String(err && err.message ? err.message : err),
        stack: err && err.stack ? String(err.stack) : null
      });
    } catch (e2) {
      log.error("[!] Failed to write strategy error file", String(e2));
    }

    await updateStatus(
      container,
      prefix,
      "strategy_error",
      "Strategy Engine failed",
      { error: String(err && err.message ? err.message : err) }
    );

    throw err;
  }
};
