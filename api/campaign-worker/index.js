// /api/campaign-worker/index.js
// 02-01-2026 — Strategy Engine Option A (content-pillars-v2), hash-locked, deterministic. V28
//
// Key upgrades:
// - Requires content-pillars-v2 (core_pillars + proof_enrichment)
// - Messaging primitives: core_pillars + buyer_logic labels only
// - Evidence: proof-only (must be claim_refs from proof enrichment)
// - Stops using evidence tag buckets for messaging drift
// - Mechanical refusal on locks + sha mismatches + provenance_validated
// - Writes workerLocked markers for audit

"use strict";

const nodeCrypto = require("crypto");

const { enqueueTo } = require("../lib/campaign-queue");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");

const ROUTER_QUEUE = process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";

// ---------------------------------------------------------------
// UTILITIES
// ---------------------------------------------------------------

function parseQueueItem(queueItem) {
  if (!queueItem) return {};
  if (typeof queueItem === "string") {
    try { return JSON.parse(queueItem); } catch { return {}; }
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

async function readJsonSafe(container, path, fallback = null) {
  try {
    const v = await getJson(container, path);
    return v ?? fallback;
  } catch {
    return fallback;
  }
}

function stableStringify(obj) {
  if (obj === null || typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return "[" + obj.map(stableStringify).join(",") + "]";
  const keys = Object.keys(obj).sort();
  return "{" + keys.map(k => JSON.stringify(k) + ":" + stableStringify(obj[k])).join(",") + "}";
}

function sha256(s) {
  return nodeCrypto.createHash("sha256").update(String(s)).digest("hex");
}

function sha1(s) {
  return nodeCrypto.createHash("sha1").update(String(s)).digest("hex");
}

function sha1OfJson(obj) {
  return sha1(stableStringify(obj ?? null));
}

function normPrefix(prefix) {
  let p = String(prefix || "").trim();
  if (!p) return "";
  p = p.replace(/^\/+/, "");
  if (!p.endsWith("/")) p += "/";
  return p;
}

function ensureArray(v) {
  return Array.isArray(v) ? v : [];
}

function isNonEmptyString(v) {
  return typeof v === "string" && v.trim().length > 0;
}

function withEvidenceTag(text, ids) {
  if (!text) return "";
  const s = text.toString().trim();
  if (!ids || !ids.length) return s;
  return `${s} [${ids[0]}]`;
}

function bulletFromClaim(c) {
  if (!c) return "";
  const body = c.summary || c.title || "";
  const id = c.claim_id || "";
  if (!body) return "";
  if (!id) return body.trim();
  return `${body.trim()} [${id}]`;
}

// ---------------------------------------------------------------
// content-pillars-v2 validation + helpers
// ---------------------------------------------------------------

function validateContentPillarsV2(cp) {
  if (!cp || typeof cp !== "object") return { ok: false, reason: "not_object" };
  if (!isNonEmptyString(cp.schema) || cp.schema !== "content-pillars-v2") return { ok: false, reason: "wrong_schema" };
  if (!cp.meta || typeof cp.meta !== "object") return { ok: false, reason: "missing_meta" };
  if (cp.meta.provenance_validated !== true) return { ok: false, reason: "provenance_not_validated" };
  if (!cp.inputs || typeof cp.inputs !== "object") return { ok: false, reason: "missing_inputs" };
  if (!Array.isArray(cp.core_pillars)) return { ok: false, reason: "missing_core_pillars" };
  if (!Array.isArray(cp.proof_enrichment)) return { ok: false, reason: "missing_proof_enrichment" };
  for (const p of cp.core_pillars) {
    if (!p || typeof p !== "object") return { ok: false, reason: "core_pillar_not_object" };
    if (!isNonEmptyString(p.id)) return { ok: false, reason: "core_pillar_missing_id" };
    if (!isNonEmptyString(p.title)) return { ok: false, reason: "core_pillar_missing_title" };
    if (!["assertable", "framing"].includes(String(p.mode || ""))) return { ok: false, reason: "core_pillar_invalid_mode" };
    if (!Array.isArray(p.source_refs) || !p.source_refs.length) return { ok: false, reason: "core_pillar_missing_source_refs" };
  }
  return { ok: true };
}

function buildProofMap(cp) {
  const map = {};
  for (const pe of ensureArray(cp?.proof_enrichment)) {
    const pid = String(pe?.pillar_id || "").trim();
    if (!pid) continue;
    map[pid] = ensureArray(pe?.claim_refs).map(x => ({
      claim_id: String(x?.claim_id || "").trim(),
      tier_group: String(x?.tier_group || "other").trim() || "other"
    })).filter(x => x.claim_id);
  }
  return map;
}

function buildAllowedClaimIdSetFromProofMap(proofMap) {
  const out = new Set();
  for (const pid of Object.keys(proofMap || {})) {
    for (const cr of ensureArray(proofMap[pid])) {
      if (cr && cr.claim_id) out.add(cr.claim_id);
    }
  }
  return out;
}

// ---------------------------------------------------------------
// VIABILITY (deterministic)
// ---------------------------------------------------------------

function buildViability({ evidence, csvNormalized, contentPillars }) {
  const claims = Array.isArray(evidence?.claims) ? evidence.claims : [];

  const csvRows =
    Number(csvNormalized?.meta?.rows) ||
    Number(csvNormalized?.rows) ||
    0;

  const hasCaseStudies = claims.some(c =>
    c.tag === "case_study" ||
    c.tier_group === "case_study" ||
    c.source_type === "pdf_extract" ||
    c.source_type === "website_case_study"
  );

  const hasSupplierProfile = claims.some(c =>
    c.tag === "supplier_profile" ||
    c.tier_group === "supplier_profile" ||
    c.source_type === "supplier_profile"
  );

  const cpCount = ensureArray(contentPillars?.core_pillars).length;

  let marketSize = "small";
  if (csvRows >= 50 && csvRows < 400) marketSize = "medium";
  if (csvRows >= 400) marketSize = "large";

  let evidenceStrength = "weak";
  if (claims.length >= 10) evidenceStrength = "moderate";
  if (claims.length >= 25) evidenceStrength = "strong";

  let positioningClarity = "low";
  if (cpCount >= 6) positioningClarity = "partial";
  if (cpCount >= 10 && hasSupplierProfile) positioningClarity = "clear";

  const viable =
    csvRows > 0 &&
    hasSupplierProfile &&
    evidenceStrength !== "weak" &&
    cpCount >= 4;

  const constraints = [];
  if (!hasCaseStudies) constraints.push("Limited case study proof");
  if (cpCount < 6) constraints.push("Limited content pillars scope");
  if (csvRows < 50) constraints.push("Small addressable cohort");

  return {
    schema: "viability-v2",
    generated_at: new Date().toISOString(),
    inputs: {
      csv_rows: csvRows,
      has_case_studies: hasCaseStudies,
      has_supplier_profile: hasSupplierProfile,
      content_pillars_count: cpCount
    },
    signals: {
      market_size: marketSize,
      evidence_strength: evidenceStrength,
      positioning_clarity: positioningClarity
    },
    verdict: {
      viable,
      confidence:
        viable && evidenceStrength === "strong" && positioningClarity === "clear"
          ? "high"
          : viable
            ? "medium"
            : "low",
      constraints
    }
  };
}

// ---------------------------------------------------------------
// STRATEGY BUILDERS (Option A: pillars + buyerLogic only)
// ---------------------------------------------------------------

function buildStorySpine({ evidence, buyerLogic, csvNormalized, mergedInput, contentPillars, proofMap }) {
  const core = ensureArray(contentPillars?.core_pillars);

  // Environment: buyerLogic labels only (no evidence-tag drift)
  const environment = uniqNonEmpty(
    []
      .concat(ensureArray(safeGet(buyerLogic, "commercial_impacts", [])).map(ci =>
        withEvidenceTag(ci.label, safeGet(ci, "origin.related_claim_ids", []))
      ))
      .concat(ensureArray(safeGet(buyerLogic, "market_conditions", [])).map(x =>
        withEvidenceTag(x.label || x.text, safeGet(x, "origin.related_claim_ids", []))
      ))
  ).slice(0, 6);

  // Case for action: buyerLogic only
  const case_for_action = uniqNonEmpty(
    []
      .concat(ensureArray(safeGet(buyerLogic, "problems", [])).map(p =>
        withEvidenceTag(p.label, safeGet(p, "origin.related_claim_ids", []))
      ))
      .concat(ensureArray(safeGet(buyerLogic, "urgency_factors", [])).map(u =>
        withEvidenceTag(u.label, safeGet(u, "origin.related_claim_ids", []))
      ))
  ).slice(0, 8);

  // How we win: core pillar titles (assertable include a single claim tag if present)
  const how_we_win = uniqNonEmpty(
    core.map(p => {
      const proof = ensureArray(proofMap?.[p.id]);
      const ids = proof.map(x => x.claim_id).filter(Boolean);
      return withEvidenceTag(p.title, ids);
    })
  ).slice(0, 8);

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
    ...ensureArray(safeGet(buyerLogic, "success_signals", [])).map(x => x.label || x.text || "")
  ]).slice(0, 4);

  const next_steps = uniqNonEmpty([
    n ? `NEXT_STEP:target_cohort_size=${n}` : "",
    environment.length ? `NEXT_STEP:align_to_environment_signals=${environment.length}` : "",
    case_for_action.length ? `NEXT_STEP:prioritise_case_for_action=${case_for_action.length}` : "",
    how_we_win.length ? `NEXT_STEP:validate_pillars=${how_we_win.length}` : ""
  ]).slice(0, 4);

  return { environment, case_for_action, how_we_win, success, next_steps };
}

function buildValueProposition({ buyerLogic, mergedInput, contentPillars }) {
  const industry = mergedInput.selected_industry || mergedInput.campaign_industry || "general";
  const buyers = mergedInput.buyer_type || "decision-makers";

  const topProblem =
    safeGet(buyerLogic, "problems.0.label") ||
    safeGet(buyerLogic, "urgency_factors.0.label") ||
    "";

  const topOutcome =
    safeGet(buyerLogic, "commercial_impacts.0.label") ||
    "";

  const firstPillar = ensureArray(contentPillars?.core_pillars)[0] || null;
  const ourSolutionCore = firstPillar ? firstPillar.title : "";

  return {
    value_proposition: {
      moore_chain: {
        for_who: `For ${industry} ${buyers}`,
        problem: topProblem ? `who struggle with ${topProblem}` : "who struggle with operational and commercial pressures",
        our_solution: ourSolutionCore ? `we provide ${ourSolutionCore}` : "we provide a supplier-aligned capability set",
        outcome: topOutcome ? `so that ${topOutcome}` : "so that outcomes improve commercially and operationally",
        unlike: "unlike undifferentiated connectivity or unmanaged propositions"
      }
    }
  };
}

function buildBuyerStrategy({ buyerLogic }) {
  return {
    problems: uniqNonEmpty(
      ensureArray(safeGet(buyerLogic, "problems", [])).map(p =>
        withEvidenceTag(p.label, safeGet(p, "origin.related_claim_ids", []))
      )
    ).slice(0, 10),

    barriers: uniqNonEmpty(
      ensureArray(safeGet(buyerLogic, "risk_tolerances", [])).map(r =>
        withEvidenceTag(r.label, safeGet(r, "origin.related_claim_ids", []))
      )
    ).slice(0, 10),

    urgency: uniqNonEmpty(
      ensureArray(safeGet(buyerLogic, "urgency_factors", [])).map(u =>
        withEvidenceTag(u.label, safeGet(u, "origin.related_claim_ids", []))
      )
    ).slice(0, 8)
  };
}

function buildGtmStrategy({ mergedInput }) {
  const routeRaw = mergedInput.sales_model || mergedInput.call_type || "";
  const lower = (routeRaw || "").toLowerCase();

  let route = "mixed";
  if (lower.includes("partner")) route = "partner";
  if (lower.includes("direct")) route = "direct";

  return { route_implications: [`ROUTE_MODEL=${route}`] };
}

function buildProofPoints({ evidenceClaimsById, proofMap, contentPillars }) {
  const core = ensureArray(contentPillars?.core_pillars);
  const out = [];

  // Prefer pillar-linked proof (assertable pillars first)
  const assertable = core.filter(p => p.mode === "assertable");
  for (const p of assertable.slice(0, 12)) {
    const refs = ensureArray(proofMap?.[p.id]);
    if (!refs.length) continue;
    // Use claim summary as proof bullet (deterministic lookup only)
    const first = refs[0];
    const claim = evidenceClaimsById[first.claim_id] || null;
    if (claim) out.push(bulletFromClaim(claim));
    else out.push(withEvidenceTag(p.title, [first.claim_id]));
  }

  return uniqNonEmpty(out).slice(0, 12);
}

function buildRightToPlay({ evidenceClaimsById, proofMap }) {
  // Deterministic: use proof enrichment claim refs only (no evidence tag drift)
  const out = [];
  for (const pid of Object.keys(proofMap || {})) {
    for (const cr of ensureArray(proofMap[pid])) {
      const claim = evidenceClaimsById[cr.claim_id];
      if (claim) out.push(bulletFromClaim(claim));
    }
  }
  return uniqNonEmpty(out).slice(0, 8);
}

function buildStrategyV2({ evidence, buyerLogic, csvNormalized, mergedInput, contentPillars, proofMap }) {
  const evidenceClaims = Array.isArray(evidence?.claims) ? evidence.claims : [];
  const evidenceClaimsById = {};
  for (const c of evidenceClaims) {
    const id = String(c?.claim_id || "").trim();
    if (id) evidenceClaimsById[id] = c;
  }

  return {
    story_spine: buildStorySpine({ evidence, buyerLogic, csvNormalized, mergedInput, contentPillars, proofMap }),
    value_proposition: buildValueProposition({ buyerLogic, mergedInput, contentPillars }).value_proposition,
    competitive_strategy: {
      competitor_map: [],
      our_advantage: [],
      angles_of_attack: [],
      defensible_differentiators: [],
      vulnerability_map: []
    },
    buyer_strategy: buildBuyerStrategy({ buyerLogic }),
    gtm_strategy: buildGtmStrategy({ mergedInput }),
    proof_points: buildProofPoints({ evidenceClaimsById, proofMap, contentPillars }),
    right_to_play: buildRightToPlay({ evidenceClaimsById, proofMap })
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

  const prefix = normPrefix(msg.prefix);
  const container = await getResultsContainerClient();

  const runId =
    msg.runId ||
    prefix.split("/").filter(Boolean).pop() ||
    "unknown";

  log("[worker] starting", { runId, prefix });

  const statusPath = `${prefix}status.json`;

  try {
    const evidence = await readJsonSafe(container, `${prefix}evidence.json`);
    const evidenceLog = await readJsonSafe(container, `${prefix}evidence_log.json`);
    const combinedEvidence = {
      claims:
        (Array.isArray(evidence?.claims) && evidence.claims) ||
        (Array.isArray(evidenceLog) && evidenceLog) ||
        []
    };

    const buyerLogic =
      (await readJsonSafe(container, `${prefix}buyer_logic.json`)) ||
      (await readJsonSafe(container, `${prefix}insights_v1/buyer_logic.json`)) ||
      {};

    const csvNormalized = (await readJsonSafe(container, `${prefix}csv_normalized.json`)) || {};
    const mergedInput = (await readJsonSafe(container, `${prefix}input.json`)) || {};

    // Load and validate content-pillars-v2 (required)
    const contentPillars = await readJsonSafe(container, `${prefix}content_pillars.json`);
    const shape = validateContentPillarsV2(contentPillars);

    if (!shape.ok) {
      const errMsg = `content_pillars.json invalid: ${shape.reason}`;
      log("[worker] ERROR", errMsg);

      const stFail = (await readJsonSafe(container, statusPath)) || { runId, markers: {}, history: [] };
      stFail.state = "worker_failed";
      stFail.markers = stFail.markers || {};
      stFail.markers.workerFailed = true;
      stFail.markers.workerFailedReason = "missing_or_invalid_content_pillars";
      stFail.history = Array.isArray(stFail.history) ? stFail.history : [];
      stFail.history.push({ at: new Date().toISOString(), phase: "worker", note: errMsg });

      await putJson(container, statusPath, stFail);
      throw new Error(errMsg);
    }

    // Enforce status lock for content pillars
    const stLock = (await readJsonSafe(container, statusPath)) || {};
    if (stLock?.markers?.contentPillarsLocked !== true) {
      throw new Error("Worker refused: contentPillarsLocked !== true");
    }

    const contentPillarsSha1 = sha1OfJson(contentPillars);
    const evidenceSha1 = sha1OfJson(evidence);

    const declaredEvidenceSha1 = contentPillars?.inputs?.evidence_sha1 || null;
    const declaredMarkdownSha1 = contentPillars?.inputs?.markdown_pack_sha1 || null;

    if (declaredEvidenceSha1 && String(declaredEvidenceSha1) !== evidenceSha1) {
      throw new Error(`Evidence hash mismatch: content_pillars.inputs.evidence_sha1=${declaredEvidenceSha1} but evidence.json sha1=${evidenceSha1}`);
    }

    const lockedCpSha1 = stLock?.markers?.contentPillarsSha1 || null;
    if (lockedCpSha1 && String(lockedCpSha1) !== contentPillarsSha1) {
      throw new Error(`Worker refused: content_pillars sha mismatch vs status lock (${lockedCpSha1} != ${contentPillarsSha1})`);
    }

    const proofMap = buildProofMap(contentPillars);
    const allowedClaimIds = buildAllowedClaimIdSetFromProofMap(proofMap);

    // Validate that proof claim ids exist in evidence
    const evidenceIdSet = new Set(ensureArray(combinedEvidence?.claims).map(c => String(c?.claim_id || "").trim()).filter(Boolean));
    const missingProofIds = Array.from(allowedClaimIds).filter(id => !evidenceIdSet.has(id));
    if (missingProofIds.length) {
      throw new Error(`Worker refused: proof claim_ids missing from evidence.json (${missingProofIds.slice(0, 12).join(", ")})`);
    }

    // Build deterministic strategy
    const strategy_v2 = buildStrategyV2({
      evidence: combinedEvidence,
      buyerLogic,
      csvNormalized,
      mergedInput,
      contentPillars,
      proofMap
    });

    const strategyBundle = {
      schema: "campaign-strategy-v2.2",
      generated_at: new Date().toISOString(),
      run_id: runId,
      inputs: {
        content_pillars_sha1: contentPillarsSha1,
        evidence_sha1: evidenceSha1,
        markdown_pack_sha1: declaredMarkdownSha1 || null
      },
      strategy_v2
    };

    const strategyHash = sha256(stableStringify(strategyBundle));

    const viability = buildViability({
      evidence: combinedEvidence,
      csvNormalized,
      contentPillars
    });

    await putJson(container, `${prefix}strategy_v2/viability.json`, viability);

    const st0 =
      (await readJsonSafe(container, statusPath)) ||
      { runId, markers: {}, history: [] };

    st0.markers = (st0.markers && typeof st0.markers === "object") ? st0.markers : {};
    if (!Array.isArray(st0.history)) st0.history = [];
    if (!Array.isArray(st0.errors)) st0.errors = [];

    st0.markers.viabilityCompleted = true;

    const prevHash = st0.markers.strategyHash || null;
    const changed = !prevHash || prevHash !== strategyHash;

    st0.markers.strategyHash = strategyHash;
    st0.markers.strategyChanged = changed;
    st0.markers.strategyCompleted = true;

    // Locks for audit
    st0.markers.contentPillarsSha1 = contentPillarsSha1;
    st0.markers.contentPillarsEvidenceSha1 = evidenceSha1;
    st0.markers.contentPillarsMarkdownSha1 = declaredMarkdownSha1 || null;
    st0.markers.contentPillarsLocked = true;

    st0.markers.workerLocked = true;
    st0.markers.workerSha1 = sha1OfJson(strategyBundle);

    st0.markers.workerProofClaimsAllowedCount = allowedClaimIds.size;
    st0.markers.workerCorePillarsCount = ensureArray(contentPillars.core_pillars).length;
    st0.markers.workerFramingPillarsCount = ensureArray(contentPillars.core_pillars).filter(p => p.mode === "framing").length;
    st0.markers.workerAssertablePillarsCount = ensureArray(contentPillars.core_pillars).filter(p => p.mode === "assertable").length;

    st0.state = "strategy_ready";

    st0.history.push({
      at: new Date().toISOString(),
      phase: "viability_verdict",
      note: viability?.verdict?.viable ? "viable" : "not_viable"
    });

    const outPath = `${prefix}strategy_v2/campaign_strategy.json`;
    await putJson(container, outPath, strategyBundle);

    st0.history.push({ at: new Date().toISOString(), phase: "strategy_written", note: outPath });

    await putJson(container, statusPath, st0);

    await enqueueTo(ROUTER_QUEUE, {
      op: "afterworker",
      runId,
      prefix,
      userId: msg.userId || "anonymous",
      page: msg.page || "campaign"
    });

    log("[worker] completed", { runId, outPath, strategyChanged: changed });

  } catch (err) {
    try {
      const stE = (await readJsonSafe(container, statusPath)) || { runId, markers: {}, history: [] };
      stE.markers = (stE.markers && typeof stE.markers === "object") ? stE.markers : {};
      if (!Array.isArray(stE.history)) stE.history = [];
      if (!Array.isArray(stE.errors)) stE.errors = [];

      stE.state = "worker_failed";
      stE.markers.workerFailed = true;

      stE.errors.push({
        at: new Date().toISOString(),
        phase: "campaign-worker",
        message: err && err.message ? String(err.message) : String(err),
        stack: err && err.stack ? String(err.stack) : ""
      });

      await putJson(container, statusPath, stE);
    } catch (e2) {
      log("[worker] failed to persist failure status", e2);
    }

    log("[worker] ERROR", err);
    throw err;
  }
};
