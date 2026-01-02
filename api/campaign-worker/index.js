// /api/campaign-worker/index.js
// 02-01-2026 — Strategy Engine Option A (content_pillars-first), hash-locked, deterministic. V27
//
// Doctrine (Option A):
// - Worker MUST consume content_pillars.json as the primary messaging truth source.
// - Worker MUST NOT interpret supplier markdown packs or regenerate supplier/industry pillars.
// - Worker MUST NOT consume insights.json for strategy construction (interpretive synthesis excluded).
// - Worker MAY consume:
//     - evidence.json (claim ids + proof anchors)
//     - buyer_logic.json (deterministic derived model from evidence)
//     - csv_normalized.json (signals only)
//     - input.json (identity + constraints)
// - Worker MUST be hash-locked:
//     - content_pillars_sha1
//     - evidence_sha1
//   and must fail if declared hashes mismatch.
// - Worker writes:
//     - strategy_v2/campaign_strategy.json
//     - strategy_v2/viability.json
//   and updates status.json markers deterministically.
//
// Output discipline:
// - Strategy text MUST be sourced from content_pillars and/or buyer_logic labels, not invented.
// - Proof points MUST carry claim_id anchors (existing claim IDs only).
//
// Notes:
// - This version preserves your queue contract: op=run_strategy, prefix required.
// - Router notify op remains: afterworker.

"use strict";

const nodeCrypto = require("crypto"); // alias to avoid TDZ/shadowing

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

// Deterministic stringify (stable key ordering)
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

// Evidence claims indexing (stable tag grouping)
function indexClaimsByTag(evidence) {
  const claims = Array.isArray(evidence?.claims) ? evidence.claims : [];
  const out = {};
  for (const c of claims) {
    const tag = c.tag || c.tier_group || "other";
    if (!out[tag]) out[tag] = [];
    out[tag].push(c);
  }
  return out;
}

function bulletFromClaim(c) {
  if (!c) return "";
  const body = c.summary || c.title || "";
  const id = c.claim_id || "";
  if (!body) return "";
  if (!id) return body.trim();
  return `${body.trim()} [${id}]`;
}

function validateContentPillarsShape(cp) {
  if (!cp || typeof cp !== "object") return { ok: false, reason: "not_object" };
  if (!isNonEmptyString(cp.schema)) return { ok: false, reason: "missing_schema" };
  if (!cp.meta || typeof cp.meta !== "object") return { ok: false, reason: "missing_meta" };
  if (!Array.isArray(cp.pillars)) return { ok: false, reason: "missing_pillars_array" };
  for (const p of cp.pillars) {
    if (!p || typeof p !== "object") return { ok: false, reason: "pillar_not_object" };
    if (!isNonEmptyString(p.id)) return { ok: false, reason: "pillar_missing_id" };
    if (!isNonEmptyString(p.title)) return { ok: false, reason: "pillar_missing_title" };
    if (!p.provenance || typeof p.provenance !== "object") return { ok: false, reason: "pillar_missing_provenance" };
  }
  return { ok: true };
}

function flattenClaimIdsFromContentPillars(cp) {
  const ids = new Set();
  for (const p of ensureArray(cp?.pillars)) {
    for (const id of ensureArray(p?.evidence_claim_ids)) {
      const s = String(id || "").trim();
      if (s) ids.add(s);
    }
  }
  return Array.from(ids);
}

function clamp(arr, n) {
  return ensureArray(arr).slice(0, Math.max(0, n | 0));
}

// ---------------------------------------------------------------
// VIABILITY (DOCTRINE SAFE; NO INSIGHTS)
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

  const cpCount = ensureArray(contentPillars?.pillars).length;

  // Deterministic signal derivation (rule-based)
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
// STRATEGY BUILDERS (OPTION A; NO MARKDOWN PACK, NO INSIGHTS)
// ---------------------------------------------------------------

function buildStorySpine({ evidence, buyerLogic, csvNormalized, mergedInput, contentPillars }) {
  const byTag = indexClaimsByTag(evidence);

  // Environment signals: strictly from Tier-1 markdown drivers/risks/persona OR buyerLogic labels.
  // We do NOT invent; we list existing summaries with claim IDs.
  const environment = uniqNonEmpty(
    []
      .concat((byTag.markdown_industry_drivers || []).map(bulletFromClaim))
      .concat((byTag.markdown_industry_risks || []).map(bulletFromClaim))
      .concat((byTag.markdown_persona || []).map(bulletFromClaim))
      .concat(ensureArray(safeGet(buyerLogic, "commercial_impacts", [])).map(ci =>
        withEvidenceTag(ci.label, safeGet(ci, "origin.related_claim_ids", []))
      ))
  ).slice(0, 6);

  // Case for action: buyerLogic problems + urgency (anchored)
  const case_for_action = uniqNonEmpty(
    []
      .concat(ensureArray(safeGet(buyerLogic, "problems", [])).map(p =>
        withEvidenceTag(p.label, safeGet(p, "origin.related_claim_ids", []))
      ))
      .concat(ensureArray(safeGet(buyerLogic, "urgency_factors", [])).map(u =>
        withEvidenceTag(u.label, safeGet(u, "origin.related_claim_ids", []))
      ))
  ).slice(0, 8);

  // How we win: content pillars titles + differentiators (anchored by evidence ids if present)
  const how_we_win = uniqNonEmpty(
    []
      .concat(ensureArray(contentPillars?.pillars).map(p => {
        const ids = ensureArray(p?.evidence_claim_ids);
        return withEvidenceTag(p.title, ids);
      }))
      .concat((byTag.supplier_profile || []).map(bulletFromClaim))
      .concat((byTag.markdown_supplier || []).map(bulletFromClaim))
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

  const firstPillar = ensureArray(contentPillars?.pillars)[0] || null;

  const ourSolutionCore = firstPillar
    ? firstPillar.title
    : "";

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

function buildCompetitiveStrategy({ evidence }) {
  const byTag = indexClaimsByTag(evidence);

  const competitor_map = uniqNonEmpty(
    (byTag.markdown_competitor || []).map(bulletFromClaim)
  ).slice(0, 8);

  const diffs = uniqNonEmpty(
    []
      .concat(byTag.markdown_supplier || [])
      .concat(byTag.supplier_profile || [])
      .map(bulletFromClaim)
  ).slice(0, 8);

  const vulnerability_map = uniqNonEmpty(
    (evidence.claims || [])
      .filter(c =>
        c.tag === "risk" ||
        c.tier_group === "markdown_industry_risks" ||
        c.tier_group === "microclaim"
      )
      .map(bulletFromClaim)
  ).slice(0, 8);

  return {
    competitor_map,
    our_advantage: diffs.slice(0, 6),
    angles_of_attack: competitor_map.slice(0, 6),
    defensible_differentiators: diffs.slice(0, 6),
    vulnerability_map
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

function buildProofPoints({ evidence, contentPillars }) {
  const byTag = indexClaimsByTag(evidence);

  // Strongest proof sources:
  // - content pillars with evidence ids
  // - case studies
  // - supplier profile
  const out = [];

  for (const p of clamp(contentPillars?.pillars, 12)) {
    const ids = ensureArray(p?.evidence_claim_ids);
    if (!ids.length) continue;
    out.push(withEvidenceTag(p.title, ids));
  }

  out.push(...(byTag.case_study || []).map(bulletFromClaim));
  out.push(...(byTag.supplier_profile || []).map(bulletFromClaim));

  return uniqNonEmpty(out).slice(0, 12);
}

function buildRightToPlay({ evidence }) {
  const byTag = indexClaimsByTag(evidence);
  return uniqNonEmpty(
    []
      .concat(byTag.supplier_profile || [])
      .concat(byTag.microclaim || [])
      .map(bulletFromClaim)
  ).slice(0, 8);
}

function buildStrategyV2({ evidence, buyerLogic, csvNormalized, mergedInput, contentPillars }) {
  return {
    story_spine: buildStorySpine({ evidence, buyerLogic, csvNormalized, mergedInput, contentPillars }),
    value_proposition: buildValueProposition({ buyerLogic, mergedInput, contentPillars }).value_proposition,
    competitive_strategy: buildCompetitiveStrategy({ evidence }),
    buyer_strategy: buildBuyerStrategy({ buyerLogic }),
    gtm_strategy: buildGtmStrategy({ mergedInput }),
    proof_points: buildProofPoints({ evidence, contentPillars }),
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

  // Hard stop on wrong op, but do not crash.
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
    // -------------------------------------------------------------------------
    // Load canonical Phase 1 artefacts
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // Option A: load content_pillars.json (required)
    // -------------------------------------------------------------------------
    const contentPillars = await readJsonSafe(container, `${prefix}content_pillars.json`);
    const shape = validateContentPillarsShape(contentPillars);

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

    // -------------------------------------------------------------------------
    // Hash locks (deterministic audit)
    // -------------------------------------------------------------------------
    const contentPillarsSha1 = sha1OfJson(contentPillars);
    const evidenceSha1 = sha1OfJson(evidence);

    const declaredEvidenceSha1 = contentPillars?.inputs?.evidence_sha1 || null;
    const declaredMarkdownSha1 = contentPillars?.inputs?.markdown_pack_sha1 || null;

    if (declaredEvidenceSha1 && String(declaredEvidenceSha1) !== evidenceSha1) {
      const errMsg =
        `Evidence hash mismatch: content_pillars.inputs.evidence_sha1=${declaredEvidenceSha1} but evidence.json sha1=${evidenceSha1}`;
      log("[worker] ERROR", errMsg);
      throw new Error(errMsg);
    }

    // -------------------------------------------------------------------------
    // Build deterministic strategy_v2 (no insights, no markdown pack)
    // -------------------------------------------------------------------------
    const strategy_v2 = buildStrategyV2({
      evidence: combinedEvidence,
      buyerLogic,
      csvNormalized,
      mergedInput,
      contentPillars
    });

    // Include audit locks in the strategy wrapper (no mutation in child objects)
    const strategyBundle = {
      schema: "campaign-strategy-v2.1",
      generated_at: new Date().toISOString(),
      run_id: runId,
      inputs: {
        content_pillars_sha1: contentPillarsSha1,
        evidence_sha1: evidenceSha1,
        markdown_pack_sha1: declaredMarkdownSha1 || null
      },
      strategy_v2
    };

    // Compute hash BEFORE writing (single source of truth)
    const strategyHash = sha256(stableStringify(strategyBundle));

    // Viability (deterministic; no insights)
    const viability = buildViability({
      evidence: combinedEvidence,
      csvNormalized,
      contentPillars
    });

    // -------------------------------------------------------------------------
    // Persist viability.json
    // -------------------------------------------------------------------------
    const viabilityPath = `${prefix}strategy_v2/viability.json`;
    await putJson(container, viabilityPath, viability);

    // -------------------------------------------------------------------------
    // Read status once, update markers deterministically, write once
    // -------------------------------------------------------------------------
    const st0 =
      (await readJsonSafe(container, statusPath)) ||
      { runId, markers: {}, history: [] };

    st0.markers = (st0.markers && typeof st0.markers === "object") ? st0.markers : {};
    if (!Array.isArray(st0.history)) st0.history = [];
    if (!Array.isArray(st0.errors)) st0.errors = [];

    // Mark viability completed
    st0.markers.viabilityCompleted = true;

    // Strategy changed flag
    const prevHash = st0.markers.strategyHash || null;
    const changed = !prevHash || prevHash !== strategyHash;

    st0.markers.strategyHash = strategyHash;
    st0.markers.strategyChanged = changed;
    st0.markers.strategyCompleted = true;

    // Store locks in status (enforced downstream)
    st0.markers.contentPillarsSha1 = contentPillarsSha1;
    st0.markers.contentPillarsEvidenceSha1 = evidenceSha1;
    st0.markers.contentPillarsMarkdownSha1 = declaredMarkdownSha1 || null;
    st0.markers.contentPillarsLocked = true;

    st0.state = "strategy_ready";

    // Deterministic audit history
    st0.history.push({
      at: new Date().toISOString(),
      phase: "viability_verdict",
      note: viability?.verdict?.viable ? "viable" : "not_viable"
    });

    // -------------------------------------------------------------------------
    // Persist strategy
    // -------------------------------------------------------------------------
    const outPath = `${prefix}strategy_v2/campaign_strategy.json`;
    await putJson(container, outPath, strategyBundle);

    st0.history.push({
      at: new Date().toISOString(),
      phase: "strategy_written",
      note: outPath
    });

    await putJson(container, statusPath, st0);

    // -------------------------------------------------------------------------
    // Notify router
    // -------------------------------------------------------------------------
    await enqueueTo(ROUTER_QUEUE, {
      op: "afterworker",
      runId,
      prefix,
      userId: msg.userId || "anonymous",
      page: msg.page || "campaign"
    });

    log("[worker] completed", { runId, outPath, strategyChanged: changed });

  } catch (err) {
    // Never deadlock: persist error into status
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
