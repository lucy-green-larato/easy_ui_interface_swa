// /api/campaign-worker/index.js 18-11-2025 Strategy Engine v3.1 (deterministic, no LLM)
// Responsibility:
//   - Read Phase 1 outputs (evidence, insights, buyer_logic, markdown_pack, csv_normalized, etc.).
//   - Build a structured strategy_v2 object (story_spine, value_proposition, competitive_strategy,
//     buyer_strategy, gtm_strategy, proof_points, right_to_play).
//   - Write: results/runs/<runId>/strategy_v2/campaign_strategy.json
//   - Update: results/runs/<runId>/status.json.state = "strategy_ready"
// No calls to prompt-harness, no packloader, no LLM – fully deterministic.

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");

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

function computePrefix(msg) {
  // Prefer explicit prefix if present
  let prefix = msg.prefix || msg.pathPrefix || msg.blobPrefix || "";
  if (prefix && typeof prefix === "string") {
    prefix = prefix.trim();
  }

  if (!prefix) {
    const runId =
      msg.runId ||
      msg.run_id ||
      msg.id ||
      msg.fileId ||
      msg.file_id ||
      "unknown";
    prefix = `runs/${String(runId).trim() || "unknown"}/`;
  }

  if (!prefix.endsWith("/")) prefix += "/";
  if (!prefix.startsWith("runs/")) prefix = `runs/${prefix}`;
  return prefix;
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

// Derive simple volume-based outcome (TAM sizing logic)
function deriveOutcomeByTam(rowCount, routeHint) {
  const n = Number.isFinite(Number(rowCount)) ? Number(rowCount) : 0;
  const route = (routeHint || "").toString().toLowerCase();
  const motion =
    route.includes("partner") || route.includes("channel")
      ? "partner-led and co-marketed opportunities"
      : "direct sales-qualified opportunities";

  if (!n || n <= 0) {
    return `Success means creating a small but well-qualified initial wave of ${motion} from the first campaign cycle, proving that the strategy works on live customers.`;
  }

  if (n < 50) {
    return `Success means creating 4–6 well-qualified ${motion} from a named list of about ${n} organisations, with clear evidence that the approach can scale.`;
  }
  if (n < 400) {
    return `Success means creating 6–10 well-qualified ${motion} from a named list of about ${n} organisations, with a repeatable pattern that can be extended to adjacent segments.`;
  }
  return `Success means creating 10–15 well-qualified ${motion} from a named list of about ${n} organisations, with a clear view of how to scale into the wider addressable market.`;
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

  // Optional summarising line (neutral and conditional)
  if (cfaBullets.length) {
    const longCycle =
      "If these issues are not addressed, organisations are likely to experience continuing operational risk, delays and avoidable cost.";
    cfaBullets.push(longCycle);
  }

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

  // next_steps: generic but structured execution steps (sector-agnostic)
  const next_steps = uniqNonEmpty([
    "Lock the target account list, chosen routes to market and campaign objectives with the leadership team.",
    "Align sales, marketing and partners on the positioning, value proof, qualification criteria and follow-up process.",
    "Prepare core enablement assets (narrative, talk-tracks, email/LinkedIn copy and discovery questions) tailored to the target personas.",
    "Start a limited first-wave launch, measure early results and refine the strategy before scaling into the full addressable market."
  ]);

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
    "leaders in the target segment";

  const buyers =
    mergedInput.buyer_type ||
    "operations, IT and commercial leaders accountable for delivery, cost and risk in their segment";

  const topProblem =
    safeGet(buyerLogic, "problems.0.label") ||
    safeGet(insights, "buyer_pressures.0.text") ||
    "a combination of operational risk, technology constraints and commercial pressure that threatens delivery, margins and reputation";

  const capClaim =
    (byTag.supplier_capability || [])[0] ||
    (byTag.right_to_play || [])[0] ||
    (byTag.supplier_overview || [])[0];

  const ourSolutionCore = capClaim
    ? bulletFromClaim(capClaim).replace(/\s*\[[A-Z0-9_:-]+\]\s*$/, "")
    : "an integrated service model that combines the supplier’s evidenced capabilities to address these needs without unnecessary complexity or long-term lock-in";

  const outcomeCore =
    safeGet(buyerLogic, "commercial_impacts.0.label") ||
    "projects and services stay on track, margins are protected and the organisation is better insulated from operational disruption";

  const unlikeCore =
    safeGet(markdownPack, "competitor_profiles.0.summary") ||
    "buyers are not forced to trade between short-term fixes and long implementation cycles with rigid commercial terms";

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
      "Vulnerability mapping is limited because specific risk or blocker signals are thin in the available evidence.",
      "Review and extend the evidence base to identify where the proposition is weakest relative to buyer expectations and competitors."
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

  let routeImplication;
  if (routeLower.includes("partner") || routeLower.includes("channel")) {
    routeImplication =
      "The primary route-to-market is via partners and the wider channel. Campaigns must combine partner enablement, joint value propositions and coordinated outreach into named end-customer accounts.";
  } else if (routeLower.includes("direct") || routeLower.includes("field")) {
    routeImplication =
      "The primary route-to-market is direct. Campaigns must equip sales with clear narratives, qualification criteria and repeatable plays into named target accounts.";
  } else {
    routeImplication =
      "Route-to-market is mixed. The strategy should support both direct teams and partners with consistent messaging, evidence and qualification criteria.";
  }

  const route_implications = [routeImplication];

  const rowCount = safeGet(csvNormalized, "meta.rows", 0);
  const successNarrative = deriveOutcomeByTam(rowCount, routeRaw);

  const success_target = {
    narrative: successNarrative,
    commercial_focus:
      "Focus on qualified opportunities and pipeline value, not just activity volume. Prioritise accounts where the risk, urgency and commercial impact are highest.",
    leading_indicators: [
      "Number of named accounts engaged with campaign assets and discovery conversations.",
      "Volume and quality of opportunities created against the target list.",
      "Conversion from first meeting to qualified opportunity and from opportunity to closed-won."
    ]
  };

  const pipeline_model = {
    tiers: [
      "Tier 1: high-fit, high-pain accounts (top of the CSV) with clear project risk or urgency.",
      "Tier 2: good-fit accounts with moderate urgency where a lower-touch, more programmatic motion is appropriate.",
      "Tier 3: wider market and emerging opportunities that can be nurtured via digital and partner-led campaigns."
    ],
    motions: [
      "Deep, consultative engagement for Tier 1 accounts, combining senior sponsorship, technical scoping and proof.",
      "Programmatic outbound and partner-led plays for Tier 2 accounts, aligned to the core value story.",
      "Always-on digital demand-gen for Tier 3 accounts, capturing interest and feeding the pipeline over time."
    ]
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
  const prefix = computePrefix(msg);
  const runId =
    msg.runId ||
    msg.run_id ||
    (prefix.startsWith("runs/") ? prefix.split("/")[1] : "unknown");

  log(`[*] Strategy Engine starting for runId=${runId}, prefix=${prefix}`);

  const container = await getResultsContainer();

  await updateStatus(
    container,
    prefix,
    "strategy_working",
    "Strategy Engine started"
  );

  try {
    // Load Phase 1 artefacts (missing files are tolerated)
    const [
      evidence,
      evidenceLog,
      insights,
      buyerLogic,
      markdownPack,
      csvNormalized,
      outline,
      baseInput
    ] = await Promise.all([
      readJsonIfExists(container, `${prefix}evidence.json`),
      readJsonIfExists(container, `${prefix}evidence_log.json`),
      readJsonIfExists(container, `${prefix}insights.json`),
      readJsonIfExists(container, `${prefix}buyer_logic.json`),
      readJsonIfExists(container, `${prefix}markdown_pack.json`),
      readJsonIfExists(container, `${prefix}csv_normalized.json`),
      readJsonIfExists(container, `${prefix}outline.json`),
      readJsonIfExists(container, `${prefix}input.json`)
    ]);

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

    const mergedInput = {
      ...(baseInput || {}),
      ...(msg.input || {}),
      ...msg
    };

    // Build strategy_v2 deterministically from inputs
    const strategy_v2 = buildStrategyV2({
      evidence: evidence || { claims: [] },
      insights: insights || {},
      buyerLogic: buyerLogic || {},
      markdownPack: markdownPack || {},
      csvNormalized: csvNormalized || {},
      mergedInput
    });

    const out = { strategy_v2 };

    const strategyPath = `${prefix}strategy_v2/campaign_strategy.json`;
    await writeJson(container, strategyPath, out);

    await updateStatus(
      container,
      prefix,
      "strategy_ready",
      "Strategy Engine completed successfully",
      { strategy_path: strategyPath }
    );

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
      await writeJson(
        container,
        errorPath,
        {
          message: String(err && err.message ? err.message : err),
          stack: err && err.stack ? String(err.stack) : null
        }
      );
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
