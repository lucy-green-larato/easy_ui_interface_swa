// /api/campaign-write/index.js // 30-11-2025 Gold Writer v5 (canonical prefix)
//
// Responsibility:
//   - Read strategy_v2 (campaign_strategy.json) produced by campaign-worker.
//   - Read viability (strategy_v3/viability.json), if present.
//   - Read input.json to recover campaign_requirement and supplier context.
//   - Build a Gold Campaign contract JSON with:
//        * executive_summary (ES-C: title + paragraphs[] + citations[])
//        * value_proposition
//        * go_to_market_plan
//        * messaging_matrix (light, buyer-centric)
//        * sales_enablement
//        * embedded strategy_v2
//        * embedded viability (V-A)
//   - Write: results/<canonicalPrefix>/campaign.json
//   - Update: status.json.state = "writer_working" / "writer_ready" / "writer_error" / "Completed"
//   - No LLM calls, no legacy dependence – pure rendering.
//   - Folders are derived ONLY via canonicalPrefix(userId,page,runId).

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const { canonicalPrefix } = require("../lib/prefix");

const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

// ---------------------- Blob helpers ---------------------- //

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

// ---------------------- Status helpers ---------------------- //

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

// ---------------------- Small helpers ---------------------- //

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

const CLAIM_ID_RE = /\[([A-Z0-9_:-]{4,})\]/g;

function extractClaimIdsFromText(text) {
  const ids = new Set();
  if (!text) return ids;
  const s = String(text);
  let m;
  while ((m = CLAIM_ID_RE.exec(s))) {
    if (m[1]) ids.add(m[1]);
  }
  return ids;
}

function extractClaimIdsFromArray(arr) {
  const ids = new Set();
  (arr || []).forEach((t) => {
    extractClaimIdsFromText(t).forEach((id) => ids.add(id));
  });
  return ids;
}

// Helper to join bullets into a readable sentence, without inventing content
function summariseBullets(bullets, max = 4) {
  const list = uniqNonEmpty(bullets).slice(0, max);
  if (!list.length) return "";
  if (list.length === 1) return list[0];
  if (list.length === 2) return `${list[0]} and ${list[1]}`;
  const head = list.slice(0, list.length - 1).join("; ");
  const last = list[list.length - 1];
  return `${head}; and ${last}`;
}

function normaliseCampaignRequirement(reqRaw) {
  const s = (reqRaw || "").toString().toLowerCase().trim();
  if (!s) return null;
  if (s.includes("upsell")) return "upsell";
  if (s.includes("win-back") || s.includes("win back") || s.includes("winback"))
    return "win-back";
  if (s.includes("growth") || s.includes("new logo") || s.includes("acquisition"))
    return "growth";
  return s; // fall back to free-text
}

function describeCampaignAim(normalised) {
  if (!normalised) return null;
  switch (normalised) {
    case "upsell":
      return "The primary aim of this campaign is to upsell additional value to existing customers.";
    case "win-back":
      return "The primary aim of this campaign is to win back previously lost or inactive customers.";
    case "growth":
      return "The primary aim of this campaign is to acquire new customers and drive net-new growth.";
    default:
      return `The primary aim of this campaign is: ${normalised}.`;
  }
}

function describeRouteModel(routeImplications) {
  const raw = (routeImplications || []).find((s) =>
    String(s || "").startsWith("ROUTE_MODEL=")
  );
  if (!raw) return null;
  const code = String(raw).split("=").pop().trim();
  if (code === "partner") {
    return "The route to market is primarily partner-led, so the campaign must equip and motivate channel partners as well as your own sales team.";
  }
  if (code === "direct") {
    return "The route to market is primarily direct, so the campaign is designed to support direct sales and account teams.";
  }
  if (code === "mixed") {
    return "The route to market is mixed, so the campaign needs to work for both direct and partner motions without diluting the value story.";
  }
  return `The route to market is recorded as: ${code}.`;
}

function describeSuccessTarget(successTarget) {
  if (!successTarget || typeof successTarget !== "object") return null;
  const narrative = successTarget.narrative || "";
  const leading = uniqNonEmpty(successTarget.leading_indicators || []);
  const leadStr = leading.length
    ? `Leading indicators to track include: ${leading.join("; ")}.`
    : "";
  if (!narrative && !leadStr) return null;
  if (narrative && leadStr) return `${narrative} ${leadStr}`;
  return narrative || leadStr;
}

function describeViabilityHeadline(viability) {
  if (!viability || typeof viability !== "object") return null;
  const grade = (viability.grade || "").toString().toLowerCase();
  const mode = viability.mode || viability.viability_mode || null;
  const modeStr = mode ? ` (mode: ${mode})` : "";
  if (!grade) return null;
  if (grade === "green") {
    return `Overall campaign viability is GREEN${modeStr}: the available data supports running this campaign as designed.`;
  }
  if (grade === "amber") {
    return `Overall campaign viability is AMBER${modeStr}: the campaign can proceed, but the flagged risks should be addressed before heavy investment.`;
  }
  if (grade === "red") {
    return `Overall campaign viability is RED${modeStr}: the current targeting or assumptions mean this campaign is unlikely to succeed without significant changes.`;
  }
  return `Overall campaign viability is recorded as: ${viability.grade}${modeStr}.`;
}

function collectViabilityMessages(viability) {
  if (!viability || typeof viability !== "object") return [];
  const msgs = [];

  const pushAll = (arr) => {
    (arr || []).forEach((x) => {
      if (!x) return;
      if (typeof x === "string") msgs.push(x);
      else if (x.message) msgs.push(String(x.message));
      else if (x.description) msgs.push(String(x.description));
    });
  };

  if (Array.isArray(viability.flags)) {
    pushAll(viability.flags);
  } else if (viability.flags && typeof viability.flags === "object") {
    pushAll(viability.flags.red);
    pushAll(viability.flags.amber);
    pushAll(viability.flags.green);
  }

  return uniqNonEmpty(msgs).slice(0, 8);
}

// ---------------------- Contract builders ---------------------- //

function buildExecutiveSummary({ strategy_v2, viability, campaignRequirement }) {
  const spine = strategy_v2?.story_spine || {};
  const buyerStrategy = strategy_v2?.buyer_strategy || {};
  const valueProp = strategy_v2?.value_proposition || {};
  const gtm = strategy_v2?.gtm_strategy || {};

  // Environment
  const envBullets = uniqNonEmpty(spine.environment || []);
  const envText = envBullets.length
    ? `Your buyers are operating in an environment where ${summariseBullets(envBullets, 4)}.`
    : null;

  // Case for action + buyer problems
  const caseBullets = uniqNonEmpty(spine.case_for_action || []);
  const problemBullets = uniqNonEmpty(buyerStrategy.problems || []);
  const cfaText =
    caseBullets.length || problemBullets.length
      ? `They face the following pressures and business problems: ${summariseBullets(
        caseBullets.concat(problemBullets),
        6
      )}.`
      : null;

  // How we win + right to play
  const hwwBullets = uniqNonEmpty(spine.how_we_win || []);
  const rtp = uniqNonEmpty(valueProp.product_fit || strategy_v2.right_to_play || []);
  const winText =
    hwwBullets.length || rtp.length
      ? `This campaign is designed to help you win by ${summariseBullets(
        hwwBullets.concat(rtp),
        6
      )}.`
      : null;

  // Success + TAM / route model
  const successBullets = uniqNonEmpty(spine.success || []);
  const successTargetText = describeSuccessTarget(gtm.success_target);
  const successTextPieces = [];
  if (successBullets.length) {
    successTextPieces.push(
      `Success is defined using the following outcome signals: ${summariseBullets(
        successBullets,
        4
      )}.`
    );
  }
  if (successTargetText) successTextPieces.push(successTargetText);
  const successText = successTextPieces.length
    ? successTextPieces.join(" ")
    : null;

  // Next steps
  const nextStepsBullets = uniqNonEmpty(spine.next_steps || []);
  const nextText = nextStepsBullets.length
    ? `The next steps focus on ${summariseBullets(nextStepsBullets, 4)}.`
    : null;

  // Campaign aim (upsell / win-back / growth)
  const aimNorm = normaliseCampaignRequirement(campaignRequirement);
  const aimText = describeCampaignAim(aimNorm);

  // Route model
  const routeText = describeRouteModel(gtm.route_implications || []);

  // Viability
  const viabilityHeadline = describeViabilityHeadline(viability);
  const viabilityMessages = collectViabilityMessages(viability);
  const viabilityText = viabilityMessages.length
    ? `${viabilityHeadline || "Viability checks have been run on this campaign."} Key points: ${summariseBullets(
      viabilityMessages,
      6
    )}.`
    : viabilityHeadline;

  // Assemble ordered paragraphs for C-suite readability
  const paragraphs = uniqNonEmpty([
    envText,
    cfaText,
    aimText,
    winText,
    routeText,
    successText,
    nextText,
    viabilityText
  ]);

  // Citations from any claim IDs in the story spine + buyer strategy + value_prop
  const claimIdSets = [
    extractClaimIdsFromArray(spine.environment),
    extractClaimIdsFromArray(spine.case_for_action),
    extractClaimIdsFromArray(spine.how_we_win),
    extractClaimIdsFromArray(spine.success),
    extractClaimIdsFromArray(buyerStrategy.problems),
    extractClaimIdsFromArray(valueProp.business_value),
    extractClaimIdsFromArray(valueProp.persona_value)
  ];
  const citations = uniqNonEmpty(
    claimIdSets.reduce((all, set) => all.concat(Array.from(set)), [])
  );

  return {
    title: "Executive summary",
    paragraphs,
    citations
  };
}

function buildValueProposition({ strategy_v2 }) {
  const vp = strategy_v2?.value_proposition || {};

  // Narrative: light stitching of business + persona value
  const business = uniqNonEmpty(vp.business_value || []);
  const persona = uniqNonEmpty(vp.persona_value || []);
  const pillars = uniqNonEmpty(vp.pillar_outcomes || []);
  const productFit = uniqNonEmpty(vp.product_fit || []);

  const narrativePieces = [];

  if (business.length) {
    narrativePieces.push(
      `Commercially, the proposition focuses on delivering: ${summariseBullets(
        business,
        6
      )}.`
    );
  }

  if (persona.length) {
    narrativePieces.push(
      `For individual decision-makers, it addresses personal and professional drivers such as: ${summariseBullets(
        persona,
        6
      )}.`
    );
  }

  if (pillars.length) {
    narrativePieces.push(
      `The value story is organised around the following outcome pillars: ${summariseBullets(
        pillars,
        6
      )}.`
    );
  }

  if (productFit.length) {
    narrativePieces.push(
      `Your product and services are positioned to deliver this value because: ${summariseBullets(
        productFit,
        6
      )}.`
    );
  }

  const narrative = narrativePieces.join(" ");

  return {
    narrative: narrative || null,
    moore: vp.moore_chain || null,
    competitive_position: null, // can be extended later if you wish
    proof_points: uniqNonEmpty(strategy_v2.proof_points || [])
  };
}

function buildGoToMarketPlan({ strategy_v2 }) {
  const gtm = strategy_v2?.gtm_strategy || {};
  const buyerStrategy = strategy_v2?.buyer_strategy || {};

  const objective =
    "Run a targeted, evidence-led campaign that converts the defined addressable cohort into qualified opportunities and revenue, aligned to the stated campaign aim.";
  const targetMarket = {
    problems: uniqNonEmpty(buyerStrategy.problems || []),
    barriers: uniqNonEmpty(buyerStrategy.barriers || []),
    urgency: uniqNonEmpty(buyerStrategy.urgency || []),
    decision_drivers: uniqNonEmpty(buyerStrategy.decision_drivers || [])
  };

  const marketingActions = {
    route_implications: uniqNonEmpty(gtm.route_implications || []),
    motions: uniqNonEmpty(gtm.pipeline_model?.motions || []),
    notes:
      "Channel, content, and cadence should be chosen to align with the recorded buyer problems, barriers, and urgency signals."
  };

  const salesActions = {
    focus:
      "Use the buyer problems, decision drivers, and proof points as the basis of all sales conversations.",
    guidance: [
      "Lead with the business problem and outcomes, not the product features.",
      "Use the discovery questions and buyer outcomes from the sales enablement pack to qualify and prioritise.",
      "Align proposals explicitly to decision drivers and commercial impacts captured in the strategy."
    ]
  };

  const pipelineModel = gtm.pipeline_model || {
    tiers: ["PIPELINE_TIER_MODEL=3", "PIPELINE_TIER_CRITERIA=urgency_and_fit"],
    motions: []
  };

  const cta =
    "Define a single, clear primary call to action for this campaign (for example: 'book a 30-minute diagnostic' or 'request a benchmark review') and keep it consistent across all channels.";

  const citations = []; // can be extended if needed later

  return {
    objective,
    target_market: targetMarket,
    marketing_actions: marketingActions,
    sales_actions: salesActions,
    pipeline_model: pipelineModel,
    cta,
    citations
  };
}

function buildMessagingMatrix({ strategy_v2 }) {
  const buyerStrategy = strategy_v2?.buyer_strategy || {};
  const vp = strategy_v2?.value_proposition || {};

  const audiences = uniqNonEmpty([
    "Economic buyer",
    "Operational / delivery owner",
    "Technical / risk stakeholder"
  ]);

  const pillars = uniqNonEmpty(vp.pillar_outcomes || vp.business_value || []);

  const supportPoints = uniqNonEmpty(
    (vp.product_fit || []).concat(strategy_v2.proof_points || [])
  );

  // Citations from value_proposition + buyer_strategy
  const claimIdSets = [
    extractClaimIdsFromArray(vp.business_value),
    extractClaimIdsFromArray(vp.persona_value),
    extractClaimIdsFromArray(buyerStrategy.problems),
    extractClaimIdsFromArray(buyerStrategy.decision_drivers)
  ];
  const citations = uniqNonEmpty(
    claimIdSets.reduce((all, set) => all.concat(Array.from(set)), [])
  );

  return {
    audiences,
    pillars,
    support_points: supportPoints,
    citations
  };
}

function buildSalesEnablement({ strategy_v2, campaignRequirement }) {
  const buyerStrategy = strategy_v2?.buyer_strategy || {};
  const vp = strategy_v2?.value_proposition || {};
  const spine = strategy_v2?.story_spine || {};

  const aimNorm = normaliseCampaignRequirement(campaignRequirement);
  const aimText = describeCampaignAim(aimNorm);

  const campaign_overview = [
    aimText,
    spine.environment && spine.environment.length
      ? `Environment: ${summariseBullets(spine.environment, 4)}.`
      : null,
    spine.case_for_action && spine.case_for_action.length
      ? `Case for action: ${summariseBullets(spine.case_for_action, 6)}.`
      : null,
    spine.how_we_win && spine.how_we_win.length
      ? `How we win: ${summariseBullets(spine.how_we_win, 6)}.`
      : null
  ]
    .filter(Boolean)
    .join(" ");

  const buyer_outcomes = uniqNonEmpty(
    (vp.business_value || []).concat(vp.persona_value || [])
  );

  const discovery_questions = uniqNonEmpty([
    "What is currently making this problem a priority for you now?",
    "How are you addressing this today, and what is not working as well as you need?",
    "Who else is involved in making this decision and what do they care about most?",
    "If we were successful together, what would good look like in 12–18 months?"
  ]);

  const master_pitch = [
    "This campaign is designed to start a business conversation, not a product demo.",
    "Lead with the specific problems and decision drivers captured in the buyer strategy, then link back to the value proposition and proof points.",
    "Use the discovery questions to quantify the impact and qualify fit before moving to solution detail.",
    "Always close with an agreed next step that aligns to the campaign call to action."
  ].join(" ");

  return {
    campaign_overview: campaign_overview || null,
    buyer_outcomes,
    discovery_questions,
    master_pitch
  };
}

// ---------------------- Main Azure Function ---------------------- //

module.exports = async function (context, queueItem) {
  const log = context.log;

  // Normalise queueItem (string → object)
  let msg = queueItem;
  if (typeof msg === "string") {
    try {
      msg = JSON.parse(msg);
    } catch {
      msg = { raw: msg };
    }
  }

  const runId =
    msg.runId ||
    msg.run_id ||
    msg.id ||
    msg.fileId ||
    msg.file_id ||
    "unknown";

  const userId =
    msg.userId ||
    msg.user ||
    "anonymous";

  const page = msg.page || "campaign";

  const prefix = canonicalPrefix({ userId, page, runId });

  log(`[*] Campaign Writer starting for runId=${runId}, userId=${userId}, page=${page}`);
  log(`[*] Using canonical prefix: ${prefix}`);

  const svc = getBlobServiceClient();
  const container = svc.getContainerClient(RESULTS_CONTAINER);
  await container.createIfNotExists();

  await updateStatus(
    container,
    prefix,
    "writer_working",
    "Campaign Writer started"
  );

  try {
    // -------- Load inputs -------- //
    const strategyWrap = await readJsonIfExists(
      container,
      `${prefix}strategy_v2/campaign_strategy.json`
    );
    const strategy_v2 =
      strategyWrap && strategyWrap.strategy_v2
        ? strategyWrap.strategy_v2
        : strategyWrap || null;

    if (!strategy_v2 || typeof strategy_v2 !== "object") {
      throw new Error(
        "strategy_v2/campaign_strategy.json missing or invalid (strategy_v2 object not found)"
      );
    }

    const viability = await readJsonIfExists(
      container,
      `${prefix}strategy_v3/viability.json`
    );

    const baseInput = await readJsonIfExists(container, `${prefix}input.json`);

    const campaignRequirement =
      msg.campaign_requirement ||
      (baseInput && baseInput.campaign_requirement) ||
      null;

    // -------- Build contract -------- //
    const executive_summary = buildExecutiveSummary({
      strategy_v2,
      viability,
      campaignRequirement
    });

    const value_proposition = buildValueProposition({ strategy_v2 });

    const go_to_market_plan = buildGoToMarketPlan({ strategy_v2 });

    const messaging_matrix = buildMessagingMatrix({ strategy_v2 });

    const sales_enablement = buildSalesEnablement({
      strategy_v2,
      campaignRequirement
    });

    const contract = {
      schema: "campaign-gold-v2",
      version: "2025-11-27",

      // IDs
      run_id: runId,
      user_id: userId,
      page,

      // Canonical location for this run (UI must use this)
      prefix,
      source_prefix: prefix,

      generated_at: new Date().toISOString(),

      // Embedded engines
      strategy_v2,
      viability: viability || null,

      // Gold sections (ES-C + supporting)
      executive_summary,
      value_proposition,
      go_to_market_plan,
      messaging_matrix,
      sales_enablement
      // Proof points are already inside strategy_v2.proof_points;
      // UI proof-points tab reads strategy_v2.proof_points directly.
    };

    const outPath = `${prefix}campaign.json`;
    await writeJson(container, outPath, contract);

    await updateStatus(
      container,
      prefix,
      "writer_ready",
      "Campaign Writer completed successfully"
    );

    await updateStatus(
      container,
      prefix,
      "Completed",
      "Campaign successfully assembled",
      {
        completed: true,
        runId,
        userId,
        page,
        prefix,
        campaign_path: outPath
      }
    );

    log("[*] Campaign Writer completed", {
      runId,
      campaignPath: outPath
    });
  } catch (err) {
    log.error("[!] Campaign Writer failed", {
      runId,
      prefix,
      error: String(err && err.message ? err.message : err)
    });

    try {
      const errorPath = `${prefix}writer_error.json`;
      await writeJson(container, errorPath, {
        message: String(err && err.message ? err.message : err),
        stack: err && err.stack ? String(err.stack) : null
      });
    } catch (e2) {
      log.error("[!] Failed to write writer_error.json", String(e2));
    }

    await updateStatus(
      container,
      prefix,
      "writer_error",
      "Campaign Writer failed",
      { error: String(err && err.message ? err.message : err) }
    );

    throw err;
  }
};
