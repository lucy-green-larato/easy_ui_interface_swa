// /api/campaign-write/index.js
// 02-01-2026 — Gold Writer v9.3 (Schema-correct, content-pillars-v2, hash-locked, deterministic)
//
// Responsibility:
//   - Read strategy_v2/campaign_strategy.json produced by campaign-worker.
//   - Read outline.json for channel themes + proof enforcement.
//   - Read input.json for campaign_requirement and supplier context.
//   - Read content_pillars.json (MUST be content-pillars-v2) and enforce claim discipline.
//   - Read evidence.json (audit-only; may evolve) and keep hashes for traceability.
//   - Build Gold Campaign contract JSON that conforms to api/schemas/campaign-gold.schema.json
//   - Update status.json.state = "writer_working" / "writer_error" / "completed"
//   - No LLM calls – pure deterministic rendering.
//
// IMPORTANT:
// - Root object must conform to campaign_gold_v2 schema (additionalProperties: false).
// - Do not add any extra top-level keys beyond schema-defined properties.
// - Include _meta (schema-defined) for traceability.
//
// Notes on volatile evidence:
// - evidence.json may evolve after content pillars (competitor enrichment, etc).
// - Writer does NOT refuse on evidence hash mismatch vs pillar-declared evidence sha.
// - Writer enforces claim-id discipline using allowedClaimIds derived from pillars + outline.
// - Writer still records evidence SHA for audit.

"use strict";

const { BlobServiceClient } = require("@azure/storage-blob");
const nodeCrypto = require("crypto");

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
  } catch {
    return null;
  }
}

async function writeJson(container, blobPath, obj) {
  const block = container.getBlockBlobClient(blobPath);
  const json = JSON.stringify(obj, null, 2);
  const data = Buffer.from(json, "utf8");
  await block.uploadData(data, {
    blobHTTPHeaders: { blobContentType: "application/json" }
  });
}

// ---------------------- Status helpers ---------------------- //

async function updateStatus(container, prefix, state, note, extra = {}) {
  const statusPath = `${prefix}status.json`;

  const cur =
    (await readJsonIfExists(container, statusPath)) || {
      state: "pending",
      history: [],
      markers: {}
    };

  const entry = {
    at: new Date().toISOString(),
    state,
    note,
    ...extra
  };

  const next = {
    ...cur,
    state,
    history: Array.isArray(cur.history) ? [...cur.history, entry] : [entry],
    markers: {
      ...(cur.markers || {}),
      ...(extra.markers || {})
    }
  };

  await writeJson(container, statusPath, next);
}

// ---------------------- Deterministic hash helpers ---------------------- //

function stableStringify(obj) {
  if (obj === null || typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return "[" + obj.map(stableStringify).join(",") + "]";
  const keys = Object.keys(obj).sort();
  return (
    "{" +
    keys
      .map((k) => JSON.stringify(k) + ":" + stableStringify(obj[k]))
      .join(",") +
    "}"
  );
}

function sha1(s) {
  return nodeCrypto.createHash("sha1").update(String(s)).digest("hex");
}

function sha1OfJson(obj) {
  return sha1(stableStringify(obj ?? null));
}

// ---------------------- Small helpers ---------------------- //

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

// ---------------------- Claim ID extraction (STRICT) ---------------------- //
// Doctrine: citations must be explicit claim ids in brackets: [CLAIM_ID]
// We DO NOT allow bare tokens (BYOD, SD-WAN, ROUTE_MODEL etc.) to be treated as claim IDs.

const CLAIM_ID_RE = /\[(CLM_[A-Z0-9_-]{3,})\]/g;

function extractClaimIdsFromText(text) {
  const ids = new Set();
  if (!text) return ids;

  const s = String(text);
  let match;

  while ((match = CLAIM_ID_RE.exec(s))) {
    if (match[1]) ids.add(match[1]);
  }

  return ids;
}

function isValidClaimId(id) {
  return /^CLM_[A-Z0-9_-]{3,}$/.test(String(id || "").trim());
}

function extractClaimIdsFromArray(arr) {
  const ids = new Set();
  (arr || []).forEach((t) => {
    extractClaimIdsFromText(t).forEach((id) => ids.add(id));
  });
  return ids;
}


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
  if (s.includes("nurture")) return "nurture";
  return s;
}

function describeCampaignAimSentence(normalised) {
  if (!normalised) return null;
  switch (normalised) {
    case "upsell":
      return "The primary aim of this campaign is to upsell additional value to existing customers.";
    case "win-back":
      return "The primary aim of this campaign is to win back previously lost or inactive customers.";
    case "growth":
      return "The primary aim of this campaign is to acquire new customers and drive net-new growth.";
    case "nurture":
      return "The primary aim of this campaign is to nurture and warm up future demand without immediate conversion pressure.";
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

// ---- Top-level field helpers ----

function deriveSupplier(baseInput, strategy_v2) {
  const s =
    baseInput?.supplier_company ||
    baseInput?.company_name ||
    baseInput?.prospect_company ||
    strategy_v2?.supplier?.name ||
    "";
  return String(s).trim() || "Unknown supplier";
}

function deriveIndustry(baseInput, strategy_v2) {
  const fromInput =
    baseInput?.selected_industry ||
    baseInput?.campaign_industry ||
    baseInput?.company_industry ||
    "";
  if (fromInput && String(fromInput).trim()) {
    return String(fromInput).trim();
  }
  const fromStrategy = strategy_v2?.buyer_strategy?.industry || "";
  return String(fromStrategy || "General").trim();
}

function derivePersona(baseInput, strategy_v2) {
  const salesModel = String(
    baseInput?.sales_model ||
    baseInput?.salesModel ||
    baseInput?.call_type ||
    strategy_v2?.gtm_strategy?.route_model ||
    ""
  )
    .trim()
    .toLowerCase();

  if (salesModel.includes("partner")) return "Economic buyer in partner channel";
  if (salesModel.includes("direct")) return "Economic buyer (direct)";
  if (salesModel.includes("mixed"))
    return "Economic buyer across direct and partner routes";
  if (!salesModel) return "Economic buyer";
  return `Economic buyer (${salesModel})`;
}

// ---------------------- content-pillars-v2 validation + allowed IDs ---------------------- //

function validateContentPillarsV2(cp) {
  if (!cp || typeof cp !== "object") return { ok: false, reason: "not_object" };
  if (String(cp.schema || "") !== "content-pillars-v2")
    return { ok: false, reason: "wrong_schema" };
  if (!cp.meta || typeof cp.meta !== "object")
    return { ok: false, reason: "missing_meta" };
  if (cp.meta.provenance_validated !== true)
    return { ok: false, reason: "provenance_not_validated" };
  if (!Array.isArray(cp.core_pillars))
    return { ok: false, reason: "missing_core_pillars" };
  if (!Array.isArray(cp.proof_enrichment))
    return { ok: false, reason: "missing_proof_enrichment" };
  return { ok: true };
}

function allowedClaimIdsFromContentPillars(cp) {
  const set = new Set();
  for (const pe of Array.isArray(cp?.proof_enrichment) ? cp.proof_enrichment : []) {
    for (const cr of Array.isArray(pe?.claim_refs) ? pe.claim_refs : []) {
      const id = String(cr?.claim_id || "").trim();
      if (id && isValidClaimId(id)) set.add(id);
    }
    for (const id of Array.isArray(pe?.claim_ids) ? pe.claim_ids : []) {
      const s = String(id || "").trim();
      if (s && isValidClaimId(s)) set.add(s);
    }
  }
  return set;
}

function allowedClaimIdsFromOutline(outline) {
  const set = new Set();
  if (!outline || typeof outline !== "object") return set;
  const sec = outline.sections || {};
  const push = (id) => {
    const s = String(id || "").trim();
    if (s && isValidClaimId(s)) set.add(s);
  };

  (Array.isArray(sec?.exec?.why_now_ids) ? sec.exec.why_now_ids : []).forEach(push);
  (Array.isArray(sec?.positioning?.differentiator_ids)
    ? sec.positioning.differentiator_ids
    : []
  ).forEach(push);
  (Array.isArray(sec?.risks?.claim_ids) ? sec.risks.claim_ids : []).forEach(push);
  (Array.isArray(sec?.compliance?.checklist_ids)
    ? sec.compliance.checklist_ids
    : []
  ).forEach(push);

  (Array.isArray(sec?.messaging) ? sec.messaging : []).forEach((m) => {
    (Array.isArray(m?.claim_ids) ? m.claim_ids : []).forEach(push);
  });

  (Array.isArray(sec?.channel?.email_themes) ? sec.channel.email_themes : []).forEach(
    (t) => {
      (Array.isArray(t?.claim_ids) ? t.claim_ids : []).forEach(push);
    }
  );

  (Array.isArray(sec?.channel?.linkedin_themes)
    ? sec.channel.linkedin_themes
    : []
  ).forEach((t) => {
    (Array.isArray(t?.claim_ids) ? t.claim_ids : []).forEach(push);
  });

  (Array.isArray(sec?.offer?.proof_ids) ? sec.offer.proof_ids : []).forEach(push);
  (Array.isArray(sec?.offer?.outcome_ids) ? sec.offer.outcome_ids : []).forEach(push);

  return set;
}

// ---------------------- Guardrail enforcement (numeric needs citations) ---------------------- //

function hasNumericOrCurrency(s) {
  const t = String(s || "");
  return /(\d|%|£|\$|€)/.test(t);
}

function redactNumericIfNoClaims(text) {
  return String(text || "")
    .replace(/[0-9]/g, "X")
    .replace(/[%£$€]/g, "X");
}

function enforceNumericClaimRequirementOnSectionStrings(sectionObj, citationsSet, notesOut) {
  const walk = (obj, path = "") => {
    if (typeof obj === "string") {
      if (hasNumericOrCurrency(obj) && citationsSet.size === 0) {
        notesOut.push(`numeric_redacted_no_citations:${path || "string"}`);
        return redactNumericIfNoClaims(obj);
      }
      return obj;
    }
    if (Array.isArray(obj)) {
      return obj.map((x, i) => walk(x, `${path}[${i}]`));
    }
    if (obj && typeof obj === "object") {
      const out = {};
      for (const k of Object.keys(obj)) {
        out[k] = walk(obj[k], path ? `${path}.${k}` : k);
      }
      return out;
    }
    return obj;
  };
  return walk(sectionObj);
}

// ---------------------- Viability helpers ---------------------- //

function mapViabilityGrade(viability) {
  if (!viability || typeof viability !== "object") return null;

  // Worker v26 emits viability.verdict.viable + viability.verdict.confidence
  if (viability.verdict && typeof viability.verdict === "object") {
    const viable = !!viability.verdict.viable;
    if (viable) return "viable";
    const conf = String(viability.verdict.confidence || "").toLowerCase();
    if (conf === "low") return "weak";
    return "borderline";
  }

  const raw = String(viability.grade || "").toLowerCase().trim();
  if (!raw) return null;

  if (raw === "green") return "viable";
  if (raw === "amber" || raw === "yellow") return "borderline";
  if (raw === "red") return "weak";

  if (["viable", "borderline", "weak"].includes(raw)) return raw;
  return "borderline";
}

function collectViabilityMessages(viability) {
  if (!viability || typeof viability !== "object") return [];
  const msgs = [];

  if (viability.verdict && Array.isArray(viability.verdict.constraints)) {
    viability.verdict.constraints.forEach((c) => {
      if (c) msgs.push(String(c));
    });
  }

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

  return uniqNonEmpty(msgs).slice(0, 12);
}

function extractViabilityReasonIds(viability) {
  const out = new Set();
  const add = (v) => {
    if (!v) return;
    const s = String(v).trim();
    if (!s) return;
    out.add(s.slice(0, 80));
  };

  if (viability?.verdict && Array.isArray(viability.verdict.constraints)) {
    viability.verdict.constraints.forEach(add);
  }

  const flags = viability.flags;
  if (Array.isArray(flags)) {
    for (const f of flags) {
      if (f && typeof f === "object") {
        add(f.code || f.id || f.tag || f.key || f.message || f.description);
      } else {
        add(f);
      }
    }
  } else if (flags && typeof flags === "object") {
    for (const bucket of ["red", "amber", "green"]) {
      const arr = Array.isArray(flags[bucket]) ? flags[bucket] : [];
      for (const f of arr) {
        if (f && typeof f === "object") {
          add(f.code || f.id || f.tag || f.key || f.message || f.description);
        } else {
          add(f);
        }
      }
    }
  }

  return Array.from(out).slice(0, 12);
}

function buildGoldViability(viabilityRaw) {
  // If already looks like gold viability (grade + dimensions), pass-through
  if (
    viabilityRaw &&
    typeof viabilityRaw === "object" &&
    viabilityRaw.grade &&
    viabilityRaw.dimensions
  ) {
    const mappedGrade = mapViabilityGrade(viabilityRaw);
    return { ...viabilityRaw, grade: mappedGrade || "borderline" };
  }

  const grade = mapViabilityGrade(viabilityRaw) || "borderline";
  const msgs = collectViabilityMessages(viabilityRaw);

  const mkDim = (label) => ({
    score: 0,
    warning_code: "",
    message:
      msgs.find((m) => String(m || "").toLowerCase().includes(label)) ||
      msgs[0] ||
      ""
  });

  return {
    grade,
    dimensions: {
      TAM: {
        value: 0,
        warning_code: "",
        message: msgs.find((m) => m.toLowerCase().includes("tam")) || msgs[0] || ""
      },
      problem_strength: mkDim("problem"),
      differentiation: mkDim("pillar"),
      urgency: mkDim("urgent")
    }
  };
}

// ---------------------- Gold section builders ---------------------- //

function buildExecutiveSummarySection({ strategy_v2, outline, viability, campaignRequirement }) {
  const sv2 = (strategy_v2 && typeof strategy_v2 === "object") ? strategy_v2 : {};
  const spine = (sv2.story_spine && typeof sv2.story_spine === "object") ? sv2.story_spine : {};
  const buyerStrategy =
    (sv2.buyer_strategy && typeof sv2.buyer_strategy === "object")
      ? sv2.buyer_strategy
      : {};

  const outlineSections =
    (outline && typeof outline === "object" && outline.sections && typeof outline.sections === "object")
      ? outline.sections
      : {};
  const outlineExec =
    (outlineSections.exec && typeof outlineSections.exec === "object")
      ? outlineSections.exec
      : {};

  const envBullets = uniqNonEmpty(Array.isArray(spine.environment) ? spine.environment : []);
  const envSentence = envBullets.length
    ? `Your buyers are operating in an environment where ${summariseBullets(envBullets, 4)}.`
    : null;

  const caseBullets = uniqNonEmpty(Array.isArray(spine.case_for_action) ? spine.case_for_action : []);
  const problemBullets = uniqNonEmpty(Array.isArray(buyerStrategy.problems) ? buyerStrategy.problems : []);

  const problemSentence =
    (caseBullets.length || problemBullets.length)
      ? `They face the following pressures and business problems: ${summariseBullets(
        uniqNonEmpty(caseBullets.concat(problemBullets)),
        6
      )}.`
      : null;

  const hwwBullets = uniqNonEmpty(Array.isArray(spine.how_we_win) ? spine.how_we_win : []);

  const productAnchors = uniqNonEmpty(
    Array.isArray(outlineExec.product_anchor_names) ? outlineExec.product_anchor_names : []
  );

  const howWeWinSentence =
    (hwwBullets.length || productAnchors.length)
      ? `This campaign is designed to help you win by ${summariseBullets(
        uniqNonEmpty(hwwBullets.concat(productAnchors)),
        6
      )}.`
      : null;

  const aimNorm = normaliseCampaignRequirement(campaignRequirement);
  const campaignAimEnum =
    ["upsell", "win-back", "growth", "nurture"].includes(aimNorm)
      ? aimNorm
      : "growth";

  const proofPoints = uniqNonEmpty(Array.isArray(sv2.proof_points) ? sv2.proof_points : []);
  const evidenceSentence =
    proofPoints.length
      ? `The evidence base includes proof points such as: ${summariseBullets(proofPoints, 6)}.`
      : null;

  const viabilityGrade = mapViabilityGrade(viability) || "borderline";
  const viabilityMsgs = collectViabilityMessages(viability);
  const viabilityReasonsIds = extractViabilityReasonIds(viability);
  const keyWarnings = uniqNonEmpty(viabilityMsgs);

  const claimIdSets = [
    extractClaimIdsFromArray(spine.environment),
    extractClaimIdsFromArray(spine.case_for_action),
    extractClaimIdsFromArray(spine.how_we_win),
    extractClaimIdsFromArray(spine.success),
    extractClaimIdsFromArray(buyerStrategy.problems),
    extractClaimIdsFromArray(proofPoints),
    new Set(Array.isArray(outlineExec.why_now_ids) ? outlineExec.why_now_ids : [])
  ];

  const citations = uniqNonEmpty(
    claimIdSets.reduce((all, set) => all.concat(Array.from(set || [])), [])
  );

  return {
    problem:
      problemSentence ||
      envSentence ||
      "The campaign addresses specific, evidence-based pressures and problems in your market.",
    why_this_playing_field:
      envSentence ||
      "This campaign is anchored on the current environment and playing field for your buyers.",
    how_we_win:
      howWeWinSentence ||
      "The campaign sets out a clear, outcome-led way for you to win relevant opportunities.",
    campaign_aim: campaignAimEnum,
    evidence_case:
      evidenceSentence ||
      "The campaign is grounded in the combined evidence across buyers, market context, and your proof points.",
    key_warnings: keyWarnings,
    citations,
    viability_grade: viabilityGrade,
    viability_reasons_ids: viabilityReasonsIds
  };
}

function buildGoToMarketSection({ strategy_v2, outline, viability }) {
  const sv2 = (strategy_v2 && typeof strategy_v2 === "object") ? strategy_v2 : {};
  const gtm = (sv2.gtm_strategy && typeof sv2.gtm_strategy === "object") ? sv2.gtm_strategy : {};
  const buyerStrategy =
    (sv2.buyer_strategy && typeof sv2.buyer_strategy === "object")
      ? sv2.buyer_strategy
      : {};

  const outlineSections =
    (outline && typeof outline === "object" && outline.sections && typeof outline.sections === "object")
      ? outline.sections
      : {};
  const outlineChannel =
    (outlineSections.channel && typeof outlineSections.channel === "object")
      ? outlineSections.channel
      : {};
  const outlineRisks =
    (outlineSections.risks && typeof outlineSections.risks === "object")
      ? outlineSections.risks
      : {};
  const outlineCompliance =
    (outlineSections.compliance && typeof outlineSections.compliance === "object")
      ? outlineSections.compliance
      : {};

  const objective =
    "Run a targeted, evidence-led campaign that converts the defined addressable cohort into qualified opportunities and revenue.";

  const problems = uniqNonEmpty(Array.isArray(buyerStrategy.problems) ? buyerStrategy.problems : []);
  const barriers = uniqNonEmpty(Array.isArray(buyerStrategy.barriers) ? buyerStrategy.barriers : []);
  const urgency = uniqNonEmpty(Array.isArray(buyerStrategy.urgency) ? buyerStrategy.urgency : []);
  const decisionDrivers = uniqNonEmpty(
    Array.isArray(buyerStrategy.decision_drivers) ? buyerStrategy.decision_drivers : []
  );

  const tmParts = [];
  if (problems.length) tmParts.push(`Key buyer problems include: ${summariseBullets(problems, 4)}.`);
  if (barriers.length)
    tmParts.push(`Common barriers and friction points are: ${summariseBullets(barriers, 4)}.`);
  if (urgency.length) tmParts.push(`Urgency is driven by: ${summariseBullets(urgency, 4)}.`);
  if (decisionDrivers.length)
    tmParts.push(`Top decision drivers are: ${summariseBullets(decisionDrivers, 4)}.`);

  const target_market =
    tmParts.join(" ") ||
    "The target market is defined using the recorded buyer problems, barriers, urgency, and decision drivers.";

  const routeImplications = uniqNonEmpty(Array.isArray(gtm.route_implications) ? gtm.route_implications : []);
  const motions = uniqNonEmpty(Array.isArray(gtm.pipeline_model?.motions) ? gtm.pipeline_model.motions : []);

  const emailThemes =
    Array.isArray(outlineChannel.email_themes)
      ? outlineChannel.email_themes
        .map((t) => (t && typeof t === "object" ? t.theme : null))
        .filter(Boolean)
      : [];

  const linkedinThemes =
    Array.isArray(outlineChannel.linkedin_themes)
      ? outlineChannel.linkedin_themes
        .map((t) => (t && typeof t === "object" ? t.theme : null))
        .filter(Boolean)
      : [];

  const marketing_actions = [];

  if (routeImplications.length) {
    marketing_actions.push(`Route-to-market implications: ${summariseBullets(routeImplications, 4)}.`);
  }
  if (motions.length) {
    marketing_actions.push(`Recommended motions: ${summariseBullets(motions, 6)}.`);
  }
  if (emailThemes.length) {
    marketing_actions.push(`Email campaigns should focus on themes such as: ${summariseBullets(emailThemes, 4)}.`);
  }
  if (linkedinThemes.length) {
    marketing_actions.push(
      `LinkedIn activity should explore themes such as: ${summariseBullets(linkedinThemes, 4)}.`
    );
  }

  marketing_actions.push(
    "Align channels, content, and cadence to the specific buyer problems, barriers, and urgency recorded in the strategy."
  );

  const sales_actions = [
    "Lead with the business problem and outcomes, not the product features.",
    "Use the discovery questions and buyer outcomes from the sales enablement pack to qualify and prioritise.",
    "Align proposals explicitly to decision drivers and commercial impacts captured in the strategy.",
    "Use proof points and evidence log claims to back up key statements with real data."
  ];

  const pipeline_model =
    (gtm.pipeline_model && typeof gtm.pipeline_model === "object")
      ? gtm.pipeline_model
      : {
        tiers: ["PIPELINE_TIER_MODEL=3", "PIPELINE_TIER_CRITERIA=urgency_and_fit"],
        motions: []
      };

  const viabilityNotes = collectViabilityMessages(viability);
  const viabilityReasonsIds = extractViabilityReasonIds(viability);

  const routeText = describeRouteModel(Array.isArray(gtm.route_implications) ? gtm.route_implications : []);
  const successText = describeSuccessTarget(gtm.success_target);

  const riskIds = Array.isArray(outlineRisks.claim_ids) ? outlineRisks.claim_ids : [];
  const complianceIds = Array.isArray(outlineCompliance.checklist_ids) ? outlineCompliance.checklist_ids : [];

  const viabilityHeadlineParts = [];
  if (routeText) viabilityHeadlineParts.push(routeText);
  if (successText) viabilityHeadlineParts.push(successText);
  if (riskIds.length) {
    viabilityHeadlineParts.push(
      "Key execution risks are recorded and must be actively managed using the risk checklist tied to the evidence log."
    );
  }
  if (complianceIds.length) {
    viabilityHeadlineParts.push(
      "Compliance considerations are captured and should be reviewed as part of every campaign launch."
    );
  }

  const viability_notes = viabilityNotes.length
    ? viabilityNotes
    : viabilityHeadlineParts.length
      ? [viabilityHeadlineParts.join(" ")]
      : [];

  const channelClaimIds = [];

  if (Array.isArray(outlineChannel.email_themes)) {
    outlineChannel.email_themes.forEach((t) => {
      if (t && Array.isArray(t.claim_ids)) {
        t.claim_ids.forEach((id) => channelClaimIds.push(id));
      }
    });
  }

  if (Array.isArray(outlineChannel.linkedin_themes)) {
    outlineChannel.linkedin_themes.forEach((t) => {
      if (t && Array.isArray(t.claim_ids)) {
        t.claim_ids.forEach((id) => channelClaimIds.push(id));
      }
    });
  }

  const citations = uniqNonEmpty([].concat(channelClaimIds).concat(riskIds).concat(complianceIds));

  return {
    objective,
    target_market,
    marketing_actions,
    sales_actions,
    pipeline_model,
    viability_notes,
    citations,
    viability_reasons_ids: viabilityReasonsIds
  };
}

function buildOfferingSection({ strategy_v2, outline }) {
  const sv2 = (strategy_v2 && typeof strategy_v2 === "object") ? strategy_v2 : {};
  const vp =
    (sv2.value_proposition && typeof sv2.value_proposition === "object")
      ? sv2.value_proposition
      : {};

  const outlineSections =
    (outline && typeof outline === "object" && outline.sections && typeof outline.sections === "object")
      ? outline.sections
      : {};
  const outlineOffer =
    (outlineSections.offer && typeof outlineSections.offer === "object")
      ? outlineSections.offer
      : {};

  const business_value = uniqNonEmpty(Array.isArray(vp.business_value) ? vp.business_value : []);
  const human_value = uniqNonEmpty(Array.isArray(vp.persona_value) ? vp.persona_value : []);

  const vpElements = Array.isArray(vp.pillar_outcomes)
    ? vp.pillar_outcomes
    : Array.isArray(vp.solution_elements)
      ? vp.solution_elements
      : [];

  const csvElements = Array.isArray(outlineOffer.what_you_get_from_csv)
    ? outlineOffer.what_you_get_from_csv
    : [];

  const solution_elements = uniqNonEmpty([].concat(vpElements).concat(csvElements));

  const fit_to_needs = uniqNonEmpty(
    []
      .concat(Array.isArray(vp.product_fit) ? vp.product_fit : [])
      .concat(Array.isArray(vp.fit_to_needs) ? vp.fit_to_needs : [])
  );

  const claimSets = [
    extractClaimIdsFromArray(business_value),
    extractClaimIdsFromArray(human_value),
    extractClaimIdsFromArray(solution_elements),
    extractClaimIdsFromArray(fit_to_needs),
    new Set(
      []
        .concat(Array.isArray(outlineOffer.proof_ids) ? outlineOffer.proof_ids : [])
        .concat(Array.isArray(outlineOffer.outcome_ids) ? outlineOffer.outcome_ids : [])
    )
  ];

  const citations = uniqNonEmpty(
    claimSets.reduce((all, set) => all.concat(Array.from(set || [])), [])
  );

  return { business_value, human_value, solution_elements, fit_to_needs, citations };
}

function buildSalesEnablementSection({ strategy_v2, outline, campaignRequirement }) {
  const sv2 = (strategy_v2 && typeof strategy_v2 === "object") ? strategy_v2 : {};

  const buyerStrategy =
    (sv2.buyer_strategy && typeof sv2.buyer_strategy === "object")
      ? sv2.buyer_strategy
      : {};

  const vp =
    (sv2.value_proposition && typeof sv2.value_proposition === "object")
      ? sv2.value_proposition
      : {};

  const spine =
    (sv2.story_spine && typeof sv2.story_spine === "object")
      ? sv2.story_spine
      : {};

  const se =
    (sv2.sales_enablement && typeof sv2.sales_enablement === "object")
      ? sv2.sales_enablement
      : {};

  const outlineSections =
    (outline && typeof outline === "object" && outline.sections && typeof outline.sections === "object")
      ? outline.sections
      : {};

  const outlineMessaging = Array.isArray(outlineSections.messaging) ? outlineSections.messaging : [];

  const aimNorm = normaliseCampaignRequirement(campaignRequirement);
  const aimSentence = describeCampaignAimSentence(aimNorm);

  const campaignOverviewParts = [
    aimSentence,
    Array.isArray(spine.environment) && spine.environment.length
      ? `Environment: ${summariseBullets(spine.environment, 4)}.`
      : null,
    Array.isArray(spine.case_for_action) && spine.case_for_action.length
      ? `Case for action: ${summariseBullets(spine.case_for_action, 6)}.`
      : null,
    Array.isArray(spine.how_we_win) && spine.how_we_win.length
      ? `How we win: ${summariseBullets(spine.how_we_win, 6)}.`
      : null
  ];

  const campaign_overview = campaignOverviewParts.filter(Boolean).join(" ");

  const buyerProblems = uniqNonEmpty(
    []
      .concat(Array.isArray(buyerStrategy.problems) ? buyerStrategy.problems : [])
      .concat(
        outlineMessaging.flatMap((m) =>
          Array.isArray(m?.pain_points_from_csv) ? m.pain_points_from_csv : []
        )
      )
  );

  const buyer_problem = buyerProblems.length
    ? summariseBullets(buyerProblems, 4)
    : "The campaign addresses specific, evidence-based problems experienced by your buyers.";

  const buyer_business_value = uniqNonEmpty(Array.isArray(vp.business_value) ? vp.business_value : []);
  const buyer_human_value = uniqNonEmpty(Array.isArray(vp.persona_value) ? vp.persona_value : []);

  const discovery_questions = uniqNonEmpty(
    Array.isArray(se.discovery_questions)
      ? se.discovery_questions
      : [
        "What is currently making this problem a priority for you now?",
        "How are you addressing this today, and what is not working as well as you need?",
        "Who else is involved in making this decision and what do they care about most?",
        "If we were successful together, what would good look like in 12–18 months?"
      ]
  );

  const qualification_criteria = uniqNonEmpty(
    Array.isArray(se.qualification_criteria)
      ? se.qualification_criteria
      : Array.isArray(buyerStrategy.qualification_criteria)
        ? buyerStrategy.qualification_criteria
        : [
          "There is a clearly acknowledged problem or opportunity linked to our value story.",
          "Budget or investment appetite exists within a realistic timeframe.",
          "Key stakeholders are identified and engaged.",
          "There is a defined next step that aligns to the campaign call to action."
        ]
  );

  const objection_handling = uniqNonEmpty(
    Array.isArray(se.objection_handling)
      ? se.objection_handling
      : [
        "We already have a supplier for this type of service.",
        "This is not a priority for us right now.",
        "We do not have budget allocated at the moment.",
        "We have tried something similar before and it did not work."
      ]
  );

  const battlecard =
    typeof se.battlecard === "string" && se.battlecard.trim()
      ? se.battlecard.trim()
      : "Use this campaign to position outcomes first, then support with product-fit and proof points. Anchor conversations around the specific problems, decision drivers, and outcomes defined in the strategy.";

  const master_pitch = [
    "This campaign is designed to start a business conversation, not a product demo.",
    "Lead with the specific problems and decision drivers captured in the buyer strategy, then link back to the value proposition and proof points.",
    "Use the discovery questions to quantify impact and qualify fit before moving to solution detail.",
    "Always close with an agreed next step that aligns to the campaign call to action."
  ].join(" ");

  const messagingClaimIds = [];
  outlineMessaging.forEach((m) => {
    (Array.isArray(m?.claim_ids) ? m.claim_ids : []).forEach((id) => messagingClaimIds.push(id));
  });

  const claimSets = [
    extractClaimIdsFromArray(buyerProblems),
    extractClaimIdsFromArray(buyer_business_value),
    extractClaimIdsFromArray(buyer_human_value),
    new Set(messagingClaimIds)
  ];

  const citations = uniqNonEmpty(
    claimSets.reduce((all, set) => all.concat(Array.from(set || [])), [])
  );

  return {
    buyer_problem,
    buyer_value: {
      business_value: buyer_business_value,
      human_value: buyer_human_value
    },
    campaign_overview: campaign_overview || aimSentence || "",
    discovery_questions,
    qualification_criteria,
    objection_handling,
    battlecard,
    master_pitch,
    citations
  };
}

async function buildProofPointsSection({ strategy_v2 }) {
  const sv2 = (strategy_v2 && typeof strategy_v2 === "object") ? strategy_v2 : {};
  const points = uniqNonEmpty(Array.isArray(sv2.proof_points) ? sv2.proof_points : []);
  const citations = uniqNonEmpty(Array.from(extractClaimIdsFromArray(points)));
  return { points, citations };
}

// ---------------------- Main Azure Function ---------------------- //

module.exports = async function (context, queueItem) {
  const log = context.log;
  let outlineAvailable = false;

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

  const userId = msg.userId || msg.user || "anonymous";
  const page = msg.page || "campaign";

  if (!msg.prefix) {
    throw new Error("Writer invoked without canonical prefix");
  }

  let prefix = String(msg.prefix).trim();
  prefix = prefix.replace(/^\/+/, "");
  if (!prefix.endsWith("/")) prefix += "/";

  log(`[*] Using canonical prefix: ${prefix}`);

  const svc = getBlobServiceClient();
  const container = svc.getContainerClient(RESULTS_CONTAINER);

  // Create-if-not-exists
  try {
    await container.createIfNotExists();
  } catch (cErr) {
    log.error("[!] Failed to create-or-confirm results container", {
      container: RESULTS_CONTAINER,
      error: String(cErr?.message || cErr)
    });
  }

  // ---- Idempotency: skip if writer already completed ----
  try {
    const existingStatus = await readJsonIfExists(container, `${prefix}status.json`);
    const campaignExists = await container
      .getBlockBlobClient(`${prefix}campaign.json`)
      .exists();

    if (existingStatus?.markers?.writerCompleted && campaignExists) {
      log("[*] Writer detected completed output; skipping re-run", { runId, prefix });
      return;
    }
  } catch (idemErr) {
    log.warn("[!] Writer idempotency check failed (continuing)", String(idemErr));
  }

  await updateStatus(container, prefix, "writer_working", "campaign writer started", {
    markers: {
      writerVersion: "v9.3",
      writerStartedAt: new Date().toISOString(),
      writerOutlineAvailable: outlineAvailable
    }
  });

  try {
    // -----------------------------------------------------------------------
    // Load canonical artefacts
    // -----------------------------------------------------------------------

    const strategyBundle = await readJsonIfExists(container, `${prefix}strategy_v2/campaign_strategy.json`);

    const strategy_v2 =
      strategyBundle && strategyBundle.strategy_v2
        ? strategyBundle.strategy_v2
        : (strategyBundle && strategyBundle.strategy_v2 === undefined ? strategyBundle : null);

    if (!strategyBundle || typeof strategyBundle !== "object") {
      throw new Error("strategy_v2/campaign_strategy.json missing or invalid");
    }
    if (!strategy_v2 || typeof strategy_v2 !== "object") {
      throw new Error("strategy_v2/campaign_strategy.json missing or invalid (strategy_v2 object not found)");
    }

    const baseInput = await readJsonIfExists(container, `${prefix}input.json`);
    const outline = await readJsonIfExists(container, `${prefix}outline.json`);

    outlineAvailable = outline !== null && typeof outline === "object"; // ✅ assign, don’t redeclare

    if (!outlineAvailable) {
      log.warn(
        "[*] Writer: outline.json missing or invalid – proceeding with strategy_v2-only intelligence"
      );
    }

    await updateStatus(container, prefix, "writer_working", "writer inputs loaded", {
      markers: {
        writerOutlineAvailable: outlineAvailable
      }
    });

    const contentPillars = await readJsonIfExists(container, `${prefix}content_pillars.json`);
    const cpShape = validateContentPillarsV2(contentPillars);
    if (!cpShape.ok) {
      throw new Error(`content_pillars.json missing or invalid (${cpShape.reason})`);
    }

    // evidence can evolve; audit hash only
    const evidenceCanon = await readJsonIfExists(container, `${prefix}evidence.json`);

    const contentPillarsSha1 = sha1OfJson(contentPillars);
    const evidenceSha1 = sha1OfJson(evidenceCanon);

    // -----------------------------------------------------------------------
    // Hash locks (strict for content pillars; tolerant for evidence volatility)
    // -----------------------------------------------------------------------

    // Prefer strategyBundle inputs when available (worker-provided)
    const expectedCpSha1 = strategyBundle?.inputs?.content_pillars_sha1 || null;
    if (expectedCpSha1 && String(expectedCpSha1) !== contentPillarsSha1) {
      throw new Error(
        `Hash lock failure: strategy inputs.content_pillars_sha1=${expectedCpSha1} but content_pillars.json sha1=${contentPillarsSha1}`
      );
    }

    // Evidence hash is NOT hard-failed here (may evolve).
    const expectedEvSha1 = strategyBundle?.inputs?.evidence_sha1 || null;

    // Also check status markers written by outline/pillars, if present
    const statusNow = await readJsonIfExists(container, `${prefix}status.json`);
    const markers = statusNow?.markers || {};
    const declaredEvidenceAtPillars = markers.contentPillarsDeclaredEvidenceSha1 || markers.contentPillarsEvidenceSha1 || null;

    // -----------------------------------------------------------------------
    // Build allowed claim IDs
    // -----------------------------------------------------------------------
    const requiredClaimIds = Array.isArray(contentPillars?.inputs?.required_claim_ids)
      ? contentPillars.inputs.required_claim_ids.map((x) => String(x || "").trim()).filter(Boolean)
      : [];

    if (!requiredClaimIds.length) {
      throw new Error("Writer refused: content_pillars.inputs.required_claim_ids missing/empty");
    }

    const allowedClaimIds = new Set(requiredClaimIds);

    // Outline cannot expand allowed ids — guardrail only
    const outlineIds = allowedClaimIdsFromOutline(outline);
    const outlineDisallowed = [...outlineIds].filter(id => !allowedClaimIds.has(id));

    if (outlineDisallowed.length) {
      throw new Error(
        `Writer refused: outline referenced claim_ids not present in content_pillars.inputs.required_claim_ids allowed set: ${outlineDisallowed.slice(0, 12).join(", ")}`
      );
    }

    if (allowedClaimIds.size === 0) {
      // This should never happen with valid content-pillars-v2
      throw new Error("Writer refused: allowedClaimIds empty (pillars proof_enrichment missing claim_refs?)");
    }

    // -----------------------------------------------------------------------
    // Campaign requirement
    // -----------------------------------------------------------------------
    const campaignRequirement =
      msg.campaign_requirement ||
      (baseInput && baseInput.campaign_requirement) ||
      null;

    // -----------------------------------------------------------------------
    // Gold viability (schema-aligned)
    // -----------------------------------------------------------------------
    const viabilityRaw = await readJsonIfExists(container, `${prefix}strategy_v2/viability.json`);
    const goldViability = buildGoldViability(viabilityRaw || {});

    // -----------------------------------------------------------------------
    // Build Gold sections (deterministic)
    // -----------------------------------------------------------------------
    const executive_summary = buildExecutiveSummarySection({
      strategy_v2,
      outline,
      viability: viabilityRaw || {},
      campaignRequirement
    });

    const go_to_market = buildGoToMarketSection({
      strategy_v2,
      outline,
      viability: viabilityRaw || {}
    });

    const offering = buildOfferingSection({ strategy_v2, outline });

    const sales_enablement = buildSalesEnablementSection({
      strategy_v2,
      outline,
      campaignRequirement
    });

    const proof_points = await buildProofPointsSection({ strategy_v2 });

    // -----------------------------------------------------------------------
    // Guardrails: numeric claim-id requirement (redact if section lacks citations)
    // -----------------------------------------------------------------------
    const enforcementNotes = [];

    const execCites = new Set(Array.isArray(executive_summary.citations) ? executive_summary.citations : []);
    const gtmCites = new Set(Array.isArray(go_to_market.citations) ? go_to_market.citations : []);
    const offerCites = new Set(Array.isArray(offering.citations) ? offering.citations : []);
    const seCites = new Set(Array.isArray(sales_enablement.citations) ? sales_enablement.citations : []);
    const proofCites = new Set(Array.isArray(proof_points.citations) ? proof_points.citations : []);

    const executive_summary_enforced = enforceNumericClaimRequirementOnSectionStrings(
      executive_summary,
      execCites,
      enforcementNotes
    );
    const go_to_market_enforced = enforceNumericClaimRequirementOnSectionStrings(
      go_to_market,
      gtmCites,
      enforcementNotes
    );
    const offering_enforced = enforceNumericClaimRequirementOnSectionStrings(
      offering,
      offerCites,
      enforcementNotes
    );
    const sales_enablement_enforced = enforceNumericClaimRequirementOnSectionStrings(
      sales_enablement,
      seCites,
      enforcementNotes
    );
    const proof_points_enforced = enforceNumericClaimRequirementOnSectionStrings(
      proof_points,
      proofCites,
      enforcementNotes
    );

    // -----------------------------------------------------------------------
    // Validate citations are allowed
    // -----------------------------------------------------------------------
    const allCitations = uniqNonEmpty(
      []
        .concat(executive_summary_enforced.citations || [])
        .concat(go_to_market_enforced.citations || [])
        .concat(offering_enforced.citations || [])
        .concat(sales_enablement_enforced.citations || [])
        .concat(proof_points_enforced.citations || [])
    );

    const disallowed = allCitations.filter((id) => id && !allowedClaimIds.has(String(id).trim()));
    if (disallowed.length) {
      throw new Error(
        `Writer refused: disallowed citation IDs detected (not in content_pillars.inputs.required_claim_ids): ${disallowed
          .slice(0, 12)
          .join(", ")}`
      );
    }

    // -----------------------------------------------------------------------
    // Build root Gold contract (schema-safe; includes _meta; NO extra top-level keys)
    // -----------------------------------------------------------------------
    const supplier = deriveSupplier(baseInput, strategy_v2);
    const industry = deriveIndustry(baseInput, strategy_v2);
    const persona = derivePersona(baseInput, strategy_v2);

    const sections = {
      executive_summary: executive_summary_enforced,
      go_to_market: go_to_market_enforced,
      offering: offering_enforced,
      sales_enablement: sales_enablement_enforced,
      proof_points: proof_points_enforced
    };

    const contract = {
      runId,
      supplier,
      industry,
      persona,
      viability: goldViability,
      sections,
      _meta: {
        version: "v9.3",
        strategy_engine: "campaign-write",
        generated_at: new Date().toISOString()
      }
    };

    // -----------------------------------------------------------------------
    // Write output
    // -----------------------------------------------------------------------
    const outPath = `${prefix}campaign.json`;
    await writeJson(container, outPath, contract);

    // -----------------------------------------------------------------------
    // Status update
    // -----------------------------------------------------------------------
    await updateStatus(container, prefix, "completed", "campaign writer completed successfully", {
      completed: true,
      runId,
      userId,
      page,
      prefix,
      campaign_path: outPath,
      markers: {
        writerCompleted: true,
        writerVersion: "v9.3",
        writerLocked: true,
        contentPillarsSha1,
        evidenceSha1,
        evidenceSha1_expected: expectedEvSha1 || null,
        evidenceSha1_at_pillars: declaredEvidenceAtPillars || null,
        contentPillarsLockedVerified: true,
        writerAllowedClaimIdsCount: allowedClaimIds.size,
        writerCitationsCount: allCitations.length,
        writerNumericRedactionsCount: enforcementNotes.length
      }
    });

    // Add enforcement notes to history only (schema safe)
    if (enforcementNotes.length) {
      await updateStatus(container, prefix, "completed", "writer guardrails applied", {
        note: `Applied ${enforcementNotes.length} redactions for numeric-without-citations`,
        markers: { writerGuardrailsApplied: true }
      });
    }

    log("[*] Campaign Writer completed (Gold v9.3)", {
      runId,
      campaignPath: outPath,
      contentPillarsSha1,
      evidenceSha1,
      expectedEvSha1: expectedEvSha1 || null,
      declaredEvidenceAtPillars: declaredEvidenceAtPillars || null
    });
  } catch (err) {
    log.error("[!] Campaign Writer failed", {
      runId,
      prefix,
      error: String(err && err.message ? err.message : err)
    });

    try {
      await writeJson(container, `${prefix}writer_error.json`, {
        message: String(err && err.message ? err.message : err),
        stack: err && err.stack ? String(err.stack) : null,
        writer_version: "v9.3",
        at: new Date().toISOString()
      });
    } catch (e2) {
      log.error("[!] Failed to write writer_error.json", String(e2));
    }

    await updateStatus(container, prefix, "writer_error", "Campaign Writer failed", {
      error: String(err && err.message ? err.message : err),
      markers: { writerVersion: "v9.3", writerFailed: true }
    });

    throw err;
  }
};
