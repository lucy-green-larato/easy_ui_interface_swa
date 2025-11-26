// /api/campaign-write/index.js (campaign-gold renderer) 21-11-2025 v1
// Queue-triggered on %Q_CAMPAIGN_WRITE%.
// - op: "section" | "write_section"  -> light markers only (no LLM).
// - op: "assemble"                   -> calls Prompt Harness v2 to render campaign.json (campaign-gold schema).

const path = require("path");
const crypto = require("crypto");
const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const campaignConfig = require("../shared/campaignConfig");

// ---- Prompt harness v2 (schema-enforced JSON) ----
const { generateCampaign } = require("../lib/prompt-harness");

// ==== ENV / CONFIG ====
const STORAGE_CONN = campaignConfig.STORAGE_CONN;
const CONTAINER = campaignConfig.RESULTS_CONTAINER;
const MAIN_QUEUE = campaignConfig.CAMPAIGN_QUEUE_NAME;

// Use explicit schema path so we are always aligned to campaign-gold
const DEFAULT_SCHEMA_PATH = path.join(__dirname, "..", "schemas", "campaign-gold.schema.json");

// Final section keys (kept for back-compat with router + markers)
const FINAL_SECTION_KEYS = [
  "executive_summary",
  "campaign_strategy",
  "positioning_and_differentiation",
  "offer_strategy",
  "messaging_matrix",
  "channel_plan",
  "sales_enablement",
  "measurement_and_learning",
  "risks_and_contingencies",
  "compliance_and_governance",
  "one_pager_summary"
];

// Back-compat short-code → final-name mapping (accept both)
const SHORT_TO_FINAL = {
  exec: "executive_summary",
  positioning: "positioning_and_differentiation",
  messaging: "messaging_matrix",
  offer: "offer_strategy",
  channel: "channel_plan",
  risks: "risks_and_contingencies",
  compliance: "compliance_and_governance",
  sales: "sales_enablement",
  one: "one_pager_summary"
};

// ==== Small utils ====
function requireStorage() {
  if (!STORAGE_CONN) throw new Error("AzureWebJobsStorage not configured");
  return BlobServiceClient.fromConnectionString(STORAGE_CONN);
}
function blobSvc() { return requireStorage(); }

async function getJson(containerClient, relPath) {
  const bc = containerClient.getBlockBlobClient(relPath);
  if (!(await bc.exists())) return null;
  const dl = await bc.download();
  const chunks = [];
  for await (const ch of dl.readableStreamBody) chunks.push(ch);
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch { return null; }
}

async function putJson(containerClient, relPath, obj) {
  const bb = containerClient.getBlockBlobClient(relPath);
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await bb.uploadData(body, { blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" } });
}

function normalizePrefix(p) {
  let x = String(p || "").trim();
  if (!x) return null;
  if (x.startsWith(`${CONTAINER}/`)) x = x.slice(`${CONTAINER}/`.length);
  x = x.replace(/^\/+/, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}

function safeForPrompt(v, max = 280000) {
  try {
    const s = typeof v === "string" ? v : JSON.stringify(v ?? "");
    if (s.length <= max) return s;
    const k = Math.floor(max / 2);
    return s.slice(0, k) + " …TRUNCATED… " + s.slice(-k);
  } catch { return "null"; }
}

function nowISO() { return new Date().toISOString(); }

function sha256OfJson(o) {
  const h = crypto.createHash("sha256");
  h.update(Buffer.from(JSON.stringify(o || {})));
  return h.digest("hex");
}

// ---- status (append-only history; do NOT bloat input) ----
async function patchStatus(container, prefix, state, extra = {}) {
  const p = `${prefix}status.json`;
  const cur = (await getJson(container, p)) || {};
  const next = { ...cur, state, history: Array.isArray(cur.history) ? cur.history.slice() : [] };
  next.history.push({ state, at: nowISO(), ...(extra.op ? { op: extra.op } : {}) });
  if (!next.markers) next.markers = {};
  // copy only explicit extras (no input echo)
  for (const [k, v] of Object.entries(extra)) {
    if (k !== "op") next[k] = v;
  }
  await putJson(container, p, next);
  return next;
}

// ==== Domain helpers ====

// Derive core meta from outline + csv
function deriveCoreMeta(runId, outline, csvCanon) {
  const inputNotes = outline?.input_notes || {};
  const meta = outline?.meta || {};
  const supplier =
    (inputNotes.supplier_company ||
      inputNotes.prospect_company ||
      meta.company ||
      "").toString().trim();

  const persona =
    (meta.persona ||
      inputNotes.persona ||
      inputNotes.target_persona ||
      "").toString().trim();

  const industry =
    (csvCanon && csvCanon.selected_industry) ||
    meta.selected_industry ||
    inputNotes.selected_industry ||
    "";

  const route_to_market =
    (inputNotes.sales_model ||
      meta.sales_model ||
      inputNotes.route_to_market ||
      "").toString().trim();

  const requirement =
    (inputNotes.campaign_requirement ||
      inputNotes.requirement ||
      inputNotes.campaign_type ||
      "").toString().trim();

  const rowCountRaw =
    csvCanon?.meta?.rows ??
    csvCanon?.row_count ??
    csvCanon?.rows ??
    inputNotes.rowCount ??
    inputNotes.row_count;

  const rowCount = Number.isFinite(Number(rowCountRaw)) ? Number(rowCountRaw) : null;

  return {
    runId: String(runId || "").trim(),
    supplier: supplier || "Unknown supplier",
    industry: (industry || "general").toString().trim(),
    persona: persona || "Primary buyer",
    route_to_market: route_to_market || "Unspecified",
    requirement: requirement || "Unspecified",
    cohort_size: rowCount
  };
}

// Summarise CSV for prompt (no row-level data)
function summariseCsvForPrompt(csvCanon) {
  if (!csvCanon || typeof csvCanon !== "object") return {};
  const meta = csvCanon.meta || {};
  const sig = csvCanon.signals || {};
  const counts = (sig && sig.counts) || {};
  return {
    meta: {
      rows: meta.rows ?? csvCanon.row_count ?? csvCanon.rows ?? null,
      industry_mode: csvCanon.industry_mode || null,
      selected_industry: csvCanon.selected_industry || null
    },
    signals: {
      top_needs_supplier: Array.isArray(sig.top_needs_supplier) ? sig.top_needs_supplier.slice(0, 10) : [],
      top_needs: Array.isArray(sig.top_needs) ? sig.top_needs.slice(0, 10) : [],
      top_blockers: Array.isArray(sig.top_blockers) ? sig.top_blockers.slice(0, 10) : [],
      top_purchases: Array.isArray(sig.top_purchases) ? sig.top_purchases.slice(0, 10) : [],
      counts: {
        by_need: counts.by_need || null,
        by_purchase: counts.by_purchase || counts.by_intent || null
      }
    }
  };
}

function summariseBuyerLogicForPrompt(buyerLogic, buyerStrategy) {
  const out = {};

  // Helper to normalise arrays to [{ type, label, claim_ids[] }]
  function norm(arr) {
    if (!Array.isArray(arr)) return [];
    return arr.slice(0, 20).map(item => ({
      type: item.type || null,
      label: item.label || "",
      claim_ids: Array.isArray(item.origin?.related_claim_ids)
        ? item.origin.related_claim_ids.slice(0, 10)
        : []
    }));
  }

  // --- Evidence-led raw buyer_logic fields (rich, structured, claim-anchored) ---
  out.problems_raw = norm(buyerLogic.problems);
  out.root_causes = norm(buyerLogic.root_causes);
  out.operational_impacts = norm(buyerLogic.operational_impacts);
  out.commercial_impacts = norm(buyerLogic.commercial_impacts);
  out.emotional_drivers = norm(buyerLogic.emotional_drivers);
  out.urgency_factors = norm(buyerLogic.urgency_factors);

  // --- Summarised strategy_v2 → keeps the high-level framing consistent ---
  if (buyerStrategy && typeof buyerStrategy === "object") {
    out.problems = Array.isArray(buyerStrategy.problems)
      ? buyerStrategy.problems.slice(0, 20)
      : [];
    out.barriers = Array.isArray(buyerStrategy.barriers)
      ? buyerStrategy.barriers.slice(0, 20)
      : [];
    out.urgency_summary = Array.isArray(buyerStrategy.urgency)
      ? buyerStrategy.urgency.slice(0, 20)
      : [];
    out.decision_drivers = Array.isArray(buyerStrategy.decision_drivers)
      ? buyerStrategy.decision_drivers.slice(0, 20)
      : [];
  }

  return out;
}

// Summarise evidence claims for prompt
function summariseEvidenceForPrompt(evidenceObj, fallbackLog) {
  let claims = [];
  if (Array.isArray(evidenceObj?.claims)) {
    claims = evidenceObj.claims;
  } else if (Array.isArray(fallbackLog)) {
    // legacy evidence_log style; treat as claims with id+summary if present
    claims = fallbackLog;
  }

  const maxClaims = 60;
  const topClaims = claims.slice(0, maxClaims);

  const claimIdSet = new Set();
  for (const c of topClaims) {
    const id = (c.claim_id || c.id || "").toString().trim();
    if (id) claimIdSet.add(id);
  }

  return {
    claims: topClaims,
    claim_ids: Array.from(claimIdSet)
  };
}

// Summarise strategy_v2 for prompt (only key branches)
function summariseStrategyForPrompt(strategyV2) {
  if (!strategyV2 || typeof strategyV2 !== "object") return {};
  const s = strategyV2;
  return {
    story_spine: {
      environment: s.story_spine?.environment || [],
      case_for_action: s.story_spine?.case_for_action || [],
      how_we_win: s.story_spine?.how_we_win || [],
      success: s.story_spine?.success || [],
      next_steps: s.story_spine?.next_steps || []
    },
    value_proposition: s.value_proposition || {},
    competitive_strategy: s.competitive_strategy || {},
    buyer_strategy: s.buyer_strategy || {},
    gtm_strategy: s.gtm_strategy || {},
    proof_points: Array.isArray(s.proof_points) ? s.proof_points : [],
    right_to_play: Array.isArray(s.right_to_play) ? s.right_to_play : []
  };
}

// Summarise products & competitors for prompt
function summariseProductsAndCompetitors(productsMeta, competitorsFile, strategyV2, outline) {
  const products = [];
  if (Array.isArray(productsMeta?.chosen)) {
    for (const p of productsMeta.chosen) {
      products.push(typeof p === "string" ? p : (p?.name || ""));
    }
  } else if (Array.isArray(productsMeta?.validated)) {
    for (const p of productsMeta.validated) {
      products.push(typeof p === "string" ? p : (p?.name || ""));
    }
  }

  const compSet = new Set();
  if (Array.isArray(competitorsFile?.competitors)) {
    for (const c of competitorsFile.competitors) {
      const n = (typeof c === "string" ? c : (c?.vendor || "")).toString().trim();
      if (n) compSet.add(n);
    }
  }
  const outlineIN = outline?.input_notes || {};
  if (Array.isArray(outlineIN.competitors)) {
    for (const c of outlineIN.competitors) {
      const n = (c || "").toString().trim();
      if (n) compSet.add(n);
    }
  }
  if (Array.isArray(outlineIN.relevant_competitors)) {
    for (const c of outlineIN.relevant_competitors) {
      const n = (c || "").toString().trim();
      if (n) compSet.add(n);
    }
  }
  // Optional hook: if strategy_v2 exposes a list of relevant_competitors, include them.
  if (Array.isArray(strategyV2?.competitive_strategy?.relevant_competitors)) {
    for (const c of strategyV2.competitive_strategy.relevant_competitors) {
      const n = (c || "").toString().trim();
      if (n) compSet.add(n);
    }
  }

  return {
    products: products.filter(Boolean).slice(0, 20),
    competitors: Array.from(compSet).slice(0, 20)
  };
}

// --- NEW: deterministic v1 shape: needsMap.items[] ---
function summariseNeedsAndMarkdown(needsMap, markdownPack) {
  const needsSummary = {};

  if (needsMap && Array.isArray(needsMap.items) && needsMap.items.length) {
    needsMap.items.slice(0, 20).forEach((item, idx) => {
      const rawNeed =
        (typeof item?.need === "string" && item.need.trim()) ||
        (typeof item?.label === "string" && item.label.trim()) ||
        "";

      const needKey = rawNeed || `need_${idx + 1}`;
      if (!needKey) return;

      // Status (gap / covered / partial / etc.)
      const status =
        typeof item?.status === "string" ? item.status.trim() : null;

      // Hits → take up to 5 names (or labels) purely from existing data
      const hits = Array.isArray(item?.hits)
        ? item.hits
          .map((h) =>
            (typeof h?.name === "string" && h.name.trim()) ||
            (typeof h?.label === "string" && h.label.trim()) ||
            ""
          )
          .filter(Boolean)
          .slice(0, 5)
        : [];

      needsSummary[needKey] = {
        status: status || null,
        hits
      };
    });
  }
  // --- Legacy fallback: mapping / map (kept for back-compat) ---
  else if (needsMap && typeof needsMap === "object") {
    const mapping = needsMap.mapping || needsMap.map || {};
    const entries = Object.entries(mapping).slice(0, 10);
    for (const [need, val] of entries) {
      if (!need) continue;
      if (Array.isArray(val?.capabilities)) {
        needsSummary[need] = {
          status: null,
          hits: val.capabilities.slice(0, 3)
        };
      } else if (Array.isArray(val)) {
        needsSummary[need] = {
          status: null,
          hits: val.slice(0, 3)
        };
      } else if (typeof val === "string") {
        needsSummary[need] = {
          status: null,
          hits: [val]
        };
      }
    }
  }

  // --- Markdown summary (unchanged) ---
  let markdownSummary = markdownPack;
  if (markdownPack && typeof markdownPack === "object") {
    markdownSummary = {};
    for (const [k, v] of Object.entries(markdownPack)) {
      if (typeof v === "string") {
        markdownSummary[k] = v.slice(0, 2000);
      } else if (Array.isArray(v)) {
        markdownSummary[k] = v.slice(0, 10);
      }
    }
  }

  return { needsSummary, markdownSummary };
}

// ---- Prompt builders ----
function buildSystemPrompt(meta, evidenceClaimIds) {
  const idsList = evidenceClaimIds && evidenceClaimIds.length
    ? evidenceClaimIds.join(", ")
    : "(no claims available — do not invent any)";

  return [
    "You are a senior UK B2B go-to-market consultant and writer.",
    "Phase 1 (Evidence & Insights) and Phase 2 (Strategy Engine, strategy_v2) are already complete.",
    "Your job is NOT to redo analysis or change the strategy.",
    "Your job is to RENDER a final campaign.json document for sales and marketing enablement,",
    "strictly following the provided campaign-gold JSON schema and target structure.",
    "",
    "Core rules:",
    "- Do not change, override or invent strategy: you must respect story_spine, value_proposition, competitive_strategy, buyer_strategy, gtm_strategy, proof_points and right_to_play as given.",
    "- Do not invent any new data structures, keys or top-level fields beyond the campaign-gold schema.",
    "- Do not hallucinate competitors, products, technologies, metrics or personas.",
    "- Use ONLY the claim_ids provided from evidence.json when citing external facts.",
    "- When you reference a specific claim, include [CLAIM_ID] inline in the paragraph.",
    "- When you write paragraphs or bullets that correspond directly to items from strategy_v2.story_spine, value_proposition, competitive_strategy, buyer_strategy, gtm_strategy, proof_points or right_to_play, reuse the same factual content and only refine the language.",
    "- Do not introduce new numerical claims, timeframes, or comparative performance statements unless they are clearly present in evidence.claims or strategy_v2.",
    "- For each section that uses evidence (executive_summary, value_proposition, messaging_matrix, sales_enablement, go_to_market_plan, compliance_and_governance),",
    "  list all used claim_ids in a citations[] array on that section.",
    "- Never invent a claim_id; if you cannot ground a statement in the provided evidence, write in qualitative terms or say 'TBD' instead of making it up.",
    "",
    "Language & tone:",
    "- UK English, consultant-grade narrative.",
    "- Structure thinking as diagnosis → insight → recommendation.",
    "- Write for senior commercial and technical leaders.",
    "- Be concise but rich in meaning; avoid fluffy or generic language.",
    "",
    "Output contract:",
    "- You MUST return a single JSON object (no markdown, no commentary, no code fences).",
    "- The object MUST include at least: runId, supplier, industry, persona, executive_summary, value_proposition.",
    "- Other sections (messaging_matrix, sales_enablement, go_to_market_plan, risks_and_contingencies, compliance_and_governance, one_pager_summary)",
    "  should be populated wherever strategy_v2 and evidence provide enough material.",
    "",
    `Allowed claim_ids (do not invent new ones): ${idsList}`,
    "",
    `This campaign is for supplier: ${meta.supplier}, industry: ${meta.industry}, persona: ${meta.persona}.`
  ].join("\n");
}

function buildUserPrompt(args) {
  const {
    meta,
    strategySummary,
    buyerLogicSummary,
    csvSummary,
    evidenceSummary,
    productsAndCompetitors,
    needsAndMarkdown
  } = args;

  const { needsSummary, markdownSummary } = needsAndMarkdown;

  const coreContext = {
    runId: meta.runId,
    supplier: meta.supplier,
    industry: meta.industry,
    persona: meta.persona,
    route_to_market: meta.route_to_market,
    requirement: meta.requirement,
    cohort_size: meta.cohort_size
  };

  return [
    "You are rendering a complete campaign.json in the campaign-gold structure.",
    "",
    "== HIGH-LEVEL CONTEXT ==",
    safeForPrompt(coreContext),
    "",
    "== STRATEGY_V2 SUMMARY ==",
    safeForPrompt(strategySummary),
    "",
    "== BUYER LOGIC SUMMARY ==",
    safeForPrompt(buyerLogicSummary),
    "",
    "The buyer_logic summary has TWO layers:",
    "- Evidence-led fields with claim_ids: problems_raw, root_causes, operational_impacts, commercial_impacts, emotional_drivers, urgency_factors.",
    "- Strategy-level summaries from strategy_v2.buyer_strategy: problems, barriers, urgency_summary, decision_drivers.",
    "You MUST treat the evidence-led fields as the factual source of truth and the summaries as framing only.",
    "",
    "== CSV NORMALISED SUMMARY (NO ROW-LEVEL DATA) ==",
    safeForPrompt(csvSummary),
    "",
    "== EVIDENCE CLAIMS (use claim_id for [CLAIM_ID] and citations[]) ==",
    safeForPrompt(evidenceSummary.claims),
    "",
    "== PRODUCTS & COMPETITORS ==",
    safeForPrompt(productsAndCompetitors),
    "",
    "== NEEDS MAP EXCERPT ==",
    safeForPrompt(needsSummary),
    "",
    "== MARKDOWN PACK EXCERPT (SUPPLIER / INDUSTRY / GENERAL NARRATIVES) ==",
    safeForPrompt(markdownSummary),
    "",
    "== REQUIRED OUTPUT SECTIONS (STRUCTURE ONLY, DO NOT ECHO THIS LITERALLY) ==",
    "- Top-level keys you MUST include:",
    "  runId, supplier, industry, persona, executive_summary, value_proposition.",
    "- Additional sections to populate wherever evidence and strategy_v2 support it:",
    "  messaging_matrix, sales_enablement, go_to_market_plan,",
    "  risks_and_contingencies, compliance_and_governance, one_pager_summary.",
    "",
    "Each section must follow the campaign-gold schema enforced by response_format.",
    "",
    "== MAPPING INSTRUCTIONS BY SECTION ==",
    "",
    "1) executive_summary:",
    "- Use strategy_v2.story_spine.environment, case_for_action, how_we_win, success and next_steps as the backbone.",
    "- Use buyer_logic.problems_raw[] as the clearest statements of buyer pain; you may tighten the wording but MUST keep the factual meaning.",
    "- Use buyer_logic.operational_impacts[] and commercial_impacts[] to explain consequences of those problems.",
    "- Use buyer_logic.urgency_factors[] to explain why this matters now.",
    "- emotional_drivers[] should inform how you describe the human / personal side of risk and motivation.",
    "- When you draw directly on a buyer_logic item with claim_ids, include [CLAIM_ID] inline (usually the first or most relevant id) and make sure that id appears in executive_summary.citations[].",
    "",
    "2) value_proposition:",
    "- Use strategy_v2.value_proposition.moore_chain to populate the Moore structure:",
    "  moore.for, moore.who, moore.the, moore.is_a, moore.that, moore.unlike, moore.we_provide.",
    "- In value_proposition.narrative:",
    "  - Frame problems using buyer_logic.problems_raw and buyer_logic.problems (from buyer_strategy).",
    "  - Explain underlying drivers using root_causes[].",
    "  - Describe business outcomes using operational_impacts[] and commercial_impacts[].",
    "  - Reflect emotional_drivers[] where relevant to the buyer’s personal risk and motivation.",
    "- competitive_position must stay consistent with strategy_v2.competitive_strategy and right_to_play (do not invent new competitor categories or claims).",
    "- proof_points[] should reflect strategy_v2.proof_points and any clearly evidenced impacts from buyer_logic (again: no new metrics).",
    "- Whenever you rely on a specific evidence-backed point, include [CLAIM_ID] inline and list the id in value_proposition.citations[].",
    "",
    "3) messaging_matrix:",
    "- audiences[] from core persona plus any clear sub-audiences implied by buyer_logic.emotional_drivers and decision_drivers.",
    "- pillars[] from recurring themes across:",
    "  - buyer_logic.problems_raw and problems,",
    "  - root_causes, and",
    "  - value_proposition.pillar_outcomes and right_to_play.",
    "- support_points[] from:",
    "  - operational_impacts and commercial_impacts,",
    "  - urgency_factors, and",
    "  - the strongest proof_points.",
    "- Do NOT introduce new pains or benefits that are not visible in strategy_v2, buyer_logic or evidence.",
    "- Use [CLAIM_ID] where a support_point depends on specific evidence; put all such ids into messaging_matrix.citations[].",
    "",
    "4) sales_enablement:",
    "- campaign_overview summarises story_spine and value_proposition in language a salesperson can use to explain what is happening, why it matters, and how we help.",
    "- buyer_outcomes[] should describe positive outcomes that are the clear opposite of operational_impacts and commercial_impacts (e.g. fewer delays, protected margins).",
    "- discovery_questions[] should help sales uncover:",
    "  - problems and root_causes (\"Where are you seeing…?\"),",
    "  - operational_impacts and commercial_impacts (\"What does that do to projects, cost, risk?\"),",
    "  - urgency_factors (\"What deadlines, renewals or regulatory pressures are in play?\").",
    "- competitive_battlecard must be grounded in strategy_v2.competitive_strategy (competitor_map, our_advantage, vulnerability_map); do not invent new competitor types or technical weaknesses/strengths.",
    "- master_pitch is a single, coherent pitch from environment → problems → impacts → urgency → how we win → proof, using only strategy_v2, buyer_logic and evidence claims.",
    "- Use [CLAIM_ID] whenever you state something sourced from a specific evidence claim and list all such ids in sales_enablement.citations[].",
    "",
    "5) go_to_market_plan:",
    "- objective from gtm_strategy.success_target.narrative and commercial_focus, expressed in terms of reducing the problems and impacts identified in buyer_logic.",
    "- target_market from gtm_strategy.pipeline_model.tiers, route_implications and CSV cohort data; do not invent new segments beyond those clearly implied.",
    "- marketing_actions[] from route_implications, story_spine.next_steps and the highest priority problems + urgency_factors.",
    "- sales_actions[] from gtm_strategy.pipeline_model.motions plus practical steps to surface buyer_logic problems, impacts and urgency in real accounts.",
    "- pipeline_model from gtm_strategy.pipeline_model; you may describe tiers and motions qualitatively, and only use numeric ranges if they are clearly implied by cohort_size or strategy_v2 (no fabricated precise numbers).",
    "- cta from story_spine.next_steps and what the supplier should concretely do next with this cohort.",
    "- Any evidence-backed statement must carry [CLAIM_ID] and those ids must be listed in go_to_market_plan.citations[].",
    "",
    "6) risks_and_contingencies:",
    "- risks[] from:",
    "  - strategy_v2.competitive_strategy.vulnerability_map,",
    "  - buyer_logic.root_causes that represent ongoing risk if left unaddressed,",
    "  - GTM risks implied by route_implications and pipeline_model (e.g. dependency on partners, resource constraints).",
    "- mitigations[] from:",
    "  - right_to_play and our_advantage,",
    "  - motions and next_steps that actively reduce the probability or impact of those risks.",
    "- Do not invent new technical capabilities or guarantees.",
    "",
    "7) compliance_and_governance:",
    "- notes should summarise any compliance / governance constraints implied by:",
    "  - urgency_factors that refer to regulation, fines, mandated dates or security expectations, and",
    "  - any regulator / compliance-related claims in the evidence block.",
    "- Do not invent new regulatory frameworks or obligations; stay within what is implied.",
    "- Where you rely on specific evidence, include [CLAIM_ID] and list all ids in compliance_and_governance.citations[].",
    "",
    "8) one_pager_summary:",
    "- positioning: short statement of how the supplier is positioned in this campaign, consistent with value_proposition.competitive_position and right_to_play.",
    "- core_message: a single line that captures environment → problem → how we win → outcome.",
    "- quick_facts[]: 5–7 bullets that combine:",
    "  - the clearest problems / problems_raw,",
    "  - the most important operational_impacts, commercial_impacts and urgency_factors,",
    "  - the strongest proof_points (with [CLAIM_ID] where relevant).",
    "- Every quick_fact must be traceable to strategy_v2, buyer_logic or evidence; do not add new claims.",
    "",
    "INSTRUCTIONS:",
    "- Populate all fields as completely as possible without inventing new keys.",
    "- Do not invent claim_ids or competitors.",
    "- If something is genuinely unknown or not supported by inputs, use qualitative language or 'TBD' instead of fabricating details.",
    "- Do not invent extra tiers or motions beyond those provided in gtm_strategy.pipeline_model; you may summarise or clarify, but the underlying tier and motion structure must stay the same.",
    "",
    "Return a single JSON object ONLY, no markdown, matching the campaign-gold structure."
  ].join("\n");
}

// ---- Main handler ----
module.exports = async function (context, queueItem) {
  const svc = blobSvc();
  const container = svc.getContainerClient(CONTAINER);
  await container.createIfNotExists();

  if (!queueItem) {
    context.log.error("[campaign-write] Empty queue message");
    return;
  }

  const opRaw = queueItem.op || queueItem.type || "";
  const op = String(opRaw).toLowerCase();
  const runId = queueItem.runId || queueItem?.data?.runId || queueItem?.id;
  if (!runId) {
    context.log.error("[campaign-write] Missing runId");
    return;
  }

  let prefix = normalizePrefix(queueItem.prefix) || `runs/${runId}/`;
  if (!prefix) {
    context.log.error("[campaign-write] Unable to resolve prefix");
    return;
  }

  // --- Lightweight section handlers: markers only, no LLM ---
  if (op === "write_section" || op === "section") {
    let requested = String(queueItem.section || "").trim().toLowerCase();
    const finalKey = FINAL_SECTION_KEYS.includes(requested)
      ? requested
      : SHORT_TO_FINAL[requested];

    if (!finalKey || !FINAL_SECTION_KEYS.includes(finalKey)) {
      context.log.warn(`[campaign-write] Unknown section "${queueItem.section}" (marker only).`);
      await patchStatus(container, prefix, "SectionWrites", {
        runId,
        writing: null,
        warning: `Unknown section "${queueItem.section}" ignored (writer now assembles whole campaign.json in one pass).`,
        updatedAt: nowISO(),
        op: "section"
      });
      return;
    }

    await patchStatus(container, prefix, "SectionWrites", {
      runId,
      writing: finalKey,
      updatedAt: nowISO(),
      op: "section"
    });

    const marker = {
      _note: "Section-level writer is deprecated. The final campaign.json is generated on the 'assemble' op only.",
      section: finalKey,
      runId,
      generatedAt: nowISO()
    };
    try {
      await putJson(container, `${prefix}sections/${finalKey}.json`, marker);
      await patchStatus(container, prefix, "SectionWrites", {
        runId,
        written: finalKey,
        updatedAt: nowISO(),
        op: "section"
      });
    } catch (err) {
      context.log.warn("[campaign-write] Failed to write section marker", String(err?.message || err));
    }

    return;
  }

  // ---- Assemble: single-pass campaign-gold generation ----
  if (op === "assemble") {
    await patchStatus(container, prefix, "writer_working", {
      runId,
      assembleStartedAt: nowISO(),
      op: "assemble"
    });

    // Common inputs (best-effort reads; tolerate missing artifacts)
    const [
      evidenceObj,
      evidenceLog,
      csvCanon,
      outline,
      buyerLogic,
      needsMap,
      markdownPack,
      competitorsFile,
      productsMeta,
      strategyV2File,
      legacyStrategy
    ] = await Promise.all([
      getJson(container, `${prefix}evidence.json`),
      getJson(container, `${prefix}evidence_log.json`),
      getJson(container, `${prefix}csv_normalized.json`),
      getJson(container, `${prefix}outline.json`),
      getJson(container, `${prefix}buyer_logic.json`),
      getJson(container, `${prefix}needs_map.json`),
      // markdown pack variants: try a combined pack first, then a generic name
      (async () => {
        const combined = await getJson(container, `${prefix}markdown_pack.json`);
        if (combined) return combined;
        // optional fallbacks if your pipeline uses more granular packs
        const supplierPack = await getJson(container, `${prefix}markdown_supplier.json`);
        const industryPack = await getJson(container, `${prefix}markdown_industry.json`);
        const generalPack = await getJson(container, `${prefix}markdown_general.json`);
        if (supplierPack || industryPack || generalPack) {
          return { supplier: supplierPack || null, industry: industryPack || null, general: generalPack || null };
        }
        return null;
      })(),
      getJson(container, `${prefix}competitors.json`),
      getJson(container, `${prefix}products_meta.json`),
      getJson(container, `${prefix}strategy_v2/campaign_strategy.json`),
      getJson(container, `${prefix}campaign_strategy.json`)
    ]);

    // Normalise strategy_v2 whether wrapped or direct
    let strategyV2 = null;
    if (strategyV2File && typeof strategyV2File === "object") {
      strategyV2 = strategyV2File.strategy_v2 || strategyV2File;
    } else if (legacyStrategy && typeof legacyStrategy === "object") {
      // simple back-compat: treat legacy campaign_strategy.json as strategy_v2 root
      strategyV2 = { ...legacyStrategy };
    }

    const meta = deriveCoreMeta(runId, outline || {}, csvCanon || {});
    const csvSummary = summariseCsvForPrompt(csvCanon || {});
    const buyerLogicSummary = summariseBuyerLogicForPrompt(
      buyerLogic || {},
      (strategyV2 || {}).buyer_strategy || {}
    );
    const evidenceSummary = summariseEvidenceForPrompt(evidenceObj, evidenceLog);
    const strategySummary = summariseStrategyForPrompt(strategyV2 || {});
    const productsAndCompetitors = summariseProductsAndCompetitors(
      productsMeta || {},
      competitorsFile || {},
      strategyV2 || {},
      outline || {}
    );
    const needsAndMarkdown = summariseNeedsAndMarkdown(needsMap || null, markdownPack || null);

    const systemPrompt = buildSystemPrompt(meta, evidenceSummary.claim_ids);
    const userPrompt = buildUserPrompt({
      meta,
      strategySummary,
      buyerLogicSummary,
      csvSummary,
      evidenceSummary,
      productsAndCompetitors,
      needsAndMarkdown
    });

    let campaign = null;
    try {
      campaign = await generateCampaign({
        systemPrompt,
        userPrompt,
        schemaPath: DEFAULT_SCHEMA_PATH
      });
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      const code = e.code || "llm_error";
      const details = e.details || null;
      context.log.error("[campaign-write] LLM/harness error during assemble", code, String(e.message || e));

      // Persist structured error for diagnostics
      try {
        const errorBlob = {
          runId,
          op: "assemble",
          at: nowISO(),
          code,
          message: e.message || String(e),
          details
        };
        await putJson(container, `${prefix}logs/campaign-write.error.json`, errorBlob);
      } catch (logErr) {
        context.log.warn("[campaign-write] Failed to write error log blob", String(logErr?.message || logErr));
      }

      await patchStatus(container, prefix, "error", {
        runId,
        errorCode: code,
        errorMessage: e.message || String(e),
        errorDetails: details || null,
        op: "assemble"
      });
      return;
    }

    // Final guard: ensure core meta populated even if model omitted or changed them
    if (!campaign || typeof campaign !== "object" || Array.isArray(campaign)) {
      context.log.error("[campaign-write] Invalid campaign object returned from harness");
      await patchStatus(container, prefix, "error", {
        runId,
        errorCode: "invalid_campaign_object",
        errorMessage: "Prompt harness returned a non-object for campaign.json",
        op: "assemble"
      });
      return;
    }

    campaign.runId = meta.runId;
    if (!campaign.supplier || typeof campaign.supplier !== "string") {
      campaign.supplier = meta.supplier;
    }
    if (!campaign.industry || typeof campaign.industry !== "string") {
      campaign.industry = meta.industry;
    }
    if (!campaign.persona || typeof campaign.persona !== "string") {
      campaign.persona = meta.persona;
    }

    // Optional: embed lightweight input proof to aid debugging without altering schema
    // (Note: campaign-gold has additionalProperties: false at top level,
    //  so we MUST NOT add extra keys here; keep hashes separate if needed.)
    // If you want hashes, write them to a companion blob instead:
    try {
      const proof = {
        outline_sha256: sha256OfJson(outline || {}),
        evidence_sha256: sha256OfJson(evidenceObj || evidenceLog || {}),
        csv_normalized_sha256: sha256OfJson(csvCanon || {}),
        strategy_v2_sha256: sha256OfJson(strategyV2 || {}),
        buyer_logic_sha256: sha256OfJson(buyerLogic || {}),
        needs_map_sha256: sha256OfJson(needsMap || {}),
        markdown_pack_sha256: sha256OfJson(markdownPack || {}),
        products_meta_sha256: sha256OfJson(productsMeta || {}),
        competitors_sha256: sha256OfJson(competitorsFile || {})
      };
      await putJson(container, `${prefix}input_proof.json`, proof);
    } catch (proofErr) {
      context.log.warn("[campaign-write] Failed to write input_proof.json", String(proofErr?.message || proofErr));
    }

    // Write campaign.json
    await putJson(container, `${prefix}campaign.json`, campaign);

    await patchStatus(container, prefix, "assembled", {
      runId,
      assembledAt: nowISO(),
      op: "assemble"
    });

    // ---- Notify orchestrator exactly once (anti-loop marker) ----
    try {
      const postStatus = (await getJson(container, `${prefix}status.json`)) || {};
      const alreadySent = !!postStatus?.markers?.afterassembleSent;
      if (!alreadySent) {
        const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
        const qc = qs.getQueueClient(MAIN_QUEUE);
        await qc.createIfNotExists();
        const page = (queueItem && (queueItem.page || queueItem?.data?.page)) || "campaign";
        await qc.sendMessage(JSON.stringify({ op: "afterassemble", runId, page, prefix }));

        postStatus.markers = postStatus.markers || {};
        postStatus.markers.afterassembleSent = true;
        await putJson(container, `${prefix}status.json`, postStatus);
      }
    } catch (notifyErr) {
      context.log.warn("[campaign-write] notify afterassemble failed", String(notifyErr?.message || notifyErr));
    }
    return;
  }

  throw new Error(`Unknown job type "${op}". Use "section" / "write_section" or "assemble".`);
};
