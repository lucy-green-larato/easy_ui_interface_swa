// /api/campaign-write/index.js v7 07-11-2025 (Option B)
// Queue-triggered writer: generates each section to <container>/<prefix>sections/<section>.json
// and assembles <prefix>campaign.json, then signals orchestrator (afterassemble).

const path = require("path");
const crypto = require("crypto");
const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");

// ==== ENV / CONFIG ====
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const CAMPAIGN_QUEUE = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 60000);

// Final section keys (exact set requested)
const FINAL_SECTION_KEYS = [
  "executive_summary",
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
  one: "one_pager_summary",
  // (we do NOT include campaign_strategy in the final stitched set)
};

// ==== Small utils ====
function blobSvc() { return BlobServiceClient.fromConnectionString(STORAGE_CONN); }
async function getJson(containerClient, p) {
  const bc = containerClient.getBlobClient(p);
  if (!(await bc.exists())) return null;
  const dl = await bc.download();
  const chunks = []; for await (const ch of dl.readableStreamBody) chunks.push(ch);
  const s = Buffer.concat(chunks).toString("utf8");
  try { return s ? JSON.parse(s) : null; } catch { return null; }
}
async function putJson(containerClient, p, obj) {
  const bb = containerClient.getBlockBlobClient(p);
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await bb.uploadData(body, { blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" } });
}
async function patchStatus(containerClient, prefix, state, extra = {}) {
  const p = `${prefix}status.json`;
  const cur = (await getJson(containerClient, p)) || {};
  const next = { ...cur, state, ...extra };
  await putJson(containerClient, p, next);
}
function normalizePrefix(p) {
  let x = String(p || "").trim();
  if (!x) return null;
  if (x.startsWith(`${CONTAINER}/`)) x = x.slice(`${CONTAINER}/`.length);
  if (x.startsWith("/")) x = x.replace(/^\/+/, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}
function sha256OfJson(o) {
  const h = crypto.createHash("sha256");
  h.update(Buffer.from(JSON.stringify(o || {})));
  return h.digest("hex");
}
function safeForPrompt(v, max = 280000) {
  try {
    const s = typeof v === "string" ? v : JSON.stringify(v ?? "");
    if (s.length <= max) return s;
    const k = Math.floor(max / 2);
    return s.slice(0, k) + " …TRUNCATED… " + s.slice(-k);
  } catch { return "null"; }
}

// ==== Harness loader (CJS → ESM) ====
let _ph;
async function loadPromptHarness() {
  if (_ph) return _ph;
  try {
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const mod = require("../lib/prompt-harness");
    const callChatJsonObject =
      mod.callChatJsonObject || mod.default?.callChatJsonObject || mod.callChat || mod.default?.callChat;
    if (typeof callChatJsonObject !== "function") throw new Error("prompt-harness missing callChatJsonObject()");
    _ph = { callChatJsonObject };
    return _ph;
  } catch (e1) {
    const modUrl = new URL("../lib/prompt-harness.js", `file://${__dirname}/`);
    const esm = await import(modUrl.href);
    const callChatJsonObject =
      esm.callChatJsonObject || esm.default?.callChatJsonObject || esm.callChat || esm.default?.callChat;
    if (typeof callChatJsonObject !== "function") throw new Error("prompt-harness missing callChatJsonObject()");
    _ph = { callChatJsonObject };
    return _ph;
  }
}

// ==== Evidence → bundle ====
function deriveSignalsFromCsv(csvCanon) {
  if (!csvCanon || typeof csvCanon !== "object") return { top_blockers: [], top_needs: [], top_purchases: [] };
  const sig = csvCanon.signals || {};
  return {
    top_blockers: Array.isArray(sig.top_blockers) ? sig.top_blockers : [],
    top_needs: Array.isArray(sig.top_needs) ? sig.top_needs : (Array.isArray(sig.top_needs_supplier) ? sig.top_needs_supplier : []),
    top_purchases: Array.isArray(sig.top_purchases) ? sig.top_purchases : []
  };
}
function makeEvidenceBundle({ evidenceLog, csvCanon, productNames }) {
  const catalog = Array.isArray(evidenceLog) ? evidenceLog : [];
  const signals = deriveSignalsFromCsv(csvCanon);
  const productNamesArr = Array.isArray(productNames) ? productNames : [];
  return { catalog, signals, productNames: productNamesArr };
}

// ==== Prompts ====
function buildSectionSystem(finalKey, persona) {
  const personaPrefix = persona ? `PERSONA\n${persona}\n\n` : "";

  if (finalKey === "executive_summary") {
    return [
      personaPrefix + "You are a senior UK B2B strategist.",
      "Write a board-ready Executive Summary (≤250 words) for a go/no-go decision.",
      "Begin exactly in this order: 1) Strategy. 2) Target prospects. 3) Buyer problems. 4) Campaign type (upsell/win-back/growth + one-line rationale).",
      "Then include: Moore value proposition; Addressable market size (use CSV row count if provided); Dependencies; Decision points; Sales enablement note.",
      "Ground claims in evidence only; cite claim_ids inline; use 'TBD' if unknown. Return JSON only.",
      `Generate STRICT JSON for "${finalKey}" as an object, not markdown.`
    ].join("\n");
  }

  if (finalKey === "positioning_and_differentiation") {
    return [
      personaPrefix + "You are a senior UK B2B strategist.",
      "Produce Geoffrey Moore’s value proposition and add competitor contrast.",
      "Ground claims in evidence only; cite claim_ids inline; Return JSON only."
    ].join("\n");
  }

  if (finalKey === "sales_enablement") {
    return [
      personaPrefix + "You are a senior UK B2B sales enablement writer.",
      "Include rationale for discovery questions ('why_it_matters').",
      "Use evidence claim_ids where relevant. Return JSON only."
    ].join("\n");
  }

  return [
    personaPrefix + "You are a senior UK B2B strategist and sales enablement writer.",
    "Base everything on outline notes, evidence (use claim_ids), and CSV signals. No fabrication.",
    `Generate STRICT JSON only for "${finalKey}".`
  ].join("\n");
}

function targetsFor(finalKey) {
  if (finalKey === "sales_enablement") {
    return `{
  "sales_enablement": {
    "campaign_summary_for_sales": {
      "rationale": "<why this campaign (evidence-led)>",
      "strategy_and_objective": "<one paragraph>",
      "target_prospect_overview": ["<why these companies/segments>"],
      "included_services_and_why": ["<service: reason tied to buyer need/evidence>"],
      "competitor_landscape": ["<points with claim_ids where applicable>"]
    },
    "master_sales_pitch": {
      "opening": "<buyer-centric>",
      "problem": "<in buyer terms>",
      "value": "<our value with proof cues>",
      "example": "<concise customer example or 'TBD'>",
      "call_to_action": "<clear next step>"
    },
    "discovery_questions": [
      { "question": "<text>", "why_it_matters": "<explanation for the rep>" }
    ]
  }
}`.trim();
  }

  if (finalKey === "positioning_and_differentiation") {
    return `{
  "positioning_and_differentiation": {
    "moore_value_prop": "For <target> who <need>, <Supplier> is a <category> that <key benefit>. Unlike <competitor(s)>, our <service> <differentiator>.",
    "competitor_contrast": [
      {
        "vendor": "<name>",
        "what_they_do": "<one line>",
        "our_differentiators": ["<short, concrete items>"],
        "evidence_claim_ids": ["<claim_id>"]
      }
    ]
  }
}`.trim();
  }

  if (finalKey === "executive_summary") {
    return `{
  "executive_summary": {
    "targets": ["<ordered bullets per instructions>"]
  }
}`.trim();
  }

  return `{"${finalKey}": {}}`;
}

function buildSectionUser(finalKey, { outline, evidenceBundle, csvCanon }) {
  const ev = safeForPrompt(evidenceBundle?.catalog || []);
  const csv = safeForPrompt(csvCanon || {});
  const prod = safeForPrompt(evidenceBundle?.productNames || []);
  const competitors = safeForPrompt((outline?.input_notes?.relevant_competitors || []).slice(0, 8));
  const services = safeForPrompt(outline?.input_notes?.supplier_usps || []);
  const objective = safeForPrompt(outline?.input_notes?.campaign_requirement || "");
  const persona = safeForPrompt(outline?.meta?.persona || "");
  const addressable_market = Number.isFinite(Number(csvCanon?.meta?.rows)) ? Number(csvCanon.meta.rows) : null;

  if (finalKey === "sales_enablement") {
    const supplier = (outline?.input_notes?.supplier_company || outline?.input_notes?.prospect_company || "").trim();
    const outlineNotes = safeForPrompt(outline?.sections?.sales || {});
    return `
Context:
- Supplier: ${supplier || "unknown"}
- Services anchors: ${services}
- Named competitors: ${competitors}
- Evidence catalog ARRAY (use claim_ids): ${ev}
- CSV signals: ${csv}
- Product anchors: ${prod}
- Campaign objective: ${objective}
- Outline fragment: ${outlineNotes}

Instructions:
- Campaign summary (rationale, strategy & objective, target prospect overview, included services & why, competitor landscape).
- Master sales pitch (opening, problem, value, example, call_to_action).
- Discovery questions: include 'why_it_matters' for each.
- Cite claim_ids where you use external evidence. No fabrication.

Emit STRICT JSON for "sales_enablement":
${targetsFor("sales_enablement")}
Return JSON only.`.trim();
  }

  if (finalKey === "executive_summary") {
    const supplier = (outline?.input_notes?.supplier_company || outline?.input_notes?.prospect_company || "").trim();
    const outlineNotes = safeForPrompt(outline?.sections?.exec || {});
    return `
Context:
- Supplier: ${supplier || "unknown"}
- Evidence catalog ARRAY (use claim_ids): ${ev}
- CSV signals: ${csv}
- Addressable market (row count): ${addressable_market ?? "unknown"}
- Campaign objective: ${objective}
- Outline fragment: ${outlineNotes}

Instructions:
- Follow system ordering. Explicitly state addressable market size using CSV row count if available.
- Cite claim_ids for external evidence.

Emit STRICT JSON for "executive_summary":
${targetsFor("executive_summary")}
Return JSON only.`.trim();
  }

  const outlineNotes = safeForPrompt(outline?.sections || {});
  return `
Context:
- Evidence catalog ARRAY (use claim_ids): ${ev}
- CSV signals: ${csv}
- Product anchors: ${prod}
- Named competitors: ${competitors}
- Outline fragment: ${outlineNotes}

Instructions:
- Produce concise, practical content aligned to "${finalKey}".
- Cite claim_ids where you use external evidence. No fabrication.

Emit STRICT JSON for "${finalKey}":
${targetsFor(finalKey)}
Return JSON only.`.trim();
}

// ==== Main handler ====
module.exports = async function (context, queueItem) {
  const svc = blobSvc();
  const container = svc.getContainerClient(CONTAINER);

  if (!queueItem) { context.log.error("[campaign-write] Empty queue message"); return; }

  const op = String(queueItem.op || queueItem.type || "").toLowerCase();
  const runId = queueItem.runId || queueItem?.data?.runId || queueItem?.id;
  if (!runId) { context.log.error("[campaign-write] Missing runId"); return; }

  let prefix = normalizePrefix(queueItem.prefix) || `runs/${runId}/`;
  if (!prefix) { context.log.error("[campaign-write] Unable to resolve prefix"); return; }

  // Common inputs
  const [evidenceLog, csvCanon, productsObj, site, outline] = await Promise.all([
    getJson(container, `${prefix}evidence_log.json`),
    getJson(container, `${prefix}csv_normalized.json`),
    getJson(container, `${prefix}products.json`),
    getJson(container, `${prefix}site.json`),
    getJson(container, `${prefix}outline.json`)
  ]);

  const evidenceBundle = makeEvidenceBundle({
    evidenceLog: Array.isArray(evidenceLog) ? evidenceLog : [],
    csvCanon: csvCanon || {},
    productNames: Array.isArray(productsObj?.products) ? productsObj.products : []
  });

  // ---- Assemble whole campaign ----
  if (op === "assemble") {
    await patchStatus(container, prefix, "Assemble", { runId, assembleStartedAt: new Date().toISOString() });

    const selectedIndustry =
      (csvCanon && csvCanon.industry_mode === "specific" && csvCanon.selected_industry)
        ? String(csvCanon.selected_industry || "").toLowerCase() || "general"
        : (outline?.meta?.selected_industry || "general");

    const pdfExtracts = await getJson(container, `${prefix}pdf_extracts.json`);
    const merged = {
      meta: {
        run_id: runId,
        phase: "Completed",
        selected_industry: selectedIndustry
      },
      input_proof: {
        outline_sha256: sha256OfJson(outline),
        evidence_log_sha256: sha256OfJson(evidenceLog),
        csv_normalized_sha256: sha256OfJson(csvCanon || {}),
        site_sha256: sha256OfJson(site || {}),
        evidence_counts: {
          website: Array.isArray(site?.pages) ? site.pages.length : 0,
          products: Array.isArray(productsObj?.products) ? productsObj.products.length : 0,
          case_studies: Array.isArray(pdfExtracts) ? pdfExtracts.length : 0
        }
      },
      evidence_log: Array.isArray(evidenceLog) ? evidenceLog : []
    };

    for (const finalKey of FINAL_SECTION_KEYS) {
      const obj = await getJson(container, `${prefix}sections/${finalKey}.json`);
      if (obj && obj[finalKey] != null) merged[finalKey] = obj[finalKey];
      else context.log.warn(`[campaign-write] missing section during assemble: ${finalKey}`);
    }

    await putJson(container, `${prefix}campaign.json`, merged);
    await patchStatus(container, prefix, "Completed", { assembledAt: new Date().toISOString() });

    // notify orchestrator
    try {
      const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
      const qc = qs.getQueueClient(CAMPAIGN_QUEUE);
      await qc.createIfNotExists();
      const page = (queueItem && (queueItem.page || queueItem?.data?.page)) || "campaign";
      await qc.sendMessage(JSON.stringify({ op: "afterassemble", runId, page, prefix }));
    } catch (notifyErr) {
      context.log.warn("[campaign-write] notify afterassemble failed", String(notifyErr?.message || notifyErr));
    }
    return;
  }

  // ---- Generate a single section ----
  if (op === "write_section" || op === "section") {
    let requested = String(queueItem.section || "").trim().toLowerCase();
    // Accept final keys directly; fall back to short-code mapping
    const finalKey = FINAL_SECTION_KEYS.includes(requested)
      ? requested
      : SHORT_TO_FINAL[requested];

    if (!finalKey || !FINAL_SECTION_KEYS.includes(finalKey)) {
      throw new Error(
        `Unknown section "${queueItem.section}". Expected one of: ${FINAL_SECTION_KEYS.join(", ")}`
      );
    }

    await patchStatus(container, prefix, "SectionWrites", {
      runId, writing: finalKey, updatedAt: new Date().toISOString()
    });

    const persona = outline?.meta?.persona || "";
    const system = buildSectionSystem(finalKey, persona);
    const user = buildSectionUser(finalKey, { outline, evidenceBundle, csvCanon });

    const { callChatJsonObject } = await loadPromptHarness();
    const obj = await callChatJsonObject({ system, user, timeoutMs: LLM_TIMEOUT_MS });

    const out = {};
    if (obj && typeof obj === "object" && obj[finalKey] != null) out[finalKey] = obj[finalKey];
    else out[finalKey] = obj || {};

    await putJson(container, `${prefix}sections/${finalKey}.json`, out);
    await patchStatus(container, prefix, "SectionWrites", {
      runId, written: finalKey, updatedAt: new Date().toISOString()
    });
    return;
  }

  throw new Error(`Unknown job type "${op}". Use "write_section" or "assemble".`);
};
