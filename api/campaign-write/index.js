// api/campaign-write/index.js v6 07-11-2025
// Queue-triggered writer that generates each campaign section to
// <container>/<prefix>sections/<section>.json, then assembles <prefix>campaign.json.

const path = require("path");
const crypto = require("crypto");
const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");

// ---- ENV / CONFIG ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const CAMPAIGN_QUEUE = process.env.CAMPAIGN_QUEUE_NAME || "campaign";

const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 60000);

// ---- Small utils ----
function blobSvc() { return BlobServiceClient.fromConnectionString(STORAGE_CONN); }
async function getJson(containerClient, p) {
  const bc = containerClient.getBlobClient(p);
  if (!(await bc.exists())) return null;
  const dl = await bc.download();
  const chunks = []; for await (const ch of dl.readableStreamBody) chunks.push(ch);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
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

// ---- Prompt helpers ----
function safeForPrompt(v, max = 280000) {
  try {
    const s = typeof v === "string" ? v : JSON.stringify(v ?? "");
    if (s.length <= max) return s;
    const k = Math.floor(max / 2);
    return s.slice(0, k) + " …TRUNCATED… " + s.slice(-k);
  } catch { return "null"; }
}
function extractJsonCandidate(s) {
  if (!s) return "";
  const fence = s.match(/```json\s*([\s\S]*?)```/i);
  if (fence && fence[1]) return fence[1].trim();
  const start = s.indexOf("{"); const end = s.lastIndexOf("}");
  if (start !== -1 && end !== -1 && end > start) return s.slice(start, end + 1).trim();
  return s.trim();
}
function tryParseOrRepair(rawText) {
  const candidate = extractJsonCandidate(String(rawText || ""));
  try { return JSON.parse(candidate); } catch { /* repair */ }
  const repaired = candidate
    .replace(/^\uFEFF/, "")
    .replace(/"([^"\\]*(\\.[^"\\]*)*)"/g, m => m.replace(/\r?\n/g, "\\n"))
    .replace(/,\s*([}\]])/g, "$1");
  try { return JSON.parse(repaired); } catch { }
  const err = new Error("draft_json_parse_error: unrecoverable JSON");
  err.code = "draft_json_parse_error";
  err.details = { length: candidate.length, head: candidate.slice(0, 1200), tail: candidate.slice(-1200) };
  throw err;
}

const SECTION_MAP = {
  exec: "executive_summary",
  positioning: "positioning_and_differentiation",
  messaging: "messaging_matrix",
  offer: "offer_strategy",
  channel: "channel_plan",
  risks: "risks_and_contingencies",
  compliance: "compliance_and_governance",
  strategy: "campaign_strategy",
  sales: "sales_enablement",
  one: "one_pager_summary"
};
const SECTION_ORDER = Object.keys(SECTION_MAP);

// ---- LLM adapter (plugs into your harness) ----
let _chat;
async function loadChat() {
  if (_chat) return _chat;
  // eslint-disable-next-line import/no-dynamic-require, global-require
  const mod = require("../lib/prompt-harness");
  const callChatJsonObject = mod.callChatJsonObject || mod.default?.callChatJsonObject || mod.callChat || mod.default?.callChat;
  if (!callChatJsonObject) throw new Error("prompt-harness missing callChatJsonObject()");
  _chat = { callChatJsonObject };
  return _chat;
}
async function callChatJsonObject(opts) {
  const { callChatJsonObject } = await loadChat();
  return callChatJsonObject(opts);
}

// ---- Evidence → bundle helpers ----
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

// ---- Section prompt builders ----
function buildSectionSystem(finalKey, persona) {
  // Persona prefix (optional)
  const personaPrefix = persona ? `PERSONA\n${persona}\n\n` : "";

  // Executive Summary — add addressable market + ordering
  if (finalKey === "executive_summary") {
    return [
      personaPrefix + "You are a senior UK B2B strategist.",
      "Write a board-ready Executive Summary (≤ 250 words) that enables a go/no-go decision.",
      "Begin in this exact order: 1) Strategy (one short paragraph). 2) Target prospects (one short paragraph). 3) Buyer problems (one short paragraph). 4) Campaign type (upsell/win-back/growth, with one-line rationale).",
      "Then include: Moore value proposition that explicitly includes the target; Addressable market size (use the CSV row count if provided); Dependencies; Decision points; and a Sales enablement note.",
      "Ground competitive/market statements in provided evidence only; cite claim_ids inline where used. Use 'TBD' if unknown. No fabrication.",
      `Generate STRICT JSON only for "${finalKey}" matching the 'targets' shape (array of strings).`,
      "Return JSON only; no markdown."
    ].join("\n");
  }

  // Campaign Strategy
  if (finalKey === "campaign_strategy") {
    return [
      personaPrefix + "You are a senior UK B2B strategist.",
      "You are formulating a campaign strategy for a technology supplier.",
      "Base your reasoning strictly on the supplied evidence and inputs.",
      "Deliver a coherent, practical plan that positions the supplier to win within the chosen prospect base.",
      "",
      "Cover these items explicitly, as concise bullets (≤ 220 words total):",
      "• Strategic rationale — why the supplier should play in this market.",
      "• Advantage — how the supplier can be better than competitors (specific differentiators).",
      "• Coherent choices — the concrete actions and constraints for the campaign (segments, offer, channels, messaging, sequencing).",
      "• Feasibility — practical enablers/limits (teams, systems, dependencies).",
      "• Specific expected outcome — quantified KPI/timeframe if available; otherwise 'TBD'.",
      "",
      `Generate STRICT JSON only for the requested section "${finalKey}".`,
      "Return JSON only, no markdown fences. Do NOT include keys for other sections.",
      "",
      "CITATION RULE (inline, end of sentences using external evidence):",
      "Use short tags in parentheses: (Company site), (LinkedIn), (PDF extract), (Directory), (Regulator), (Trade press).",
      "",
      "STYLE: UK English, concise, specific, evidence-led. Prefer concrete buyer outcomes with inline citations where used.",
      "VALIDATION: All URLs https. Arrays required by the schema contain at least 1 item if any evidence exists."
    ].join("\n");
  }

  // Default system for other sections remains concise & evidence-led
  return [
    personaPrefix + "You are a senior UK B2B strategist and sales enablement writer.",
    "Base everything on the supplied outline notes, evidence_log (use claim_ids), and CSV signals. No fabrication.",
    `Generate STRICT JSON only for the requested section "${finalKey}".`,
    "Return JSON only; no markdown fences."
  ].join("\n");
}

function targetsFor(finalKey) {
  // Minimal JSON shape contracts per section
  if (finalKey === "sales_enablement") {
    return `{
  "sales_enablement": {
    "campaign_summary_for_sales": {
      "rationale": "<why this campaign (evidence-led)>",
      "strategy_and_objective": "<one paragraph, specific>",
      "target_prospect_overview": ["<why these companies / segments>"],
      "included_services_and_why": ["<service: reason tied to buyer need or evidence>"],
      "competitor_landscape": ["<short points with claim_ids where applicable>"]
    },
    "master_sales_pitch": {
      "opening": "<buyer-centric opener>",
      "problem": "<pain reframed in buyer terms>",
      "value": "<our value with proof cues>",
      "example": "<concise customer example if available; otherwise 'no external citation available'>",
      "call_to_action": "<clear next step>"
    },
    "discovery_questions": [
      { "question": "<text>", "why_it_matters": "<explanation for the rep>" }
    ]
  }
}`;
  }
  if (finalKey === "positioning_and_differentiation") {
    return `{
   "positioning_and_differentiation": {
    "moore_value_prop": "For <target> who <compelling-need>, <Supplier> is a <category> that <key benefit>. Unlike <primary competitor(s)>, our product/service <key differentiator>.",
    "competitor_contrast": [
      {
        "vendor": "<name>",
        "what_they_do": "<one line>",
        "our_differentiators": ["<short, concrete items tied to our services>"],
        "evidence_claim_ids": ["<claim_id>", "<claim_id>"]
      }
    ]
  }
}`;
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

  // Addressable market from CSV (row count)
  const addressable_market =
    Number.isFinite(Number(csvCanon?.meta?.rows)) ? Number(csvCanon.meta.rows) : null;

  if (finalKey === "sales_enablement") {
    const inNotes = (outline && outline.input_notes) ? outline.input_notes : {};
    const supplier = (inNotes.supplier_company || inNotes.prospect_company || "").trim();

    const outlineNotes = safeForPrompt(outline?.sections?.sales || {});
    return `
Context:
- Supplier: ${supplier || "unknown"}
- Services (anchors): ${safeForPrompt(services)}
- Named competitors (if any): ${safeForPrompt(competitors)}
- Evidence catalog ARRAY (use claim_ids where citing): ${ev}
- CSV signals: ${csv}
- Product anchors: ${prod}
- Campaign objective (from UI): ${objective}
- Outline fragment for this section: ${outlineNotes}

Instructions:
- Write for frontline salespeople: clear, brief, usable.
- Campaign summary must include rationale, strategy & objective, target prospect overview (why these companies), services included and why, and competitor landscape.
- Master sales pitch: buyer-centric, compelling, realistic; include proof cues if evidence available.
- Discovery questions: include an explanation 'why_it_matters' for each so a rep knows why they’re asking.
- Use claim_ids where you cite external evidence; do not fabricate.

Emit exactly the following JSON object for "sales_enablement":
${targetsFor("sales_enablement")}

Return JSON only.
`.trim();
  }

  if (finalKey === "executive_summary") {
    const inNotes = (outline && outline.input_notes) ? outline.input_notes : {};
    const supplier = (inNotes.supplier_company || inNotes.prospect_company || "").trim();
    const outlineNotes = safeForPrompt(outline?.sections?.exec || {});
    return `
Context:
- Supplier: ${supplier || "unknown"}
- Evidence catalog ARRAY (use claim_ids where citing): ${ev}
- CSV signals (include rowCount if present): ${csv}
- Addressable market (row count if available): ${addressable_market ?? "unknown"}
- Campaign objective (from UI): ${objective}
- Outline fragment for this section: ${outlineNotes}

Instructions:
- Follow the ordering and rules in the system message.
- Explicitly state the addressable market size using the CSV row count if available.
- Use claim_ids where you cite external evidence; do not fabricate.

Emit exactly the following JSON object for "executive_summary":
${targetsFor("executive_summary")}

Return JSON only.
`.trim();
  }

  const outlineNotes = safeForPrompt(outline?.sections || {});
  return `
Context:
- Evidence catalog ARRAY (use claim_ids where citing): ${ev}
- CSV signals: ${csv}
- Product anchors: ${prod}
- Named competitors (if any): ${competitors}
- Outline fragment for this section: ${outlineNotes}

Instructions:
- Produce concise, practical content aligned to the section.
- Use claim_ids where you cite external evidence; no fabrication.

Emit exactly the following JSON object for "${finalKey}":
${targetsFor(finalKey)}

Return JSON only.
`.trim();
}

// ---- Main handler ----
module.exports = async function (context, queueItem) {
  const svc = blobSvc();
  const container = svc.getContainerClient(CONTAINER);

  if (!queueItem) {
    context.log.error("[campaign-write] Empty queue message");
    return;
  }
  const op = (queueItem.op || queueItem.type || "").toLowerCase();
  const runId = queueItem.runId || queueItem?.data?.runId || queueItem?.id;
  if (!runId) {
    context.log.error("[campaign-write] Missing runId in queue message");
    return;
  }
  const outlineSectionRaw = queueItem.section || ""; // may be blank for assemble jobs

  // Resolve authoritative container-relative prefix
  let prefix = (typeof queueItem.prefix === "string" && queueItem.prefix.trim())
    ? queueItem.prefix.trim()
    : `runs/${runId}/`;
  if (prefix.startsWith(`${CONTAINER}/`)) prefix = prefix.slice(`${CONTAINER}/`.length);
  if (prefix.startsWith("/")) prefix = prefix.replace(/^\/+/, "");
  if (!prefix.endsWith("/")) prefix += "/";

  // Load inputs
  const evidenceLog = await getJson(container, `${prefix}evidence_log.json`);
  const csvCanon = await getJson(container, `${prefix}csv_normalized.json`);
  const productsObj = await getJson(container, `${prefix}products.json`);
  const site = await getJson(container, `${prefix}site.json`);
  const outline = await getJson(container, `${prefix}outline.json`);

  const evidenceBundle = makeEvidenceBundle({
    evidenceLog: Array.isArray(evidenceLog) ? evidenceLog : [],
    csvCanon: csvCanon || {},
    productNames: Array.isArray(productsObj?.products) ? productsObj.products : []
  });

  // Assemble
  if (op === "assemble") {
    await patchStatus(container, prefix, "Assemble", {
      runId,
      assembleStartedAt: new Date().toISOString()
    });

    const selectedIndustry =
      (csvCanon && csvCanon.industry_mode === "specific" && csvCanon.selected_industry)
        ? String(csvCanon.selected_industry || "").toLowerCase() || "general"
        : (outline?.meta?.selected_industry || "general");

    const outlineSha = sha256OfJson(outline);
    const evidenceSha = sha256OfJson(evidenceLog);
    const csvSha = sha256OfJson(csvCanon || {});
    const siteSha = sha256OfJson(site || {});
    const evidenceCounts = {
      website: Array.isArray(site?.pages) ? site.pages.length : 0,
      products: Array.isArray(productsObj?.products) ? productsObj.products.length : 0,
      case_studies: Array.isArray(await getJson(container, `${prefix}pdf_extracts.json`)) ? (await getJson(container, `${prefix}pdf_extracts.json`)).length : 0
    };

    const merged = {
      meta: {
        run_id: runId,
        phase: "Completed",
        selected_industry: selectedIndustry
      },
      input_proof: {
        outline_sha256: outlineSha,
        evidence_log_sha256: evidenceSha,
        csv_normalized_sha256: csvSha,
        site_sha256: siteSha,
        evidence_counts: evidenceCounts
      },
      evidence_log: Array.isArray(evidenceLog) ? evidenceLog : []
    };

    for (const outlineKey of SECTION_ORDER) {
      const finalKey = SECTION_MAP[outlineKey];
      const obj = await getJson(container, `${prefix}sections/${finalKey}.json`);
      if (obj && obj[finalKey] != null) merged[finalKey] = obj[finalKey];
      else context.log.warn(`[campaign-write] missing section during assemble: ${finalKey}`);
    }

    await putJson(container, `${prefix}campaign.json`, merged);
    await patchStatus(container, prefix, "Completed", { assembledAt: new Date().toISOString() });

    // Notify orchestrator (afterassemble)
    try {
      const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
      const qc = qs.getQueueClient(CAMPAIGN_QUEUE);
      await qc.createIfNotExists();
      const page = (queueItem && (queueItem.page || queueItem?.data?.page)) || "campaign";
      const msg = { op: "afterassemble", runId, page, prefix };
      await qc.sendMessage(JSON.stringify(msg));
    } catch (notifyErr) {
      context.log.warn("[campaign-write] notify afterassemble failed", String(notifyErr?.message || notifyErr));
    }
    return;
  }

  // Section generation
  if (op === "write_section" || op === "section") {
    const outlineKey = String(outlineSectionRaw || "").toLowerCase();
    if (!SECTION_MAP[outlineKey]) {
      throw new Error(`Unknown section "${outlineSectionRaw}". Expected one of: ${Object.keys(SECTION_MAP).join(", ")}`);
    }
    const finalKey = SECTION_MAP[outlineKey];

    await patchStatus(container, prefix, "SectionWrites", {
      runId,
      writing: finalKey,
      updatedAt: new Date().toISOString()
    });

    // Persona from outline meta (optional)
    const persona = (outline && outline.meta && outline.meta.persona) ? outline.meta.persona : "";

    const system = buildSectionSystem(finalKey, persona);
    const user = buildSectionUser(finalKey, { outline, evidenceBundle, csvCanon });

    const obj = await callChatJsonObject({ system, user, timeoutMs: LLM_TIMEOUT_MS });
    const filtered = filterToSection(finalKey, obj);

    await putJson(container, `${prefix}sections/${finalKey}.json`, filtered);
    await patchStatus(container, prefix, "SectionWrites", {
      runId,
      written: finalKey,
      updatedAt: new Date().toISOString()
    });
    return;
  }

  // Unknown op
  throw new Error(`Unknown job type "${op}". Use "write_section" or "assemble".`);
};

function filterToSection(finalKey, obj) {
  const out = {};
  if (obj && typeof obj === "object" && obj[finalKey] != null) out[finalKey] = obj[finalKey];
  else out[finalKey] = obj || {};
  return out;
}
function sha256OfJson(o) {
  const h = crypto.createHash("sha256");
  h.update(Buffer.from(JSON.stringify(o || {})));
  return h.digest("hex");
}
