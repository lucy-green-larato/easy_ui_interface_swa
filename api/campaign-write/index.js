// /api/campaign-write/index.js 07-11-2025 v10 (Option B – Writer/Assembler)
// Queue-triggered on %Q_CAMPAIGN_WRITE%.
// - op: "section" | "write_section"  -> writes sections/<finalKey>.json
// - op: "assemble"                   -> stitches campaign.json and sends {op:"afterassemble"} to %CAMPAIGN_QUEUE_NAME%

const path = require("path");
const crypto = require("crypto");
const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");

// ==== ENV / CONFIG ====
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const MAIN_QUEUE = process.env.CAMPAIGN_QUEUE_NAME || "campaign";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 60000);

// Final section keys (exact, stable set)
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

function nowISO() { return new Date().toISOString(); }

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
  return next; // return to allow callers to check markers etc.
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
  } catch {
    const url = path.join(__dirname, "../lib/prompt-harness.mjs");
    const esm = await import(url);
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
// Executive Summary must be ARRAY OF STRINGS so the UI can render it fully.
function buildSectionSystem(finalKey, persona) {
  const personaPrefix = persona ? `PERSONA\n${persona}\n\n` : "";

  if (finalKey === "executive_summary") {
    return [
      personaPrefix + "You are a senior UK B2B strategist.",
      "Write a board-ready Executive Summary (≤250 words) for a go/no-go decision.",
      "Begin exactly in this order (one compact paragraph): Strategy → Target prospects → Buyer problems → Campaign type (upsell/win-back/growth + one-line rationale).",
      "Then add 3–6 concise bullets covering: Moore value proposition; Addressable market (CSV row count if present); Dependencies; Decision points; Sales enablement note.",
      "Ground claims in evidence only; cite claim_ids inline; use 'TBD' if unknown.",
      'Return STRICT JSON: executive_summary as an array of strings (first = paragraph, rest = bullets).'
    ].join("\n");
  }

  if (finalKey === "positioning_and_differentiation") {
    return [
      personaPrefix + "You are a senior UK B2B strategist.",
      "Provide Geoffrey Moore’s value proposition and a competitor contrast table.",
      "Ground claims in evidence only; include claim_ids inline.",
      'Return STRICT JSON for "positioning_and_differentiation".'
    ].join("\n");
  }

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
      "• Coherent choices — the concrete actions and constraints that define the campaign (segments, offer, channels, messaging, sequencing).",
      "• Feasibility — practical enablers/limits (teams, systems, dependencies).",
      "• Specific expected outcome — quantified KPI/timeframe if available; otherwise 'TBD'.",
      "",
      "Rules:",
      "• No fabrication. Cite only what is supported by inputs/evidence.",
      "• Prefer specifics over generalities; avoid marketing fluff.",
      "• Keep to bullets; do not repeat headings in prose.",
      "",
      `Generate STRICT JSON only for the requested section "${finalKey}".`,
      "Return JSON only, no markdown fences. Do NOT include keys for other sections.",
      "",
      "CITATION RULE (inline, end of sentences using external evidence):",
      "Use short tags in parentheses: (Company site), (LinkedIn), (CSV), (Ofcom), (ONS), (DSIT), (PDF extract), (Trade press), (Directory).",
      "",
      "STYLE: UK English, concise, specific, evidence-led. Prefer concrete buyer outcomes with inline citations where used.",
      "VALIDATION: All URLs https. Arrays required by the schema must be present (empty if necessary). No invented numbers/sources; write 'no external citation available' if needed."
    ].join("\n");
  }

  if (finalKey === "sales_enablement") {
    return [
      (persona ? `PERSONA\n${persona}\n\n` : "") + "You are a senior UK B2B sales enablement lead.",
      "Produce a practical pack for salespeople that they can use immediately.",
      "Include: campaign rationale, campaign strategy & objective, target prospect overview (why these companies), services included and why, competitor landscape; a master sales pitch; and discovery questions each with a brief explanation of why it matters.",
      "Ground content in provided evidence and CSV signals; no fabrication.",
      "",
      `Generate STRICT JSON only for "${finalKey}" and match the target shapes.`,
      "Return JSON only; no markdown."
    ].join("\n");
  }
}

function targetsFor(finalKey) {
  if (finalKey === "executive_summary") {
    // Array of strings (first = paragraph; rest = bullets)
    return `{
  "executive_summary": [
    "<paragraph: Strategy → Target prospects → Buyer problems → Campaign type>",
    "<bullet 1: Moore value proposition (with claim_ids)>",
    "<bullet 2: Addressable market (CSV row count or 'unknown')>",
    "<bullet 3: Dependencies>",
    "<bullet 4: Decision points>",
    "<bullet 5: Sales enablement note>"
  ]
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
  if (finalKey === "campaign_strategy") {
    return `{
  "campaign_strategy": {
    "strategic_rationale": "<why we should play in this prospect base; tie to CSV addressable_market and evidence (claim_ids)>",
    "advantage": [
      "<how we are better than competitors (specific differentiators; cite claim_ids)>"
    ],
    "coherent_choices": [
      "target_segments: <which segments and why>",
      "offer_outline: <what we will offer and why>",
      "primary_channels: <channels to prioritise and why>",
      "messaging_focus: <core themes anchored to evidence>",
      "sequencing/ordering: <phasing across weeks/waves>"
    ],
    "feasibility": [
      "<key teams/systems/dependencies/constraints>"
    ],
    "expected_outcome": "<quantified KPI & timeframe if available; otherwise 'TBD'>"
  }
}`.trim();
  }
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
- Persona: ${persona}
- Outline fragment: ${outlineNotes}

Instructions:
- First element: one compact paragraph in this order → Strategy, Target prospects, Buyer problems, Campaign type.
- Next elements: 3–6 short bullets (Moore value prop, AM size, Dependencies, Decision points, Sales enablement note).
- Cite claim_ids where you use external evidence. No fabrication.

Emit STRICT JSON:
${targetsFor("executive_summary")}
Return JSON only.`.trim();
  }

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
- Persona: ${persona}
- Outline fragment: ${outlineNotes}

Instructions:
- Campaign summary for sales; Master sales pitch; Discovery questions (each with 'why_it_matters').
- Cite claim_ids where you use external evidence. No fabrication.

Emit STRICT JSON:
${targetsFor("sales_enablement")}
Return JSON only.`.trim();
  }

  const outlineNotes = safeForPrompt(outline?.sections || {});
  return `
Context:
- Evidence catalog ARRAY (use claim_ids): ${ev}
- CSV signals: ${csv}
- Product anchors: ${prod}
- Named competitors: ${competitors}
- Persona: ${persona}
- Outline fragment: ${outlineNotes}

Instructions:
- Produce concise, practical content aligned to "${finalKey}". Cite claim_ids for external evidence. No fabrication.

Emit STRICT JSON:
${targetsFor(finalKey)}
Return JSON only.`.trim();
}

// ==== Main handler ====
module.exports = async function (context, queueItem) {
  const svc = blobSvc();
  const container = svc.getContainerClient(CONTAINER);
  await container.createIfNotExists();

  if (!queueItem) { context.log.error("[campaign-write] Empty queue message"); return; }

  const opRaw = queueItem.op || queueItem.type || "";
  const op = String(opRaw).toLowerCase();
  const runId = queueItem.runId || queueItem?.data?.runId || queueItem?.id;
  if (!runId) { context.log.error("[campaign-write] Missing runId"); return; }

  let prefix = normalizePrefix(queueItem.prefix) || `runs/${runId}/`;
  if (!prefix) { context.log.error("[campaign-write] Unable to resolve prefix"); return; }

  // Common inputs (best-effort reads; tolerate missing artifacts)
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
    // Phase: Assemble (append to history)
    const status = await patchStatus(container, prefix, "Assemble", { runId, assembleStartedAt: nowISO(), op: "assemble" });

    const selectedIndustry =
      (csvCanon && csvCanon.industry_mode === "specific" && csvCanon.selected_industry)
        ? String(csvCanon.selected_industry || "").toLowerCase() || "general"
        : (outline?.meta?.selected_industry || "general");

    const pdfExtracts = await getJson(container, `${prefix}pdf_extracts.json`);

    // Stitch in the canonical order; warn on missing sections (no throw)
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
      const piece = await getJson(container, `${prefix}sections/${finalKey}.json`);
      if (piece && piece[finalKey] != null) {
        merged[finalKey] = piece[finalKey];
      } else {
        context.log.warn(`[campaign-write] missing section during assemble: ${finalKey}`);
        // Ensure executive_summary is at least an empty array for UI
        if (finalKey === "executive_summary" && !Array.isArray(merged.executive_summary)) {
          merged.executive_summary = [];
        }
      }
    }

    await putJson(container, `${prefix}campaign.json`, merged);
    await patchStatus(container, prefix, "Completed", { assembledAt: nowISO(), op: "assemble" });

    // ---- Notify orchestrator exactly once (anti-loop marker) ----
    try {
      // Check/flip marker to ensure single-shot dispatch
      const postStatus = await getJson(container, `${prefix}status.json`);
      const alreadySent = !!postStatus?.markers?.afterassembleSent;
      if (!alreadySent) {
        const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
        const qc = qs.getQueueClient(MAIN_QUEUE);
        await qc.createIfNotExists();
        const page = (queueItem && (queueItem.page || queueItem?.data?.page)) || "campaign";
        await qc.sendMessage(JSON.stringify({ op: "afterassemble", runId, page, prefix }));

        // set marker
        postStatus.markers = postStatus.markers || {};
        postStatus.markers.afterassembleSent = true;
        await putJson(container, `${prefix}status.json`, postStatus);
      }
    } catch (notifyErr) {
      context.log.warn("[campaign-write] notify afterassemble failed", String(notifyErr?.message || notifyErr));
    }
    return;
  }

  // ---- Generate a single section ----
  if (op === "write_section" || op === "section") {
    let requested = String(queueItem.section || "").trim().toLowerCase();
    const finalKey = FINAL_SECTION_KEYS.includes(requested)
      ? requested
      : SHORT_TO_FINAL[requested];

    if (!finalKey || !FINAL_SECTION_KEYS.includes(finalKey)) {
      throw new Error(`Unknown section "${queueItem.section}". Expected one of: ${FINAL_SECTION_KEYS.join(", ")}`);
    }

    await patchStatus(container, prefix, "SectionWrites", {
      runId, writing: finalKey, updatedAt: nowISO(), op: "section"
    });

    const persona = outline?.meta?.persona || "";
    const system = buildSectionSystem(finalKey, persona);
    const user = buildSectionUser(finalKey, { outline, evidenceBundle, csvCanon });

    const { callChatJsonObject } = await loadPromptHarness();
    const raw = await callChatJsonObject({ system, user, timeoutMs: LLM_TIMEOUT_MS });

    // normalise shape: expect the section key at root
    const out = {};
    if (raw && typeof raw === "object" && raw[finalKey] != null) out[finalKey] = raw[finalKey];
    else out[finalKey] = raw || (finalKey === "executive_summary" ? [] : {});

    // Ensure executive_summary is array (UI contract)
    if (finalKey === "executive_summary" && !Array.isArray(out.executive_summary)) {
      if (typeof out.executive_summary === "string") out.executive_summary = [out.executive_summary];
      else out.executive_summary = [];
    }

    await putJson(container, `${prefix}sections/${finalKey}.json`, out);
    await patchStatus(container, prefix, "SectionWrites", {
      runId, written: finalKey, updatedAt: nowISO(), op: "section"
    });
    return;
  }

  throw new Error(`Unknown job type "${op}". Use "write_section" or "assemble".`);
};
