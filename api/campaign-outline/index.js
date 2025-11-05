// api/campaign-outline/index.js // v5 · 05-11-2025
// Queue-triggered function that builds a prose-free campaign outline
// Inputs:  <container>/<prefix>{evidence_log.json, csv_normalized.json, products.json, site.json, input.json}
// Output:  <container>/<prefix>outline.json
// Status:  Outline

const { BlobServiceClient } = require("@azure/storage-blob");
const { QueueServiceClient } = require("@azure/storage-queue");
const path = require("path");
const os = require("os");
const fs = require("fs");

// ---- CONFIG ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 45000);
const CAMPAIGN_QUEUE = process.env.CAMPAIGN_QUEUE_NAME || "campaign";

// ---- Minimal JSON schema the model must obey (no prose; claim_ids only) ----
const OUTLINE_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  title: "campaign_outline",
  type: "object",
  additionalProperties: false,
  required: ["meta", "sections"],
  properties: {
    meta: {
      type: "object",
      additionalProperties: false,
      required: ["run_id", "phase", "selected_industry"],
      properties: {
        run_id: { type: "string" },
        phase: { type: "string", enum: ["Outline"] },
        selected_industry: { type: "string" }
      }
    },
    input_notes: {
      type: "object",
      additionalProperties: false,
      properties: {
        spend_band: { type: "string" },
        top_blockers: { type: "array", items: { type: "string" } },
        top_needs_supplier: { type: "array", items: { type: "string" } },
        top_purchases: { type: "array", items: { type: "string" } },
        product_mentions: { type: "array", items: { type: "string" } },
        supplier_company: { type: "string" },
        supplier_website: { type: "string" },
        supplier_linkedin: { type: "string" },
        supplier_usps: { type: "array", items: { type: "string" } },
        campaign_industry: { type: "string" },
        campaign_requirement: { type: "string" },
        relevant_competitors: { type: "array", items: { type: "string" } },
        notes: { type: "string" }
      }
    },
    sections: {
      type: "object",
      additionalProperties: false,
      required: ["exec", "positioning", "messaging", "offer", "channel", "risks", "compliance"],
      properties: {
        exec: {
          type: "object",
          additionalProperties: false,
          required: ["why_now_ids", "product_anchor_names"],
          properties: {
            why_now_ids: { type: "array", items: { type: "string" } },
            product_anchor_names: { type: "array", items: { type: "string" } }
          }
        },
        positioning: {
          type: "object",
          additionalProperties: false,
          required: ["differentiator_ids"],
          properties: {
            differentiator_ids: { type: "array", items: { type: "string" } }
          }
        },
        messaging: {
          type: "array",
          items: {
            type: "object",
            additionalProperties: false,
            required: ["persona", "pain_points_from_csv", "claim_ids"],
            properties: {
              persona: { type: "string" },
              pain_points_from_csv: { type: "array", items: { type: "string" } },
              claim_ids: { type: "array", items: { type: "string" } }
            }
          }
        },
        offer: {
          type: "object",
          additionalProperties: false,
          required: ["what_you_get_from_csv", "proof_ids", "outcome_ids"],
          properties: {
            what_you_get_from_csv: { type: "array", items: { type: "string" } },
            proof_ids: { type: "array", items: { type: "string" } },
            outcome_ids: { type: "array", items: { type: "string" } }
          }
        },
        channel: {
          type: "object",
          additionalProperties: false,
          required: ["email_themes", "linkedin_themes"],
          properties: {
            email_themes: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: false,
                required: ["theme", "claim_ids"],
                properties: {
                  theme: { type: "string" },
                  claim_ids: { type: "array", items: { type: "string" } }
                }
              }
            },
            linkedin_themes: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: false,
                required: ["theme", "claim_ids"],
                properties: {
                  theme: { type: "string" },
                  claim_ids: { type: "array", items: { type: "string" } }
                }
              }
            }
          }
        },
        risks: {
          type: "object",
          additionalProperties: false,
          required: ["claim_ids"],
          properties: {
            claim_ids: { type: "array", items: { type: "string" } }
          }
        },
        compliance: {
          type: "object",
          additionalProperties: false,
          required: ["checklist_ids"],
          properties: {
            checklist_ids: { type: "array", items: { type: "string" } }
          }
        }
      }
    }
  }
};

// ---- tiny helpers ----
function blobSvc() {
  if (!STORAGE_CONN) throw new Error("AzureWebJobsStorage not configured");
  return BlobServiceClient.fromConnectionString(STORAGE_CONN);
}
async function streamToString(readable) {
  if (!readable) return "";
  const chunks = [];
  for await (const c of readable) chunks.push(c);
  return Buffer.concat(chunks).toString("utf8");
}
async function getJson(containerClient, blobPath) {
  const b = containerClient.getBlockBlobClient(blobPath);
  if (!(await b.exists())) return null;
  const dl = await b.download();
  const text = await streamToString(dl.readableStreamBody);
  try { return JSON.parse(text); } catch { return null; }
}
async function putJson(containerClient, blobPath, obj) {
  const b = containerClient.getBlockBlobClient(blobPath);
  const buf = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await b.uploadData(buf, { blobHTTPHeaders: { blobContentType: "application/json" } });
}
async function patchStatus(containerClient, prefix, state, extra = {}) {
  const p = `${prefix}status.json`;
  const cur = (await getJson(containerClient, p)) || {};
  const next = { ...cur, state, ...extra };
  await putJson(containerClient, p, next);
}
function safeForPrompt(v, max = 300000) {
  try {
    const s = typeof v === "string" ? v : JSON.stringify(v ?? "");
    if (s.length <= max) return s;
    const keep = Math.floor(max / 2);
    return s.slice(0, keep) + " …TRUNCATED… " + s.slice(-keep);
  } catch { return "null"; }
}
function extractJsonCandidate(s) {
  if (!s) return "";
  const fence = s.match(/```json\s*([\s\S]*?)```/i);
  if (fence && fence[1]) return fence[1].trim();
  const start = s.indexOf("{");
  const end = s.lastIndexOf("}");
  if (start !== -1 && end !== -1 && end > start) return s.slice(start, end + 1).trim();
  return s.trim();
}
function tryParseOrRepair(rawText) {
  const candidate = extractJsonCandidate(String(rawText || ""));
  try { return JSON.parse(candidate); } catch { }
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

// ---- harness (reuse your existing module) ----
function loadHarness() {
  const modPath = path.join(__dirname, "..", "lib", "prompt-harness.js");
  // eslint-disable-next-line global-require, import/no-dynamic-require
  return require(modPath);
}

// Persist a per-run outline schema and return its absolute path
function writeTempOutlineSchema(runId) {
  const tmp = path.join(os.tmpdir(), `outline_${runId}.schema.json`);
  fs.writeFileSync(tmp, JSON.stringify(OUTLINE_SCHEMA), "utf8");
  return tmp; // absolute path
}

// Lightweight product extraction from a single HTML snippet (fallback)
function extractProductsFromSnippet(html) {
  if (!html || typeof html !== "string") return [];
  const lines = html.split(/\r?\n/).slice(0, 2000);
  const out = new Set();
  for (const line of lines) {
    if (/(<h1|<h2|<h3|<li|product|solutions|services)/i.test(line)) {
      const m = line.match(/>([^<>]{3,120})</);
      if (m) {
        const val = m[1].trim();
        if (
          val &&
          !/^(home|about|contact|login|support|learn|blog|cookie|privacy|terms|partners|resources)$/i.test(val) &&
          !/^(read more|learn more)$/i.test(val)
        ) out.add(val);
      }
    }
  }
  return Array.from(out);
}

module.exports = async function (context, queueItem) {
  const startedAt = new Date().toISOString();

  // locals for catch block
  let runId = "unknown";
  let prefix = null;
  let container = null;

  try {
    if (!queueItem) throw new Error("Queue message is empty");
    runId = queueItem.runId || queueItem?.data?.runId || queueItem?.id;
    if (!runId) throw new Error("Missing runId in queue message");

    const svc = blobSvc();
    container = svc.getContainerClient(CONTAINER);

    // ---- Normalize prefix to container-relative (strip container name, fix slashes, ensure trailing) ----
    const rawPrefix = (typeof queueItem.prefix === "string" && queueItem.prefix.trim())
      ? queueItem.prefix.trim()
      : `runs/${runId}/`;
    prefix = rawPrefix.startsWith(`${CONTAINER}/`) ? rawPrefix.slice(`${CONTAINER}/`.length) : rawPrefix;
    if (prefix.startsWith("/")) prefix = prefix.replace(/^\/+/, "");
    if (!prefix.endsWith("/")) prefix += "/";

    const page = queueItem.page || queueItem?.data?.page || "campaign";
    const runConfig = (queueItem && queueItem.runConfig) || {};

    await patchStatus(container, prefix, "Outline", { runId, outlineStartedAt: startedAt });
  
    // ---- Load artifacts ----
    const evidenceRaw = await getJson(container, `${prefix}evidence_log.json`);
    let evidenceLog = Array.isArray(evidenceRaw) ? evidenceRaw
      : (Array.isArray(evidenceRaw?.evidence_log) ? evidenceRaw.evidence_log : []);
    evidenceLog = (evidenceLog || []).filter(it => it && it.claim_id && it.title && it.summary && it.url);
    const evidenceForPrompt = evidenceLog.slice(0, 24);

    const csvNorm = await getJson(container, `${prefix}csv_normalized.json`);
    const siteJson = await (async () => {
      const v = await getJson(container, `${prefix}site.json`);
      return Array.isArray(v) ? v : (v ? [v] : []);
    })();
    const productsFile = await getJson(container, `${prefix}products.json`);
    const input = await getJson(container, `${prefix}input.json`); // ← (bug fix) used below

    // Prefer products.json; fallback to homepage snippet
    let productNames = Array.isArray(productsFile?.products) && productsFile.products.length
      ? productsFile.products
      : (() => {
          const first = siteJson[0]?.snippet || "";
          return extractProductsFromSnippet(first).slice(0, 12);
        })();

    // Supplier / run inputs
    const supplierBlock = {
      supplier_company: (queueItem?.supplier_company || queueItem?.company_name || queueItem?.prospect_company || "")?.trim() || "",
      supplier_website: (queueItem?.supplier_website || queueItem?.company_website || queueItem?.prospect_website || "")?.trim() || "",
      supplier_linkedin: (queueItem?.supplier_linkedin || queueItem?.company_linkedin || queueItem?.prospect_linkedin || "")?.trim() || "",
      supplier_usps: Array.isArray(queueItem?.supplier_usps)
        ? queueItem.supplier_usps
        : (typeof queueItem?.user_usps === "string"
            ? queueItem.user_usps.split(/[,;\n]/).map(s => s.trim()).filter(Boolean).slice(0, 12)
            : []),
      notes: (queueItem?.notes || "")?.trim() || ""
    };

    // ---- CSV → derive mode + signals ----
    const mode = (csvNorm && csvNorm.industry_mode === "specific" && csvNorm.selected_industry)
      ? "specific" : "agnostic";
    const selectedIndustryCsv = (mode === "specific")
      ? String(csvNorm.selected_industry || "").trim().toLowerCase()
      : null;
    const srcSignals = (mode === "specific") ? (csvNorm?.signals || {}) : (csvNorm?.global_signals || {});
    const csvSignal = {
      industry_mode: mode,
      selected_industry: selectedIndustryCsv,
      spend_band: srcSignals?.spend_band ?? null,
      top_blockers: Array.isArray(srcSignals?.top_blockers) ? srcSignals.top_blockers : [],
      top_needs_supplier: Array.isArray(srcSignals?.top_needs_supplier) ? srcSignals.top_needs_supplier : [],
      top_purchases: Array.isArray(srcSignals?.top_purchases) ? srcSignals.top_purchases : []
    };

    // === Persona + Industry selection + System goals (Outline) ===
    const salesModel = String(
      (input?.sales_model ?? input?.salesModel ?? input?.call_type ?? "")
    ).trim().toLowerCase();

    const PARTNER_PERSONA = `
You are a top-performing UK B2B channel strategist (tech markets) and CMO.
You understand partner recruitment, enablement, and common channel constraints (M&A, higher interest rates, lead-gen pressure).
Your job is to produce an evidence-only campaign that adds value to the partners by making them more competitive, improving their customer service, and differentiating them in the market.
`.trim();

    const DIRECT_PERSONA = `
You are a top-performing UK B2B tech market strategist and CMO.
You understand the challenges that your customers face (need better productivity, understand how to invest in the right technologies and why)
Your job is to produce an evidence-only campaign that adds value to direct customers by making them more efficient and productive, improving customer service they provide, and differentiating them in the market. (Key technologies: cybersecurity, artificial intelligence, IoT, mobile data connectivity)
`.trim();

    const PERSONA = (salesModel === "partner") ? PARTNER_PERSONA : DIRECT_PERSONA;

    // String mode
    function modeOfStrings(arr) {
      const counts = new Map();
      for (const v of arr.map(x => String(x || "").trim()).filter(Boolean)) {
        counts.set(v, (counts.get(v) || 0) + 1);
      }
      let best = null, bestN = 0;
      for (const [k, n] of counts.entries()) if (n > bestN) { best = k; bestN = n; }
      return best;
    }
    function inferIndustryFromCsv(csvNormalized) {
      try {
        const top = csvNormalized?.meta?.top_industries;
        if (Array.isArray(top) && top.length) {
          return String(top.sort((a, b) => (b?.count || 0) - (a?.count || 0))[0]?.name || "").trim() || null;
        }
        const rows = Array.isArray(csvNormalized?.rows) ? csvNormalized.rows : [];
        const candidates = [];
        for (const r of rows) candidates.push(r?.industry, r?.buyer_industry, r?.vertical, r?.sector);
        return modeOfStrings(candidates.filter(Boolean)) || null;
      } catch { return null; }
    }

    const userIndustry =
      (input?.selected_industry || input?.campaign_industry || input?.company_industry || "").trim();
    const csvIndustry = (!userIndustry && csvNorm) ? inferIndustryFromCsv(csvNorm) : null;
    const SELECTED_INDUSTRY = userIndustry || csvIndustry || "General";

    const SYSTEM = `
Return STRICTLY valid JSON that conforms to the provided outline schema.
NO prose, NO markdown fences. Use ONLY claim_ids that exist in evidenceLog[].claim_id

PERSONA
${PERSONA}

GOALS
- Use this selected_industry: "${SELECTED_INDUSTRY}".
- Build a campaign OUTLINE only (no sentences): select claim_ids for each section using existing evidence.
- Balance evidence weighting: regulator/government (Ofcom/ONS/DSIT) SHOULD NOT outweigh market signals from CSV and reputable "good sources" — treat them as peers.
- Use product names found in site data only for product_anchor_names.

CONSTRAINTS
- Do not invent claim_ids.
- Keep arrays compact and relevant.
- If a required section has no suitable evidence, leave it empty ([]) rather than inventing content.
`.trim();

    const USER = `
Input: evidence_log (claim catalog; at most 24 items):
${safeForPrompt(evidenceForPrompt)}

Input: csv signals (buyer landscape):
${safeForPrompt(csvSignal)}

Input: site product/name hints (if any):
${safeForPrompt(productNames)}

Input: supplier context (verbatim; do not ignore):
${safeForPrompt(supplierBlock)}

Objective (campaignRequirement):
${safeForPrompt(runConfig.campaign_requirement || "unspecified")}

Competitors (if any):
${safeForPrompt(runConfig.relevant_competitors || [])}

Return only JSON for this outline schema:
{
  "meta": { "run_id": "<string>", "phase": "Outline", "selected_industry": "<string>" },
  "input_notes": {
    "spend_band": "<string or 'unknown'>",
    "top_blockers": ["<from CSV>"],
    "top_needs_supplier": ["<from CSV>"],
    "top_purchases": ["<from CSV>"],
    "product_mentions": ["<from site productNames>"],
    "supplier_company": "<from supplierBlock>",
    "supplier_website": "<from supplierBlock>",
    "supplier_linkedin": "<from supplierBlock>",
    "supplier_usps": ["<from supplierBlock>"],
    "campaign_industry": "<if provided>",
    "campaign_requirement": "<if provided>",
    "relevant_competitors": ["<if provided>"],
    "notes": "<verbatim if provided>"
  },
  "sections": {
    "exec": { "why_now_ids": ["<claim_id>", "..."], "product_anchor_names": ["<product>", "..."] },
    "positioning": { "differentiator_ids": ["<claim_id>", "..."] },
    "messaging": [
      { "persona": "<industry persona>", "pain_points_from_csv": ["..."], "claim_ids": ["<claim_id>", "..."] }
    ],
    "offer": { "what_you_get_from_csv": ["..."], "proof_ids": ["<claim_id>", "..."], "outcome_ids": ["<claim_id>", "..."] },
    "channel": {
      "email_themes": [{ "theme": "<short>", "claim_ids": ["<claim_id>", "..."] }],
      "linkedin_themes": [{ "theme": "<short>", "claim_ids": ["<claim_id>", "..."] }]
    },
    "risks": { "claim_ids": ["<claim_id>", "..."] },
    "compliance": { "checklist_ids": ["<claim_id>", "..."] }
  }
}
`.trim();

    const schemaPathAbs = writeTempOutlineSchema(runId);
    const harness = loadHarness();

    let out;
    try {
      const messages = [
        { role: "system", content: SYSTEM },
        { role: "user", content: USER }
      ];

      out = await harness.generate({
        schemaPath: schemaPathAbs,
        input: {
          industry_mode: csvSignal.industry_mode,
          selected_industry: csvSignal.selected_industry,
          csv_signals: {
            spend_band: csvSignal.spend_band ?? null,
            top_blockers: csvSignal.top_blockers || [],
            top_needs_supplier: csvSignal.top_needs_supplier || [],
            top_purchases: csvSignal.top_purchases || []
          },
          product_names: productNames,
          supplier: supplierBlock
        },
        options: {
          messages,
          timeoutMs: LLM_TIMEOUT_MS,
          azure: {
            endpoint: process.env.AZURE_OPENAI_ENDPOINT,
            apiKey: process.env.AZURE_OPENAI_API_KEY,
            apiVersion: process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview",
            deployment: process.env.AZURE_OPENAI_DEPLOYMENT,
            api: "chat"
          },
          retry: {
            attempts: Number(process.env.LLM_ATTEMPTS ?? 2),
            backoffMs: Number(process.env.LLM_BACKOFF_MS ?? 500)
          },
          temperature: Number(process.env.LLM_TEMPERATURE ?? 0)
        }
      });

      if (typeof out === "string") {
        try { out = JSON.parse(out); } catch { out = tryParseOrRepair(out); }
      }
    } catch (e) {
      const code = e?.code || "draft_error";
      const details = e?.details && typeof e.details === "object" ? e.details : undefined;
      if (details) {
        try {
          await putJson(container, `${prefix}draft_parse_debug.json`, {
            code,
            ...details,
            head: String(details.head || "").slice(0, 4000),
            tail: String(details.tail || "").slice(-4000)
          });
        } catch (dbgErr) {
          context.log.warn("[campaign-outline] draft_parse_debug write failed", String(dbgErr?.message || dbgErr));
        }
      }
      await patchStatus(container, prefix, "Failed", {
        error: { code, message: String(e?.message || e), ...(details ? { details: { length: details.length || null } } : {}) },
        outlineFailedAt: new Date().toISOString()
      });
      context.log.error("[campaign-outline] failed", String(e?.message || e));
      return;
    }

    // ---- Validate / repair outline BEFORE persisting ----
    if (!out || typeof out !== "object") throw new Error("outline_generation_failed: no outline object returned");

    if (!out.meta || typeof out.meta !== "object") out.meta = {};
    out.meta.run_id = out.meta.run_id || runId;
    out.meta.phase = "Outline";

    const outInd = String(out.meta.selected_industry || "").trim().toLowerCase();
    if (mode === "specific") {
      if (!outInd || outInd !== selectedIndustryCsv) {
        context.log.warn("[campaign-outline] forcing meta.selected_industry to CSV industry",
          { csvInd: selectedIndustryCsv, outInd });
        out.meta.selected_industry = selectedIndustryCsv || "general";
      }
    } else {
      out.meta.selected_industry = "General";
    }

    // Ensure input_notes exists and populate once (no duplicates)
    if (!out.input_notes || typeof out.input_notes !== "object") out.input_notes = {};
    const inNotes = out.input_notes;

    // Supplier & notes
    inNotes.supplier_company = inNotes.supplier_company || supplierBlock.supplier_company || queueItem?.prospect_company || "";
    inNotes.supplier_website = inNotes.supplier_website || supplierBlock.supplier_website || queueItem?.prospect_website || "";
    inNotes.supplier_linkedin = inNotes.supplier_linkedin || supplierBlock.supplier_linkedin || queueItem?.prospect_linkedin || "";
    if (!Array.isArray(inNotes.supplier_usps)) inNotes.supplier_usps = [];
    if (inNotes.supplier_usps.length === 0) inNotes.supplier_usps = supplierBlock.supplier_usps.slice(0, 12);
    inNotes.notes = inNotes.notes || supplierBlock.notes || "";

    // Industry / objective / competitors
    inNotes.campaign_industry = inNotes.campaign_industry || input?.campaign_industry || input?.company_industry || "";
    if (typeof inNotes.campaign_requirement !== "string") {
      const req = runConfig?.campaign_requirement;
      if (typeof req === "string" && ["upsell", "win-back", "growth"].includes(req)) {
        inNotes.campaign_requirement = req;
      } else {
        inNotes.campaign_requirement = null;
      }
    }
    if (!Array.isArray(inNotes.relevant_competitors)) inNotes.relevant_competitors = [];
    if (inNotes.relevant_competitors.length === 0 && Array.isArray(runConfig?.relevant_competitors)) {
      inNotes.relevant_competitors = runConfig.relevant_competitors
        .filter(x => typeof x === "string" && x.trim())
        .map(x => x.trim())
        .slice(0, 8);
    }

    // CSV-derived notes and product names
    inNotes.spend_band = inNotes.spend_band ?? (csvSignal.spend_band ?? "unknown");
    if (!Array.isArray(inNotes.top_blockers)) inNotes.top_blockers = [];
    if (!Array.isArray(inNotes.top_needs_supplier)) inNotes.top_needs_supplier = [];
    if (!Array.isArray(inNotes.top_purchases)) inNotes.top_purchases = [];
    if (!Array.isArray(inNotes.product_mentions)) inNotes.product_mentions = [];
    if (inNotes.top_blockers.length === 0) inNotes.top_blockers = csvSignal.top_blockers;
    if (inNotes.top_needs_supplier.length === 0) inNotes.top_needs_supplier = csvSignal.top_needs_supplier;
    if (inNotes.top_purchases.length === 0) inNotes.top_purchases = csvSignal.top_purchases;
    if (inNotes.product_mentions.length === 0 && Array.isArray(productNames) && productNames.length) {
      inNotes.product_mentions = productNames.slice(0, 12);
    }

    // Ensure required section scaffolds
    if (!out.sections || typeof out.sections !== "object") out.sections = {};
    const reqObj = (name) => { if (!out.sections[name] || typeof out.sections[name] !== "object") out.sections[name] = {}; return out.sections[name]; };

    const exec = reqObj("exec");
    if (!Array.isArray(exec.why_now_ids)) exec.why_now_ids = [];
    if (!Array.isArray(exec.product_anchor_names)) exec.product_anchor_names = [];

    const positioning = reqObj("positioning");
    if (!Array.isArray(positioning.differentiator_ids)) positioning.differentiator_ids = [];

    if (!Array.isArray(out.sections.messaging)) out.sections.messaging = [];

    const offer = reqObj("offer");
    if (!Array.isArray(offer.what_you_get_from_csv)) offer.what_you_get_from_csv = [];
    if (!Array.isArray(offer.proof_ids)) offer.proof_ids = [];
    if (!Array.isArray(offer.outcome_ids)) offer.outcome_ids = [];

    const channel = reqObj("channel");
    if (!Array.isArray(channel.email_themes)) channel.email_themes = [];
    if (!Array.isArray(channel.linkedin_themes)) channel.linkedin_themes = [];

    const risks = reqObj("risks");
    if (!Array.isArray(risks.claim_ids)) risks.claim_ids = [];

    const compliance = reqObj("compliance");
    if (!Array.isArray(compliance.checklist_ids)) compliance.checklist_ids = [];

    // ---- Persist + status ----
    await putJson(container, `${prefix}outline.json`, out);
    await patchStatus(container, prefix, "Outline", { outlineCompletedAt: new Date().toISOString() });

    // ---- Notify orchestrator (afteroutline) ----
    try {
      const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
      const qc = qs.getQueueClient(CAMPAIGN_QUEUE);
      await qc.createIfNotExists();
      const msg = { op: "afteroutline", runId, page, prefix };
      await qc.sendMessage(JSON.stringify(msg)); // SDK base64-encodes
    } catch (notifyErr) {
      context.log.warn("[campaign-outline] notify orchestrator failed", String(notifyErr?.message || notifyErr));
    }

    context.log("[campaign-outline] success", { runId });
  } catch (err) {
    context.log.error("[campaign-outline] top-level error", String(err?.message || err));
    try {
      const pref = (typeof prefix === "string" && prefix) ? (prefix.endsWith("/") ? prefix : `${prefix}/`) : `runs/${runId}/`;
      const cont = container || BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage).getContainerClient(CONTAINER);
      await patchStatus(cont, pref, "Failed", {
        error: { code: "outline_error", message: String(err?.message || err) },
        outlineFailedAt: new Date().toISOString()
      });
    } catch (writeErr) {
      context.log.warn("[campaign-outline] failed to write failure status", String(writeErr?.message || writeErr));
    }
  }
};
