// api/campaign-write/index.js
// v1 · 28-10-2025
//
// Queue-triggered writer that generates each campaign section to runs/<runId>/sections/<section>.json,
// then merges all sections into runs/<runId>/campaign.json.
//
// Messages on the queue:
//  { "type":"write_section", "runId":"<uuid>", "section":"exec|positioning|messaging|offer|channel|risks|compliance|one" }
//  { "type":"assemble", "runId":"<uuid>" }
//
// Inputs read from blobs:
//  runs/<runId>/outline.json          (required)
//  runs/<runId>/evidence_log.json     (required)
//  runs/<runId>/csv_normalized.json   (optional)
//  runs/<runId>/site.json             (optional)
//
// Outputs:
//  runs/<runId>/sections/<finalKey>.json
//  runs/<runId>/campaign.json
//  runs/<runId>/status.json  (patched)
//
// Status flow:
//  - When writing sections:   Status: "SectionWrites" (with progress marks)
//  - When assembling:         Status: "Assemble"  → finally "Completed"
//
// SAFETY:
//  - Strong defensive JSON parse/repair for model outputs
//  - Idempotent writes (overwrites same blob path safely)
//  - No schema stringify in prompts; we use response_format=json_object and validate ourselves

const path = require("path");
const { BlobServiceClient } = require("@azure/storage-blob");
const crypto = require("crypto");

// ---- ENV / CONFIG ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CONTAINER = process.env.RESULTS_CONTAINER || "results";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 60000);

// Azure OpenAI (Chat Completions)
const AZO_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT;
const AZO_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const AZO_API_VER = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";
const AZO_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT;

// ---- SECTION MAPS ----
// Outline → final campaign keys
const SECTION_MAP = {
  exec: "executive_summary",
  positioning: "positioning_and_differentiation",
  messaging: "messaging_matrix",
  offer: "offer_strategy",
  channel: "channel_plan",
  risks: "risks_and_contingencies",
  compliance: "compliance_and_governance",
  one: "one_pager_summary"
};

const SECTION_ORDER = [
  "exec", "positioning", "messaging", "offer", "channel", "risks", "compliance", "one"
];

// ---- UTIL: blob + json ----
function blobSvc() {
  if (!STORAGE_CONN) throw new Error("AzureWebJobsStorage not configured");
  return BlobServiceClient.fromConnectionString(STORAGE_CONN);
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
async function streamToString(rs) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    rs.on("data", d => chunks.push(d));
    rs.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    rs.on("error", reject);
  });
}
async function patchStatus(containerClient, prefix, state, extra = {}) {
  const p = `${prefix}status.json`;
  const cur = (await getJson(containerClient, p)) || {};
  const next = { ...cur, state, ...extra };
  await putJson(containerClient, p, next);
}
function safeForPrompt(v, max = 280000) {
  try {
    const s = typeof v === "string" ? v : JSON.stringify(v ?? "");
    if (s.length <= max) return s;
    const keep = Math.floor(max / 2);
    return s.slice(0, keep) + " …TRUNCATED… " + s.slice(-keep);
  } catch { return "null"; }
}

// ---- UTIL: robust JSON repair ----
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

// ---- LLM call (we bypass prompt-harness here to avoid schema coupling) ----
async function callChatJsonObject({ system, user, timeoutMs }) {
  if (!AZO_ENDPOINT || !AZO_API_KEY || !AZO_DEPLOYMENT) {
    throw Object.assign(new Error("Missing Azure OpenAI configuration"), { code: "config_missing" });
  }
  const base = AZO_ENDPOINT.replace(/\/+$/, "");
  const dep = encodeURIComponent(AZO_DEPLOYMENT);
  const ver = encodeURIComponent(AZO_API_VER);
  const url = `${base}/openai/deployments/${dep}/chat/completions?api-version=${ver}`;

  const body = {
    temperature: Number(process.env.LLM_TEMPERATURE ?? 0),
    max_tokens: Number(process.env.LLM_MAX_TOKENS || process.env.AZURE_OPENAI_MAX_TOKENS || 4096),
    response_format: { type: "json_object" },
    messages: [
      { role: "system", content: system },
      { role: "user", content: user }
    ]
  };

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort("llm_timeout"), Number(timeoutMs || LLM_TIMEOUT_MS));

  try {
    const res = await fetch(url, {
      method: "POST",
      signal: controller.signal,
      headers: { "Content-Type": "application/json", "api-key": AZO_API_KEY },
      body: JSON.stringify(body)
    });
    const raw = await res.text();
    if (!res.ok) {
      const msg = raw && raw.length > 2000 ? raw.slice(0, 2000) + "…(truncated)" : (raw || res.statusText);
      const e = new Error(`OpenAI error ${res.status}: ${msg}`);
      e.code = "llm_error";
      throw e;
    }
    let wire; try { wire = raw ? JSON.parse(raw) : null; } catch { }
    const content = wire?.choices?.[0]?.message?.content || "";
    return tryParseOrRepair(content);
  } finally {
    clearTimeout(timer);
  }
}

// ---- SECTION PROMPTS ----
function buildSectionSystem(finalKey) {
  // Strong, section-specific constraints with cited-string regex expectations only in prose fields
  return `
You are a senior UK B2B strategist. Generate STRICT JSON only for the requested section "${finalKey}".
Return JSON only, no markdown fences. Do NOT include keys for other sections.

CITATION RULE (inline, at end of sentences where evidence is used):
- Use short tags in parentheses: (Company site), (LinkedIn), (CSV), (Ofcom), (ONS), (DSIT), (PDF extract), (Trade press), (Directory).

STYLE:
- UK English, concise, specific, evidence-led.
- No generic advice. Keep bullets tight.

VALIDATION:
- All URLs must be https.
- Arrays must be present even if empty when specified below.

MANDATORY INPUT USAGE:
- If an objective (campaignRequirement) is provided, tune tone, prioritisation, channels, and KPIs to that objective ("upsell" | "win-back" | "growth").
- If competitors are provided, reflect them concisely in Positioning (clear, fair contrasts with citations). Do not fabricate claims; if no citation exists, state "no external citation available".
`.trim();
}

function buildSectionUser(finalKey, { outline, sectionPlan, evidence, csvSignal, products }) {
  const outlineNotes = safeForPrompt(sectionPlan);
  const ev = safeForPrompt(evidence);
  const csv = safeForPrompt(csvSignal);
  const prod = safeForPrompt(products);
  // Pull objective + competitors from outline.input_notes (persisted by campaign-outline)
  const inNotes = (outline && outline.input_notes) || {};
  const campaignRequirement = (typeof inNotes.campaign_requirement === "string" &&
    ["upsell", "win-back", "growth"].includes(inNotes.campaign_requirement))
    ? inNotes.campaign_requirement
    : null;

  const competitors = Array.isArray(inNotes.relevant_competitors)
    ? inNotes.relevant_competitors.filter(x => typeof x === "string" && x.trim()).slice(0, 8)
    : [];

  // Minimal target JSON shape is described to the model per section
  const targets = {
    executive_summary: `{
  "executive_summary": [
    "<~110–140 word para naming supplier + exact product anchors from site>",
    "Why now bullet (cited)",
    "Why now bullet (cited)",
    "Why now bullet (cited)",
    "Why now bullet (cited)"
  ]
}`,
    positioning_and_differentiation: `{
  "positioning_and_differentiation": {
    "value_prop": "<1–2 sentences>",
    "differentiators": ["<3–6 items; some may map to USER_USPS if present>"]
  }
}`,
    messaging_matrix: `{
  "messaging_matrix": [
    { "persona": "<string>", "pain": "<string>", "value_statement": "<cited>", "proof": "<cited>", "cta": "<short>" }
  ]
}`,
    offer_strategy: `{
  "offer_strategy": {
    "landing_page": {
      "hero": "<one-liner>",
      "why_it_matters": ["<cited bullets>"],
      "what_you_get": ["<from CSV + site product anchors; cited where possible>"],
      "how_it_works": ["<3–5 steps>"],
      "outcomes": ["<cited>"],
      "proof": ["<cited>"],
      "cta": "<short>"
    },
    "assets_checklist": ["<≥5 items>"]
  }
}`,
    channel_plan: `{
  "channel_plan": {
    "emails": [
      { "subject": "<≤80 chars>", "body": "<~90–120 words; ≥1 citation>" },
      { "subject": "<≤80 chars>", "body": "<~90–120 words; ≥1 citation>" },
      { "subject": "<≤80 chars>", "body": "<~90–120 words; ≥1 citation>" },
      { "subject": "<≤80 chars>", "body": "<~90–120 words; ≥1 citation>" }
    ],
    "linkedin": {
      "connect_note": "<short>",
      "insight_post": "<post with citations>",
      "dm": "<short DM with a cited fact>",
      "comment_strategy": "<bullet list>"
    }
  }
}`,
    risks_and_contingencies: `{
  "risks_and_contingencies": [
    "<risk item (cited if external)>",
    "<risk item (cited if external)>"
  ]
}`,
    compliance_and_governance: `{
  "compliance_and_governance": {
    "substantiation_file": "<string>",
    "gdpr_pecr_checklist": ["<items>"],
    "brand_accessibility_checks": ["<items>"],
    "approval_log_note": "<string>"
  }
}`,
    one_pager_summary: `{
  "one_pager_summary": {
    "bullets": ["<sharp bullets, cited where relevant>"]
  }
}`
  };

  return `
Context:
- Outline plan for "${finalKey}": ${outlineNotes}
- Evidence catalog (claim_id, title, url, source_type, summary, quote): ${ev}
- CSV signals (industry, spend band, blockers, needs, purchases): ${csv}
- Product name anchors (from site): ${prod}
- Objective (campaignRequirement): ${campaignRequirement ?? "unspecified"}
- Competitors (if any): ${safeForPrompt(competitors)}

Use ONLY claim_ids present in the evidence catalog when citing; prefer Ofcom/ONS/DSIT where relevant.
Select content aligned with the outline plan (claim_ids/themes provided for this section).
Always produce exactly the following JSON shape for "${finalKey}":

${targets[finalKey] || "{}"}

Return JSON only.
`.trim();
}

// ---- MAIN HANDLER ----
module.exports = async function (context, queueItem) {
  const svc = blobSvc();
  const container = svc.getContainerClient(CONTAINER);

  if (!queueItem) {
    context.log.error("[campaign-write] Empty queue message");
    return;
  }
  const type = (queueItem.type || "").toLowerCase();
  const runId = queueItem.runId || queueItem?.data?.runId || queueItem?.id;
  if (!runId) {
    context.log.error("[campaign-write] Missing runId in queue message");
    return;
  }
  const outlineSectionRaw = queueItem.section || ""; // may be blank for assemble jobs
  const prefix = `runs/${runId}/`;

  try {
    // Load common inputs
    const outline = await getJson(container, `${prefix}outline.json`);
    const rawEvidence = await getJson(container, `${prefix}evidence_log.json`);
    const csvNorm = await getJson(container, `${prefix}csv_normalized.json`);
    const site = await getJson(container, `${prefix}site.json`);
    const evidenceLog = rawEvidence.evidence_log;

    // ---- Product names (single definition) ----
    let productNames = Array.isArray(outline?.input_notes?.product_mentions)
      ? outline.input_notes.product_mentions
      : null;

    if (!productNames || productNames.length === 0) {
      const productsFile = await getJson(container, `${prefix}products.json`).catch(() => null);
      productNames = Array.isArray(productsFile?.products) ? productsFile.products : null;
    }

    if (!productNames || productNames.length === 0) {
      // site.json fallback — note: NO const here; reuse the same variable
      productNames = (() => {
        const names = new Set();
        const add = v => { if (typeof v === "string" && v.trim()) names.add(v.trim()); };
        try {
          if (Array.isArray(site?.products)) site.products.forEach(add);
          if (Array.isArray(site?.pages)) {
            site.pages.forEach(p => {
              if (typeof p?.product === "string") add(p.product);
              if (Array.isArray(p?.headings)) p.headings.forEach(add);
            });
          }
        } catch { /* best-effort */ }
        return Array.from(names).slice(0, 12);
      })();
    }

    if (!outline || !outline.sections) {
      throw new Error("Missing or invalid outline.json");
    }
    if (!rawEvidence || !Array.isArray(rawEvidence.evidence_log)) {
      throw new Error("Missing or invalid evidence_log.json");
    }

    // Normalized CSV signal
    const csvSignal = {
      industry:
        csvNorm?.selectedIndustry ||
        csvNorm?.industry ||
        csvNorm?.meta?.industry ||
        outline?.meta?.selected_industry ||
        "General",
      spend_band: csvNorm?.ITSpendBand || csvNorm?.SpendBand || "unknown",
      top_blockers: csvNorm?.TopBlockers || csvNorm?.top_blockers || [],
      top_needs_supplier: csvNorm?.TopNeedsSupplier || csvNorm?.top_needs_supplier || [],
      top_purchases: csvNorm?.TopPurchases || csvNorm?.top_purchases || []
    };

    // Branch by job type
    if (type === "write_section") {
      const outlineKey = String(outlineSectionRaw || "").toLowerCase();
      if (!SECTION_MAP[outlineKey]) {
        throw new Error(`Unknown section "${outlineSectionRaw}". Expected one of: ${Object.keys(SECTION_MAP).join(", ")}`);
      }
      const finalKey = SECTION_MAP[outlineKey];

      // Status: we're starting this section
      await patchStatus(container, prefix, "SectionWrites", {
        runId,
        writing: finalKey,
        updatedAt: new Date().toISOString()
      });

      // Plan for this section (from outline); empty object if missing
      const plan = outline?.sections?.[outlineKey] ?? {};

      // Hard rule to force product→need mapping in every section
      const MAP_PRODUCTS_TO_NEEDS_RULE =
        "When creating the campaign outline or final messaging, always map the company’s products (from product_names) " +
        "to the buyer needs and blockers (from csv_signals). Each section must reference how at least one of the products " +
        "addresses a specific buyer need or purchase driver.";

      // Build the section JSON using your builder (LLM or deterministic)
      // NOTE: evidenceLog is the ARRAY (validated earlier). Use it for prompts/builders.
      const sectionJson = await buildSectionJson({
        finalKey,
        outline,
        sectionPlan: plan,
        evidence: evidenceLog,      // ← array form only
        csvSignal,
        products: productNames,
        site,
        rules: { mapProductsToNeeds: MAP_PRODUCTS_TO_NEEDS_RULE }
        // If your builder supports extra context (industry_mode/selected_industry), add here.
      });

      // Keep only the requested section key (defensive)
      const filtered = filterToSection(finalKey, sectionJson);

      // Invariant: ensure at least one explicit product→need mapping exists
      function hasProductMapping(sec, names) {
        if (!sec || !names || names.length === 0) return false;
        // Preferred: structured mappings, if your builder emits them
        if (Array.isArray(sec.mappings) && sec.mappings.some(m => m?.product && m?.need)) return true;
        // Fallback: simple heuristic — any product name present in this section’s JSON
        const hay = JSON.stringify(sec).toLowerCase();
        return names.some(n => typeof n === "string" && n && hay.includes(n.toLowerCase()));
      }

      if (!hasProductMapping(filtered[finalKey], productNames)) {
        const msg = `[campaign-write] invariant failed: no product→need mapping for section '${finalKey}'`;
        context.log.warn(msg);
        await patchStatus(container, prefix, "Failed", {
          error: { code: "product_mapping_missing", message: msg },
          section: finalKey,
          failedAt: new Date().toISOString()
        });
        throw new Error("product_mapping_missing");
      }

      // Persist this section
      await putJson(container, `${prefix}sections/${finalKey}.json`, filtered);
      context.log(`[campaign-write] wrote section ${finalKey}`);

      // Notify orchestrator AFTER successful write
      try {
        const { QueueClient } = require("@azure/storage-queue");
        const qc = new QueueClient(process.env.AzureWebJobsStorage, process.env.CAMPAIGN_QUEUE_NAME || "campaign");
        await qc.createIfNotExists();
        const page = (queueItem && (queueItem.page || queueItem?.data?.page)) || "campaign";
        const msg = { op: "aftersection", runId, page, prefix, section: finalKey };
        await qc.sendMessage(Buffer.from(JSON.stringify(msg), "utf8").toString("base64"));
      } catch (notifyErr) {
        context.log.warn("[campaign-write] notify aftersection failed", String(notifyErr?.message || notifyErr));
      }

      return;
    }
    // ASSEMBLE
    if (type === "assemble") {
      await patchStatus(container, prefix, "Assemble", {
        runId,
        assembleStartedAt: new Date().toISOString()
      });

      // Read all available section files and merge
      const merged = {
        meta: {
          run_id: runId,
          phase: "Completed",
          selected_industry: outline?.meta?.selected_industry || csvSignal.industry || "General"
        },
        input_proof: {
          outline_sha256: sha256OfJson(outline),
          evidence_log_sha256: sha256OfJson(rawEvidence),
          csv_normalized_sha256: sha256OfJson(csvNorm || {}),
          site_sha256: sha256OfJson(site || {})
        }
      };

      for (const outlineKey of SECTION_ORDER) {
        const finalKey = SECTION_MAP[outlineKey];
        const blobPath = `${prefix}sections/${finalKey}.json`;
        const obj = await getJson(container, blobPath);
        if (obj && obj[finalKey]) {
          merged[finalKey] = obj[finalKey];
        } else {
          // keep missing sections simply absent; UI can handle partials
          context.log.warn(`[campaign-write] missing section during assemble: ${finalKey}`);
        }
      }

      // After assemble (all sections merged)
      await putJson(container, `${prefix}campaign.json`, merged);

      // Patch statuses in order: Assemble -> Completed
      await patchStatus(container, prefix, "Assemble", {
        runId,
        assembleCompletedAt: new Date().toISOString()
      });
      await patchStatus(container, prefix, "Completed", {
        runId,
        completedAt: new Date().toISOString()
      });

      context.log("[campaign-write] assemble complete");

      // Notify orchestrator (afterassemble) — only after successful write + status patches
      try {
        const { QueueClient } = require("@azure/storage-queue");
        const qc = new QueueClient(
          process.env.AzureWebJobsStorage,
          process.env.CAMPAIGN_QUEUE_NAME || "campaign"
        );
        await qc.createIfNotExists();
        const page =
          (queueItem && (queueItem.page || queueItem?.data?.page)) || "campaign";
        const msg = { op: "afterassemble", runId, page, prefix };
        await qc.sendMessage(
          Buffer.from(JSON.stringify(msg), "utf8").toString("base64")
        );
      } catch (notifyErr) {
        context.log.warn(
          "[campaign-write] notify afterassemble failed",
          String(notifyErr?.message || notifyErr)
        );
      }
      return;
    }

    throw new Error(`Unknown job type "${type}". Use "write_section" or "assemble".`);
  } catch (err) {
    context.log.error("[campaign-write] error", String(err?.message || err));
    // Best-effort failure mark
    try {
      const prefix = `runs/${runId}/`;
      await patchStatus(
        svc.getContainerClient(CONTAINER),
        prefix,
        "Failed",
        { error: { code: err.code || "writer_error", message: String(err?.message || err) } }
      );
    } catch { }
  }
};

// ---- helpers specific to writer ----
function buildStubFor(finalKey, csvSignal, products) {
  const common = {
    executive_summary: { executive_summary: ["(staged) Executive summary paragraph.", "(staged) Why now A.", "(staged) Why now B.", "(staged) Why now C.", "(staged) Why now D."] },
    positioning_and_differentiation: { positioning_and_differentiation: { value_prop: "(staged) Value prop.", differentiators: ["(staged) Diff A", "(staged) Diff B", "(staged) Diff C"] } },
    messaging_matrix: { messaging_matrix: [{ persona: (csvSignal.industry || "General") + " lead", pain: "(staged) pain", value_statement: "(staged) value (CSV)", proof: "(staged) proof (CSV)", cta: "(staged) CTA" }] },
    offer_strategy: { offer_strategy: { landing_page: { hero: "(staged) hero", why_it_matters: ["(staged) why"], what_you_get: products.slice(0, 3).map(p => `(staged) ${p}`), how_it_works: ["(staged) step1", "(staged) step2", "(staged) step3"], outcomes: ["(staged) outcome"], proof: ["(staged) proof"], cta: "(staged) CTA" }, assets_checklist: ["(staged) asset A", "(staged) asset B", "(staged) asset C", "(staged) asset D", "(staged) asset E"] } },
    channel_plan: { channel_plan: { emails: [{ subject: "(staged) s1", body: "(staged) b1" }, { subject: "(staged) s2", body: "(staged) b2" }, { subject: "(staged) s3", body: "(staged) b3" }, { subject: "(staged) s4", body: "(staged) b4" }], linkedin: { connect_note: "(staged) note", insight_post: "(staged) post", dm: "(staged) dm", comment_strategy: "(staged) comments" } } },
    risks_and_contingencies: { risks_and_contingencies: ["(staged) risk A", "(staged) risk B"] },
    compliance_and_governance: { compliance_and_governance: { substantiation_file: "(staged) substantiation", gdpr_pecr_checklist: ["(staged) item"], brand_accessibility_checks: ["(staged) item"], approval_log_note: "(staged) note" } },
    one_pager_summary: { one_pager_summary: { bullets: ["(staged) point A", "(staged) point B", "(staged) point C"] } }
  };
  return common[finalKey] || { [finalKey]: {} };
}

function filterToSection(finalKey, obj) {
  const out = {};
  if (obj && typeof obj === "object" && obj[finalKey]) {
    out[finalKey] = obj[finalKey];
  } else {
    // If model returned a flat object of the section, wrap it.
    out[finalKey] = obj || {};
  }
  return out;
}

function sha256OfJson(o) {
  const h = crypto.createHash("sha256");
  h.update(Buffer.from(JSON.stringify(o || {})));
  return h.digest("hex");
}
