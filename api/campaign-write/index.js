// api/campaign-write/index.js v3 04-11-2025  (schema-aligned, evidence/CSV canonical shapes)
// Queue-triggered writer that generates each campaign section to runs/<runId>/sections/<section>.json,
// then merges all sections into runs/<runId>/campaign.json.

const path = require("path");
const { BlobServiceClient } = require("@azure/storage-blob");
const crypto = require("crypto");

// ---- ENV / CONFIG ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 60000);

// Azure OpenAI (Chat Completions)
const AZO_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT;
const AZO_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const AZO_API_VER = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";
const AZO_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT;

// ---- SECTION MAPS ----
// Outline → final campaign keys (must match new schema)
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
  try { return JSON.parse(candidate); } catch { /* repair below */ }
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

// ---- LLM call (json_object) ----
async function callChatJsonObject({ system, user, timeoutMs }) {
  if (!AZO_ENDPOINT || !AZO_API_KEY || !AZO_DEPLOYMENT) {
    const e = new Error("Missing Azure OpenAI configuration");
    e.code = "config_missing";
    throw e;
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

// ---- SECTION PROMPTS (schema-aligned targets) ----
function buildSectionSystem(finalKey, persona) {
  return `
${persona ? `PERSONA\n${persona}\n\n` : ""}You are a senior UK B2B strategist. Generate STRICT JSON only for the requested section "${finalKey}".
Return JSON only, no markdown fences. Do NOT include keys for other sections.

CITATION RULE (inline, end of sentences using external evidence):
- Use short tags in parentheses: (Company site), (LinkedIn), (CSV), (Ofcom), (ONS), (DSIT), (PDF extract), (Trade press), (Directory).

STYLE:
- UK English, concise, specific, evidence-led. Prefer concrete buyer outcomes with inline citations where used.

VALIDATION:
- All URLs must be https.
- Arrays required by the schema must be present (empty if necessary).
- Do not invent numbers or sources. If a required datum is unavailable, write a short, honest line and include "no external citation available".

OUTPUT DISCIPLINE:
- Emit exactly the JSON object for "${finalKey}" matching the shapes described in the user message. No extra fields.
`.trim();
}

function targetsFor(finalKey) {
  // Minimal, schema-true shapes to guide the model per section
  const t = {
    executive_summary: `{
  "executive_summary": [
    "<~500 words naming supplier + exact product anchors> (CSV).",
    "Why now point (Ofcom|ONS|DSIT|Company site|PDF extract).",
    "Why now point (Ofcom|ONS|DSIT|Company site|PDF extract).",
    "Supplier fit/outcome (Company site)."
  ]
}`,
    positioning_and_differentiation: `{
  "positioning_and_differentiation": {
    "value_prop": "<1–2 sentences>",
    "swot": {
      "strengths": ["<items>"],
      "weaknesses": ["<items>"],
      "opportunities": ["<items>"],
      "threats": ["<items>"]
    },
    "differentiators": ["<3–6 cited items>"],
    "competitor_set": [
      { "vendor": "<name>", "reason_in_set": "<short>", "url": "https://<domain>" }
    ]
  }
}`,
    messaging_matrix: `{
  "messaging_matrix": {
    "nonnegotiables": ["<3+ items>"],
    "matrix": [
      {
        "persona": "<role>",
        "pain": "<short>",
        "value_statement": "<cited>",
        "proof": "<cited>",
        "cta": "<short>"
      }
    ]
  }
}`,
    offer_strategy: `{
  "offer_strategy": {
    "landing_page": {
      "hero": "<one-liner>",
      "why_it_matters": ["<cited bullets>"],
      "what_you_get": ["<from CSV + product anchors; cited where possible>"],
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
      { "subject": "<≤80 chars>", "preview": "<one line>", "body": "<~90–140 words; ≥1 citation>" },
      { "subject": "<≤80 chars>", "preview": "<one line>", "body": "<~90–140 words; ≥1 citation>" },
      { "subject": "<≤80 chars>", "preview": "<one line>", "body": "<~90–140 words; ≥1 citation>" },
      { "subject": "<≤80 chars>", "preview": "<one line>", "body": "<~90–140 words; ≥1 citation>" }
    ],
    "linkedin": {
      "connect_note": "<short>",
      "insight_post": "<post with inline citations>",
      "dm": "<short DM with a cited fact>",
      "comment_strategy": "<bullet list or short para>"
    },
    "paid": [],
    "event": { "concept": "", "agenda": "", "speakers": "", "cta": "" }
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
    "gdpr_pecr_checklist": "<string>",
    "brand_accessibility_checks": "<string>",
    "approval_log_note": "<string>"
  }
}`,
    one_pager_summary: `{
  "one_pager_summary": [
    "<sharp bullet, cited where relevant>",
    "<sharp bullet, cited where relevant>",
    "<sharp bullet, cited where relevant>"
  ]
}`
  };
  return t[finalKey] || "{}";
}

function buildSectionUser(finalKey, { outline, sectionPlan, evidence, csvCanon, products }) {
  const outlineNotes = safeForPrompt(sectionPlan);
  const ev = safeForPrompt(evidence);
  const prod = safeForPrompt(products);

  // From normalized CSV canonical (industry_mode/signals/global_signals/meta.rows)
  const mode = (csvCanon && csvCanon.industry_mode === "specific" && csvCanon.selected_industry) ? "specific" : "agnostic";
  const sig = mode === "specific" ? (csvCanon?.signals || {}) : (csvCanon?.global_signals || {});
  const csv = safeForPrompt({
    industry_mode: mode,
    selected_industry: mode === "specific" ? (csvCanon?.selected_industry || null) : null,
    row_count: csvCanon?.meta?.rows || 0,
    top_blockers: sig.top_blockers || [],
    top_needs_supplier: sig.top_needs_supplier || [],
    top_purchases: sig.top_purchases || []
  });

  // Inputs from outline.input_notes (supplier + objective + competitors)
  const inNotes = (outline && outline.input_notes) ? outline.input_notes : {};
  const campaignRequirement =
    (typeof inNotes.campaign_requirement === "string" &&
      ["upsell", "win-back", "growth"].includes(inNotes.campaign_requirement))
      ? inNotes.campaign_requirement
      : "unspecified";

  const competitors = Array.isArray(inNotes.relevant_competitors)
    ? inNotes.relevant_competitors.map(x => String(x || "").trim()).filter(Boolean).slice(0, 8)
    : [];

  const company = (inNotes.supplier_company || inNotes.prospect_company || "").trim();
  const website = (inNotes.supplier_website || inNotes.prospect_website || "").trim();
  const linkedin = (inNotes.supplier_linkedin || inNotes.prospect_linkedin || "").trim();
  const usps = Array.isArray(inNotes.supplier_usps) && inNotes.supplier_usps.length
    ? inNotes.supplier_usps
    : (Array.isArray(inNotes.user_usps) ? inNotes.user_usps : []);

  return `
Context:
- Outline plan for "${finalKey}": ${outlineNotes}
- Evidence catalog ARRAY (items have claim_id, title, url, source_type, summary, quote): ${ev}
- CSV canonical (industry_mode/selected_industry/rows/signals): ${csv}
- Product name anchors (from site): ${prod}
- Supplier: ${company || "unknown"} | website: ${website || "unknown"} | LinkedIn: ${linkedin || "unknown"}
- Supplier USPs (if any): ${safeForPrompt(usps)}
- Objective (campaignRequirement): ${campaignRequirement}
- Competitors (if any): ${safeForPrompt(competitors)}

RULES:
- Use ONLY claim_ids present in the evidence catalog when a claim is made from external evidence.
- Balance evidence weighting: regulator/government (Ofcom/ONS/DSIT) are peers to CSV market signals and reputable "good sources" — do not overweight regulator/government claims.
- Map at least one product_name to a specific CSV buyer need or intended purchase in this section.

Emit exactly the following JSON object for "${finalKey}":
${targetsFor(finalKey)}

Return JSON only.
`.trim();
}

// ---- Section builder using the LLM helper (kept local, no external harness) ----
async function buildSectionJson({ finalKey, outline, sectionPlan, evidence, csvCanon, products, persona }) {
  const system = buildSectionSystem(finalKey, persona);
  const user = buildSectionUser(finalKey, { outline, sectionPlan, evidence, csvCanon, products });
  const obj = await callChatJsonObject({ system, user, timeoutMs: LLM_TIMEOUT_MS });
  return filterToSection(finalKey, obj);
}

// ---- MAIN HANDLER ----
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
  const prefix = `runs/${runId}/`;

  try {
    // Load common inputs
    const outline = await getJson(container, `${prefix}outline.json`);
    const evRaw = await getJson(container, `${prefix}evidence_log.json`);
    const csvCanon = await getJson(container, `${prefix}csv_normalized.json`);
    const site = await getJson(container, `${prefix}site.json`);
    // Load normalized input for persona (sales_model)
    const input = await getJson(container, `${prefix}input.json`);
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
    context.log({ event: "campaign_write_persona", salesModel, personaType: (salesModel === "partner" ? "PARTNER" : "DIRECT") });

    if (!outline || !outline.sections) throw new Error("Missing or invalid outline.json");

    // evidence_log as array (new canonical) with old-shape fallback
    const evidenceLog = Array.isArray(evRaw) ? evRaw
      : (Array.isArray(evRaw?.evidence_log) ? evRaw.evidence_log : []);

    if (!Array.isArray(evidenceLog)) throw new Error("Missing or invalid evidence_log.json (expected array)");

    // ---- Product names (priority: products.json → outline notes → site heuristic) ----
    let productNames = Array.isArray(outline?.input_notes?.product_mentions)
      ? outline.input_notes.product_mentions
      : null;

    if (!productNames || productNames.length === 0) {
      try {
        const productsFile = await getJson(container, `${prefix}products.json`);
        if (Array.isArray(productsFile?.products)) productNames = productsFile.products;
      } catch { /* ignore */ }
    }

    if (!productNames || productNames.length === 0) {
      // Strict, local heuristic on site.json snapshot
      productNames = (() => {
        const names = new Set();
        const add = v => { if (typeof v === "string" && v.trim()) names.add(v.trim()); };
        try {
          if (Array.isArray(site?.products)) site.products.forEach(add);
          if (Array.isArray(site)) {
            // older shape: array of snapshots
            site.forEach(p => {
              if (Array.isArray(p?.headings)) p.headings.forEach(add);
            });
          } else if (Array.isArray(site?.pages)) {
            site.pages.forEach(p => {
              if (Array.isArray(p?.headings)) p.headings.forEach(add);
            });
          }
        } catch { /* best-effort */ }
        return Array.from(names).slice(0, 12);
      })();
    }

    // Branch by job type
    if (op === "write_section" || op === "section") {
      const outlineKey = String(outlineSectionRaw || "").toLowerCase();
      if (!SECTION_MAP[outlineKey]) {
        throw new Error(`Unknown section "${outlineSectionRaw}". Expected one of: ${Object.keys(SECTION_MAP).join(", ")}`);
      }
      const finalKey = SECTION_MAP[outlineKey];

      // Status: starting this section
      await patchStatus(container, prefix, "SectionWrites", {
        runId,
        writing: finalKey,
        updatedAt: new Date().toISOString()
      });

      // Section plan (outline.sections.<key>) — object or array depending on section
      const plan = outline?.sections?.[outlineKey] ?? {};

      // Build the section via LLM
      const sectionJson = await buildSectionJson({
        finalKey,
        outline,
        sectionPlan: plan,
        evidence: evidenceLog,
        csvCanon,
        products: productNames,
        persona: PERSONA
      });

      // Persist this section (exact section object only)
      await putJson(container, `${prefix}sections/${finalKey}.json`, sectionJson);
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

    if (op === "assemble") {
      await patchStatus(container, prefix, "Assemble", {
        runId,
        assembleStartedAt: new Date().toISOString()
      });

      // Normalise CSV selected industry to carry into meta
      const selectedIndustry =
        (csvCanon && csvCanon.industry_mode === "specific" && csvCanon.selected_industry)
          ? String(csvCanon.selected_industry || "").toLowerCase() || "general"
          : (outline?.meta?.selected_industry || "General");
      const productsObj = await getJson(container, `${prefix}products.json`);
      const pdfExtracts = await getJson(container, `${prefix}pdf_extracts.json`);

      const evidenceCounts = {
        csv: Math.max(0, Number(csvCanon?.meta?.rows || 0)),
        site: Array.isArray(site) ? site.length
          : (Array.isArray(site?.pages) ? site.pages.length : 0),
        products: Array.isArray(productsObj?.products) ? productsObj.products.length : 0,
        case_studies: Array.isArray(pdfExtracts) ? pdfExtracts.length : 0
      };

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
          evidence_counts: evidenceCounts
        },

        // evidence_log is required by the new schema — include array directly
        evidence_log: evidenceLog
      };

      for (const outlineKey of SECTION_ORDER) {
        const finalKey = SECTION_MAP[outlineKey];
        const blobPath = `${prefix}sections/${finalKey}.json`;
        const obj = await getJson(container, blobPath);
        if (obj && obj[finalKey] != null) {
          merged[finalKey] = obj[finalKey];
        } else {
          context.log.warn(`[campaign-write] missing section during assemble: ${finalKey}`);
        }
      }

      const normalised = normaliseContract(merged);
      await putJson(container, `${prefix}campaign.json`, normalised);

      // Patch statuses in order
      await patchStatus(container, prefix, "Assemble", {
        runId,
        assembleCompletedAt: new Date().toISOString()
      });
      await patchStatus(container, prefix, "Completed", {
        runId,
        completedAt: new Date().toISOString()
      });

      context.log("[campaign-write] assemble complete");

      // Notify orchestrator (afterassemble)
      try {
        const { QueueClient } = require("@azure/storage-queue");
        const qc = new QueueClient(process.env.AzureWebJobsStorage, process.env.CAMPAIGN_QUEUE_NAME || "campaign");
        await qc.createIfNotExists();
        const page = (queueItem && (queueItem.page || queueItem?.data?.page)) || "campaign";
        const msg = { op: "afterassemble", runId, page, prefix };
        await qc.sendMessage(Buffer.from(JSON.stringify(msg), "utf8").toString("base64"));
      } catch (notifyErr) {
        context.log.warn("[campaign-write] notify afterassemble failed", String(notifyErr?.message || notifyErr));
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
    } catch { /* ignore */ }
  }
};

// ---- UI schema normalisers (ensure stable shapes for the frontend) ----
function rowsOf(v) { return Array.isArray(v) ? v : (v == null ? [] : [v]); }

function toStringArray(listish) {
  const out = [];
  for (const it of rowsOf(listish)) {
    if (typeof it === "string" || typeof it === "number") { out.push(String(it)); continue; }
    if (it && typeof it === "object") {
      const prefer = it.paragraph || it.text || it.value || it.content;
      if (typeof prefer === "string") { out.push(prefer); continue; }
      const vals = Object.values(it).filter(v => typeof v === "string");
      if (vals.length) { out.push(vals.join(" ")); continue; }
    }
  }
  return out.filter(Boolean);
}

function normaliseContract(raw) {
  const c = raw && typeof raw === "object" ? raw : {};

  // 1) Executive summary as array<string>
  c.executive_summary = toStringArray(c.executive_summary);

  // 2) Messaging matrix shape
  const mm = c.messaging_matrix || {};
  c.messaging_matrix = {
    nonnegotiables: rowsOf(mm.nonnegotiables).map(String).filter(Boolean),
    matrix: rowsOf(mm.matrix).map(r => ({
      persona: r?.persona || "",
      pain: r?.pain || "",
      value_statement: r?.value_statement || "",
      proof: r?.proof || "",
      cta: r?.cta || ""
    }))
  };

  // 3) Case studies: prefer case_study_library, fallback from case_studies
  if (!Array.isArray(c.case_study_library) && Array.isArray(c.case_studies)) {
    c.case_study_library = c.case_studies;
  }

  // 4) Ensure objects exist for optional sections the UI reads
  c.positioning_and_differentiation = c.positioning_and_differentiation || {};
  c.offer_strategy = c.offer_strategy || {};
  c.channel_plan = c.channel_plan || {};
  c.compliance_and_governance = c.compliance_and_governance || {};

  // 5) Lists that the UI iterates
  c.one_pager_summary = rowsOf(c.one_pager_summary).map(String);

  return c;
}

// ---- helpers specific to writer ----
function filterToSection(finalKey, obj) {
  const out = {};
  if (obj && typeof obj === "object" && obj[finalKey] != null) {
    out[finalKey] = obj[finalKey];
  } else {
    out[finalKey] = obj || {};
  }
  return out;
}

function sha256OfJson(o) {
  const h = crypto.createHash("sha256");
  h.update(Buffer.from(JSON.stringify(o || {})));
  return h.digest("hex");
}
