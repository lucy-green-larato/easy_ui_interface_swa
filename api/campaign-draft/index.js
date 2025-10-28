// api/campaign-draft/index.js // v1 · 28-10-2025
// Queue-triggered function that converts an OUTLINE into a full campaign draft (prose)
// Inputs:  runs/<runId>/{outline.json, evidence_log.json, csv_normalized.json?, site.json?}
// Output:  runs/<runId>/campaign.json
// Status:  DraftCampaign
//
// DESIGN:
// - Uses prompt-harness.generate() with schemaPath => api/schemas/campaign.schema.json
// - No schema stringify in the prompt (harness enforces json_schema mode)
// - CSV signals & product names are surfaced and required to be used
// - evidence claim_ids must be reused; no fabrication
// - Robust JSON repair + draft_parse_debug.json head/tail
// - CAMPAIGN_STAGED short-circuits to a valid, minimal campaign scaffold
// - Idempotent Blob I/O and status progression

const { BlobServiceClient } = require("@azure/storage-blob");
const path = require("path");
const fs = require("fs");

// ---- CONFIG ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CONTAINER = process.env.RESULTS_CONTAINER || "results";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 60000);
const CAMPAIGN_STAGED = String(process.env.CAMPAIGN_STAGED || "").toLowerCase() === "true";

// Absolute path to your main campaign JSON schema (ship this file with your repo)
const CAMPAIGN_SCHEMA_PATH = path.join(__dirname, "..", "schemas", "campaign.schema.json");

// ---- util helpers ----
function blobSvc() {
  if (!STORAGE_CONN) throw new Error("AzureWebJobsStorage not configured");
  return BlobServiceClient.fromConnectionString(STORAGE_CONN);
}
async function streamToString(rs) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    rs.on("data", d => chunks.push(d));
    rs.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    rs.on("error", reject);
  });
}
async function getJson(container, blobPath) {
  const b = container.getBlockBlobClient(blobPath);
  if (!(await b.exists())) return null;
  const dl = await b.download();
  const text = await streamToString(dl.readableStreamBody);
  try { return JSON.parse(text); } catch { return null; }
}
async function putJson(container, blobPath, obj) {
  const b = container.getBlockBlobClient(blobPath);
  const buf = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await b.uploadData(buf, { blobHTTPHeaders: { blobContentType: "application/json" } });
}
async function patchStatus(container, prefix, state, extra = {}) {
  const p = `${prefix}status.json`;
  const cur = (await getJson(container, p)) || {};
  const next = { ...cur, state, ...extra };
  await putJson(container, p, next);
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
  try { return JSON.parse(candidate); } catch {}
  const repaired = candidate
    .replace(/^\uFEFF/, "")
    .replace(/"([^"\\]*(\\.[^"\\]*)*)"/g, m => m.replace(/\r?\n/g, "\\n"))
    .replace(/,\s*([}\]])/g, "$1");
  try { return JSON.parse(repaired); } catch {}
  const err = new Error("draft_json_parse_error: unrecoverable JSON");
  err.code = "draft_json_parse_error";
  err.details = { length: candidate.length, head: candidate.slice(0, 1200), tail: candidate.slice(-1200) };
  throw err;
}

// Very small safety check for case_study_library: keep only same-host URLs (when site host known)
function sanitizeCaseStudies(draft, prospectHost) {
  try {
    if (!draft || typeof draft !== "object") return draft;
    const arr = draft?.case_study_library || draft?.case_studies || [];
    if (!Array.isArray(arr) || !prospectHost) return draft;
    const filtered = arr.filter(item => {
      try {
        const u = new URL(String(item?.url || ""));
        return u.host.toLowerCase().endsWith(prospectHost.toLowerCase());
      } catch { return false; }
    });
    if (draft.case_study_library) draft.case_study_library = filtered;
    if (draft.case_studies) draft.case_studies = filtered;
    return draft;
  } catch { return draft; }
}

// ---- harness loader ----
function loadHarness() {
  const modPath = path.join(__dirname, "..", "lib", "prompt-harness.js");
  // eslint-disable-next-line global-require, import/no-dynamic-require
  return require(modPath);
}

module.exports = async function (context, queueItem) {
  const startedAt = new Date().toISOString();

  try {
    if (!queueItem) throw new Error("Queue message is empty");
    const runId = queueItem.runId || queueItem?.data?.runId || queueItem?.id;
    if (!runId) throw new Error("Missing runId in queue message");

    const svc = blobSvc();
    const container = svc.getContainerClient(CONTAINER);
    const prefix = `runs/${runId}/`;

    context.log("[campaign-draft] begin", { runId, staged: CAMPAIGN_STAGED });
    await patchStatus(container, prefix, "DraftCampaign", { runId, draftStartedAt: startedAt });

    // ---- STAGED SHORT-CIRCUIT ----
    if (CAMPAIGN_STAGED) {
      // Minimal yet schema-friendly scaffold (safe empty arrays/objects)
      const stub = {
        meta: { run_id: runId, phase: "DraftCampaign", selected_industry: "General" },
        executive_summary: [],
        evidence_log: [],
        case_study_library: [],
        positioning_and_differentiation: { value_prop: "", differentiators: [], competitor_set: [] },
        messaging_matrix: { nonnegotiables: [], matrix: [] },
        offer_strategy: {
          landing_page: { hero: "", why_it_matters: [], what_you_get: [], how_it_works: [], outcomes: [], proof: [], cta: "" },
          assets_checklist: []
        },
        channel_plan: { emails: [], linkedin: { connect_note: "", insight_post: "", dm: "", comment_strategy: "" } },
        sales_enablement: { discovery_questions: [], objection_cards: [] },
        measurement_and_learning: { kpis: [], weekly_test_plan: [], utm_and_crm: [], evidence_freshness_rule: "" },
        compliance_and_governance: { substantiation_file: "", gdpr_pecr_checklist: [], brand_accessibility_checks: [], approval_log_note: "" },
        risks_and_contingencies: [],
        one_pager_summary: [],
        input_proof: { outline_used: true }
      };
      await putJson(container, `${prefix}campaign.json`, stub);
      await patchStatus(container, prefix, "DraftCampaign", { draftCompletedAt: new Date().toISOString() });
      context.log("[campaign-draft] staged stub written");
      return;
    }

    // ---- Inputs ----
    const outline = await getJson(container, `${prefix}outline.json`);
    if (!outline || !outline.sections) throw new Error("Missing or invalid outline.json");

    const evidenceLog = await getJson(container, `${prefix}evidence_log.json`);
    if (!evidenceLog || !Array.isArray(evidenceLog.evidence_log)) {
      throw new Error("Missing or invalid evidence_log.json");
    }

    const csvNorm = await getJson(container, `${prefix}csv_normalized.json`); // optional
    const site = await getJson(container, `${prefix}site.json`);             // optional

    // Prospect host (for case study safety)
    const prospectSite = (csvNorm?.prospect_website || csvNorm?.company_website || "").trim();
    let prospectHost = "";
    try { prospectHost = prospectSite ? new URL(prospectSite).host : ""; } catch {}

    // CSV signal recap for prompt conditioning
    const csvSignal = {
      industry:
        outline?.meta?.selected_industry ??
        csvNorm?.selectedIndustry ??
        csvNorm?.industry ??
        csvNorm?.meta?.industry ??
        "General",
      spend_band: csvNorm?.ITSpendBand ?? csvNorm?.SpendBand ?? null,
      top_blockers: csvNorm?.TopBlockers ?? csvNorm?.top_blockers ?? [],
      top_needs_supplier: csvNorm?.TopNeedsSupplier ?? csvNorm?.top_needs_supplier ?? [],
      top_purchases: csvNorm?.TopPurchases ?? csvNorm?.top_purchases ?? []
    };

    // Product names (for explicit product references in draft prose)
    const productNames = (() => {
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
      } catch {}
      return Array.from(names).slice(0, 20);
    })();

    // ---- Harness + Prompts ----
    const harness = loadHarness();

    // System rules: JSON-only, citation discipline, outline adherence
    const SYSTEM = `
Return STRICTLY valid JSON that conforms to the campaign schema (provided separately). Do NOT include markdown fences or prose outside JSON.

REUSE ONLY claim_ids that exist in evidence_log.evidence_log[].claim_id.
NO fabricated customers/URLs in case_study_library: only accept items on the same host as the prospect website or present in evidence_log (source_type="Company site").

DO:
- Honour the OUTLINE selections (claim_ids and structure); expand into full prose.
- Explicitly reference real product names from the site data in the executive summary paragraph #1.
- End bullets/sentences with inline citation tags: (Company site), (LinkedIn), (CSV), (Ofcom), (ONS), (DSIT), (PDF extract), (Trade press), (Directory).
- Prefer Ofcom/ONS/DSIT claims for "Why now", proofs, and outcomes.
- UK style for currency and numerals.

DON'T:
- Invent claim_ids, customers, or URLs.
- Quote homepages when a deep link exists in evidence.
`.trim();

    // User content: the outline + evidence catalog + csv/site signals
    const USER = `
OUTLINE (authoritative structure and claim choices):
${safeForPrompt(outline)}

EVIDENCE CATALOG (claims with claim_id):
${safeForPrompt(evidenceLog)}

CSV SIGNALS (buyer landscape to integrate into messaging/offer):
${safeForPrompt(csvSignal)}

SITE PRODUCT NAMES (use exact strings when citing products/services):
${safeForPrompt(productNames)}

OUTPUT: Only JSON conforming to the campaign schema (no fences). Populate all required sections.
- Executive summary item #1 must mention specific product names and tie them to the selected industry context.
- Every bullet in evidence-driven sections must end with a citation tag.
- Messaging matrix should reflect CSV pains/needs/purchases.
- Channel plan: 4 emails with ~90–120 word bodies, each with at least one inline citation.
- case_study_library: only company-host case studies or items present in evidence; otherwise return [].
`.trim();

    if (!fs.existsSync(CAMPAIGN_SCHEMA_PATH)) {
      throw new Error(`campaign.schema.json not found at ${CAMPAIGN_SCHEMA_PATH}`);
    }

    let draft;
    try {
      const messages = [
        { role: "system", content: SYSTEM },
        { role: "user", content: USER }
      ];

      draft = await harness.generate({
        schemaPath: CAMPAIGN_SCHEMA_PATH,        // <— CRITICAL: enforce main campaign schema
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
            backoffMs: Number(process.env.LLM_BACKOFF_MS ?? 600)
          },
          temperature: Number(process.env.LLM_TEMPERATURE ?? 0)
        }
      });

      if (typeof draft === "string") {
        try { draft = JSON.parse(draft); } catch { draft = tryParseOrRepair(draft); }
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
          context.log.warn("[campaign-draft] draft_parse_debug write failed", String(dbgErr?.message || dbgErr));
        }
      }
      await patchStatus(container, prefix, "Failed", {
        error: { code, message: String(e?.message || e), ...(details ? { details: { length: details.length || null } } : {}) },
        draftFailedAt: new Date().toISOString()
      });
      context.log.error("[campaign-draft] failed", String(e?.message || e));
      return;
    }

    // ---- Safety pass (case studies)
    draft = sanitizeCaseStudies(draft, prospectHost);

    // ---- Persist & status ----
    await putJson(container, `${prefix}campaign.json`, draft);
    await patchStatus(container, prefix, "DraftCampaign", { draftCompletedAt: new Date().toISOString() });

    context.log("[campaign-draft] success", { runId });
  } catch (err) {
    context.log.error("[campaign-draft] top-level error", String(err?.message || err));
    // best-effort status update
    try {
      const runId = queueItem?.runId || queueItem?.data?.runId || queueItem?.id || "unknown";
      const svc = blobSvc();
      const container = svc.getContainerClient(CONTAINER);
      const prefix = `runs/${runId}/`;
      await patchStatus(container, prefix, "Failed", {
        error: { code: "draft_error", message: String(err?.message || err) },
        draftFailedAt: new Date().toISOString()
      });
    } catch {}
  }
};
