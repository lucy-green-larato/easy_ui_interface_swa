// /api/campaign-outline/index.js 03-12-2025 v14
// Queue-triggered on %Q_CAMPAIGN_OUTLINE% (by router) to create <prefix>outline.json,
// then posts a single {op:"afteroutline"} to %CAMPAIGN_QUEUE_NAME%.
//
// Inputs under results/<prefix>/ : evidence_log.json, csv_normalized.json, products.json, site.json, input.json
// Output: outline.json
// Status: Append history at phase start; set state="Outline" exactly once at completion
//
// Non-negotiables observed:
// - No loops: never re-enqueue itself; single-shot afteroutline (status.markers.afteroutlineSent)
// - Idempotent: if outline already completed (status.markers.outlineCompleted), skip work
// - Prefix hygiene: container-relative; tolerate overrides
// - Robust prompt-harness loader (CJS/ESM)

const { BlobServiceClient } = require("@azure/storage-blob");
const { enqueueTo } = require("../lib/campaign-queue");
const { canonicalPrefix } = require("../lib/prefix");
const path = require("path");
const os = require("os");
const fs = require("fs");

// ---- ENV ----
const STORAGE_CONN = process.env.AzureWebJobsStorage;
const CONTAINER = process.env.CAMPAIGN_RESULTS_CONTAINER || "results";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 45000);
const ROUTER_QUEUE_NAME = process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";


// ---- Outline schema (prose-free; claim_ids only) ----
const OUTLINE_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  title: "campaign_outline",
  type: "object",
  additionalProperties: false,
  required: ["meta", "input_notes", "sections"],
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
      },
      required: [
        "spend_band",
        "top_blockers",
        "top_needs_supplier",
        "top_purchases",
        "product_mentions",
        "supplier_company",
        "supplier_website",
        "supplier_linkedin",
        "supplier_usps",
        "campaign_industry",
        "campaign_requirement",
        "relevant_competitors",
        "notes"
      ]
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
            product_anchor_names: { type: "array", items: { type: "string" } },
            viability_grade: {
              type: "string"
            },
            viability_reason_ids: {
              type: "array",
              items: { type: "string" }
            }
          }
        },
        positioning: {
          type: "object",
          additionalProperties: false,
          required: ["differentiator_ids"],
          properties: {
            differentiator_ids: { type: "array", items: { type: "string" } },
            viability_grade: {
              type: "string"
            },
            viability_reason_ids: {
              type: "array",
              items: { type: "string" }
            }
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

// ---- utils ----
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
async function getJson(containerClient, relPath) {
  const b = containerClient.getBlockBlobClient(relPath);
  if (!(await b.exists())) return null;
  const dl = await b.download();
  const txt = await streamToString(dl.readableStreamBody);
  try { return JSON.parse(txt); } catch { return null; }
}
async function putJson(containerClient, relPath, obj) {
  const b = containerClient.getBlockBlobClient(relPath);
  const body = Buffer.from(JSON.stringify(obj, null, 2), "utf8");
  await b.uploadData(body, { blobHTTPHeaders: { blobContentType: "application/json; charset=utf-8" } });
}
function normalizePrefix(p) {
  let x = String(p || "").trim();
  if (!x) return null;
  if (x.startsWith(`${CONTAINER}/`)) x = x.slice(`${CONTAINER}/`.length);
  x = x.replace(/^\/+/, "");
  if (!x.endsWith("/")) x += "/";
  return x;
}
function nowISO() { return new Date().toISOString(); }
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

// ---- harness loader (CJS fast path; ESM fallback; no top-level throw) ----
let _harness;
async function loadHarness() {
  if (_harness) return _harness;
  try {
    // eslint-disable-next-line import/no-dynamic-require, global-require
    const cjs = require("../lib/prompt-harness");
    const generate = cjs?.generate || cjs?.default?.generate || cjs?.default || cjs;
    if (typeof generate !== "function") throw new Error("prompt-harness.generate missing");
    _harness = { generate };
    return _harness;
  } catch (e1) {
    try {
      const modUrl = new URL("../lib/prompt-harness.js", `file://${__dirname}/`);
      const esm = await import(modUrl.href);
      const generate = esm?.generate || esm?.default?.generate || esm?.default || esm;
      if (typeof generate !== "function") throw new Error("prompt-harness.generate missing");
      _harness = { generate };
      return _harness;
    } catch (e2) {
      throw new Error(`prompt-harness load failed: ${e1?.message || e1} | ${e2?.message || e2}`);
    }
  }
}

// ---- temp schema file (for harness.validate) ----
function writeTempOutlineSchema(runId) {
  const f = path.join(os.tmpdir(), `outline_${runId}.schema.json`);
  fs.writeFileSync(f, JSON.stringify(OUTLINE_SCHEMA), "utf8");
  return f;
}

// Prefer claim_ids that mention any named competitors
function claimIdsForCompetitors(evidenceArr, namedCompetitors = [], limit = 8) {
  if (!Array.isArray(evidenceArr) || !evidenceArr.length) return [];
  const names = new Set(
    (namedCompetitors || [])
      .map(s => (typeof s === "string" ? s.trim().toLowerCase() : ""))
      .filter(Boolean)
  );
  if (!names.size) return [];
  const hits = [];
  for (const it of evidenceArr) {
    const text = `${it.title || ""} ${it.summary || ""} ${it.quote || ""}`.toLowerCase();
    if ([...names].some(n => text.includes(n)) && it.claim_id) {
      hits.push(it.claim_id);
      if (hits.length >= limit) break;
    }
  }
  return hits;
}

// ---- status helpers (append-only history; single state flip at end) ----
async function readStatus(container, prefix) {
  return (await getJson(container, `${prefix}status.json`)) || {};
}
async function patchStatus(container, prefix, patch, historyNode) {
  const cur = await readStatus(container, prefix);
  const next = { ...cur };
  if (!Array.isArray(next.history)) next.history = [];
  if (historyNode) next.history.push({ at: nowISO(), ...historyNode });
  for (const [k, v] of Object.entries(patch || {})) next[k] = v;
  await putJson(container, `${prefix}status.json`, next);
  return next;
}

module.exports = async function (context, queueItem) {
  const startedAt = nowISO();
  let runId = "unknown";
  let prefix = null;
  let container = null;

  try {
    if (!queueItem) throw new Error("Queue message is empty");

    // Normalise string payloads to JSON
    if (typeof queueItem === "string") {
      try {
        queueItem = JSON.parse(queueItem);
      } catch (e) {
        throw new Error("Queue message must be valid JSON when sent as a string");
      }
    }

    if (queueItem == null || typeof queueItem !== "object") {
      throw new Error("Queue message must be an object");
    }

    // Support legacy or direct kickoff messages (safety only)
    if (queueItem.op === "kickoff" && queueItem.prefix) {
      queueItem.op = "afterevidence"; // treat as if evidence just completed
    }

    runId = queueItem.runId || queueItem?.data?.runId || queueItem?.id;
    if (!runId) throw new Error("Missing runId");

    if (!STORAGE_CONN) throw new Error("AzureWebJobsStorage not configured");
    const svc = blobSvc();
    container = svc.getContainerClient(CONTAINER);

    // ---- PREFIX AUTHORITY  ----
    if (queueItem.prefix) {
      // queue-provided prefix is always canonical, must be used as-is
      prefix = normalizePrefix(queueItem.prefix);
    } else {
      // fallback ONLY if prefix was not provided by router
      prefix = canonicalPrefix({
        userId: queueItem.userId || queueItem.user || "anonymous",
        page: queueItem.page || "campaign",
        runId,
        date: queueItem.date ? new Date(queueItem.date) : undefined
      });
    }
    const page = queueItem.page || queueItem?.data?.page || "campaign";
    const runConfig = queueItem.runConfig || {};

    // Idempotence: skip if already completed
    const status0 = await readStatus(container, prefix);
    if (status0?.markers?.outlineCompleted) {
      if (!status0?.markers?.afteroutlineSent) {
        try {
          await enqueueTo(ROUTER_QUEUE_NAME, { op: "afteroutline", runId, page, prefix });

          const st = await readStatus(container, prefix);
          st.markers = { ...(st.markers || {}), afteroutlineSent: true };
          await putJson(container, `${prefix}status.json`, st);
        } catch (e) {
          context.log.warn("[outline] resend afteroutline failed", String(e?.message || e));
        }
      }
      context.log("[outline] already completed; skipping", { runId });
      return;
    }

    // Append phase start (history only; no state flip yet)
    await patchStatus(
      container,
      prefix,
      { runId, markers: { ...(status0.markers || {}) } },
      { phase: "Outline", note: "start" }
    );

    // ---- Load artifacts ----
    const evidenceRaw = await getJson(container, `${prefix}evidence_log.json`);
    let evidenceArr = [];

    try {
      const evCanon = await getJson(container, `${prefix}evidence.json`);
      if (evCanon && Array.isArray(evCanon.claims)) evidenceArr = evCanon.claims;
    } catch {
      /* non-fatal */
    }

    if (!Array.isArray(evidenceArr) || !evidenceArr.length) {
      if (Array.isArray(evidenceRaw)) {
        evidenceArr = evidenceRaw;
      } else if (evidenceRaw && Array.isArray(evidenceRaw.evidence_log)) {
        evidenceArr = evidenceRaw.evidence_log;
      } else {
        evidenceArr = [];
      }
    }

    // Keep only items that have a claim id, some text, and a URL
    let evidenceLog = (evidenceArr || []).filter(it =>
      it && it.claim_id && (it.title || it.summary || it.quote) && (it.url || it.source_url || it.text)
    );

    const evidenceForPrompt = evidenceLog.slice(0, 24);

    const csvNorm = await getJson(container, `${prefix}csv_normalized.json`);
    const siteLoaded = await getJson(container, `${prefix}site.json`);
    const site = Array.isArray(siteLoaded) ? siteLoaded : (siteLoaded ? [siteLoaded] : []);
    const productsFile = await getJson(container, `${prefix}products.json`);
    const input = await getJson(container, `${prefix}input.json`);

    // products: prefer products.json; fallback to homepage snippet
    let productNames = Array.isArray(productsFile?.products) && productsFile.products.length
      ? productsFile.products
      : (() => {
        const first = site?.[0]?.snippet || "";
        if (!first) return [];
        const lines = first.split(/\r?\n/).slice(0, 2000);
        const out = new Set();
        for (const line of lines) {
          if (/(<h1|<h2|<h3|<li|product|solutions|services)/i.test(line)) {
            const m = line.match(/>([^<>]{3,120})</);
            if (m) {
              const val = m[1].trim();
              if (
                val &&
                !/[<>{}]/.test(val) &&
                val.split(/\s+/).length <= 6 &&
                !/^(home|about|contact|login|support|learn|blog|cookie|privacy|terms|partners|resources)$/i.test(val) &&
                !/^(read more|learn more)$/i.test(val)
              ) out.add(val);
            }
          }
        }
        return Array.from(out);
      })();

    // CSV → derive mode + signals
    const modeSpecific = (csvNorm && csvNorm.industry_mode === "specific" && csvNorm.selected_industry);
    const selectedIndustryCsv = modeSpecific ? String(csvNorm.selected_industry || "").trim().toLowerCase() : null;
    const srcSignals = modeSpecific ? (csvNorm?.signals || {}) : (csvNorm?.global_signals || {});
    const csvSignal = {
      industry_mode: modeSpecific ? "specific" : "agnostic",
      selected_industry: selectedIndustryCsv,
      spend_band: srcSignals?.spend_band ?? null,
      top_blockers: Array.isArray(srcSignals?.top_blockers) ? srcSignals.top_blockers : [],
      top_needs_supplier: Array.isArray(srcSignals?.top_needs_supplier) ? srcSignals.top_needs_supplier : [],
      top_purchases: Array.isArray(srcSignals?.top_purchases) ? srcSignals.top_purchases : []
    };

    // Persona
    const salesModel = String((input?.sales_model ?? input?.salesModel ?? input?.call_type ?? "")).trim().toLowerCase();
    const PERSONA = (salesModel === "partner")
      ? `
You are a top-performing UK B2B channel strategist (tech markets) and CMO.
You understand partner recruitment, enablement, and common channel constraints (M&A, higher interest rates, lead-gen pressure).
Your job is to produce an evidence-only campaign that adds value to the partners by making them more competitive, improving their customer service, and differentiating them in the market.`.trim()
      : `
You are a top-performing UK B2B tech market strategist and CMO.
You understand customer challenges (productivity, investing in the right technologies).
Your job is to produce an evidence-only campaign that adds value to direct customers by making them more efficient and productive, improving customer service, and differentiating them in the market.`.trim();

    const supplierBlock = {
      supplier_company: (input?.supplier_company || input?.company_name || input?.prospect_company || "")?.trim() || "",
      supplier_website: (input?.supplier_website || input?.company_website || input?.prospect_website || "")?.trim() || "",
      supplier_linkedin: (input?.supplier_linkedin || input?.company_linkedin || input?.prospect_linkedin || "")?.trim() || "",
      supplier_usps: Array.isArray(input?.supplier_usps) ? input.supplier_usps : [],
      notes: (input?.notes || "")?.trim() || ""
    };

    // competitor-steering
    const userCompetitors = Array.isArray(input?.relevant_competitors)
      ? input.relevant_competitors
      : (Array.isArray(input?.competitors) ? input.competitors : []);

    const cfgCompetitors = Array.isArray(runConfig?.relevant_competitors)
      ? runConfig.relevant_competitors
      : [];

    const preferUserThenCfg = [...userCompetitors, ...cfgCompetitors]
      .map(s => (typeof s === "string" ? s.trim() : ""))
      .filter(Boolean)
      .slice(0, 12);

    const competitorClaimIds = claimIdsForCompetitors(
      evidenceLog,
      preferUserThenCfg,
      8
    );

    // Load viability + map viability → claim_ids
    const viability = await getJson(container, `${prefix}strategy_v3/viability.json`);

    let viabilityGrade = null;
    let viabilityReasonTexts = [];
    let viabilityClaimIds = [];

    if (viability && typeof viability === "object") {
      // 1. grade
      viabilityGrade = viability.grade || viability.grade_final || null;

      // 2. collect messages (red + amber + dimensions)
      const pushMsg = (x) => {
        if (!x) return;
        const s = String(x).trim();
        if (s) viabilityReasonTexts.push(s);
      };

      // flags.red, flags.amber, flags.green
      if (viability.flags) {
        ["red", "amber", "green"].forEach(level => {
          if (Array.isArray(viability.flags[level])) {
            viability.flags[level].forEach(m => pushMsg(m?.message || m));
          }
        });
      }

      // dimensions.*.message
      if (viability.dimensions && typeof viability.dimensions === "object") {
        for (const dim of Object.values(viability.dimensions)) {
          pushMsg(dim?.message);
        }
      }

      // Deduplicate
      viabilityReasonTexts = [...new Set(viabilityReasonTexts)];

      // 3. Map messages → evidence claim_ids (keyword overlap)
      const matchMsgToClaimIds = (msg) => {
        if (!msg || !evidenceLog.length) return [];
        const m = msg.toLowerCase();
        const out = [];

        for (const ev of evidenceLog) {
          const hay = `${ev.title || ""} ${ev.summary || ""} ${ev.quote || ""}`.toLowerCase();
          // simple overlap test: any shared meaningful word > 3 chars
          const words = m.split(/\W+/).filter(w => w.length > 3);
          if (!words.length) continue;
          if (words.some(w => hay.includes(w))) {
            if (ev.claim_id) out.push(ev.claim_id);
          }
        }
        return out;
      };

      viabilityReasonTexts.forEach(vtxt => {
        matchMsgToClaimIds(vtxt).forEach(cid => viabilityClaimIds.push(cid));
      });

      viabilityClaimIds = [...new Set(viabilityClaimIds)].slice(0, 12);
    }

    // Industry resolution
    const SELECTED_INDUSTRY = (() => {
      const userIndustry = (input?.selected_industry || input?.campaign_industry || input?.company_industry || "").trim();
      if (userIndustry) return userIndustry;
      if (modeSpecific) return selectedIndustryCsv || "General";
      return "General";
    })();

    // SYSTEM + USER messages (prose-free outline; claim_ids only)
    const SYSTEM = `
Return STRICTLY valid JSON that conforms to the provided outline schema.
NO prose, NO markdown fences. Use ONLY claim_ids that exist in evidenceLog[].claim_id

PERSONA
${PERSONA}

GOALS
- selected_industry: "${SELECTED_INDUSTRY}"
- Build a campaign OUTLINE only (no sentences): select claim_ids for each section using existing evidence.
- Balance evidence weighting: regulator/government (Ofcom/ONS/DSIT) are peers to CSV + reputable sources; do not overweight regulators.
- Use product names found in site data only for product_anchor_names.

CONSTRAINTS
- Do not invent claim_ids.
- Keep arrays compact and relevant.
- If a required section has no suitable evidence, leave it empty ([]) rather than inventing content.
`.trim();

    const USER = `
Input: evidence_log (claim catalog; ≤24 items):
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
`.trim();

    // Generate outline
    const { generate } = await loadHarness();
    let outline = await generate({
      schemaPath: writeTempOutlineSchema(runId),
      input: {
        industry_mode: csvSignal.industry_mode,
        selected_industry: csvSignal.selected_industry,
        csv_signals: {
          spend_band: csvSignal.spend_band ?? null,
          top_blockers: csvSignal.top_blockers,
          top_needs_supplier: csvSignal.top_needs_supplier,
          top_purchases: csvSignal.top_purchases
        },
        product_names: productNames,
        supplier: supplierBlock
      },
      options: {
        messages: [
          { role: "system", content: SYSTEM },
          { role: "user", content: USER }
        ],
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

    // ---- Cleanup temp schema file (avoid accumulation in /tmp) ----
    try {
      const schemaPath = path.join(os.tmpdir(), `outline_${runId}.schema.json`);
      fs.unlinkSync(schemaPath);
    } catch { /* cleanup best-effort */ }


    if (typeof outline === "string") {
      try { outline = JSON.parse(outline); } catch { outline = tryParseOrRepair(outline); }
    }
    if (!outline || typeof outline !== "object") throw new Error("outline_generation_failed: no outline object");

    // Validate/repair essentials
    if (!outline.meta || typeof outline.meta !== "object") outline.meta = {};
    outline.meta.run_id = outline.meta.run_id || runId;
    outline.meta.phase = "Outline";
    if (csvSignal.industry_mode === "specific") {
      outline.meta.selected_industry = csvSignal.selected_industry || "general";
    } else {
      outline.meta.selected_industry = "General";
    }

    if (!outline.input_notes || typeof outline.input_notes !== "object") outline.input_notes = {};
    const inNotes = outline.input_notes;

    inNotes.spend_band = inNotes.spend_band ?? (csvSignal.spend_band ?? "unknown");
    if (!Array.isArray(inNotes.top_blockers)) inNotes.top_blockers = csvSignal.top_blockers;
    if (!Array.isArray(inNotes.top_needs_supplier)) inNotes.top_needs_supplier = csvSignal.top_needs_supplier;
    if (!Array.isArray(inNotes.top_purchases)) inNotes.top_purchases = csvSignal.top_purchases;
    if (!Array.isArray(inNotes.product_mentions)) inNotes.product_mentions = Array.isArray(productNames) ? productNames.slice(0, 12) : [];

    if (!("supplier_company" in inNotes)) {
      inNotes.supplier_company = supplierBlock.supplier_company || "";
    }
    inNotes.supplier_website = inNotes.supplier_website || supplierBlock.supplier_website || "";
    inNotes.supplier_linkedin = inNotes.supplier_linkedin || supplierBlock.supplier_linkedin || "";
    if (!Array.isArray(inNotes.supplier_usps)) inNotes.supplier_usps = supplierBlock.supplier_usps.slice(0, 12);
    inNotes.campaign_industry = inNotes.campaign_industry || input?.campaign_industry || input?.company_industry || "";
    if (typeof inNotes.campaign_requirement !== "string") {
      const req = runConfig?.campaign_requirement;
      inNotes.campaign_requirement = (typeof req === "string" && ["upsell", "win-back", "growth"].includes(req)) ? req : null;
    }
    if (!Array.isArray(inNotes.relevant_competitors)) inNotes.relevant_competitors = [];
    if (inNotes.relevant_competitors.length === 0 && Array.isArray(runConfig?.relevant_competitors)) {
      inNotes.relevant_competitors = runConfig.relevant_competitors.filter(x => typeof x === "string" && x.trim()).slice(0, 8);
    }
    inNotes.notes = inNotes.notes || supplierBlock.notes || "";

    if (!outline.sections || typeof outline.sections !== "object") outline.sections = {};
    const reqObj = (n) => { if (!outline.sections[n] || typeof outline.sections[n] !== "object") outline.sections[n] = {}; return outline.sections[n]; };
    const exec = reqObj("exec");
    if (!Array.isArray(exec.why_now_ids)) exec.why_now_ids = [];
    if (!Array.isArray(exec.product_anchor_names)) exec.product_anchor_names = [];
    if (viabilityGrade) {
      exec.viability_grade = viabilityGrade;
    }
    if (Array.isArray(viabilityClaimIds) && viabilityClaimIds.length) {
      exec.viability_reason_ids = viabilityClaimIds.slice(0, 12);
    }

    const positioning = reqObj("positioning");
    if (!Array.isArray(positioning.differentiator_ids)) {
      positioning.differentiator_ids = [];
    }
    if (Array.isArray(competitorClaimIds) && competitorClaimIds.length) {
      const merged = new Set([
        ...(positioning.differentiator_ids || []),
        ...competitorClaimIds
      ]);
      positioning.differentiator_ids = Array.from(merged).slice(0, 12);
    }
    if (viabilityGrade) {
      positioning.viability_grade = viabilityGrade;
    }
    if (Array.isArray(viabilityClaimIds) && viabilityClaimIds.length) {
      positioning.viability_reason_ids = viabilityClaimIds.slice(0, 12);
    }


    if (!Array.isArray(outline.sections.messaging)) outline.sections.messaging = [];
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

    // Persist + single state flip (Outline)
    await putJson(container, `${prefix}outline.json`, outline);
    await new Promise(r => setTimeout(r, 75));

    const stDone = await readStatus(container, prefix);
    stDone.state = "Outline";
    stDone.history = Array.isArray(stDone.history) ? stDone.history : [];
    stDone.history.push({ at: nowISO(), phase: "Outline", note: "completed" });
    stDone.markers = { ...(stDone.markers || {}), outlineCompleted: true };
    await putJson(container, `${prefix}status.json`, stDone);

    // Single-shot notify {op:"afteroutline"} on main queue
    if (!stDone.markers.afteroutlineSent) {
      try {
        await enqueueTo(ROUTER_QUEUE_NAME, { op: "afteroutline", runId, page, prefix });

        const st2 = await readStatus(container, prefix);
        st2.markers = { ...(st2.markers || {}), afteroutlineSent: true };
        await putJson(container, `${prefix}status.json`, st2);
      } catch (notifyErr) {
        context.log.warn("[outline] notify afteroutline failed", String(notifyErr?.message || notifyErr));
      }
    }
    context.log("[outline] success", { runId });
  } catch (err) {
    context.log.error("[outline] failure", String(err?.message || err));
    try {
      const pref = typeof prefix === "string" ? (prefix.endsWith("/") ? prefix : `${prefix}/`) : `runs/${runId}/`;
      const cont = container || BlobServiceClient.fromConnectionString(process.env.AzureWebJobsStorage).getContainerClient(CONTAINER);
      const cur = (await getJson(cont, `${pref}status.json`)) || {};
      cur.state = "Failed";
      cur.failedAt = nowISO();
      cur.error = { code: "outline_error", message: String(err?.message || err) };
      cur.history = Array.isArray(cur.history) ? cur.history : [];
      cur.history.push({ at: nowISO(), phase: "Outline", note: "failed", error: String(err?.message || err) });
      await putJson(cont, `${pref}status.json`, cur);
    } catch { /* ignore */ }
  }
};
