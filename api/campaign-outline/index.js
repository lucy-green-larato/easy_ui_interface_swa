// /api/campaign-outline/index.js
// Clean rewrite 08-12-2025 — fully aligned with shared storage + shared status + router logic. V4

const { enqueueTo } = require("../lib/campaign-queue");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");
const { updateStatus } = require("../shared/status");
const { nowIso } = require("../shared/utils");
const path = require("path");
const os = require("os");
const fs = require("fs");

// ---- ENV ----
const ROUTER_QUEUE = process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";
const LLM_TIMEOUT_MS = Number(process.env.LLM_TIMEOUT_MS || 45000);

// ---- LLM harness loader ----
let _harness = null;
async function loadHarness() {
  if (_harness) return _harness;

  try {
    const cjs = require("../lib/prompt-harness");
    const generate = cjs.generate || cjs.default?.generate || cjs.default;
    if (typeof generate !== "function") throw new Error("prompt-harness missing generate()");
    _harness = { generate };
    return _harness;
  } catch (e1) {
    // ESM fallback
    const modUrl = new URL("../lib/prompt-harness.js", `file://${__dirname}/`);
    const esm = await import(modUrl.href);
    const generate = esm.generate || esm.default?.generate || esm.default;
    if (typeof generate !== "function") throw new Error("prompt-harness missing generate()");
    _harness = { generate };
    return _harness;
  }
}

// ---- Write temporary schema file for AJV ----
function writeTempSchema(schema) {
  const f = path.join(os.tmpdir(), `outline_schema_${Date.now()}.json`);
  fs.writeFileSync(f, JSON.stringify(schema), "utf8");
  return f;
}

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
          required: [
            "why_now_ids",
            "product_anchor_names",
            "viability_grade",
            "viability_reason_ids"
          ],
          properties: {
            why_now_ids: { type: "array", items: { type: "string" } },
            product_anchor_names: { type: "array", items: { type: "string" } },

            viability_grade: {
              type: ["string", "null"],
              nullable: true
            },

            viability_reason_ids: {
              type: "array",
              items: { type: "string" },
              nullable: true
            }
          }
        },
        positioning: {
          type: "object",
          additionalProperties: false,

          required: [
            "differentiator_ids",
            "viability_grade",
            "viability_reason_ids"
          ],

          properties: {
            differentiator_ids: { type: "array", items: { type: "string" } },

            viability_grade: {
              type: ["string", "null"],
              nullable: true
            },

            viability_reason_ids: {
              type: "array",
              items: { type: "string" },
              nullable: true
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

// ---- Utility: safe truncation for prompt ----
function safeForPrompt(v, max = 300000) {
  try {
    const s = typeof v === "string" ? v : JSON.stringify(v ?? "");
    if (s.length <= max) return s;
    const half = Math.floor(max / 2);
    return s.slice(0, half) + " …TRUNCATED… " + s.slice(-half);
  } catch {
    return "null";
  }
}

// ---- MAIN FUNCTION ----
module.exports = async function (context, queueItem) {
  const startedAt = nowIso();
  let prefix = ""; // make visible to catch

  try {
    if (!queueItem) throw new Error("Queue message empty");

    // Accept stringified JSON messages
    if (typeof queueItem === "string") {
      try { queueItem = JSON.parse(queueItem); }
      catch { throw new Error("Queue message must be JSON"); }
    }

    if (typeof queueItem !== "object") throw new Error("Queue message must be an object");

    const runId = queueItem.runId;
    const page = queueItem.page || "campaign";
    prefix = queueItem.prefix;

    if (!runId) throw new Error("Missing runId");
    if (!prefix) throw new Error("Missing prefix from router");

    // Normalise prefix
    prefix = String(prefix || "").replace(/^\/+/, "");
    if (!prefix.endsWith("/")) prefix += "/";

    // Open container
    const container = await getResultsContainerClient();

    // ---- IDEMPOTENCY GUARD ----
    // If outline was already completed, only resend router notification once.
    const status0 = await getJson(container, `${prefix}status.json`);
    if (status0?.markers?.outlineCompleted) {
      if (!status0?.markers?.afteroutlineSent) {
        await enqueueTo(ROUTER_QUEUE, { op: "afteroutline", runId, prefix, page });
        await updateStatus(container, prefix, {
          markers: { ...(status0.markers || {}), afteroutlineSent: true }
        });
      }
      context.log("[outline] already completed; skipping", { runId });
      return;
    }

    // ---- Phase start in status ----
    await updateStatus(
      container,
      prefix,
      { runId },
      { phase: "Outline", note: "start" }
    );

    // ---- LOAD ALL INPUT ARTIFACTS ----
    const evidenceJson = await getJson(container, `${prefix}evidence.json`);
    const evidenceLogJson = await getJson(container, `${prefix}evidence_log.json`);
    const csvNorm = await getJson(container, `${prefix}csv_normalized.json`);
    const siteJson = await getJson(container, `${prefix}site.json`);
    const productsJson = await getJson(container, `${prefix}products.json`);
    const input = await getJson(container, `${prefix}input.json`);

    // -------- Evidence selection (same as your old logic) --------
    let evidenceArr = [];

    if (Array.isArray(evidenceJson?.claims)) {
      evidenceArr = evidenceJson.claims;
    } else if (Array.isArray(evidenceLogJson?.evidence_log)) {
      evidenceArr = evidenceLogJson.evidence_log;
    } else if (Array.isArray(evidenceLogJson)) {
      evidenceArr = evidenceLogJson;
    }
    evidenceArr = evidenceArr.filter(
      it => it?.claim_id && (it.title || it.summary || it.quote)
    );

    const evidenceForPrompt = evidenceArr.slice(0, 24);

    // ---- Derive product names from site ----
    let productNames = [];

    if (Array.isArray(productsJson?.products) && productsJson.products.length) {
      productNames = productsJson.products;
    } else {
      // NEW: handle array-based site.json correctly
      const snippet = Array.isArray(siteJson)
        ? (siteJson[0]?.snippet || "")
        : (siteJson?.snippet || "");

      if (snippet) {
        const lines = snippet.split(/\r?\n/).slice(0, 2000);
        const out = new Set();

        for (const line of lines) {
          if (/(<h1|<h2|<h3|<li|product|solutions|services)/i.test(line)) {
            const m = line.match(/>([^<>]{3,120})</);
            if (m) {
              const v = m[1].trim();
              if (v && !/[{}<>]/.test(v)) out.add(v);
            }
          }
        }

        productNames = Array.from(out);
      }
    }

    // ---- CSV signals ----
    const modeSpecific = csvNorm?.industry_mode === "specific";
    const selectedIndustryCsv =
      modeSpecific
        ? (csvNorm?.selected_industry || "general").toLowerCase()
        : "general";

    const csvSignals = modeSpecific
      ? (csvNorm?.signals || {})
      : (csvNorm?.global_signals || {});

    // ---- Supplier block ----
    const supplier = {
      supplier_company:
        (input?.supplier_company || input?.prospect_company || "")?.trim(),
      supplier_website:
        (input?.supplier_website || input?.prospect_website || "")?.trim(),
      supplier_linkedin:
        (input?.supplier_linkedin || input?.prospect_linkedin || "")?.trim(),
      supplier_usps: Array.isArray(input?.supplier_usps)
        ? input.supplier_usps
        : [],
      notes: input?.notes || ""
    };

    // ---- Competitors from runConfig + input ----
    const userCompetitors =
      Array.isArray(input?.relevant_competitors)
        ? input.relevant_competitors
        : [];

    const cfgCompetitors =
      Array.isArray(queueItem?.runConfig?.relevant_competitors)
        ? queueItem.runConfig.relevant_competitors
        : [];

    const competitors = [...userCompetitors, ...cfgCompetitors]
      .filter(Boolean)
      .slice(0, 12);

    // ---- SELECTED INDUSTRY ----
    const SELECTED_INDUSTRY =
      input?.selected_industry ||
      input?.campaign_industry ||
      (modeSpecific ? selectedIndustryCsv : "General");

    // ---- Construct SYSTEM + USER messages ----
    const salesModel = String(input?.sales_model || "").toLowerCase();
    const PERSONA = (salesModel === "partner")
      ? "You are a UK B2B channel strategist. Produce claim-ID-only outline."
      : "You are a UK B2B tech strategist. Produce claim-ID-only outline.";

    const SYSTEM = `
${PERSONA}
Return STRICTLY valid JSON that conforms to the provided outline schema.
Use ONLY existing claim_ids.
All arrays must use ONLY values already provided in Evidence, CSV signals, Site products, Supplier, or Competitors.
No prose.
`.trim();

    const USER = `
Evidence (≤24 items):
${safeForPrompt(evidenceForPrompt)}

CSV signals:
${safeForPrompt(csvSignals)}

Site products:
${safeForPrompt(productNames)}

Supplier:
${safeForPrompt(supplier)}

Competitors:
${safeForPrompt(competitors)}
`.trim();

    // ---- Generate outline via harness ----
    const { generate } = await loadHarness();
    const schemaPath = writeTempSchema(OUTLINE_SCHEMA);

    let outline = await generate({
      schemaPath,
      input: {
        industry_mode: modeSpecific ? "specific" : "agnostic",
        selected_industry: selectedIndustryCsv,
        csv_signals: csvSignals,
        product_names: productNames,
        supplier
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
          apiVersion: process.env.AZURE_OPENAI_API_VERSION,
          deployment: process.env.AZURE_OPENAI_DEPLOYMENT,
          api: "chat"
        }
      }
    });

    try { fs.unlinkSync(schemaPath); } catch { }

    if (typeof outline === "string") outline = JSON.parse(outline);
    if (!outline || typeof outline !== "object") throw new Error("Invalid outline");

    // ---- Ensure essential metadata ----
    outline.meta = outline.meta || {};
    outline.meta.run_id = runId;
    outline.meta.phase = "Outline";
    outline.meta.selected_industry = SELECTED_INDUSTRY;

    // ---- Write outline.json ----
    await putJson(container, `${prefix}outline.json`, outline);

    // ---- Update status: state=Outline, outlineCompleted=true ----
    await updateStatus(
      container,
      prefix,
      {
        state: "Outline",
        markers: {
          ...(status0?.markers || {}),
          outlineCompleted: true
        }
      },
      { phase: "Outline", note: "completed" }
    );

    // ---- Send router event ONCE ----
    const st2 = await getJson(container, `${prefix}status.json`);
    if (!st2?.markers?.afteroutlineSent) {
      await enqueueTo(ROUTER_QUEUE, { op: "afteroutline", runId, prefix, page });

      await updateStatus(container, prefix, {
        markers: { ...(st2.markers || {}), afteroutlineSent: true }
      });
    }

    context.log("[outline] success", { runId, startedAt, completedAt: nowIso() });

  } catch (err) {
    context.log.error("[outline] failure", String(err?.message || err));
    // Minimal fail-safe update
    try {
      const container = await getResultsContainerClient();
      const status = (await getJson(container, `${prefix}status.json`)) || {};
      status.state = "Failed";
      status.error = String(err?.message || err);
      status.failedAt = nowIso();
      await putJson(container, `${prefix}status.json`, status);
    } catch { }
  }
};
