// api/campaign-write/index.js v5 07-11-2025
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

// Azure OpenAI (Chat Completions)
const AZO_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT;
const AZO_API_KEY = process.env.AZURE_OPENAI_API_KEY;
const AZO_API_VER = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";
const AZO_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT;

// ---- SECTION MAPS ----
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

// ---- Blob helpers ----
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
  const err = new Error("draft_json_parse_error");
  err.code = "draft_json_parse_error";
  err.details = { length: candidate.length, head: candidate.slice(0, 1000), tail: candidate.slice(-1000) };
  throw err;
}

// ---- LLM (json_object) ----
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
    const wire = raw ? JSON.parse(raw) : null;
    const content = wire?.choices?.[0]?.message?.content || "";
    return tryParseOrRepair(content);
  } finally {
    clearTimeout(timer);
  }
}
// ---- Evidence bundling (catalog + signals + productNames) ----
function deriveSignalsFromCsv(csvCanon) {
  const isSpecific = (
    csvCanon &&
    csvCanon.industry_mode === "specific" &&
    csvCanon.selected_industry
  );
  const mode = isSpecific ? "specific" : "agnostic";
  const sig = isSpecific ? (csvCanon?.signals || {}) : (csvCanon?.global_signals || {});
  return {
    industry_mode: mode,
    selected_industry: isSpecific ? (csvCanon?.selected_industry || null) : null,
    row_count: Number(csvCanon?.meta?.rows || 0),
    top_blockers: Array.isArray(sig.top_blockers) ? sig.top_blockers : [],
    top_needs: Array.isArray(sig.top_needs) ? sig.top_needs
      : (Array.isArray(sig.top_needs_supplier) ? sig.top_needs_supplier : []),
    top_purchases: Array.isArray(sig.top_purchases) ? sig.top_purchases : []
  };
}
function makeEvidenceBundle({ evidenceLog, csvCanon, productNames }) {
  const catalog = Array.isArray(evidenceLog) ? evidenceLog : [];
  const signals = deriveSignalsFromCsv(csvCanon);
  const productNamesArr = Array.isArray(productNames) ? productNames : [];
  return { catalog, signals, productNames: productNamesArr };
}
// ---- Section prompt builders (unchanged in spirit) ----
function buildSectionSystem(finalKey, persona) {
  // Persona prefix (optional)
  const personaPrefix = persona ? `PERSONA\n${persona}\n\n` : "";
  if (finalKey === "executive_summary") {
    return [
      (persona ? `PERSONA\n${persona}\n\n` : "") + "You are a senior UK B2B strategist.",
      "Write a board-ready Executive Summary (≤ 180 words) that enables a go/no-go decision.",
      "Begin in this exact order: 1) Strategy (one line). 2) Target prospects and why. 3) Buyer problems solved (top 3). 4) Campaign type (upsell/win-back/growth, with one-line rationale).",
      "Then include: Moore value proposition that explicitly includes the supplier's services; Objective & scope; Expected outcomes (KPIs/timeframes); Competitive position (2–3 differentiators, cite claim_ids); Go-to-market sketch; Risks (Top 3 with Likelihood/Impact/Mitigation/RAG); Dependencies; Decision points; and a Sales enablement note.",
      "Ground competitive/market statements in provided evidence; include claim_ids inline where used. Use 'TBD' if unknown. No fabrication.",
      `Generate STRICT JSON only for "${finalKey}" matching the 'targets' shape (array of strings).`,
      "Return JSON only; no markdown."
    ].join("\n");
  }
  // Specialised system message for campaign strategy
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
  if (finalKey === "positioning_and_differentiation") {
    return [
      (persona ? `PERSONA\n${persona}\n\n` : "") + "You are a senior UK B2B strategist.",
      "Enforce Geoffrey Moore's value proposition template and include a compact competitor comparison.",
      "Rules:",
      "• Produce a single, crisp Moore-style value prop that explicitly includes the supplier's services.",
      "• Build a short competitor contrast using named competitors and evidence_log claim_ids for substantiation.",
      "• Use only evidence we provide (claim_ids). No fabrication; use 'no external citation available' if needed.",
      "• Keep UK English, concise, specific.",
      "",
      `Generate STRICT JSON only for "${finalKey}" and match the shapes provided.`,
      "Return JSON only; no markdown."
    ].join("\n");
  }

  // Default system message for all other sections
  return [
    personaPrefix + "You are a senior UK B2B strategist.",
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
function targetsFor(finalKey) {
  // Schema-true, guided targets to steer quality & citations
  const t = {
    executive_summary: `{
  "executive_summary": [
    "Strategy: one-line synopsis of the campaign strategy (specific and actionable).",
    "Target prospects: who we are going after and why (1–2 lines; evidence-led).",
    "Buyer problems solved: top 3 pains we address (concise).",
    "Campaign type: upsell | win-back | growth (state which and why).",
    "Moore value proposition: For <target> who <need>, <Supplier> provides <category/services> that <primary benefit>. Unlike <primary competitors>, we <key differentiator>.",
    "Objective & scope: one sentence objective; bullet scope (in/out).",
    "Expected outcomes: 2–4 bullets with KPIs and timeframes (use 'TBD' if unknown).",
    "Competitive position: 2–3 differentiators vs named competitors (cite claim_ids where applicable).",
    "Go-to-market sketch: primary segments, core offer, main channels (1–2 bullets).",
    "Risks (Top 3): each with Likelihood / Impact / Mitigation / RAG.",
    "Dependencies: critical inputs/teams/systems.",
    "Decision points: go/no-go criteria with date or trigger.",
    "Sales enablement note: how sales will use this (one bullet)."
  ]
}`,
    // === PLACE inside your sectionTargets map ===
    campaign_strategy: `{
  "campaign_strategy": {
    "strategic_rationale": "<why we should play in this prospect base>",
    "advantage": ["<how we are better than competitors (specific differentiators)>"],
    "coherent_choices": [
      "<target segments>",
      "<offer outline>",
      "<primary channels>",
      "<messaging focus>",
      "<sequencing/ordering>"
    ],
    "feasibility": ["<key teams/systems/dependencies/constraints>"],
    "expected_outcome": "<quantified KPI/timeframe if available; otherwise 'TBD'>"
  }
}`,
    positioning_and_differentiation: `{
   "positioning_and_differentiation": {
    "moore_value_prop": "For <target> who <compelling-need>, <Supplier> provides <category/services> that <primary benefit>. Unlike <primary competitor(s)>, our product/service <key differentiator>.",
    "competitor_contrast": [
      {
        "vendor": "<name>",
        "what_they_do": "<one line>",
        "our_differentiators": ["<short, concrete items tied to our services>"],
        "evidence_claim_ids": ["<claim_id>", "<claim_id>"]
      }
    ],
    "differentiators": ["<3–6 concise items tied to evidence or site>"],
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
  // --- Normalise evidence to a bundle (backward compatible) ---
  const evBundle = (evidence && typeof evidence === "object" && Array.isArray(evidence.catalog))
    ? evidence
    : makeEvidenceBundle({
      evidenceLog: Array.isArray(evidence) ? evidence : [],
      csvCanon,
      productNames: Array.isArray(products) ? products : []
    });

  const outlineNotes = safeForPrompt(sectionPlan);
  const ev = safeForPrompt(evBundle.catalog);
  const prod = safeForPrompt(evBundle.productNames);
  const csv = safeForPrompt({
    industry_mode: evBundle.signals.industry_mode,
    selected_industry: evBundle.signals.selected_industry,
    row_count: evBundle.signals.row_count,
    top_blockers: evBundle.signals.top_blockers,
    top_needs: evBundle.signals.top_needs,
    top_purchases: evBundle.signals.top_purchases
  });

  // --- Specialised user message for campaign strategy ---
  if (finalKey === "campaign_strategy") {
    const inNotes = (outline && outline.input_notes) ? outline.input_notes : {};
    const supplier_company = (inNotes.supplier_company || inNotes.prospect_company || "").trim();
    const company_website = (inNotes.supplier_website || inNotes.prospect_website || "").trim();
    const supplier_usps = Array.isArray(inNotes.supplier_usps) && inNotes.supplier_usps.length
      ? inNotes.supplier_usps : (Array.isArray(inNotes.user_usps) ? inNotes.user_usps : []);
    const relevant_competitors = Array.isArray(inNotes.relevant_competitors)
      ? inNotes.relevant_competitors.map(String).filter(Boolean).slice(0, 8) : [];
    const selected_industry = inNotes.selected_industry || inNotes.campaign_industry || null;
    const campaign_requirement =
      ["upsell", "win-back", "growth"].includes(inNotes.campaign_requirement)
        ? inNotes.campaign_requirement : "unspecified";
    const campaign_context = inNotes.campaign_context || null;
    const competitive_advantage = inNotes.competitive_advantage || null;

    const strategyEvidence = evBundle.catalog.filter(e =>
      /market|competitor|trend|customer|segment|buying/i.test(e?.category || "")
    );

    return `
Context:
- Supplier: ${supplier_company || "unknown"} | website: ${company_website || "unknown"}
- Industry/segment: ${selected_industry || "General"}
- Campaign type: ${campaign_requirement}
- Services/USPs (anchors): ${safeForPrompt(supplier_usps)}
- Named competitors: ${safeForPrompt(relevant_competitors)}
- Optional strategy inputs: campaign_context=${safeForPrompt(campaign_context)}, competitive_advantage=${safeForPrompt(competitive_advantage)}
- Evidence catalog (strategy-filtered): ${safeForPrompt(strategyEvidence)}
- Full evidence catalog (for citations if needed): ${ev}
- CSV signals: ${csv}
- Product anchors: ${prod}
- Outline plan for "${finalKey}": ${outlineNotes}

Rules:
- Be concrete and feasible; no fabrication. Cite only from provided evidence (claim_ids) where external facts are stated.
- Prefer specifics over generalities; UK English; concise bullets (≤ 220 words total).

Emit exactly the following JSON object for "${finalKey}":
${targetsFor("campaign_strategy")}

Return JSON only.
`.trim();
  }

  if (finalKey === "executive_summary") {
    const inNotes = (outline && outline.input_notes) ? outline.input_notes : {};
    const supplier = (inNotes.supplier_company || inNotes.prospect_company || "").trim();
    const services = Array.isArray(inNotes.supplier_usps) && inNotes.supplier_usps.length
      ? inNotes.supplier_usps : (Array.isArray(inNotes.user_usps) ? inNotes.user_usps : []);
    const targetSegments = Array.isArray(inNotes.target_segments) ? inNotes.target_segments : [];
    const competitors = Array.isArray(inNotes.relevant_competitors)
      ? inNotes.relevant_competitors.map(String).filter(Boolean).slice(0, 8) : [];
    const campaignType = (typeof inNotes.campaign_requirement === "string" &&
      ["upsell", "win-back", "growth"].includes(inNotes.campaign_requirement))
      ? inNotes.campaign_requirement : "unspecified";

    // Evidence + CSV signals already normalised into evBundle
    const buyerProblems = (evBundle.signals.top_blockers || []).slice(0, 3);
    const buyerNeeds = (evBundle.signals.top_needs || []).slice(0, 3);

    return `
Context:
- Outline plan for "${finalKey}": ${outlineNotes}
- Supplier: ${supplier || "unknown"}
- Services to include in Moore value prop: ${safeForPrompt(services)}
- Target prospects (segments, industries): ${safeForPrompt(targetSegments)}
- Buyer problems (from CSV signals top_blockers): ${safeForPrompt(buyerProblems)}
- Buyer needs (from CSV signals top_needs): ${safeForPrompt(buyerNeeds)}
- Campaign type (from UI): ${campaignType}
- Named competitors: ${safeForPrompt(competitors)}
- Evidence catalog ARRAY (use claim_ids when citing): ${ev}
- CSV signals snapshot: ${csv}
- Product anchors: ${prod}

Instructions:
- Start with: Strategy one-liner → Target prospects and why → Buyer problems solved (top 3) → Campaign type (upsell/win-back/growth + brief rationale).
- Then add the remaining bullets exactly as per targets.
- When mentioning competitors or market facts, append claim_ids from the evidence catalog.
- Use specifics if present; otherwise 'TBD'. UK English, concise, decision-oriented.

Emit exactly the following JSON object for "${finalKey}":
${targetsFor(finalKey)}

Return JSON only.
`.trim();
  }
  if (finalKey === "positioning_and_differentiation") {
    const inNotes = (outline && outline.input_notes) ? outline.input_notes : {};
    const supplier = (inNotes.supplier_company || inNotes.prospect_company || "").trim();
    const services = Array.isArray(inNotes.supplier_usps) && inNotes.supplier_usps.length
      ? inNotes.supplier_usps : (Array.isArray(inNotes.user_usps) ? inNotes.user_usps : []);
    const competitors = Array.isArray(inNotes.relevant_competitors)
      ? inNotes.relevant_competitors.map(String).filter(Boolean).slice(0, 8) : [];

    // Use the evidence bundle you already normalise at the top (evBundle)
    const evIdsHint = Array.isArray(evBundle.catalog) ? evBundle.catalog.slice(0, 400).map(x => x.claim_id).filter(Boolean) : [];
    return `
Context:
- Outline plan for "${finalKey}": ${outlineNotes}
- Supplier: ${supplier || "unknown"}
- Services to include in value prop (anchors): ${safeForPrompt(services)}
- Named competitors to compare against (if any): ${safeForPrompt(competitors)}
- Evidence catalog ARRAY with claim_ids (use for competitor contrast substantiation): ${ev}
- Product anchors: ${prod}
- CSV signals snapshot: ${csv}

Instructions:
- Produce a Moore-style value proposition (explicitly include the supplier's services).
- Create a compact competitor_contrast list/table using the named competitors when present; otherwise pick the most relevant competitors from evidence.
- For each competitor contrast item, include 1–3 'our_differentiators' and cite evidence via 'evidence_claim_ids' taken from the evidence catalog.
- Keep it concise and decision-oriented.

Emit exactly the following JSON object for "${finalKey}":
${targetsFor(finalKey)}

Return JSON only.
`.trim();
  }

  // --- Default user message for other sections (your original) ---
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
- Product name anchors (from site or products file): ${prod}
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

  // Resolve authoritative container-relative prefix from the queue message,
  // fallback to legacy runs/<runId>/ if not provided.
  let prefix = (typeof queueItem.prefix === "string" && queueItem.prefix.trim())
    ? queueItem.prefix.trim()
    : `runs/${runId}/`;
  if (prefix.startsWith(`${CONTAINER}/`)) prefix = prefix.slice(`${CONTAINER}/`.length);
  if (prefix.startsWith("/")) prefix = prefix.replace(/^\/+/, "");
  if (!prefix.endsWith("/")) prefix += "/";

  context.log("[campaign-write] resolved prefix", { runId, prefix });

  try {
    // Load common inputs
    const outline = await getJson(container, `${prefix}outline.json`);
    const evRaw = await getJson(container, `${prefix}evidence_log.json`);
    const csvCanon = await getJson(container, `${prefix}csv_normalized.json`);
    const site = await getJson(container, `${prefix}site.json`);
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

    // ---- Product names
    let productNames = Array.isArray(outline?.input_notes?.product_mentions)
      ? outline.input_notes.product_mentions : null;

    if (!productNames || productNames.length === 0) {
      const productsFile = await getJson(container, `${prefix}products.json`);
      if (Array.isArray(productsFile?.products)) productNames = productsFile.products;
    }
    if (!productNames || productNames.length === 0) {
      // Heuristic on site.json snapshot
      productNames = (() => {
        const names = new Set();
        const add = v => { if (typeof v === "string" && v.trim()) names.add(v.trim()); };
        try {
          if (Array.isArray(site?.products)) site.products.forEach(add);
          if (Array.isArray(site)) {
            site.forEach(p => { if (Array.isArray(p?.headings)) p.headings.forEach(add); });
          } else if (Array.isArray(site?.pages)) {
            site.pages.forEach(p => { if (Array.isArray(p?.headings)) p.headings.forEach(add); });
          }
        } catch { /* best-effort */ }
        return Array.from(names).slice(0, 12);
      })();
    }

    // ---------- Branch by job type ----------
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

      const plan = outline?.sections?.[outlineKey] ?? {};
      const evidenceBundle = makeEvidenceBundle({ evidenceLog, csvCanon, productNames });
      const sectionJson = await buildSectionJson({
        finalKey,
        outline,
        sectionPlan: plan,
        evidence: evidenceBundle, // <-- now a bundle with catalog + signals + productNames
        csvCanon,                 // keep passing for backward compatibility (used in prompt text)
        products: productNames,   // keep passing for backward compatibility (used in prompt text)
        persona: PERSONA
      });
      await putJson(container, `${prefix}sections/${finalKey}.json`, sectionJson);
      context.log(`[campaign-write] wrote section ${finalKey}`);

      // Notify orchestrator AFTER successful write
      try {
        const qs = QueueServiceClient.fromConnectionString(STORAGE_CONN);
        const qc = qs.getQueueClient(CAMPAIGN_QUEUE);
        await qc.createIfNotExists();
        const page = (queueItem && (queueItem.page || queueItem?.data?.page)) || "campaign";
        const msg = { op: "aftersection", runId, page, prefix, section: finalKey };
        await qc.sendMessage(JSON.stringify(msg)); // SDK base64 encodes for us
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
        evidence_log: evidenceLog
      };

      for (const outlineKey of SECTION_ORDER) {
        const finalKey = SECTION_MAP[outlineKey];
        const obj = await getJson(container, `${prefix}sections/${finalKey}.json`);
        if (obj && obj[finalKey] != null) {
          merged[finalKey] = obj[finalKey];
        } else {
          context.log.warn(`[campaign-write] missing section during assemble: ${finalKey}`);
        }
      }

      const normalised = normaliseContract(merged);
      await putJson(container, `${prefix}campaign.json`, normalised);

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

    throw new Error(`Unknown job type "${op}". Use "write_section" or "assemble".`);
  } catch (err) {
    context.log.error("[campaign-write] error", String(err?.message || err));
    try {
      await patchStatus(
        blobSvc().getContainerClient(CONTAINER),
        prefix,
        "Failed",
        { error: { code: err.code || "writer_error", message: String(err?.message || err) } }
      );
    } catch { /* ignore */ }
  }
};

// ---- UI schema normalisers ----
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

  c.executive_summary = toStringArray(c.executive_summary);

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

  if (!Array.isArray(c.case_study_library) && Array.isArray(c.case_studies)) {
    c.case_study_library = c.case_studies;
  }

  c.positioning_and_differentiation = c.positioning_and_differentiation || {};
  c.offer_strategy = c.offer_strategy || {};
  c.channel_plan = c.channel_plan || {};
  c.compliance_and_governance = c.compliance_and_governance || {};
  c.one_pager_summary = rowsOf(c.one_pager_summary).map(String);

  return c;
}
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
