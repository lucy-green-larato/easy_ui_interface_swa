// /api/campaign-write/index.js 11-11-2025 v17 (Writer/Assembler)
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
// Sections we render deterministically (strategy + CSV + evidence only)
const DETERMINISTIC_SECTIONS = new Set([
  "positioning_and_differentiation",
  "messaging_matrix",
  "sales_enablement"
]);

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
  const needs = Array.isArray(sig.top_needs_supplier) && sig.top_needs_supplier.length
    ? sig.top_needs_supplier
    : (Array.isArray(sig.top_needs) ? sig.top_needs : []);
  return {
    top_blockers: Array.isArray(sig.top_blockers) ? sig.top_blockers : [],
    top_needs: needs,
    top_purchases: Array.isArray(sig.top_purchases) ? sig.top_purchases : []
  };
}
function makeEvidenceBundle({ evidenceLog, csvCanon, productNames }) {
  const catalog = Array.isArray(evidenceLog) ? evidenceLog : [];
  const signals = deriveSignalsFromCsv(csvCanon);
  const productNamesArr = Array.isArray(productNames) ? productNames : [];
  return { catalog, signals, productNames: productNamesArr };
}
// PATCH WR-COMP: prefer user competitors; merge with evidence; keep shape
function renderPositioningFromStrategy({ strategy, evidenceBundle, csvCanon }) {
  const s = strategy || {};
  const meta = s.meta || {};
  const usps = Array.isArray(meta.usps) ? meta.usps.filter(v => typeof v === "string" && v.trim()) : [];

  // Buyer pains directly from CSV signals
  const sig = (csvCanon && csvCanon.signals) || {};
  const pains = Array.isArray(sig.top_blockers) ? sig.top_blockers.filter(Boolean) : [];
    const needs = (Array.isArray(sig.top_needs_supplier) && sig.top_needs_supplier.length
    ? sig.top_needs_supplier
    : (Array.isArray(sig.top_needs) ? sig.top_needs : [])
  ).filter(Boolean);

  // ---- Build a quick lookup from evidence catalog (by vendor -> { url, title })
  const catalog = Array.isArray(evidenceBundle?.catalog) ? evidenceBundle.catalog : [];
  const byVendor = new Map();
  for (const it of catalog) {
    const vendor = String(it.vendor || it.source_vendor || "").trim();
    if (!vendor) continue;
    const url = String(it.url || "").trim();
    const title = String(it.title || "Relevant in consideration");
    const key = vendor.toLowerCase();
    // prefer first seen (stable)
    if (!byVendor.has(key)) byVendor.set(key, { vendor, url: url && url.startsWith("https") ? url : "", title });
  }

  // ---- Gather user-supplied competitors (two possible locations)
  const userListA = Array.isArray(s?.meta?.relevant_competitors) ? s.meta.relevant_competitors : [];
  const userListB = Array.isArray(s?.input_notes?.relevant_competitors) ? s.input_notes.relevant_competitors : [];
  const userCandidates = [...userListA, ...userListB]
    .map(v => (typeof v === "string" ? v.trim() : ""))
    .filter(Boolean);

  // ---- Compose the final competitor set
  const seen = new Set();
  const compSet = [];

  // 1) User-supplied first (normalised to object shape)
  for (const name of userCandidates) {
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    const fromEvidence = byVendor.get(key);
    compSet.push({
      vendor: fromEvidence?.vendor || name,
      reason_in_set: "Provided by supplier",
      url: fromEvidence?.url || ""
    });
    seen.add(key);
  }

  // 2) Evidence-derived vendors next (only those with a usable https URL)
  for (const { vendor, url, title } of byVendor.values()) {
    const key = vendor.toLowerCase();
    if (seen.has(key)) continue;
    if (url && url.startsWith("https")) {
      compSet.push({
        vendor,
        reason_in_set: title || "Relevant in consideration",
        url
      });
      seen.add(key);
    }
  }

  // ---- Value prop: no placeholders; only emit if we have a company & at least one USP
  const valueProp =
    meta.company && usps.length
      ? `${meta.company} aligns ${usps.join("; ")}`
      : "";

  return {
    value_prop: valueProp,                              // empty string if not defensible
    swot: {
      strengths: usps,                                  // only real USPs
      weaknesses: Array.isArray(s.gaps) ? s.gaps.filter(Boolean) : [],
      opportunities: needs,                              // from CSV needs
      threats: []                                        // leave empty if not evidenced
    },
    differentiators: usps,                               // only real USPs
    competitor_set: compSet                              // user-first, then evidence; normalised objects
  };
}
function renderMessagingFromStrategy({ strategy, evidenceBundle, csvCanon, outline }) {
  const s = strategy || {};
  const meta = s.meta || {};
  const usps = Array.isArray(meta.usps) ? meta.usps.filter(Boolean) : [];

  const sig = (csvCanon && csvCanon.signals) || {};
  const pains = Array.isArray(sig.top_blockers) ? sig.top_blockers.filter(Boolean) : [];
    const needs = (Array.isArray(sig.top_needs_supplier) && sig.top_needs_supplier.length
    ? sig.top_needs_supplier
    : (Array.isArray(sig.top_needs) ? sig.top_needs : [])
  ).filter(Boolean);

  // Build matrix rows ONLY when we have both a pain (from CSV) and a proof (claim_id/URL) from evidence
  const catalog = Array.isArray(evidenceBundle?.catalog) ? evidenceBundle.catalog : [];
  const claimsByTopic = [];
  for (const it of catalog) {
    const topicCue = (it.title || it.note || "").toString().toLowerCase();
    const claim = it.claim_id || it.url || "";
    if (!claim) continue;
    claimsByTopic.push({ topicCue, claim });
  }

  const personas = [
    (outline && outline.meta && outline.meta.persona) || "Budget holder",
    "IT lead",
    "Site operations"
  ];

  const matrix = [];
  let personaIdx = 0;
  for (const pain of pains) {
    // find a claim whose topic roughly matches the pain words
    const cue = String(pain).toLowerCase();
    const hit = claimsByTopic.find(c => c.topicCue.includes(cue.split(" ")[0] || ""));
    if (!hit) continue; // skip rows without proof
    matrix.push({
      persona: personas[personaIdx % personas.length],
      pain: pain,
      value_statement: usps.length ? `Address ${pain} via ${usps.join("; ")}` : "", // only real USPs; else empty
      proof: hit.claim, // claim_id or URL
      cta: "Agree the first site review" // concise, non-generic next step; acceptable as call-to-action wording
    });
    personaIdx += 1;
  }

  // nonnegotiables: only USPs or CSV needs if USPs are absent; never placeholders
  const nonnegotiables = usps.length ? usps : needs;

  return {
    nonnegotiables,
    matrix // may be < 3 if we cannot justify more without placeholders
  };
}

function renderSalesEnablementFromStrategy({ strategy, evidenceBundle, csvCanon }) {
  const s = strategy || {};
  const meta = s.meta || {};
  const usps = Array.isArray(meta.usps) ? meta.usps.filter(Boolean) : [];

  // Discovery questions: derive from CSV buyer data only
  const sig = (csvCanon && csvCanon.signals) || {};
  const blockers = Array.isArray(sig.top_blockers) ? sig.top_blockers.filter(Boolean) : [];
    const needs = (Array.isArray(sig.top_needs_supplier) && sig.top_needs_supplier.length
    ? sig.top_needs_supplier
    : (Array.isArray(sig.top_needs) ? sig.top_needs : [])
  ).filter(Boolean);

  // Turn blockers/needs into questions (no placeholders)
  const toQuestion = (txt) => {
    const t = String(txt).trim();
    if (!t) return null;
    // Make it interrogative without inventing content
    if (/[\.\?]$/.test(t)) return t.replace(/\.$/, "?");
    return `How are you addressing ${t.toLowerCase()}?`;
  };

  const dqSet = [];
  for (const b of blockers) {
    const q = toQuestion(b);
    if (q) dqSet.push(q);
  }
  for (const n of needs) {
    const q = toQuestion(n);
    if (q) dqSet.push(q);
  }

  // Objection cards ONLY where we have evidence claim/URL
  const catalog = Array.isArray(evidenceBundle?.catalog) ? evidenceBundle.catalog : [];
  const objections = [];
  for (let i = 0; i < catalog.length; i++) {
    const e = catalog[i];
    const claim = e.claim_id || e.url || "";
    if (!claim) continue;
    const title = (e.title || "").toString();
    // Map a nearby blocker/need to the objection if possible; else skip (no placeholders)
    const topic = (title || "").toLowerCase();
    const match = blockers.find(b => topic.includes(String(b).toLowerCase().split(" ")[0] || "")) ||
      needs.find(n => topic.includes(String(n).toLowerCase().split(" ")[0] || ""));
    if (!match) continue;
    objections.push({
      blocker: match,
      reframe_with_claimid: claim,
      proof: claim,
      risk_reversal: "" // if you do not have a concrete, evidenced risk reversal, leave empty (no placeholder)
    });
    if (objections.length >= 5) break;
  }

  // Proof pack outline: ONLY include items when relevant signals exist—no generic placeholders
  const proof_pack_outline = [];
  if (catalog.length) proof_pack_outline.push("Customer reference summaries");
  if (usps.length) proof_pack_outline.push("Service/architecture overview aligned to USPs");
  if (dqSet.length) proof_pack_outline.push("Discovery question set and notes");

  // Handoff rules: emit only if we have a specific campaign outcome stated
  const handoff_rules = s.specific_outcome
    ? `Sales to run pilot scoping and confirm: ${s.specific_outcome}`
    : "";

  return {
    discovery_questions: dqSet,
    objection_cards: objections,
    proof_pack_outline,
    handoff_rules
  };
}

// -------- Exec Summary helpers (writer-only; no schema changes) --------

// A) Tone polish for exec summary lines (remove internal project wording)
function polishExecSummaryLines(lines) {
  if (!Array.isArray(lines)) return lines;
  const repl = [
    [/\bapprove\b/gi, "proceed with"],
    [/\breport weekly\b/gi, "regular reporting"],
    [/\bfirst[-\s]?wave\b/gi, "initial cohort"],
    [/^dependencies:/i, "Requirements:"],
    [/^pre-conditions:/i, "Requirements:"],
    [/^decision:/i, "Decision point:"],
    [/^cta:/i, "Action:"]
  ];
  return lines.map(s => {
    let t = String(s || "").trim();
    for (const [rx, sub] of repl) t = t.replace(rx, sub);
    return t.replace(/\s{2,}/g, " ");
  }).filter(Boolean);
}

// B) Derive AM line + focused subset from CSV/outline (robust fallbacks; no guesses)
function deriveAmLine(csvCanon, outline) {
  // Resolve total rows from several plausible homes
  const n = (v) => Number.isFinite(Number(v)) ? Number(v) : null;
  const total =
    n(csvCanon?.meta?.rows) ??
    n(csvCanon?.row_count) ??
    n(csvCanon?.rows) ??
    n(outline?.meta?.rowCount) ??
    n(outline?.input_notes?.rowCount) ??
    n(outline?.input_notes?.row_count);

  // Resolve focus label from user notes first, then outline meta
  const focusLabel = String(
    outline?.input_notes?.campaign_focus ||
    outline?.input_notes?.selected_product ||
    outline?.meta?.campaign_focus ||
    ""
  ).trim();

  // Resolve focus count (prefer explicit counts; else approximate via top_purchases match)
  let focusCount = null;
  const countsByNeed = csvCanon?.signals?.counts?.by_need || null;
  if (countsByNeed && focusLabel && Number.isFinite(Number(countsByNeed[focusLabel]))) {
    focusCount = Number(countsByNeed[focusLabel]);
  } else if (focusLabel) {
    // soft match against top_purchases (no numbers, so just state label)
    const tps = Array.isArray(csvCanon?.signals?.top_purchases) ? csvCanon.signals.top_purchases : [];
    const hit = tps.find(tp => String(tp).toLowerCase().includes(focusLabel.toLowerCase()));
    if (hit) focusCount = null; // we have evidence of focus label, but no numeric subset
  }

  if (total != null && total > 0) {
    if (focusLabel && focusCount != null) {
      return `Campaign addressable market: there are ${total.toLocaleString()} companies in scope; ${focusCount.toLocaleString()} of them plan to purchase ${focusLabel}.`;
    }
    return `Addressable market in scope: **${total.toLocaleString()}** organisations (from campaign source data).`;
  }
  // No reliable total → no AM line (we’ll let bullets carry evidence instead of printing "0")
  return "";
}

// C) Market context + blockers + F→O→BV from evidence + signals + USPs (data-driven, no hard-wiring)
function buildExecAugments({ evidenceBundle, csvCanon, outline }) {
  const catalog = Array.isArray(evidenceBundle?.catalog) ? evidenceBundle.catalog : [];
  const sig = csvCanon?.signals || {};
  const needs = Array.isArray(sig.top_needs) ? sig.top_needs.filter(Boolean) : [];
  const blockers = Array.isArray(sig.top_blockers) ? sig.top_blockers.filter(Boolean) : [];
  const usps = Array.isArray(outline?.input_notes?.supplier_usps) ? outline.input_notes.supplier_usps.filter(Boolean) : [];
  const company = (outline?.input_notes?.supplier_company || outline?.input_notes?.prospect_company || "").trim();
  const industry = (csvCanon?.selected_industry || outline?.meta?.selected_industry || "").toString().trim();

  // Market context (evidence-first)
  const textOf = (it) => `${it.title || ""} ${it.summary || it.quote || ""}`.toLowerCase();
  const isReg = (s) => /(ofcom|gov\.uk|ons|citb|regulator|industry)/.test(s);
  const ranked = catalog
    .map(it => {
      const t = textOf(it);
      let s = 0;
      if (industry && t.includes(industry.toLowerCase())) s += 5;
      if (isReg(t) || isReg(String(it.source_type || "").toLowerCase())) s += 8;
      return { it, s };
    })
    .sort((a, b) => b.s - a.s)
    .slice(0, 8)
    .map(x => x.it);

  const ctxTerms = (() => {
    if (!ranked.length) return [];
    const STOP = new Set(["the", "and", "for", "with", "from", "this", "their", "your", "our", "it", "they", "be", "was", "were"]);
    const text = ranked.map(it => String(it.summary || it.quote || it.title || "")).join(" ").toLowerCase();
    const toks = text.split(/[^a-z0-9+]+/).filter(w => w && w.length > 2 && !STOP.has(w));
    const freq = new Map();
    for (const w of toks) freq.set(w, (freq.get(w) || 0) + 1);
    for (const w of (industry || "").toLowerCase().split(/[^a-z0-9]+/)) freq.delete(w);
    for (const w of (company || "").toLowerCase().split(/[^a-z0-9]+/)) freq.delete(w);
    return [...freq.entries()].sort((a, b) => b[1] - a[1]).slice(0, 4).map(([w]) => w);
  })();

  const marketContext = (() => {
    const need = needs[0] || "";
    const usp = usps.slice(0, 3).join("; ");
    const a = [];
    if (company && (need || usp)) {
      const needPart = need ? `meet buyers’ needs for ${need}` : "address priority buyer needs";
      const uspPart = usp ? ` via ${usp}` : "";
      a.push(`${company} can ${needPart}${uspPart}.`);
    }
    if (ctxTerms.length) a.push(`Peers in the sector emphasise ${ctxTerms.join(", ")} to secure specific operational benefits.`);
    return a.join(" ").trim();
  })();

  const blockersLine = blockers.length ? `Key buyer blockers to investment: ${blockers.slice(0, 3).join("; ")}.` : "";

  // F→O→BV: align USPs to needs/blockers via token overlap (no keyword lists)
  function tokens(s) { return String(s || "").toLowerCase().split(/[^a-z0-9]+/).filter(Boolean); }
  function jac(a, b) {
    const A = new Set(a), B = new Set(b);
    let i = 0; for (const x of A) if (B.has(x)) i++;
    const denom = A.size + B.size - i;
    return denom ? i / denom : 0;
  }
  const fov = [];
  for (const usp of usps) {
    const ut = tokens(usp); if (!ut.length) continue;
    let bestN = null, nScore = 0; for (const n of needs) { const s = jac(ut, tokens(n)); if (s > nScore) { nScore = s; bestN = n; } }
    let bestB = null, bScore = 0; for (const b of blockers) { const s = jac(ut, tokens(b)); if (s > bScore) { bScore = s; bestB = b; } }
    if (!bestN && !bestB) continue;
    const parts = [String(usp).trim()];
    if (bestN) parts.push(`to advance ${bestN}`);
    if (bestB) parts.push(`→ Business value: reduces risk from ${bestB}`);
    fov.push(parts.join(" "));
    if (fov.length >= 3) break;
  }

  const amLine = deriveAmLine(csvCanon, outline);
  return { amLine, marketContext, blockersLine, fov };
}
// ---- Exec Summary helpers: headline detection + paragraph synthesis ----
function looksLikeHeadline(s) {
  const t = String(s || "").trim();
  if (!t) return true;
  const words = t.split(/\s+/).length;
  const hasPeriod = /[.!?]["']?$/.test(t);
  const manyCaps = (t.match(/\b[A-Z][a-z]/g) || []).length >= Math.max(2, Math.floor(words * 0.4));
  return (words <= 12 && !hasPeriod) || (!hasPeriod && manyCaps);
}

// ---- Exec Summary helpers: headline detection + paragraph synthesis ----
function synthesizeExecParagraph({ outline, csvCanon, strategy }) {
  // Preferred: derive from strategy (Moore VP + problems + outcome)
  const moore = strategy?.value_proposition_moore || {};
  const f = moore.fields || {};
  const moorePara = (typeof moore.paragraph === "string" && moore.paragraph.trim()) ? moore.paragraph.trim() : "";

  const buyerProblems = Array.isArray(strategy?.buyer_problems) ? strategy.buyer_problems.filter(Boolean).slice(0, 2) : [];
  const outcome = (strategy?.specific_outcome && String(strategy.specific_outcome).trim()) || "";

  if (moorePara || (f.for_who && f.the && f.is_a && f.that)) {
    // Build a reasoned, benefit-led sentence (not just inputs)
    const parts = [];
    if (f.for_who) parts.push(`For ${String(f.for_who).trim()}`);
    if (f.the && f.is_a) parts.push(`${String(f.the).trim()} is a ${String(f.is_a).trim()}`);
    if (f.that) parts.push(`that ${String(f.that).trim()}`);
    if (buyerProblems.length) parts.push(`to address ${buyerProblems.join(" and ")}`);
    if (outcome) parts.push(`with a specific objective: ${outcome}`);

    const paragraph = parts.join(", ").replace(/\s{2,}/g, " ").replace(/,\s*$/, "") + ".";
    return paragraph;
  }

  // Fallback: light-touch input-echo (polished)
  const company = String(outline?.input_notes?.supplier_company || outline?.meta?.company || "").trim();
  const industry = String(csvCanon?.selected_industry || outline?.meta?.selected_industry || outline?.input_notes?.selected_industry || "").trim();
  const campaignType = String(outline?.input_notes?.campaign_type || outline?.meta?.campaign_type || outline?.input_notes?.objective || "").trim();
  const needs = (csvCanon?.signals?.top_needs_supplier || csvCanon?.signals?.top_needs || []).filter(Boolean).slice(0, 2);
  const blockers = (csvCanon?.signals?.top_blockers || []).filter(Boolean).slice(0, 2);

  const who = industry ? `For ${industry} companies` : `For target companies`;
  const what = company ? `${company} will` : `This campaign will`;
  const why = campaignType ? `${campaignType}` : `drive measurable growth`;
  const needsPart = needs.length ? ` by addressing ${needs.join(" and ")}` : "";
  const blockPart = blockers.length ? ` and reducing risk from ${blockers.join(" and ")}` : "";

  return `${who}, ${what} ${why}${needsPart}${blockPart}.`;
}

// ==== Prompts ====
// Executive Summary must be ARRAY OF STRINGS so the UI can render it fully.
function buildSectionSystem(finalKey, persona) {
  const personaPrefix = persona ? `PERSONA\n${persona}\n\n` : "";

  if (finalKey === "executive_summary") {
    return [
      personaPrefix + "You are a senior UK B2B strategist.",
      "Write a board-ready Executive Summary (≤350 words) for a go/no-go decision.",
      "Begin exactly in this order: Strategy → Target prospects → Buyer problems → Campaign type (upsell/win-back/growth + one-line rationale).",
      "Then add bullets covering each of: Moore value proposition; Addressable market; Market context (with citations); Buyer blockers (from CSV); Sales enablement note.",
      "Use ONLY these data sources: CSV canonical (cohort size + signals), industry packs, company profile pack, and explicit input notes.",
      "If competitors are provided in the input, USE ONLY those; do not introduce others.",
      "For Addressable market, DO NOT estimate; the writer will insert the cohort and focus subset from CSV.",
      "Cite external facts inline using short tags or https URLs.",
      "Return STRICT JSON: executive_summary as an array of strings (first = paragraph, rest = bullets)."
    ].join("\n");
  }

  if (finalKey === "positioning_and_differentiation") {
    return [
      personaPrefix + "You are a senior UK B2B strategist.",
      "Provide Geoffrey Moore’s value proposition and a competitor contrast table.",
      "Use ONLY competitors supplied in the input; if none supplied, write 'TBD' rather than introducing generic telcos.",
      "Ground claims in CSV signals, industry packs, and the customer profile pack; include claim_ids or short citations inline.",
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
      "Use short tags in parentheses (e.g., (Company site), (CSV)) or https URLs: (Company site), (LinkedIn), (CSV), (Ofcom), (ONS), (DSIT), (PDF extract), (Trade press), (Directory).",
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
  const competitorsArr = (outline?.input_notes?.competitors || outline?.input_notes?.relevant_competitors || []);
  const competitors = safeForPrompt(competitorsArr.slice(0, 8));
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
  const [evidenceLog, csvCanon, productsObj, site, outline, strategy] = await Promise.all([
    getJson(container, `${prefix}evidence_log.json`),
    getJson(container, `${prefix}csv_normalized.json`),
    getJson(container, `${prefix}products.json`),
    getJson(container, `${prefix}site.json`),
    getJson(container, `${prefix}outline.json`),
    getJson(container, `${prefix}campaign_strategy.json`)
  ]);

  // PATCH COM-EVID-1: prefer canonical evidence.json; fallback to legacy array
  let evidenceClaims = [];
  try {
    const eb = await getJson(container, `${prefix}evidence.json`);
    if (Array.isArray(eb?.claims) && eb.claims.length) evidenceClaims = eb.claims;
  } catch { /* tolerate absence or parse issues */ }

  // PATCH COM-PROD-1: ground productNames (Strategy → products_meta → products.json)
  let productNames = [];
  // 1) Strategy-chosen (authoritative for Communication)
  const chosenFromStrategy =
    (Array.isArray(strategy?.value_proposition_moore?.chosen_products) && strategy.value_proposition_moore.chosen_products.length
      ? strategy.value_proposition_moore.chosen_products
      : (Array.isArray(strategy?.prospect_base?.products?.chosen) ? strategy.prospect_base.products.chosen : []));

  if (chosenFromStrategy.length) {
    productNames = chosenFromStrategy;
  } else {
    // 2) products_meta.json (chosen → validated) if present
    try {
      const pm = await getJson(container, `${prefix}products_meta.json`);
      const pmChosen = Array.isArray(pm?.chosen) ? pm.chosen : [];
      const pmValidated = Array.isArray(pm?.validated)
        ? pm.validated.map(v => (typeof v === "string" ? v : (v?.name || ""))).filter(Boolean)
        : [];
      productNames = pmChosen.length ? pmChosen : pmValidated;
    } catch { /* optional */ }

    // 3) fallback to products.json
    if (!productNames.length) {
      productNames = Array.isArray(productsObj?.products) ? productsObj.products : [];
    }
  }

  // Final evidence bundle for downstream composition
  const evidenceBundle = makeEvidenceBundle({
    evidenceLog: evidenceClaims.length ? evidenceClaims : (Array.isArray(evidenceLog) ? evidenceLog : []),
    csvCanon: csvCanon || {},
    productNames
  });

  // ---- Assemble whole campaign ----
  if (op === "assemble") {
    // Phase: writer working (append-only; terminal-guard)
    {
      const cur = (await getJson(container, `${prefix}status.json`)) || {};
      const prev = String(cur.state || "");
      const terminal = new Set(["assembled", "error", "Failed"]);
      if (!terminal.has(prev)) {
        await patchStatus(container, prefix, "writer_working", { runId, assembleStartedAt: nowISO(), op: "assemble" });
      }
    }

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
        // Copy the section through
        merged[finalKey] = piece[finalKey];

        // Special handling for Executive Summary: ensure both shapes + carry AM
        if (finalKey === "executive_summary") {
          const es = merged.executive_summary;

          // Normalise to object + legacy array, regardless of how the section was written
          if (Array.isArray(es)) {
            const lead = String(es[0] || "").trim();
            const bullets = es.slice(1).map(s => String(s || "").trim()).filter(Boolean);
            merged.executive_summary = { lead_paragraph: lead, bullets };
            merged.executive_summary_legacy = [lead, ...bullets].filter(Boolean);
          } else if (es && typeof es === "object") {
            const lead = typeof es.lead_paragraph === "string" ? es.lead_paragraph : "";
            const bullets = Array.isArray(es.bullets)
              ? es.bullets.map(s => String(s || "").trim()).filter(Boolean)
              : [];
            merged.executive_summary = { lead_paragraph: lead, bullets };
            if (Array.isArray(piece.executive_summary_legacy)) {
              merged.executive_summary_legacy = piece.executive_summary_legacy.filter(Boolean);
            } else {
              merged.executive_summary_legacy = [lead, ...bullets].filter(Boolean);
            }
          } else {
            merged.executive_summary = { lead_paragraph: "", bullets: [] };
            merged.executive_summary_legacy = [];
          }

          // Carry Addressable Market when present in the ES section file
          if (Object.prototype.hasOwnProperty.call(piece, "addressable_market") && piece.addressable_market != null) {
            merged.addressable_market = piece.addressable_market;
          }
        }
      } else {
        context.log.warn(`[campaign-write] missing section during assemble: ${finalKey}`);

        // Ensure Executive Summary never breaks the UI (both shapes exist)
        if (finalKey === "executive_summary") {
          if (
            !merged.executive_summary ||
            typeof merged.executive_summary !== "object" ||
            (!Array.isArray(merged.executive_summary.bullets) && !merged.executive_summary.lead_paragraph)
          ) {
            merged.executive_summary = { lead_paragraph: "", bullets: [] };
          }
          if (!Array.isArray(merged.executive_summary_legacy)) {
            merged.executive_summary_legacy = [];
          }
        }
      }
    }

    await putJson(container, `${prefix}campaign.json`, merged);
    {
      const cur = (await getJson(container, `${prefix}status.json`)) || {};
      const prev = String(cur.state || "");
      const terminal = new Set(["assembled", "error", "Failed"]);
      if (!terminal.has(prev)) {
        await patchStatus(container, prefix, "assembled", { assembledAt: nowISO(), op: "assemble" });
      }
    }

    // ---- Notify orchestrator exactly once (anti-loop marker) ----
    try {
      // Check/flip marker to ensure single-shot dispatch
      const postStatus = (await getJson(container, `${prefix}status.json`)) || {};
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

    if (DETERMINISTIC_SECTIONS.has(finalKey)) {
      let out;
      if (finalKey === "positioning_and_differentiation") {
        out = { positioning_and_differentiation: renderPositioningFromStrategy({ strategy, evidenceBundle, csvCanon }) };
      } else if (finalKey === "messaging_matrix") {
        out = { messaging_matrix: renderMessagingFromStrategy({ strategy, evidenceBundle, csvCanon, outline }) };
      } else { // sales_enablement
        out = { sales_enablement: renderSalesEnablementFromStrategy({ strategy, evidenceBundle, csvCanon }) };
      }

      await putJson(container, `${prefix}sections/${finalKey}.json`, out);
      await patchStatus(container, prefix, "SectionWrites", {
        runId, written: finalKey, updatedAt: nowISO(), op: "section"
      });
      return; // short-circuit: do NOT call the LLM for these sections
    }

    const persona = outline?.meta?.persona || "";
    const system = buildSectionSystem(finalKey, persona);
    const user = buildSectionUser(finalKey, { outline, evidenceBundle, csvCanon });

    const { callChatJsonObject } = await loadPromptHarness();
    const raw = await callChatJsonObject({ system, user, timeoutMs: LLM_TIMEOUT_MS });
    // normalise shape: expect the section key at root
    const out = {};
    const sect = (raw && typeof raw === "object" && raw[finalKey] != null) ? raw[finalKey] : raw;

    if (finalKey !== "executive_summary") {
      out[finalKey] = sect || {};
    } else {
      // Exec summary must be array<string>. Coerce common shapes.
      if (Array.isArray(sect)) {
        out.executive_summary = sect.map(s => String(s || "").trim()).filter(Boolean);
      } else if (typeof sect === "string") {
        out.executive_summary = [sect.trim()];
      } else if (sect && typeof sect === "object") {
        // Common object payload (like your sample) → synthesise a paragraph + bullets
        const headline = [sect.headline, sect.title].map(x => String(x || "").trim()).find(Boolean) || "";
        const paraBits = [
          headline,
          (sect.industry ? `Industry: ${sect.industry}` : ""),
          (sect.objective ? `Objective: ${sect.objective}` : "")
        ].filter(Boolean);
        const paragraph = paraBits.join(" — ");

        const bulletsObj = [];
        if (Array.isArray(sect.why_now) && sect.why_now.length) bulletsObj.push(...sect.why_now.map(String));
        if (sect.addressable_market && Number.isFinite(Number(sect.addressable_market.cohort_size))) {
          bulletsObj.push(`Addressable market (payload): ${Number(sect.addressable_market.cohort_size).toLocaleString()}.`);
        }
        if (Array.isArray(sect.buyer_needs)) bulletsObj.push(`Buyer needs: ${sect.buyer_needs.join("; ")}`);
        if (Array.isArray(sect.buyer_blockers)) bulletsObj.push(`Buyer blockers: ${sect.buyer_blockers.join("; ")}`);

        out.executive_summary = [paragraph, ...bulletsObj].map(s => String(s || "").trim()).filter(Boolean);
      } else {
        out.executive_summary = [];
      }
    }
    // ---- Exec Summary post-processor (tone + data-led bullets; headline guard; no hard-wiring) ----
    if (finalKey === "executive_summary") {
      try {
        const current = Array.isArray(out.executive_summary) ? out.executive_summary.slice() : [];
        let paragraph = current[0] || "";
        const llmBullets = current.slice(1).map(s => String(s || "").trim()).filter(Boolean);

        const eb = evidenceBundle || {};
        const csvC = csvCanon || {};
        const ol = outline || {};

        // Deterministic augments (if helper available)
        const aug = (typeof buildExecAugments === "function")
          ? (buildExecAugments({ evidenceBundle: eb, csvCanon: csvC, outline: ol, strategy }) || {})
          : {};

        // COM–AM–GUARD: avoid “0” by falling back to Strategy coverage
        if (!aug.amLine || /0\s*(companies|orgs|organisations)/i.test(String(aug.amLine))) {
          const stratTotal =
            Number(strategy?.insight?.coverage?.total ?? 0) ||
            Number(strategy?.prospect_base?.tam ?? 0);

          if (stratTotal > 0) {
            aug.amLine = `Target market of ${stratTotal.toLocaleString()} organisations identified in CSV.`;
          }
        }

        // Filter LLM bullets (generic, no brand lists)
        const notAmLine = (s) => !/^addressable market\b/i.test(s);
        const looksCited = (s) => /https?:\/\/\S+/.test(s) || /\([^)]{3,}\)/.test(s) || /\[[^\]]{3,}\]/.test(s);
        const notUncitedWhyNow = (s) => {
          const isWhy = /\bwhy now\b/i.test(s) || /\bcontext\b/i.test(s) || /\bmarket\b/i.test(s);
          return !isWhy || looksCited(s);
        };
        const company = String(ol?.input_notes?.supplier_company || "").toLowerCase();
        const allowed = new Set(
          [company]
            .concat(Array.isArray(ol?.input_notes?.competitors) ? ol.input_notes.competitors : (Array.isArray(ol?.input_notes?.relevant_competitors) ? ol.input_notes.relevant_competitors : []))
            .map(x => String(x).toLowerCase())
            .filter(Boolean)
        );
        const comparisonCue = /\b(vs|versus|against|compared|than|alternative|competitor|compare)\b/i;
        const mentionsDisallowedBrandInComparison = (s) => {
          if (!allowed.size) return false;
          if (!comparisonCue.test(s)) return false;
          const tokens = s.match(/\b[A-Z][a-z][A-Za-z0-9&.-]+\b/g) || [];
          const whitelist = new Set(["UK", "EU", "RTO", "RPO", "SLA", "QoS", "WAN", "SD-WAN", "IoT", "AI", "CCTV", "POS", "VPN", "MPLS", "LTE", "FTTP", "SoGEA", "DSL", "IP"]);
          for (const t of tokens) {
            if (whitelist.has(t)) continue;
            if (!allowed.has(t.toLowerCase())) return true;
          }
          return false;
        };
        const filteredLlmBullets = llmBullets
          .filter(notAmLine)
          .filter(notUncitedWhyNow)
          .filter(s => !mentionsDisallowedBrandInComparison(s));

        // Build the final bullets, always injecting deterministic lines first (if present)
        const newBullets = [
          ...((aug.fov || []).filter(Boolean)),                // Feature→Outcome→Value (0–3)
          ...(aug.amLine ? [aug.amLine] : []),                 // AM from CSV (enforced here)
          ...(aug.marketContext ? [aug.marketContext] : []),   // market context from evidence/profile
          ...(aug.blockersLine ? [aug.blockersLine] : []),     // CSV TopBlockers
          ...filteredLlmBullets
        ].filter(Boolean);

        // Headline guard: if paragraph is empty or looks like a headline, synthesise one from data
        if (!paragraph || looksLikeHeadline(paragraph)) {
          const pSynth = synthesizeExecParagraph({ outline: ol, csvCanon: csvC, strategy });
          if (pSynth && pSynth.trim()) paragraph = pSynth.trim();
        }

        // If we still have no bullets, ensure we at least output AM/context/blockers from aug
        const ensuredBullets = newBullets.length
          ? newBullets
          : [
            ...(aug.amLine ? [aug.amLine] : []),
            ...(aug.marketContext ? [aug.marketContext] : []),
            ...(aug.blockersLine ? [aug.blockersLine] : [])
          ].filter(Boolean);

        const polished = (typeof polishExecSummaryLines === "function")
          ? polishExecSummaryLines([paragraph, ...ensuredBullets])
          : [paragraph, ...ensuredBullets];

        const polishedArr = polished.filter(Boolean);
        const lead = polishedArr[0] || "";
        const bullets = polishedArr.slice(1).filter(Boolean);

        // New structured shape for modern UI
        out.executive_summary = {
          lead_paragraph: lead,
          bullets
        };

        // Legacy array for older consumers
        out.executive_summary_legacy = [lead, ...bullets].filter(Boolean);
        // ---- Persist Addressable Market facts (cohort + focus subset) ----
        const cohortSize = Number.isFinite(Number(csvCanon?.meta?.rows)) ? Number(csvCanon.meta.rows) : null;
        const focusLabel = (outline?.input_notes?.campaign_focus || outline?.input_notes?.selected_product || "").toString().trim() || null;
        const focusCount = (focusLabel && Number.isFinite(Number(csvCanon?.signals?.counts?.by_need?.[focusLabel])))
          ? Number(csvCanon.signals.counts.by_need[focusLabel])
          : null;

        out.addressable_market = {
          cohort_size: cohortSize,
          focus_label: focusLabel,
          focus_count: focusCount
        };
        context.log("[writer] ES diag",
          {
            hasOutline: !!outline, hasCsv: !!csvCanon, hasEvidence: !!evidenceBundle,
            aug: {
              hasFOV: !!(aug && aug.fov && aug.fov.length),
              hasAM: !!(aug && aug.amLine),
              hasCtx: !!(aug && aug.marketContext),
              hasBlockers: !!(aug && aug.blockersLine)
            },
            llmBullets: llmBullets.length
          }
        );
      } catch (esErr) {
        context.log.warn("[writer] exec-summary post-processor failed; using unprocessed ES", String(esErr?.message || esErr));
      }
    }
    await putJson(container, `${prefix}sections/${finalKey}.json`, out);
    await patchStatus(container, prefix, "SectionWrites", {
      runId, written: finalKey, updatedAt: nowISO(), op: "section"
    });
    return;
  }

  throw new Error(`Unknown job type "${op}". Use "write_section" or "assemble".`);
};
