// /api/campaign-write/index.js 12-11-2025 v35 (Writer/Assembler)
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

function buildPersonaBrief({ outline, csvCanon, needsMap }) {
  try {
    const personaName =
      (outline?.meta?.persona || outline?.input_notes?.persona || outline?.input_notes?.target_persona || "").toString().trim();

    const route =
      (outline?.input_notes?.sales_model || outline?.meta?.sales_model || "").toString().trim(); // direct/partner/etc.

    const topNeeds = Array.isArray(csvCanon?.signals?.top_needs_supplier)
      ? csvCanon.signals.top_needs_supplier.filter(Boolean).slice(0, 3)
      : [];

    const topBlockers = Array.isArray(csvCanon?.signals?.top_blockers)
      ? csvCanon.signals.top_blockers.filter(Boolean).slice(0, 3)
      : [];

    // Map first need through needs_map to produce capability hints (non-deterministic text, just cues)
    let capabilityHints = [];
    if (needsMap && typeof needsMap === "object") {
      const nm = needsMap.mapping || needsMap.map || {};
      for (const n of topNeeds) {
        const m = nm[n];
        if (m && Array.isArray(m.capabilities)) {
          capabilityHints = capabilityHints.concat(m.capabilities.slice(0, 2));
        }
      }
      capabilityHints = capabilityHints.filter(Boolean).slice(0, 3);
    }

    return {
      persona: personaName || null,
      route: route || null,
      top_needs: topNeeds,
      top_blockers: topBlockers,
      capability_hints: capabilityHints
    };
  } catch {
    return { persona: null, route: null, top_needs: [], top_blockers: [], capability_hints: [] };
  }
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
    value_prop: (valueProp && valueProp.trim())
      ? valueProp
      : "Advisory: current evidence is insufficient to assert a differentiated value proposition for this campaign. Strengthen CSV signals (needs, blockers, purchases), add validated outcomes, and sharpen competitor contrast before launch.",
    swot: {
      strengths: usps,                                  // only real USPs
      weaknesses: Array.isArray(s.gaps) ? s.gaps.filter(Boolean) : [],
      opportunities: needs,                              // from CSV needs
      threats: pains                                        // leave empty if not evidenced
    },
    differentiators: usps,                               // only real USPs
    competitor_set: compSet                              // user-first, then evidence; normalised objects
  };
}
// -------- Exec Summary helpers (writer-only; no schema changes) --------
// A) Tone polish for exec summary lines
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

  // Resolve focus count (prefer explicit counts; else soft evidence only)
  let focusCount = null;
  const countsByNeed = csvCanon?.signals?.counts?.by_need || null;
  if (countsByNeed && focusLabel && Number.isFinite(Number(countsByNeed[focusLabel]))) {
    focusCount = Number(countsByNeed[focusLabel]);
  } else if (focusLabel) {
    // match presence against top_purchases if we lack numbers
    const tps = Array.isArray(csvCanon?.signals?.top_purchases) ? csvCanon.signals.top_purchases : [];
    const hit = tps.find(tp => String(tp).toLowerCase().includes(focusLabel.toLowerCase()));
    if (hit) focusCount = null; // evidence of focus label, but no numeric subset
  }

  if (total != null && total > 0) {
    if (focusLabel && focusCount != null) {
      return `Campaign addressable market: there are ${total.toLocaleString()} companies in scope; ${focusCount.toLocaleString()} of them plan to purchase ${focusLabel}.`;
    }
    return `Addressable market in scope: **${total.toLocaleString()}** organisations (from campaign source data).`;
  }
  // No reliable total → omit (avoid printing "0")
  return "";
}

// C) Market context + blockers + F→O→BV from evidence + signals + chosen products (with citations)
function buildExecAugments({ evidenceBundle, csvCanon, outline, strategy }) {
  const catalog = Array.isArray(evidenceBundle?.catalog) ? evidenceBundle.catalog : [];
  const sig = csvCanon?.signals || {};
  const needs = Array.isArray(sig.top_needs_supplier) && sig.top_needs_supplier.length
    ? sig.top_needs_supplier.filter(Boolean)
    : (Array.isArray(sig.top_needs) ? sig.top_needs.filter(Boolean) : []);
  const blockers = Array.isArray(sig.top_blockers) ? sig.top_blockers.filter(Boolean) : [];

  // Chosen/validated products (reasoned anchor → ES bullets)
  const chosen =
    (Array.isArray(strategy?.prospect_base?.products?.chosen) && strategy.prospect_base.products.chosen.length
      ? strategy.prospect_base.products.chosen
      : (Array.isArray(evidenceBundle?.productNames) ? evidenceBundle.productNames : []))
      .map(s => String(s || "").trim()).filter(Boolean);

  // ---- CITE: prefer https URLs; else short tag from source_type/title
  const citeOf = (it) => {
    const url = String(it.url || "").trim();
    if (url && /^https:\/\//i.test(url)) return url;
    const src = String(it.source_type || it.source || "").trim();
    if (/ons|ofcom|dsit|gov\.uk/i.test(src)) return `(${src})`;
    const t = String(it.title || "").trim();
    if (t) return `(${t.slice(0, 40)}…)`;
    return "(evidence)";
  };

  // ---- Rank evidence for market context (industry/regulator weight)
  const industry = String(csvCanon?.selected_industry || outline?.meta?.selected_industry || "").toLowerCase();
  const rank = (it) => {
    const text = `${it.title || ""} ${it.summary || it.quote || ""}`.toLowerCase();
    let s = 0;
    if (industry && text.includes(industry)) s += 5;
    if (/(ofcom|ons|dsit|gov\.uk|citb|regulator)/.test(text) || /(ofcom|ons|dsit|gov\.uk|citb|regulator)/.test(String(it.source_type || "").toLowerCase())) s += 8;
    if (/case|reference|customer|site/i.test(text)) s += 2;
    return s;
  };
  const topEv = catalog.slice().sort((a, b) => rank(b) - rank(a)).slice(0, 4);

  // ---- Market context line WITH citations so it passes your filter
  const marketContext = (() => {
    if (!topEv.length) return "";
    const phrases = [];
    for (const it of topEv.slice(0, 2)) {
      const t = String(it.summary || it.quote || it.title || "").trim();
      if (!t) continue;
      phrases.push(`${t} ${citeOf(it)}`.trim());
    }
    return phrases.length ? phrases.join(" ") : "";
  })();

  // ---- Blockers line (CSV-led; no placeholders)
  const blockersLine = blockers.length ? `Key buyer blockers to investment: ${blockers.slice(0, 3).join("; ")}.` : "";

  // ---- F→O→BV: map chosen products to nearest need/blocker (token overlap; no guesses)
  const tok = (s) => String(s || "").toLowerCase().split(/[^a-z0-9]+/).filter(Boolean);
  const jac = (A, B) => {
    const a = new Set(A), b = new Set(B);
    let i = 0; for (const x of a) if (b.has(x)) i++;
    const d = a.size + b.size - i;
    return d ? i / d : 0;
  };
  const fov = [];
  for (const prod of chosen) {
    const pt = tok(prod); if (!pt.length) continue;
    let bestN = null, nScore = 0; for (const n of needs) { const s = jac(pt, tok(n)); if (s > nScore) { nScore = s; bestN = n; } }
    let bestB = null, bScore = 0; for (const b of blockers) { const s = jac(pt, tok(b)); if (s > bScore) { bScore = s; bestB = b; } }
    if (!bestN && !bestB) continue;
    const parts = [prod];
    if (bestN) parts.push(`→ outcome: advances ${bestN}`);
    if (bestB) parts.push(`→ business value: reduces risk from ${bestB}`);
    fov.push(parts.join(" "));
    if (fov.length >= 3) break;
  }

  // ---- AM line (use your existing deterministic resolver)
  const amLine = deriveAmLine(csvCanon, outline);

  return { amLine, marketContext, blockersLine, fov };
}

// ---- Exec Summary helper: produce a single Moore bullet from strategy ----
function mooreBulletFromStrategy(strategy) {
  const vpm = strategy?.value_proposition_moore || {};
  if (typeof vpm?.paragraph === "string" && vpm.paragraph.trim()) {
    return vpm.paragraph.trim();
  }
  const f = vpm?.fields || {};
  const haveCore = f.for_who && f.the && f.is_a && f.that;
  if (!haveCore) return ""; // not grounded enough → no bullet

  const norm = (s) => String(s || "").trim().replace(/[.\s]+$/g, "");
  const theRaw = norm(f.the);
  const thePart = /^the\s/i.test(theRaw) ? theRaw : `the ${theRaw}`;

  const parts = [];
  if (f.for_who) parts.push(`For ${norm(f.for_who)}`);
  if (f.who_need) parts.push(`who need ${norm(f.who_need)}`);
  if (f.the && f.is_a) parts.push(`${thePart} is a ${norm(f.is_a)}`);
  if (f.that) parts.push(`that ${norm(f.that)}`);
  if (f.unlike) parts.push(`Unlike ${norm(f.unlike)}`);
  if (f.provides) parts.push(`provides ${norm(f.provides)}`);

  return parts.join(", ").replace(/,\s*$/, "") + ".";
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
function buildSectionSystem(finalKey, persona) {
  const personaPrefix = persona ? `PERSONA\n${persona}\n\n` : "";

  if (finalKey === "executive_summary") {
    return [
      personaPrefix + "You are a senior UK B2B strategist.",
      "Write a board-ready Executive Summary for a go/no-go decision.",
      "Deliver a persuasive narrative in exactly five short paragraphs (no bullets):",
      "1) Market environment — key trends; exact addressable cohort size from CSV; buyer landscape (current investments, blockers, spend cues, and needs from a supplier).",
      "2) Strategic rationale — why the supplier should implement this campaign now, explicitly connecting the market environment to the strategy.",
      "3) How to win — use Geoffrey Moore’s value proposition and reasoned SWOT plus brief, evidenced competitor commentary (use ONLY supplied competitors).",
      "4) What success looks like — quantified campaign outcome (ranges ok) and time frame, grounded in evidence and supplier capability.",
      "5) Next steps — concrete actions to begin.",
      "Use ONLY these data sources: CSV canonical (cohort size, signals), evidence.json / evidence_log.json (with URLs or short source tags), industry packs, company profile pack, and explicit input notes.",
      "Cite external facts inline with short tags or https URLs. Do not invent competitors; if none supplied, omit competitor commentary.",
      'Return STRICT JSON with the key "executive_summary" as an OBJECT:',
      '{',
      '  "environment_paragraph": "<string>",',
      '  "rationale_paragraph": "<string>",',
      '  "how_to_win_paragraph": "<string>",',
      '  "success_paragraph": "<string>",',
      '  "next_steps_paragraph": "<string>"',
      '}'
    ].join("\n");
  }

  if (finalKey === "positioning_and_differentiation") {
    return [
      personaPrefix + "You are a senior UK B2B strategist.",
      "Provide a deeper Value Proposition narrative than the Executive Summary.",
      "Consultant standard: Evidence-based, Interpreted, Connected, Strategic, Commercial. Cite claim_id where appropriate.",
      "Include Geoffrey Moore’s value proposition (clean sentence) and expand it into several short paragraphs:",
      "- Customer problem and impact (grounded in CSV/evidence).",
      "- Right-to-play for the supplier.",
      "- Differentiation and proof points.",
      "- Competitor commentary: USE ONLY competitors supplied in input; if none, write 'TBD' for commentary.",
      "Also include SWOT (S/W/O/T arrays), a concise differentiators array, and (optionally) a competitor_set table with vendor, reason_in_set, url.",
      "Cite external facts inline via short tags or URLs.",
      'Return STRICT JSON for "positioning_and_differentiation" as an OBJECT that may include:',
      '{',
      '  "value_prop": "<one-line generic value if needed>",',
      '  "value_prop_moore": { "paragraph": "<clean sentence>", "fields": { "for": "...", "who_need": "...", "the": "...", "is_a": "...", "that": "...", "unlike": "...", "provides": "...", "proof_points": ["...", "..."] } },',
      '  "value_prop_narrative": {',
      '    "lead": "<Moore sentence again (clean)>",',
      '    "customer_problem_paragraph": "<string>",',
      '    "right_to_play_paragraph": "<string>",',
      '    "differentiation_paragraph": "<string>",',
      '    "competitor_positions_paragraph": "<string or \\"TBD\\">",',
      '    "proof_points_paragraph": "<string>"',
      '  },',
      '  "swot": { "strengths": [], "weaknesses": [], "opportunities": [], "threats": [] },',
      '  "differentiators": [],',
      '  "competitor_set": [{ "vendor": "Acme", "reason_in_set": "…", "url": "https://…" }]',
      '}'
    ].join("\n");
  }

  if (finalKey === "sales_enablement") {
    return `You are a senior UK B2B go-to-market consultant.
    "Consultant standard: Evidence-based, Interpreted, Connected, Strategic, Commercial. Cite claim_id where appropriate.",
    "Write a concise, persuasive sales enablement 'Battle Card' grounded ONLY in CSV signals, evidence claims, outline, and allowed competitors.",
Write a concise, persuasive sales enablement “Battle Card” grounded ONLY in the supplied CSV signals, evidence claims (cite claim_ids or short [Tag] where applicable), outline inputs, and allowed competitors.
Do NOT fabricate facts, metrics, or competitor names. If a specific proof element is missing, write "TBD" rather than inventing it.
Use plain UK business English and keep every section decision-oriented.

Emit STRICT JSON:
{
  "sales_enablement": {
    "overview_and_rationale": {
      "purpose": "<one sentence>",
      "market_driver": "<one or two sentences tying cohort and blockers/needs to why this matters now (cite claim_ids or [Tag])>",
      "campaign_focus": "<one sentence explaining who/what we are targeting and why (cohort/focus)>"
    },
    "buyer_value": [
      { "outcome": "<buyer outcome>", "why_it_matters": "<commercial reason>" }
    ],
    "discovery_questions": [
      { "question": "<text>", "why_it_matters": "<diagnostic reason linked to blockers/needs/proof>" }
    ],
    "competitive_positioning": {
      "allowed_competitors_only": true,
      "how_we_win": "<short paragraph: differentiation vs allowed competitors only; if none supplied, write 'TBD'>"
    },
    "master_sales_pitch": {
      "opening": "<buyer-centred opener>",
      "problem": "<in buyer terms>",
      "value": "<our value with proof cues>",
      "example": "<concise customer example or 'TBD'>",
      "call_to_action": "<clear next step>"
    }
  }
}`.trim();
  }
  if (finalKey === "messaging_matrix") {
    return `You are a senior UK B2B strategist.
Using the supplied CSV signals, evidence claims, outline, and campaign strategy, write a Go-to-Market Messaging Matrix for this supplier’s campaign.
Use the data as context only; do not hard-code example names.
If any proof or metrics are missing, write "TBD" rather than inventing.

Emit STRICT JSON:
{
  "messaging_matrix": {
    "campaign_overview_and_rationale": {
      "objective": "<one paragraph: commercial goal and why now>",
      "strategic_rationale": "<one paragraph: market facts, blockers, or evidence claims (cite claim_ids or [Tag])>"
    },
    "target_market_actions": {
      "marketing": ["<bullet points for marketing team actions>"],
      "sales": ["<bullet points for sales team actions>"]
    },
    "product_proposition_and_positioning": {
      "proposition": "<short one-line proposition>",
      "positioning": "<paragraph: differentiation and proof cues>",
      "proof_points": ["<short, evidenced items>"]
    },
    "marketing_and_sales_enablement_required": [
      { "area": "<area>", "deliverable": "<what to create>", "purpose": "<why it matters>" }
    ],
    "pipeline_model": {
      "cohort_basis": "<summary sentence referencing campaign cohort size from CSV>",
      "stages": [
        { "stage": "MQL", "conversion_rate": "<percent>", "derived_volume": "<number>", "description": "<short explanation>" },
        { "stage": "SAL", "conversion_rate": "<percent>", "derived_volume": "<number>", "description": "<short explanation>" },
        { "stage": "SQL", "conversion_rate": "<percent>", "derived_volume": "<number>", "description": "<short explanation>" },
        { "stage": "Deal", "conversion_rate": "<percent>", "derived_volume": "<number>", "description": "<short explanation>" }
      ],
      "pipeline_yield": "<one-sentence summary of total funnel yield>"
    },
    "summary_call_to_action": "<short paragraph directing next steps>"
  }
}`.trim();
  }

  // default fallback
  return "";
}

function buildSectionUser(finalKey, { outline, evidenceBundle, csvCanon, allowedCompetitors }) {
  // ---- existing inputs, preserved ----
  const ev = safeForPrompt(evidenceBundle?.catalog || []);
  const csv = safeForPrompt(csvCanon || {});
  const prod = safeForPrompt(evidenceBundle?.productNames || []);
  const competitorsArr = (Array.isArray(allowedCompetitors) && allowedCompetitors.length)
    ? allowedCompetitors
    : (outline?.input_notes?.competitors || outline?.input_notes?.relevant_competitors || []);
  const competitors = safeForPrompt(competitorsArr.slice(0, 8));
  const services = safeForPrompt(outline?.input_notes?.supplier_usps || []);
  const objective = safeForPrompt(outline?.input_notes?.campaign_requirement || "");
  const persona = safeForPrompt(outline?.meta?.persona || "");
  const addressable_market = Number.isFinite(Number(csvCanon?.meta?.rows)) ? Number(csvCanon.meta.rows) : null;

  // ---- small persona brief (no new schema; just prompt context) ----
  const personaBrief = (() => {
    try {
      const route = (outline?.input_notes?.sales_model || outline?.meta?.sales_model || "").toString().trim();
      const topNeeds = Array.isArray(csvCanon?.signals?.top_needs_supplier)
        ? csvCanon.signals.top_needs_supplier.filter(Boolean).slice(0, 3)
        : [];
      const topBlockers = Array.isArray(csvCanon?.signals?.top_blockers)
        ? csvCanon.signals.top_blockers.filter(Boolean).slice(0, 3)
        : [];
      // light capability hints from needs_map (if present in evidenceBundle)
      const nm = evidenceBundle?.needsMap || {};
      let capability_hints = [];
      if (nm && typeof nm === "object") {
        const mapping = nm.mapping || nm.map || {};
        for (const n of topNeeds) {
          const m = mapping[n];
          if (m && Array.isArray(m.capabilities)) capability_hints = capability_hints.concat(m.capabilities.slice(0, 2));
        }
        capability_hints = capability_hints.filter(Boolean).slice(0, 3);
      }
      return {
        persona: persona || null,
        route: route || null,
        top_needs: topNeeds,
        top_blockers: topBlockers,
        capability_hints
      };
    } catch {
      return { persona: persona || null, route: null, top_needs: [], top_blockers: [], capability_hints: [] };
    }
  })();

  // consultant standard (kept terse; applied in both ES and other sections)
  const CONSULTANT_STANDARD =
    "Every statement must be: 1) Evidence-based (cite claim_id where appropriate); " +
    "2) Interpreted (convert facts into insight); 3) Connected (tie to supplier right-to-play); " +
    "4) Strategic (facts → why it matters → how to win); 5) Commercial (decision- and outcome-oriented).";

  if (finalKey === "executive_summary") {
    const supplier = (outline?.input_notes?.supplier_company || outline?.input_notes?.prospect_company || "").trim();
    const outlineNotes = safeForPrompt(outline?.sections?.exec || {});
    return [
      `You are a senior UK B2B go-to-market consultant.`,
      CONSULTANT_STANDARD,
      // keep your system brief for ES, but we strengthen the user task to force 5 paras (no bullets)
      buildSectionSystem(finalKey, persona),
      "",
      "Task:",
      "Write the Executive Summary as exactly FIVE short paragraphs (no bullets).",
      "Paragraphs:",
      "1) Market environment — use CSV signals (needs/blockers/spend) and one relevant evidence claim (cite claim_id). Include cohort size if available.",
      "2) Strategic rationale — why act now, cite one high-signal claim by claim_id (e.g., regulator/ONS), connect to supplier focus and buyer context.",
      "3) How to win — one clean Moore sentence plus TWO strengths and TWO opportunities woven into the paragraph (no lists). Contrast only with allowed competitors when needed.",
      "4) What success looks like — quantified, timebound outcome anchored to cohort and route.",
      "5) Next steps — concrete, route-aware actions and pre-conditions in one paragraph.",
      "",
      "Context:",
      `- Cohort size (addressable_market): ${addressable_market ?? "unknown"}`,
      `- Supplier: ${supplier}`,
      `- PersonaBrief: ${JSON.stringify(personaBrief)}`,
      `- Evidence catalog ARRAY (use claim_ids): ${ev}`,
      `- CSV signals: ${csv}`,
      `- Product anchors: ${prod}`,
      `- Named competitors (allowed): ${competitors}`,
      `- Persona: ${persona}`,
      `- Outline fragment: ${outlineNotes}`,
      "",
      "Instructions:",
      `- Produce concise, persuasive content aligned to "${finalKey}".`,
      "- Cite claim_ids for external evidence where used.",
      "- No fabrication; if a specific proof is missing, write 'TBD' rather than inventing.",
      "",
      "Emit STRICT JSON ONLY:",
      `{"executive_summary": ["<p1>", "<p2>", "<p3>", "<p4>", "<p5>"]}`
    ].join("\n").trim();
  }

  // All other sections keep your current pattern, but we include persona brief + consultant standard
  return [
    `You are a senior UK B2B go-to-market consultant.`,
    CONSULTANT_STANDARD,
    buildSectionSystem(finalKey, persona),
    "",
    "Context:",
    `- PersonaBrief: ${JSON.stringify(personaBrief)}`,
    `- Evidence catalog ARRAY (use claim_ids): ${ev}`,
    `- CSV signals: ${csv}`,
    `- Product anchors: ${prod}`,
    `- Named competitors (allowed): ${competitors}`,
    `- Persona: ${persona}`,
    "",
    "Instructions:",
    `- Produce concise, practical content aligned to "${finalKey}". Cite claim_ids for external evidence where appropriate. No fabrication.`,
    "",
    "Emit STRICT JSON:",
    `${targetsFor(finalKey)}`,
    "Return JSON only."
  ].join("\n").trim();
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
  // Load needs map (best-effort) for deterministic Feature→Outcome→Value cues
  let needsMap = null;
  try {
    needsMap = await getJson(container, `${prefix}needs_map.json`);
  } catch { /* tolerate absence */ }

  // Helper: derive focus count & label from csv + outline (prefer csv.meta.focus_count)
  function deriveFocus({ csvCanon, outline }) {
    try {
      const meta = (csvCanon && csvCanon.meta) || {};
      const signals = (csvCanon && csvCanon.signals) || {};
      const counts = (signals && signals.counts) || {};
      const byNeed = counts.by_need || {};
      const byPurchase = counts.by_purchase || counts.by_intent || {};
      const total = Number.isFinite(Number(meta.rows)) ? Number(meta.rows) : null;

      const fromOutline =
        (outline?.input_notes?.selected_product || outline?.meta?.selected_product ||
          outline?.input_notes?.selected_offer || outline?.meta?.selected_offer || "");

      const focusLabel = String(fromOutline || "").trim();
      let focusCount = (meta && Number.isFinite(Number(meta.focus_count))) ? Number(meta.focus_count) : null;

      if (focusLabel && focusCount == null) {
        focusCount = Number.isFinite(Number(byNeed[focusLabel])) ? Number(byNeed[focusLabel])
          : Number.isFinite(Number(byPurchase[focusLabel])) ? Number(byPurchase[focusLabel])
            : null;
      }
      return { total, focusLabel, focusCount };
    } catch {
      return { total: null, focusLabel: "", focusCount: null };
    }
  }

  // Helper: compose one concise market-context sentence
  function composeMarketContext({ csvCanon, outline }) {
    try {
      const { total, focusLabel, focusCount } = deriveFocus({ csvCanon, outline });
      const sig = (csvCanon && csvCanon.signals) || {};
      const blockers = Array.isArray(sig.top_blockers) ? sig.top_blockers.filter(Boolean).slice(0, 2) : [];
      const parts = [];

      if (Number.isFinite(Number(total))) {
        parts.push(`Within the current campaign cohort, ${total.toLocaleString()} organisations have been identified`);
      }
      if (focusLabel && Number.isFinite(Number(focusCount))) {
        parts.push(`and ${focusCount.toLocaleString()} of them show explicit intent for “${focusLabel}”`);
      }
      if (parts.length) {
        let s = parts.join(", ") + ".";
        if (blockers.length) {
          const bl = blockers.join(" and ");
          s += ` Common blockers include ${bl}.`;
        }
        return s;
      }
      return "";
    } catch { return ""; }
  }

  // Helper: choose one high-signal, recent “Why now” claim from evidence
  function pickWhyNowClaim(evidenceBundle) {
    try {
      const cat = Array.isArray(evidenceBundle?.catalog) ? evidenceBundle.catalog : [];
      if (!cat.length) return "";

      // Simple priority: regulator/government tags first; else first claim with a short title
      const priority = (c) => {
        const t = (c.tag || c.source_tag || c.source || "").toString().toLowerCase();
        if (/\b(ofcom|ons|nso|gov|regulator|government)\b/.test(t)) return 0;
        if (/\b(ofcom|ons)\b/i.test(c?.title || "")) return 1;
        return 2;
      };

      const sorted = cat
        .map(c => ({
          title: (c.title || c.summary || "").toString().trim(),
          tag: (c.tag || c.source_tag || c.source || "").toString().trim()
        }))
        .filter(c => c.title)
        .sort((a, b) => priority(a) - priority(b));

      if (!sorted.length) return "";
      const top = sorted[0];
      const tag = top.tag ? ` [${top.tag}]` : "";
      return `${top.title}${tag}`; // e.g., "UK 5G coverage varies ... [Ofcom]"
    } catch { return ""; }
  }

  // Helper: one allowed-competitor sentence (cap to two names)
  function composeCompetitorLine(allowedCompetitors) {
    try {
      const arr = Array.isArray(allowedCompetitors) ? allowedCompetitors.map(s => String(s || "").trim()).filter(Boolean) : [];
      if (!arr.length) return "";
      const shortlist = arr.slice(0, 2);
      if (shortlist.length === 1) {
        return `Against ${shortlist[0]}, the supplier differentiates on execution, resilience, and speed to deploy.`;
      }
      return `Against competitors such as ${shortlist[0]} and ${shortlist[1]}, the supplier differentiates on execution, resilience, and speed to deploy.`;
    } catch { return ""; }
  }

  // Helper: compose up to 3 Feature→Outcome→Value sentences from CSV signals + needs map
  function composeFOV({ csvCanon, needsMapObj, max = 3 }) {
    try {
      const sig = (csvCanon && csvCanon.signals) || {};
      const needs = Array.isArray(sig.top_needs_supplier) ? sig.top_needs_supplier.filter(Boolean) : [];
      const take = needs.slice(0, Math.max(0, Math.min(max, 3)));
      if (!take.length) return "";

      // Resolve simple mappings if present; stay generic if not.
      const getCap = (need) => {
        if (!needsMapObj) return "";
        // common shapes: needsMap.items[], needsMap.map{}, needsMap.mapping{}
        if (Array.isArray(needsMapObj.items)) {
          const hit = needsMapObj.items.find(x => String(x?.need || "").toLowerCase() === String(need || "").toLowerCase());
          return (hit?.capability || hit?.service || hit?.maps_to || "").toString();
        }
        const maps = needsMapObj.map || needsMapObj.mapping || {};
        const key = String(need || "").toLowerCase();
        for (const [k, v] of Object.entries(maps)) {
          if (String(k || "").toLowerCase() === key) return (Array.isArray(v) ? v[0] : v) || "";
        }
        return "";
      };

      const sentences = [];
      for (const need of take) {
        const cap = String(getCap(need) || "").trim();
        if (cap) {
          sentences.push(`Deliver ${cap} to address ${need}, resulting in faster execution and lower operational risk.`);
        } else {
          // generic but still interprets the signal
          sentences.push(`Solve ${need} decisively to unlock near-term operational gains and a stronger compliance posture.`);
        }
      }
      // Join as one compact paragraph
      return sentences.join(" ");
    } catch {
      return "";
    }
  }
  // Authoritative competitor allow-list: prefer competitors.json; fallback to outline/strategy lists
  let allowedCompetitors = [];
  try {
    const cj = await getJson(container, `${prefix}competitors.json`);
    if (Array.isArray(cj?.competitors) && cj.competitors.length) {
      allowedCompetitors = cj.competitors.map(s => String(s || "").trim()).filter(Boolean);
    }
  } catch { /* tolerate missing file */ }
  if (!allowedCompetitors.length) {
    const fromOutlineA = Array.isArray(outline?.input_notes?.competitors) ? outline.input_notes.competitors : [];
    const fromOutlineB = Array.isArray(outline?.input_notes?.relevant_competitors) ? outline.input_notes.relevant_competitors : [];
    const fromStrategy = Array.isArray(strategy?.meta?.relevant_competitors) ? strategy.meta.relevant_competitors : [];
    const merged = [...fromOutlineA, ...fromOutlineB, ...fromStrategy].map(s => String(s || '').trim()).filter(Boolean);
    const seen = new Set();
    allowedCompetitors = merged.filter(n => {
      const key = n.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    }).slice(0, 12);
  }

  // ---- Assemble whole campaign ----
  if (op === "assemble") {
    // Phase: writer working (append-only; terminal-guard)
    {
      const cur = (await getJson(container, `${prefix}status.json`)) || {};
      const prev = String(cur.state || "");
      const terminal = new Set(["assembled", "error", "Failed"]);
      if (!terminal.has(prev)) {
        const STATE_ORDER = ["ingest", "Outline", "EvidenceDigest", "StrategySynthesis", "strategy_working", "strategy_ready", "SectionWrites", "writer_working", "assembled", "Failed", "error"];
        async function safeSetState(next) {
          const st = (await getJson(container, `${prefix}status.json`)) || {};
          const cur = String(st.state || "ingest");
          if (STATE_ORDER.indexOf(next) <= STATE_ORDER.indexOf(cur)) return; // refuse backward
          await patchStatus(container, prefix, next, { runId, updatedAt: nowISO(), op: "assemble" });
        }
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

        if (finalKey === "executive_summary") {
          const es = merged.executive_summary;

          // 1) If we already have the new narrative shape, pass it through untouched
          const hasNarrative =
            es && typeof es === "object" && !Array.isArray(es) &&
            ("environment_paragraph" in es || "rationale_paragraph" in es || "how_to_win_paragraph" in es);

          if (hasNarrative) {
            merged.executive_summary = es; // keep narrative
          } else if (Array.isArray(es)) {
            // 2) Old runs: best-effort normalisation for backward compatibility
            const lead = String(es[0] || "").trim();
            const rest = es.slice(1).map(s => String(s || "").trim()).filter(Boolean);
            merged.executive_summary = {
              environment_paragraph: lead || "",
              rationale_paragraph: rest[0] || "",
              how_to_win_paragraph: rest[1] || "",
              success_paragraph: rest[2] || "",
              next_steps_paragraph: rest[3] || ""
            };
          } else {
            // 3) Nothing available → empty narrative shell
            merged.executive_summary = {
              environment_paragraph: "",
              rationale_paragraph: "",
              how_to_win_paragraph: "",
              success_paragraph: "",
              next_steps_paragraph: ""
            };
          }

          // Carry Addressable Market when present in the ES section file
          if (Object.prototype.hasOwnProperty.call(piece, "addressable_market") && piece.addressable_market != null) {
            merged.addressable_market = piece.addressable_market;
          }
        }
      }
    }
    // PATCH COM-ORDER-LOCK: guarantee deterministic section ordering
    const ordered = {};
    for (const k of FINAL_SECTION_KEYS) {
      if (k in merged) ordered[k] = merged[k];
    }
    // Append any non-section keys afterwards (preserve them but after sections)
    for (const [k, v] of Object.entries(merged)) {
      if (!(k in ordered)) ordered[k] = v;
    }

    // PATCH COM-NORMALISE: ensure safe arrays (operate on the ordered object)
    try {
      if (!Array.isArray(ordered.evidence_log)) ordered.evidence_log = [];
      for (const k of ["bullets", "matrix", "competitor_set"]) {
        if (ordered[k] && !Array.isArray(ordered[k])) ordered[k] = [ordered[k]];
      }
    } catch { /* non-fatal */ }

    // Enforce competitor allow-list on final campaign payload
    try {
      const allowed = new Set((Array.isArray(allowedCompetitors) ? allowedCompetitors : []).map(s => String(s || '').toLowerCase()));
      if (ordered && ordered.positioning_and_differentiation && Array.isArray(ordered.positioning_and_differentiation.competitor_set)) {
        let filtered = ordered.positioning_and_differentiation.competitor_set
          .map(c => (typeof c === "string" ? { vendor: c } : c))
          .filter(c => allowed.size ? allowed.has(String(c?.vendor || "").trim().toLowerCase()) : true);
        if (!filtered.length && allowed.size) {
          filtered = [...allowed].slice(0, 5).map(name => ({ vendor: name }));
        }
        ordered.positioning_and_differentiation.competitor_set = filtered;
      }
    } catch { /* non-fatal */ }

    // Mirror campaign_title to title (do not rename keys)
    try {
      if (ordered && ordered.campaign_title && (!ordered.title || ordered.title !== ordered.campaign_title)) {
        ordered.title = ordered.campaign_title;
      }
    } catch { /* non-fatal */ }

    // Write in deterministic order
    await putJson(container, `${prefix}campaign.json`, ordered);

    // Status patch (use your existing timestamp helper name)
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
// --- ES quality guard: single repair pass if shape is wrong (idempotent) ---
if (finalKey === "executive_summary") {
  try {
    const arr = (sect && Array.isArray(sect.executive_summary)) ? sect.executive_summary : (Array.isArray(sect) ? sect : null);
    const needsRepair =
      !Array.isArray(arr) ||
      arr.length !== 5 ||
      arr.some(p => typeof p !== "string" || !p.trim());

    if (needsRepair) {
      // Build a compact persona brief (same logic as in buildSectionUser)
      const personaBrief = (() => {
        try {
          const route = (outline?.input_notes?.sales_model || outline?.meta?.sales_model || "").toString().trim();
          const topNeeds = Array.isArray(csvCanon?.signals?.top_needs_supplier)
            ? csvCanon.signals.top_needs_supplier.filter(Boolean).slice(0, 3)
            : [];
          const topBlockers = Array.isArray(csvCanon?.signals?.top_blockers)
            ? csvCanon.signals.top_blockers.filter(Boolean).slice(0, 3)
            : [];
          const nm = evidenceBundle?.needsMap || {};
          let capability_hints = [];
          if (nm && typeof nm === "object") {
            const mapping = nm.mapping || nm.map || {};
            for (const n of topNeeds) {
              const m = mapping[n];
              if (m && Array.isArray(m.capabilities)) capability_hints = capability_hints.concat(m.capabilities.slice(0, 2));
            }
            capability_hints = capability_hints.filter(Boolean).slice(0, 3);
          }
          return {
            persona: (outline?.meta?.persona || "").toString().trim() || null,
            route: route || null,
            top_needs: topNeeds,
            top_blockers: topBlockers,
            capability_hints
          };
        } catch {
          return { persona: (outline?.meta?.persona || "").toString().trim() || null, route: null, top_needs: [], top_blockers: [], capability_hints: [] };
        }
      })();

      const repairSystem =
        "Repair the Executive Summary to exactly five short paragraphs, following the consultant standard: " +
        "Evidence-based (cite claim_id where appropriate), Interpreted, Connected, Strategic, Commercial. " +
        "Return STRICT JSON only: {\"executive_summary\":[\"p1\",\"p2\",\"p3\",\"p4\",\"p5\"]}.";

      const repairUser = [
        "CONTEXT (for repair):",
        `- PersonaBrief: ${JSON.stringify(personaBrief)}`,
        `- Allowed competitors: ${safeForPrompt(allowedCompetitors || [])}`,
        `- Evidence (catalog; cite claim_ids): ${safeForPrompt(evidenceBundle?.catalog || [])}`,
        `- CSV signals: ${safeForPrompt(csvCanon || {})}`,
        `- Product anchors: ${safeForPrompt(evidenceBundle?.productNames || [])}`,
        "",
        "PRIOR JSON (to repair):",
        JSON.stringify(sect || {})
      ].join("\n");

      const { callChatJsonObject } = await loadPromptHarness();
      const repaired = await callChatJsonObject({ system: repairSystem, user: repairUser, timeoutMs: LLM_TIMEOUT_MS });

      if (repaired && Array.isArray(repaired.executive_summary) && repaired.executive_summary.length === 5
          && repaired.executive_summary.every(p => typeof p === "string" && p.trim())) {
        sect = { executive_summary: repaired.executive_summary };
      }
    }
  } catch (e) {
    context.log && context.log.warn && context.log.warn("[writer] ES repair pass failed", String(e?.message || e));
    // non-fatal; fall through and persist whatever we have
  }
}
// --- end ES quality guard ---

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
    const user = buildSectionUser(finalKey, { outline, evidenceBundle, csvCanon, allowedCompetitors });
    // Deterministic summaries available for ES augmentation (no duplicates, pure append-only)
    const aug = buildExecAugments({ evidenceBundle, csvCanon, outline, strategy });
    const fovES = composeFOV({ csvCanon, needsMapObj: needsMap, max: 3 });
    const marketContext = composeMarketContext({ csvCanon, outline });
    const competitorLine = composeCompetitorLine(allowedCompetitors);
    const whyNow = pickWhyNowClaim(evidenceBundle);


    const { callChatJsonObject } = await loadPromptHarness();
    const raw = await callChatJsonObject({ system, user, timeoutMs: LLM_TIMEOUT_MS });
    // normalise shape: expect the section key at root
    const out = {};
    const sect = (raw && typeof raw === "object" && raw[finalKey] != null) ? raw[finalKey] : raw;

    if (finalKey !== "executive_summary") {
      out[finalKey] = sect || {};
    } else {
      // Expect object with 5 narrative paragraphs; coerce common shapes defensively.
      const esIn = (sect && typeof sect === "object") ? sect : {};
      const mooreLine = mooreBulletFromStrategy(strategy) || "";
      const trim = (v) => (v == null ? "" : String(v)).replace(/\s+/g, " ").trim();

      let environment_paragraph = trim(esIn.environment_paragraph || "");
      let rationale_paragraph = trim(esIn.rationale_paragraph || "");
      let how_to_win_paragraph = trim(esIn.how_to_win_paragraph || "");
      let success_paragraph = trim(esIn.success_paragraph || "");
      let next_steps_paragraph = trim(esIn.next_steps_paragraph || "");

      // Ensure Moore leads the how-to-win paragraph (if missing, prepend it)
      if (mooreLine && how_to_win_paragraph && !how_to_win_paragraph.toLowerCase().includes(mooreLine.toLowerCase())) {
        how_to_win_paragraph = `${mooreLine} ${how_to_win_paragraph}`.replace(/\s+/g, " ").trim();
      } else if (mooreLine && !how_to_win_paragraph) {
        how_to_win_paragraph = mooreLine;
      }

      // Optional, safe guardrails:
      // - Suppress invented competitor comparisons if none were supplied in outline.
      try {
        const allowed = new Set((Array.isArray(allowedCompetitors) ? allowedCompetitors : []).map(s => String(s || '').toLowerCase()));
        const whitelist = new Set(['market', 'environment', 'buyer', 'demand', 'need', 'needs', 'blockers', 'spend', 'investment']);
        const hasDisallowed = (txt) => {
          if (!txt) return false;
          const tokens = String(txt).toLowerCase().split(/[^a-z0-9]+/);
          const comparisonCue = /\b(vs|versus|against|compared|than|alternative|competitor|compare)\b/i;
          if (!comparisonCue.test(txt)) return false;
          for (const t of tokens) {
            if (whitelist.has(t)) continue;
            if (allowed.has(t)) return false;
          }
          return allowed.size === 0;
        };
        if (!allowed.size) {
          if (hasDisallowed(environment_paragraph)) environment_paragraph = environment_paragraph.replace(/\b(vs|versus|against|compared|than|alternative|competitor|compare)\b/gi, ' ').trim();
          if (hasDisallowed(rationale_paragraph)) rationale_paragraph = rationale_paragraph.replace(/\b(vs|versus|against|compared|than|alternative|competitor|compare)\b/gi, ' ').trim();
          if (hasDisallowed(how_to_win_paragraph)) how_to_win_paragraph = how_to_win_paragraph.replace(/\b(vs|versus|against|compared|than|alternative|competitor|compare)\b/gi, ' ').trim();
        }
      } catch { /* non-fatal */ }

      try {
        // Environment: add AM + market context + blockers if not present
        const envParts = [environment_paragraph];
        if (aug?.amLine && !environment_paragraph.includes(aug.amLine)) envParts.push(aug.amLine);
        if (aug?.marketContext && !environment_paragraph.includes(aug.marketContext)) envParts.push(aug.marketContext);
        if (aug?.blockersLine && !environment_paragraph.includes(aug.blockersLine)) envParts.push(aug.blockersLine);
        environment_paragraph = envParts.filter(Boolean).join(" ").trim();

        // Rationale: add single why-now claim
        if (whyNow && !rationale_paragraph.includes(whyNow)) {
          rationale_paragraph = [rationale_paragraph, whyNow].filter(Boolean).join(" ").trim();
        }

        // How to win: add allowed-competitor line + F→O→V
        if (competitorLine && !how_to_win_paragraph.includes(competitorLine)) {
          how_to_win_paragraph = [how_to_win_paragraph, competitorLine].filter(Boolean).join(" ").trim();
        }
        if (Array.isArray(aug?.fov) && aug.fov.length) {
          const fovJoined = aug.fov.join(" ");
          if (fovJoined && !how_to_win_paragraph.includes(fovJoined)) {
            how_to_win_paragraph = [how_to_win_paragraph, fovJoined].filter(Boolean).join(" ").trim();
          }
        }

        // Final tone polish (no admin-y phrasing)
        const polished = polishExecSummaryLines([
          environment_paragraph,
          rationale_paragraph,
          how_to_win_paragraph,
          success_paragraph,
          next_steps_paragraph
        ]);
        [environment_paragraph, rationale_paragraph, how_to_win_paragraph, success_paragraph, next_steps_paragraph] = polished;
      } catch { /* non-fatal */ }

      out.executive_summary = {
        environment_paragraph,
        rationale_paragraph,
        how_to_win_paragraph,
        success_paragraph,
        next_steps_paragraph
      };
    }

    if (finalKey === "executive_summary") {
      try {
        // Persist Addressable Market facts (cohort + focus subset) next to the narrative
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
      } catch { /* non-fatal */ }
    }

    await putJson(container, `${prefix}sections/${finalKey}.json`, out);
    await patchStatus(container, prefix, "SectionWrites", {
      runId, written: finalKey, updatedAt: nowISO(), op: "section"
    });
    return;
  }

  throw new Error(`Unknown job type "${op}". Use "write_section" or "assemble".`);
};
