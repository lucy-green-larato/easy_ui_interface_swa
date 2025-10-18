5
/* ---------- Shared helpers ---------- */
const basePrefix = (() => {
  const path = location.pathname || "/";
  const segs = path.split("/").filter(Boolean);
  const last = segs[segs.length - 1] || "";
  if (/\.[a-z0-9]+$/i.test(last)) return "";
  return segs.length ? "/" + segs[0] : "";
})();

const STORAGE_PREFIX = "lead_qualification_v1";
const FORM_ID = "qual-form";
const OUTPUT_KEY = STORAGE_PREFIX + ".last_output";
const ACTIVITY_KEY = STORAGE_PREFIX + ".activity";
const FORM_KEY = STORAGE_PREFIX + ".form";
const NOTES_KEY_PREFIX = STORAGE_PREFIX + ".notes";
const CALL_PREF_KEY = STORAGE_PREFIX + ".call_pref";

const esc = s => String(s || "").replace(/[&<>]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
const strongify = s => String(s || "").replace(/\*\*([^\*\n][\s\S]*?)\*\*/g, "<strong>$1</strong>");

// --- Theme toggle (qualification app) ---
(function () {
  const root = document.documentElement;
  const btn = document.getElementById("themeToggle");
  if (!btn) return;

  // Present the toggle beside the user badge (not inside the calltype group)
  try {
    const userBadge = document.getElementById("user-badge");
    const headerRight = document.querySelector(".app-header .header-right");
    if (userBadge && headerRight && btn.parentElement !== headerRight) {
      headerRight.insertBefore(btn, userBadge);
    }
  } catch { }

  const readTheme = () =>
    (localStorage.getItem("it-theme") || localStorage.getItem("theme") || "").toLowerCase();

  const writeTheme = (v) => {
    localStorage.setItem("it-theme", v);
    // keep legacy key in sync
    localStorage.setItem("theme", v);
  };

  const apply = (theme) => {
    const t = (theme === "dark" ? "dark" : "light");
    root.setAttribute("data-theme", t);
    btn.setAttribute("aria-pressed", String(t === "dark"));
    btn.textContent = t === "dark" ? "Light mode" : "Night mode";
  };

  // initial
  const initial = readTheme() || (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  apply(initial);

  // click → toggle
  btn.addEventListener("click", () => {
    const next = root.getAttribute("data-theme") === "dark" ? "light" : "dark";
    writeTheme(next);
    apply(next);
  });

  // keep in sync if another tab flips it
  window.addEventListener("storage", (e) => {
    if (e.key === "it-theme" || e.key === "theme") {
      const v = (e.newValue || "").toLowerCase();
      if (v === "dark" || v === "light") apply(v);
    }
  });
})();

// === Highlighter helpers (wrap selected text in <mark>) ===
let QUAL_highlightOn = false;

function QUAL_wrapSelectionInMark(root) {
  const sel = window.getSelection && window.getSelection();
  if (!sel || sel.rangeCount === 0) return;
  const range = sel.getRangeAt(0);
  if (!root || !root.contains(range.commonAncestorContainer)) return;
  if (range.collapsed) return;

  const mark = document.createElement("mark");
  try { range.surroundContents(mark); }
  catch {
    // Fallback: clone contents to avoid splitting nodes
    const frag = range.cloneContents();
    mark.appendChild(frag);
    range.deleteContents();
    range.insertNode(mark);
  }
  sel.removeAllRanges();
}

function QUAL_clearHighlights(root) {
  if (!root) return;
  const marks = root.querySelectorAll("mark");
  marks.forEach(m => {
    const parent = m.parentNode;
    while (m.firstChild) parent.insertBefore(m.firstChild, m);
    parent.removeChild(m);
    parent.normalize();
  });
}

// checklist helpers //
// --- Upload button -> hidden <input type="file"> trigger ------------------
function initUploadReports() {
  const btn = document.getElementById('upload-reports');
  const input = document.getElementById('report_files');
  if (!btn || !input) return;            // safe no-op if markup not present
  btn.addEventListener('click', () => input.click());
}

// Minimal markdown → HTML (H2/H3, lists, paragraphs)
function mdToHtml(md) {
  var s = String(md || "").replace(/\r/g, "");
  var lines = s.split("\n");
  var out = [];
  var inList = false;
  function flushList() { if (inList) { out.push("</ul>"); inList = false; } }
  for (var i = 0; i < lines.length; i++) {
    var line = lines[i].trim();
    if (!line) { flushList(); continue; }
    if (/^#{2,}\s+/.test(line)) {
      flushList();
      out.push("<h3>" + esc(line.replace(/^#{2,}\s+/, "")) + "</h3>");
      continue;
    }
    if (/^- /.test(line)) {
      if (!inList) { out.push("<ul>"); inList = true; }
      out.push("<li>" + strongify(esc(line.slice(2))) + "</li>");
      continue;
    }
    flushList();
    out.push("<p>" + strongify(esc(line)) + "</p>");
  }
  flushList();
  return out.join("");
}

// --- API auth helper (shared-secret via <meta name="qual-api-key"> or window var) ---
function authHeaders() {
  const meta = document.querySelector('meta[name="qual-api-key"]');
  const key =
    (window.__QUAL_API_KEY && String(window.__QUAL_API_KEY).trim()) ||
    (meta && String(meta.content || "").trim());
  return key ? { Authorization: "Bearer " + key } : {};
}


// Buyer behaviour → slug
function buyerSlug(label) {
  const l = String(label || "").toLowerCase();
  if (l.includes("innovator")) return "innovator";
  if (l.includes("early adopter")) return "early-adopter";
  if (l.includes("early majority")) return "early-majority";
  if (l.includes("late majority")) return "late-majority";
  if (l.includes("sceptic") || l.includes("skeptic")) return "sceptic";
  return "early-majority";
}

// --- Prioritisation (local-only, no server calls) ---
function scoreOpportunityFromText(text) {
  const t = String(text || "").toLowerCase();

  // You can tune these lists anytime without affecting other logic
  const strategicTerms = [
    "board", "strategy", "strategic", "transformation", "digital transformation", "operating model",
    "market entry", "expansion", "acquisition", "merger", "regulatory", "compliance mandate",
    "capex", "multi-year plan", "growth thesis", "turnaround", "replatform", "modernisation"
  ];
  const operationalTerms = [
    "process", "workflow", "cost", "efficiency", "automation", "sla", "ticket", "backlog",
    "incident", "downtime", "utilisation", "throughput", "rollout", "migration",
    "integration", "time to value", "productivity", "service desk", "operational"
  ];

  function countHits(terms) {
    let c = 0;
    for (const s of terms) {
      const rx = new RegExp("\\b" + s.replace(/\s+/g, "\\s+") + "\\b", "gi");
      const m = t.match(rx);
      if (m) c += m.length;
    }
    return c;
  }

  const sCount = countHits(strategicTerms);
  const oCount = countHits(operationalTerms);

  // Simple thresholds; adjust if you wish
  const hasStrategic = sCount >= 2;
  const hasOperational = oCount >= 2;

  // "Only partially" if exactly one bucket is strong OR both are weak
  const partialFit = (hasStrategic ^ hasOperational) || (!hasStrategic && !hasOperational);

  return {
    strategicImpact: {
      yesNo: hasStrategic,
      rationale: hasStrategic
        ? `Found ${sCount} strong strategic signal(s).`
        : `Insufficient strategic signals (found ${sCount}).`
    },
    operationalImpact: {
      yesNo: hasOperational,
      rationale: hasOperational
        ? `Found ${oCount} strong operational signal(s).`
        : `Insufficient operational signals (found ${oCount}).`
    },
    partialFit: {
      yesNo: partialFit,
      rationale: partialFit
        ? "Signals indicate incomplete coverage (strong in one area or weak overall)."
        : "Signals indicate good coverage across strategic and operational."
    }
  };
}

function buildPrioritisationPayload({ scores, inputs, scriptText }) {
  return {
    createdAt: new Date().toISOString(),
    sourceTool: "InsideTrack-LeadQualification",
    // The three fields you want to hand to the ranking tool later:
    strategicImpact: scores.strategicImpact,     // { yesNo, rationale }
    operationalImpact: scores.operationalImpact, // { yesNo, rationale }
    partialFit: scores.partialFit,               // { yesNo, rationale }
    // Optional extras (for future use)
    prospect_company: inputs.prospect_company || "",
    product_service: inputs.product_service || "",
    buyer_type: inputs.buyer_type || "",
    excerpt: String(scriptText || "").slice(0, 800)
  };
}

function renderPrioritisationPreviewHTML(scores) {
  function badge(val) {
    return `<span class="pill" style="margin-left:.5rem">${val ? "Yes" : "No"}</span>`;
  }
  return `
    <h4>Review</h4>
    <ul>
      <li><strong>Strategic problem solved?</strong> ${badge(scores.strategicImpact.yesNo)}<br>
          <span class="muted">${esc(scores.strategicImpact.rationale)}</span></li>
      <li style="margin-top:.5rem"><strong>Operational problem solved?</strong> ${badge(scores.operationalImpact.yesNo)}<br>
          <span class="muted">${esc(scores.operationalImpact.rationale)}</span></li>
      <li style="margin-top:.5rem"><strong>Only partially solves the buyer’s problems?</strong> ${badge(scores.partialFit.yesNo)}<br>
          <span class="muted">${esc(scores.partialFit.rationale)}</span></li>
    </ul>
    <p class="fineprint" style="margin-top:var(--s-3)">This is a lightweight client-side assessment based on the generated report’s text.</p>
  `;
}

// DOM refs
const form = document.getElementById(FORM_ID);
const statusEl = document.getElementById("status");
const diag = document.getElementById("diagnostics");
const diagJson = document.getElementById("diag-json");
const outputEl = document.getElementById("output");
const outputMeta = document.getElementById("output-meta");
const copyBtn = document.getElementById("copy-report");
const dlBtn = document.getElementById("download-report");
const docxBtn = document.getElementById("export-docx");
const emailBtn = document.getElementById("email-report");
const notesArea = document.getElementById("notes");
const saveNotesBtn = document.getElementById("save-notes");
const downloadNotesBtn = document.getElementById("download-notes");
const deltaLog = document.getElementById("delta-log");
const activityLog = document.getElementById("activity-log");
const ixbrlBtn = document.getElementById("fetch-ixbrl");
const ixbrlStatus = document.getElementById("ixbrl-status");
const ixbrlChips = document.getElementById("ixbrl-chips");
const chdiBtn = document.getElementById("fetch-chdi");
const chdiStatus = document.getElementById("chdi-status");
const flagsList = document.getElementById("flags-list");
const tipsList = document.getElementById("tips-list");
const tipsEmpty = document.getElementById("tips-empty");
const sourcesCard = document.getElementById("sources");
const sourceList = document.getElementById("source-list");
// Prioritisation UI
const prioBtn = document.getElementById("send-prioritise");
const prioModal = document.getElementById("prio-modal");
const prioClose = document.getElementById("prio-close");
const prioBody = document.getElementById("prio-body");
const prioCopy = document.getElementById("prio-copy");

let LAST_PRIO_PAYLOAD = null; // set on each score for Copy JSON

const callTypeSel = document.getElementById("call_type");
const callTypeSelXs = document.getElementById("call_type_xs");
const rememberCallType = document.getElementById("remember_call_type");
const modelIndicator = document.getElementById("model-indicator");
const mapLabel = document.getElementById("map-label");

const chModal = document.getElementById("ch-modal");
const chHelp = document.getElementById("ch-help");
const chClose = document.getElementById("ch-close");

const helpBtn = document.getElementById("help");
const helpModal = document.getElementById("help-modal");
const helpClose = document.getElementById("help-close");

// **File input: correct IDs**
const reportInput = document.getElementById("report_files");
const reportList = document.getElementById("file_list");

const requiredIds = [
  "call_type",
  "seller_name",
  "seller_company",
  "prospect_name",
  "prospect_role",
  "prospect_company",
  "prospect_website",
  "product_service"
];

// Accessibility helpers
function setStatus(msg, kind = "info") {
  if (!statusEl) return;
  statusEl.textContent = msg;
  statusEl.dataset.kind = kind;
}
function validateField(id) {
  const el = document.getElementById(id);
  if (!el) return true;
  const val = (el.value || "").trim();
  const ok = requiredIds.includes(id) ? !!val : true;
  const errEl = document.getElementById(id + "_error");
  if (errEl) {
    if (ok) { errEl.textContent = ""; el.classList.remove("invalid"); el.removeAttribute("aria-invalid"); }
    else { errEl.textContent = "This field is required."; el.classList.add("invalid"); el.setAttribute("aria-invalid", "true"); }
  }
  return ok;
}
function allRequiredFilled() {
  const missing = requiredIds.filter(id => !String(document.getElementById(id)?.value || "").trim());
  if (missing.length) setStatus("Please complete: " + missing.map(id => id.replace("_", " ")).join(", ") + ".", "error");
  else setStatus("");
  return missing.length === 0;
}

// Persist
function saveForm() {
  if (!form) return;
  const fd = Object.fromEntries(new FormData(form).entries());
  try { localStorage.setItem(FORM_KEY, JSON.stringify(fd)); } catch { }
}
function loadForm() {
  if (!form) return;
  try {
    const raw = localStorage.getItem(FORM_KEY); if (!raw) return;
    const data = JSON.parse(raw);
    Object.entries(data).forEach(([k, v]) => {
      const el = form.elements[k] || document.getElementById(k);
      if (!el) return;
      el.value = v;
    });
  } catch { }
}
const notesKey = () => {
  const pn = (form?.elements?.prospect_name?.value || "").trim().toLowerCase();
  const pc = (form?.elements?.prospect_company?.value || "").trim().toLowerCase();
  return NOTES_KEY_PREFIX + "::" + pc + "::" + pn;
};
function loadNotes() { try { notesArea.value = localStorage.getItem(notesKey()) || ""; } catch { } }
function saveNotes() { try { localStorage.setItem(notesKey(), notesArea.value || ""); setStatus("Notes saved."); } catch { setStatus("Could not save notes.", "error"); } }
function updateMapIndicator() {
  modelIndicator.textContent = callTypeSel?.value || "Not set";
  mapLabel.textContent = callTypeSel?.value || "Not set";
}
function loadCallPref() {
  try {
    const pref = JSON.parse(localStorage.getItem(CALL_PREF_KEY) || "null");
    if (pref?.call_type && callTypeSel && !callTypeSel.value) callTypeSel.value = pref.call_type;
    if (pref?.remember === true && rememberCallType) rememberCallType.checked = true;
  } catch { }
  updateMapIndicator();
}
function saveCallPrefIfRequested() {
  if (rememberCallType?.checked && callTypeSel?.value) {
    localStorage.setItem(CALL_PREF_KEY, JSON.stringify({ remember: true, call_type: callTypeSel.value }));
  } else if (rememberCallType && !rememberCallType.checked) {
    localStorage.removeItem(CALL_PREF_KEY);
  }
  updateMapIndicator();
}

// File list (optional; never blocks submit)
reportInput?.addEventListener("change", () => {
  reportList && (reportList.innerHTML = "");
  const files = Array.from(reportInput.files || []).slice(0, 2);
  files.forEach(f => {
    const li = document.createElement("li");
    li.textContent = f.name + " (" + Math.ceil((f.size || 0) / 1024) + " KB)";
    reportList?.appendChild(li);
  });
  enableSubmitIfValid();
});

// iXBRL fetch
async function fetchIxbrl(companyNumber) {
  if (!companyNumber) return { ok: false, status: 400, data: null };
  ixbrlStatus.textContent = "Fetching…";
  const endpoints = [
    "/api/ixbrl-financials?company=" + encodeURIComponent(companyNumber),
    "/api/ixbrl-financials/" + encodeURIComponent(companyNumber),
    "/api/ixbrl-financials?company_number=" + encodeURIComponent(companyNumber)
  ];
  for (let i = 0; i < endpoints.length; i++) {
    const url = endpoints[i];
    try {
      const r = await fetch(url, { headers: { "Accept": "application/json" }, cache: "no-store" });
      if (r.status === 204) { ixbrlStatus.textContent = "No iXBRL available."; return { ok: false, status: 204, data: null }; }
      let data = {};
      try { data = await r.json(); } catch { data = {}; }
      if (!r.ok) throw new Error((data && data.error) || ("HTTP " + r.status));
      ixbrlStatus.textContent = "Loaded.";
      return { ok: true, status: 200, data };
    } catch (e) {
      // try the next endpoint
    }
  }
  ixbrlStatus.textContent = "Could not fetch iXBRL.";
  return { ok: false, status: 500, data: null };
}

// CHDI fetch (latest two Accounts PDFs → OCR text) via qualification-generate GET
async function fetchChdi(companyNumber) {
  if (!companyNumber) return { ok: false, status: 400, data: null };
  if (chdiStatus) chdiStatus.textContent = "Fetching…";

  // Call the unified endpoint (GET) which now serves the CHDI response shape: { pdfs }
  const endpoints = [
    "/api/qualification-generate?company=" + encodeURIComponent(companyNumber),
    "/api/qualification-generate?company_number=" + encodeURIComponent(companyNumber),
    "/api/qualification-generate?id=" + encodeURIComponent(companyNumber)
  ];

  for (let i = 0; i < endpoints.length; i++) {
    const url = endpoints[i];
    try {
      const r = await fetch(url, { headers: { "Accept": "application/json" }, cache: "no-store" });
      if (r.status === 204) {
        if (chdiStatus) chdiStatus.textContent = "No filings found.";
        return { ok: false, status: 204, data: null };
      }
      let data = {};
      try { data = await r.json(); } catch { data = {}; }
      if (!r.ok) throw new Error((data && data.error) || ("HTTP " + r.status));

      // Expect unified shape: { pdfs: [{ filename, text }, ...] }
      if (chdiStatus) chdiStatus.textContent = "Loaded.";
      return { ok: true, status: 200, data };
    } catch (e) {
      // try the next endpoint
    }
  }
  if (chdiStatus) chdiStatus.textContent = "Could not fetch filings.";
  return { ok: false, status: 500, data: null };
}


function pct(n) { return (n == null || isNaN(n)) ? "—" : (Math.round(n * 10) / 10) + "%"; }
function ratio(n) { return (n == null || isNaN(n)) ? "—" : String(Math.round(n * 100) / 100); }

function renderIxbrlChips(summaryLike) {
  ixbrlChips.innerHTML = "";
  if (!summaryLike) return;

  const summary = summaryLike.summary || summaryLike; // tolerate either shape
  if (!summary?.derived && !(summary?.years && summary.years.length)) return;

  const d = summary.derived || {};
  const chips = [];

  if (d.revenueYoYPct != null) chips.push('<span class="chip">Revenue YoY: ' + pct(d.revenueYoYPct) + '</span>');
  if (d.grossMarginPct && d.grossMarginPct.y1 != null) chips.push('<span class="chip">Gross margin (Y1): ' + pct(d.grossMarginPct.y1) + '</span>');
  if (d.grossMarginPct && d.grossMarginPct.y2 != null) chips.push('<span class="chip">Gross margin (Y2): ' + pct(d.grossMarginPct.y2) + '</span>');
  if (d.currentRatio != null) chips.push('<span class="chip">Current ratio: ' + ratio(d.currentRatio) + '</span>');
  if (d.cashRatio != null) chips.push('<span class="chip">Cash ratio: ' + ratio(d.cashRatio) + '</span>');
  ixbrlChips.innerHTML = chips.join(" ");

  // Auto-flags → auto tips
  const flags = [];
  if (d.revenueYoYPct != null && d.revenueYoYPct < 0) flags.push("Revenue contracted YoY.");
  if (d.currentRatio != null && d.currentRatio < 1) flags.push("Current ratio below 1.0 (liquidity pressure).");
  if (d.cashRatio != null && d.cashRatio < 0.25) flags.push("Cash ratio very low.");
  if (d.netDebtToEquity != null && d.netDebtToEquity > 1) flags.push("High leverage (net debt / equity > 1).");

  if (flagsList) {
    flagsList.innerHTML = "";
    flags.forEach(f => { const li = document.createElement("li"); li.textContent = f; flagsList.appendChild(li); });
  }
  applyAutoTips(flags);;
}

// -------- Tips system (unified) --------
const TIPS_JSON_URL = "./assets/qualification-tips.json";
let QUAL_TIPS = { default: [], byBuyerType: {}, byMode: {} };
let BASE_TIPS = []; // from LLM or bank
let AUTO_TIPS = []; // from flags

async function loadTipsBank() {
  try {
    const res = await fetch(TIPS_JSON_URL, { cache: "no-store" });
    if (res.ok) {
      const bank = await res.json();
      if (bank && typeof bank === "object") QUAL_TIPS = bank;
    }
  } catch { /* fallback keeps defaults */ }
}

function pickBankTips(buyerType, mode) {
  const byType = (QUAL_TIPS.byBuyerType && QUAL_TIPS.byBuyerType[String(buyerType || "").toLowerCase()]) || [];
  const byMode = (QUAL_TIPS.byMode && QUAL_TIPS.byMode[String(mode || "").toLowerCase()]) || [];
  const def = QUAL_TIPS.default || [];
  return [...byType, ...byMode, ...def].filter(Boolean).slice(0, 3);
}

function applyAutoTips(flags) {
  const auto = [];
  flags.forEach(f => {
    if (/Revenue contracted/i.test(f)) auto.push("Frame growth levers (coverage, conversion, ARPU) with conservative targets.");
    if (/Current ratio below 1/i.test(f)) auto.push("Propose low-friction onboarding and activity-gated MDF to de-risk cash usage.");
    if (/Cash ratio very low/i.test(f)) auto.push("Position light-touch pilots and postcode-led campaigns before scale.");
    if (/High leverage/i.test(f)) auto.push("Avoid heavy upfront commitments; tie discounts to first wins.");
  });
  AUTO_TIPS = Array.from(new Set(auto)).slice(0, 5);
  renderTips(); // merge with BASE_TIPS
}

function renderTips() {
  const list = document.getElementById("tips-list");
  if (!list) return;
  list.innerHTML = "";
  const merged = Array.from(new Set([...AUTO_TIPS, ...BASE_TIPS])).slice(0, 8);
  merged.forEach(t => { const li = document.createElement("li"); li.textContent = t; list.appendChild(li); });
  tipsEmpty.style.display = merged.length ? "none" : "";
}

// -------- SWOT (derived from generated report) --------

// Small helper to grab text of a markdown section: "## <heading>" (until next "##")
function QUAL_mdSection(md, heading) {
  const h = String(heading || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const rx = new RegExp("^\\s*##\\s*" + h + "\\s*$([\\s\\S]*?)(?=^\\s*##\\s+|\\Z)", "mi");
  const m = String(md || "").match(rx);
  return m ? m[1].trim() : "";
}

function QUAL_takeTopThree(text) {
  const t = String(text || "").replace(/\r/g, "").trim();
  if (!t) return [];
  // Prefer existing bullets; else split into sentences.
  const bullets = t.split(/\n+/).map(s => s.replace(/^\s*[-*•]\s*/, "").trim()).filter(Boolean);
  const lines = bullets.length >= 3 ? bullets : t.split(/(?<=[.!?])\s+(?=[A-Z(])/).map(s => s.trim()).filter(Boolean);
  return lines.slice(0, 3);
}

/**
 * Map report sections → SWOT buckets:
 *  S: Relationship value
 *  W: What we could not evidence (and why)
 *  O: Pain points
 *  T: Competition & differentiation
 */
function QUAL_buildSwotFromMd(md) {
  return {
    S: QUAL_takeTopThree(QUAL_mdSection(md, "Relationship value")),
    W: QUAL_takeTopThree(QUAL_mdSection(md, "What we could not evidence (and why)")),
    O: QUAL_takeTopThree(QUAL_mdSection(md, "Pain points")),
    T: QUAL_takeTopThree(QUAL_mdSection(md, "Competition & differentiation")),
  };
}

function QUAL_renderSwot(swot) {
  const sEl = document.getElementById("swot-s");
  const wEl = document.getElementById("swot-w");
  const oEl = document.getElementById("swot-o");
  const tEl = document.getElementById("swot-t");

  function fill(listEl, arr) {
    if (!listEl) return;
    listEl.innerHTML = "";
    (arr || []).forEach(x => {
      const li = document.createElement("li");
      li.textContent = x;
      listEl.appendChild(li);
    });
  }

  fill(sEl, swot.S || []);
  fill(wEl, swot.W || []);
  fill(oEl, swot.O || []);
  fill(tEl, swot.T || []);
}

// Click a SWOT card → append its text to the Qualification notes, with a header.
function QUAL_wireSwotClicks() {
  const notes = document.getElementById("notes");
  const panel = document.getElementById("swot-panel");
  if (!panel || !notes) return;

  panel.addEventListener("click", (e) => {
    const card = e.target.closest && e.target.closest(".swot-card");
    if (!card) return;
    const kind = card.getAttribute("data-kind") || "";
    const title = kind === "S" ? "Strengths"
      : kind === "W" ? "Weaknesses"
        : kind === "O" ? "Opportunities"
          : kind === "T" ? "Threats" : "Notes";

    const list = card.querySelectorAll("ol > li");
    const lines = Array.from(list).map(li => "- " + (li.textContent || "").trim()).filter(Boolean);

    if (!lines.length) return;
    const block = `\n${title}:\n${lines.join("\n")}\n`;
    const before = notes.value || "";
    notes.value = before ? (before.replace(/\s+$/, "") + block) : block.trimStart();
    notes.dispatchEvent(new Event("input", { bubbles: true }));
    setStatus(`${title} added to notes.`);
  });

  // keyboard activation on cards
  panel.addEventListener("keydown", (e) => {
    if ((e.key === "Enter" || e.key === " ") && e.target?.classList?.contains("swot-card")) {
      e.preventDefault();
      e.target.click();
    }
  });
}

// Show/Hide like Helpful tips
document.getElementById("toggle-swot")?.addEventListener("click", () => {
  const btn = document.getElementById("toggle-swot");
  const body = document.getElementById("swot-body");
  const expanded = btn.getAttribute("aria-expanded") === "true";
  btn.setAttribute("aria-expanded", String(!expanded));
  body.hidden = expanded;
  btn.textContent = expanded ? "Show" : "Hide";
});

// Source rendering
function renderSources(citations) {
  sourceList.innerHTML = "";
  const items = Array.isArray(citations) ? citations : [];
  if (!items.length) { sourcesCard.style.display = "none"; return; }
  for (let i = 0; i < items.length; i++) {
    const c = items[i] || {};
    const li = document.createElement("li");
    const label = (c.label || c.title || c.url || "Source");
    const url = (c.url || "");
    if (url) {
      const a = document.createElement("a");
      a.href = url;
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      a.textContent = label;
      li.appendChild(a);
    } else {
      li.textContent = label;
    }
    sourceList.appendChild(li);
  }
  sourcesCard.style.display = "";
}

// Buttons state
function setBusy(isBusy) {
  const submitBtn = document.getElementById("submit");
  if (submitBtn) {
    submitBtn.disabled = isBusy || !allRequiredFilled();
    submitBtn.classList.toggle('busy', isBusy);
  }
  document.getElementById("resetBtn")?.setAttribute("aria-disabled", isBusy ? "true" : "false");

  const hasText = !!(outputEl?.textContent?.trim());
  copyBtn && (copyBtn.disabled = isBusy || !hasText);
  dlBtn && (dlBtn.disabled = isBusy || !hasText);
  emailBtn && (emailBtn.disabled = isBusy || !hasText);
  docxBtn && (docxBtn.disabled = isBusy || !hasText);
  prioBtn && (prioBtn.disabled = isBusy || !hasText);
}

// Change log
function computeDelta(oldText, newText) {
  const oldLines = (oldText || "").split("\n"), newLines = (newText || "").split("\n");
  let added = 0, removed = 0;
  const oldSet = new Set(oldLines), newSet = new Set(newLines);
  newLines.forEach(l => { if (!oldSet.has(l) && l.trim()) added++; });
  oldLines.forEach(l => { if (!newSet.has(l) && l.trim()) removed++; });
  return { added, removed };
}
function renderDelta(delta) {
  deltaLog.innerHTML = "";
  const a = document.createElement("li");
  a.textContent = "+" + delta.added + " new line" + (delta.added === 1 ? "" : "s");
  const r = document.createElement("li");
  r.textContent = "−" + delta.removed + " removed line" + (delta.removed === 1 ? "" : "s");
  deltaLog.appendChild(a);
  deltaLog.appendChild(r);
}

function addActivity(entry) {
  const list = JSON.parse(localStorage.getItem(ACTIVITY_KEY) || "[]");
  list.unshift(entry);
  localStorage.setItem(ACTIVITY_KEY, JSON.stringify(list.slice(0, 3)));
  renderActivity();
}
function renderActivity() {
  const list = JSON.parse(localStorage.getItem(ACTIVITY_KEY) || "[]");
  activityLog.innerHTML = "";
  list.forEach(item => {
    const li = document.createElement("li");
    const timeSpan = document.createElement("span");
    timeSpan.className = "time";
    timeSpan.textContent = item.time || "";
    li.appendChild(timeSpan);
    li.appendChild(document.createTextNode(" "));
    const pill1 = document.createElement("span");
    pill1.className = "pill";
    pill1.textContent = item.product || "";
    li.appendChild(pill1);
    li.appendChild(document.createTextNode(" "));
    const pill2 = document.createElement("span");
    pill2.className = "pill";
    pill2.textContent = item.buyer || "";
    li.appendChild(pill2);
    activityLog.appendChild(li);
  });
}

// Copy / Download / .docx / Email
copyBtn?.addEventListener("click", async () => {
  const txt = outputEl?.innerText || "";
  if (!txt.trim()) return;
  try { await navigator.clipboard.writeText(txt); setStatus("Copied."); }
  catch { setStatus("Could not copy.", "error"); }
});
dlBtn?.addEventListener("click", () => {
  const txt = outputEl?.textContent || "";
  const who = (form?.elements?.prospect_company?.value || "report").trim().replace(/\s+/g, "-");
  const blob = new Blob([txt], { type: "text/plain" });
  const URL_ = window.URL || window.webkitURL;
  const url = URL_.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `lead-qualification_${who}_${new Date().toISOString().slice(0, 10)}.txt`;
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL_.revokeObjectURL(url);
});
docxBtn?.addEventListener("click", () => {
  try {
    setStatus("Preparing .docx…");
    const payload = {
      kind: "qualification-docx",
      basePrefix,
      variables: collectVariables(),
      html: outputEl?.innerHTML || "",
      citations: safeJson(outputEl?.getAttribute("data-citations")) || []
    };

    fetch("/api/qualification-generate", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders() },
      body: JSON.stringify(payload)
    })
      .then(r => r.ok ? r.blob() : r.json().then(d => { throw new Error(d?.error || ("HTTP " + r.status)); }))
      .then(blob => {
        const URL_ = window.URL || window.webkitURL;
        if (blob && blob.size) {
          const url = URL_.createObjectURL(blob);
          const who = (form?.elements?.prospect_company?.value || "report").replace(/\s+/g, "-");
          const a = document.createElement("a");
          a.href = url; a.download = `lead-qualification_${who}.docx`;
          document.body.appendChild(a); a.click(); document.body.removeChild(a);
          URL_.revokeObjectURL(url);
          setStatus("Exported .docx");
        } else {
          throw new Error("empty-blob");
        }
      })
      .catch(() => {
        const URL_ = window.URL || window.webkitURL;
        const htmlBlob = new Blob([outputEl?.innerHTML || ""], { type: "text/html" });
        const url = URL_.createObjectURL(htmlBlob);
        const a = document.createElement("a");
        a.href = url; a.download = "lead-qualification.html";
        document.body.appendChild(a); a.click(); document.body.removeChild(a);
        URL_.revokeObjectURL(url);
        setStatus("Exported HTML (server .docx not available).", "info");
      });
  } catch { setStatus("Could not export .docx", "error"); }
});
emailBtn?.addEventListener("click", () => {
  try {
    const payload = {
      kind: "qualification-email",
      basePrefix,
      variables: {
        seller_name: (form.elements.seller_name?.value || "").trim(),
        seller_company: (form.elements.seller_company?.value || "").trim(),
        prospect_name: (form.elements.prospect_name?.value || "").trim(),
        prospect_role: (form.elements.prospect_role?.value || "").trim(),
        prospect_company: (form.elements.prospect_company?.value || "").trim(),
        buyer_type: (form.elements.buyer_type?.value || "").trim()
      },
      reportMdText: localStorage.getItem(OUTPUT_KEY) || "",
      notes: (notesArea?.value || "")
    };
    setStatus("Building email…");
    fetch("/api/qualification-generate", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders() },
      body: JSON.stringify(payload)
    })
      .then(r => r.json().then(data => { if (!r.ok) throw new Error(data?.error || "Could not build email."); return data; }))
      .then(data => {
        const email = data?.email?.text || data?.followup?.email || "";
        if (email) {
          const to = "";
          const subj = data?.email?.subject
            || `Summary of opportunity with ${(form.elements.prospect_company?.value || "Lead")} for sales management`;
          const href = "mailto:" + encodeURIComponent(to) + "?subject=" + encodeURIComponent(subj) + "&body=" + encodeURIComponent(email);
          location.href = href;
          setStatus("Email composed.");
        } else {
          setStatus("No email content produced.", "error");
        }
      })
      .catch(e => { setStatus("Could not compose email", "error"); diag.open = true; diagJson.textContent = String(e?.message || e); });
  } catch (e) { setStatus("Could not compose email", "error"); diag.open = true; diagJson.textContent = String(e?.message || e); }
});

prioBtn?.addEventListener("click", () => {
  const scriptText = (outputEl?.innerText || "").trim() || (localStorage.getItem(OUTPUT_KEY) || "");
  if (!scriptText) { setStatus("No report to score.", "error"); return; }

  const inputs = collectVariables();                // reuse your existing function
  const scores = scoreOpportunityFromText(scriptText);
  LAST_PRIO_PAYLOAD = buildPrioritisationPayload({ scores, inputs, scriptText });

  if (prioBody) prioBody.innerHTML = renderPrioritisationPreviewHTML(scores);
  prioModal?.showModal();
});

prioClose?.addEventListener("click", () => prioModal?.close());

prioCopy?.addEventListener("click", async () => {
  try {
    await navigator.clipboard.writeText(JSON.stringify(LAST_PRIO_PAYLOAD || {}, null, 2));
    setStatus("Prioritisation JSON copied.");
  } catch {
    setStatus("Could not copy prioritisation JSON.", "error");
  }
});

// CH modal
chHelp?.addEventListener("click", () => chModal?.showModal());
chClose?.addEventListener("click", () => chModal?.close());

// Help modal
helpBtn?.addEventListener("click", () => helpModal?.showModal());
helpClose?.addEventListener("click", () => helpModal?.close());

// Toggle tips panel
document.getElementById("toggle-intel")?.addEventListener("click", () => {
  const btn = document.getElementById("toggle-intel");
  const body = document.getElementById("intel-body");
  const expanded = btn.getAttribute("aria-expanded") === "true";
  btn.setAttribute("aria-expanded", String(!expanded));
  body.hidden = expanded;
  btn.textContent = expanded ? "Show" : "Hide";
});

// Fetch iXBRL click
ixbrlBtn?.addEventListener("click", async () => {
  const num = (form.elements.company_number?.value || "").trim();
  if (!num) { setStatus("Enter a Companies House number or upload PDFs.", "error"); return; }
  setBusy(true);
  const res = await fetchIxbrl(num);
  setBusy(false);
  if (res.ok) {
    const summary = res.data?.summary || res.data || {};
    renderIxbrlChips({ summary });
    form.dataset.ixbrl = JSON.stringify(summary);
  } else if (res.status === 204) {
    renderIxbrlChips(null);
    form.dataset.ixbrl = JSON.stringify({});
    setStatus("iXBRL not available; you can upload PDFs or open the panel to enter metrics manually.", "info");
  }
});

// Fetch filings (OCR) click
chdiBtn?.addEventListener("click", async (ev) => {
  ev.preventDefault();

  // 1) Normalise input and guard
  const numRaw = (form?.elements?.company_number?.value || "").trim();
  const num = numRaw.replace(/\s+/g, ""); // remove spaces in CH numbers
  if (!num) {
    setStatus("Enter a Companies House number or upload PDFs.", "error");
    return;
  }

  // 2) Reset previous state
  form.dataset.pdfTexts = "[]";
  const list = document.getElementById("file_list");
  if (list) list.innerHTML = "";
  if (chdiStatus) chdiStatus.textContent = "Fetching latest filings…";

  // 3) Debounce/disable while busy
  chdiBtn.disabled = true;
  try {
    if (typeof setBusy === "function") setBusy(true);

    const res = await fetchChdi(num); // expects { ok, status, data }

    if (res.ok) {
      const pdfs = Array.isArray(res.data?.pdfs) ? res.data.pdfs.slice(0, 2) : [];
      form.dataset.pdfTexts = JSON.stringify(pdfs);

      // Render filenames into the file list (so user sees what's attached)
      if (list) {
        for (const p of pdfs) {
          const li = document.createElement("li");
          li.textContent = p?.filename || "accounts.pdf";
          list.appendChild(li);
        }
      }

      if (chdiStatus) chdiStatus.textContent = pdfs.length
        ? `Loaded ${pdfs.length} filing(s).`
        : "No usable text.";
      if (!pdfs.length) setStatus("No usable text in latest filings. You can still upload PDFs.", "info");

    } else if (res.status === 204) {
      // No recent accounts found
      if (chdiStatus) chdiStatus.textContent = "No filings found.";
      setStatus("No recent Accounts filings via Companies House; you can still upload PDFs.", "info");

    } else {
      if (chdiStatus) chdiStatus.textContent = "Could not fetch filings.";
      setStatus("Could not fetch filings from Companies House.", "error");
    }
  } catch (e) {
    if (chdiStatus) chdiStatus.textContent = "Could not fetch filings.";
    setStatus("Could not fetch filings from Companies House.", "error");
  } finally {
    chdiBtn.disabled = false;
    if (typeof setBusy === "function") setBusy(false);
  }
});

function safeJson(raw) { if (!raw) return null; try { return JSON.parse(raw); } catch { return null; } }
function num(x) { const n = Number(String(x || "").replace(/[,£\s]/g, "")); return Number.isFinite(n) ? n : null; }
function normUrl(u) {
  u = String(u || "").trim();
  return u && !/^https?:\/\//i.test(u) ? "https://" + u : u;
}
["seller_company_url", "prospect_website"].forEach(id => {
  const el = document.getElementById(id);
  el?.addEventListener("blur", () => {
    el.value = normUrl(el.value);
    saveForm();
    validateField(id);
  });
});

function collectVariables() {
  return {
    // --- top-level selectors / controls ---
    call_type: (callTypeSel?.value || "").trim(),
    detail: (form.elements.detail?.value || "full").trim(),

    // --- you / your company ---
    seller_name: (form.elements.seller_name?.value || "").trim(),
    seller_company: (form.elements.seller_company?.value || "").trim(),
    seller_company_url: normUrl(form.elements.seller_company_url?.value),

    // --- prospect person & company ---
    prospect_name: (form.elements.prospect_name?.value || "").trim(),
    prospect_role: (form.elements.prospect_role?.value || "").trim(),
    prospect_company: (form.elements.prospect_company?.value || "").trim(),
    prospect_website: normUrl(form.elements.prospect_website?.value),
    company_number: (form.elements.company_number?.value || "").trim(),

    // --- offer / context ---
    product_service: (form.elements.product_service?.value || "").trim(),
    competitors: (form.elements.competitors?.value || "").trim(),
    existing_provider: (form.elements.existing_provider?.value || "").trim(),
    context: (form.elements.context?.value || "").trim(),

    // --- links ---
    company_linkedin: (form.elements.company_linkedin?.value || "").trim(),
    contact_linkedins: (form.elements.contact_linkedins?.value || "").trim(),
    events: (form.elements.events?.value || "").trim(),

    // --- buyer ---
    buyer_type: (form.elements.buyer_type?.value || "").trim(),

    // --- manual metrics (unchanged) ---
    manual: {
      y1: {
        endDate: (document.getElementById("m_end_y1")?.value || "").trim(),
        turnover: num(document.getElementById("m_turnover_y1")?.value),
        grossProfit: num(document.getElementById("m_gp_y1")?.value),
        operatingProfit: num(document.getElementById("m_op_y1")?.value),
        currentAssets: num(document.getElementById("m_ca_y1")?.value),
        currentLiabilities: num(document.getElementById("m_cl_y1")?.value)
      },
      y2: {
        endDate: (document.getElementById("m_end_y2")?.value || "").trim(),
        turnover: num(document.getElementById("m_turnover_y2")?.value),
        grossProfit: num(document.getElementById("m_gp_y2")?.value),
        operatingProfit: num(document.getElementById("m_op_y2")?.value),
        currentAssets: num(document.getElementById("m_ca_y2")?.value),
        currentLiabilities: num(document.getElementById("m_cl_y2")?.value)
      }
    }
  };
}

// Sales model mirror + remember
callTypeSel?.addEventListener("change", () => {
  if (callTypeSelXs) callTypeSelXs.value = callTypeSel.value;
  validateField("call_type"); saveForm(); updateMapIndicator();
  if (rememberCallType) saveCallPrefIfRequested();
  enableSubmitIfValid();
});
callTypeSelXs?.addEventListener("change", () => {
  if (callTypeSel) callTypeSel.value = callTypeSelXs.value;
  validateField("call_type"); saveForm(); updateMapIndicator();
  if (rememberCallType) saveCallPrefIfRequested();
  enableSubmitIfValid();
});

// Notes
saveNotesBtn?.addEventListener("click", async () => {
  const txt = notesArea?.value || "";
  if (!txt.trim()) return;
  try {
    await navigator.clipboard.writeText(txt);
    setStatus("Notes copied.");
  } catch {
    setStatus("Could not copy notes.", "error");
  }
});

downloadNotesBtn?.addEventListener("click", () => {
  const who = (form?.elements?.prospect_company?.value || "notes").trim().replace(/\s+/g, "-");
  const txt = notesArea?.value || "";
  const blob = new Blob([txt], { type: "text/plain" });
  const URL_ = window.URL || window.webkitURL;
  const url = URL_.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = `qualification-notes_${who}_${new Date().toISOString().slice(0, 10)}.txt`;
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  URL_.revokeObjectURL(url);
});

// Enable submit
function enableSubmitIfValid() {
  const submitBtn = document.getElementById("submit");
  if (submitBtn) submitBtn.disabled = !allRequiredFilled();
}

// === Delegated CLICK handling (keeps behaviour in one place) =============

// Minimal “MD → safe HTML” for headings, bullets, and paragraphs.
function renderSimpleMarkdown(md) {
  return md
    .replace(/^### (.*)$/gm, '<h3>$1</h3>')
    .replace(/^## (.*)$/gm, '<h2>$1</h2>')
    .replace(/^---$/gm, '<hr>')
    .split(/\n{2,}/).map(blk => {
      if (/^<h[23]>/.test(blk) || /^<hr>/.test(blk)) return blk;
      if (/^\* /.test(blk)) {
        return '<ul>' + blk.split('\n')
          .map(li => '<li>' + li.replace(/^\* /, '') + '</li>').join('') + '</ul>';
      }
      return '<p>' + blk + '</p>';
    }).join('\n');
}

let quickChecklistHTML = '';  // cache after first load

async function openQuickChecklistModal() {
  const modal = document.getElementById('quickcheck-modal');
  const body = document.getElementById('quickcheck-body');
  const headerBtn = document.getElementById('quickcheck-top'); // header chip (may exist)
  if (!modal || !body) return;

  if (!quickChecklistHTML) {
    body.innerHTML = '<p class="muted">Loading…</p>';
    try {
      const res = await fetch('/content/call-library/quick-checklist-90s.md', { cache: 'no-store' });
      const md = await (res.ok ? res.text() : Promise.reject(res.status));
      quickChecklistHTML = renderSimpleMarkdown(md);   // make sure this helper exists
    } catch {
      quickChecklistHTML = '<p class="muted">Sorry — the checklist couldn’t be loaded right now.</p>';
    }
  }
  body.innerHTML = quickChecklistHTML;

  if (!modal.dataset.bound) {
    modal.addEventListener('cancel', (e) => { e.preventDefault(); closeQuickChecklistModal(); });
    modal.dataset.bound = '1';
  }

  modal.showModal();
  if (headerBtn) headerBtn.setAttribute('aria-expanded', 'true');
}

function closeQuickChecklistModal() {
  const modal = document.getElementById('quickcheck-modal');
  const headerBtn = document.getElementById('quickcheck-top');
  if (!modal) return;
  modal.close();
  if (headerBtn) {
    headerBtn.setAttribute('aria-expanded', 'false');
    headerBtn.focus({ preventScroll: true });
  }
}

document.addEventListener('click', (e) => {
  const t = e.target;

  // Upload reports (PDF) -> trigger hidden input
  if (t.closest && t.closest('#upload-reports')) {
    const input = document.getElementById('report_files');
    if (input) input.click();
    return;
  }

  // Header Quick checklist button -> open modal
  if (t.closest && t.closest('#quickcheck-top')) {
    openQuickChecklistModal();
    return;
  }

  // Modal close button
  if (t.closest && t.closest('#quickcheck-close')) {
    closeQuickChecklistModal();
    return;
  }
});

// Submit: build payload and call /api/qualification-generate
form?.addEventListener("submit", (e) => {
  e.preventDefault();
  if (!allRequiredFilled()) return;

  const values = collectVariables();
  const ixbrlSummary = safeJson(form.dataset.ixbrl);
  const files = Array.from(reportInput?.files || []).slice(0, 2); // optional

  const policy = { evidenceOnly: true, dropUnsupportedClaims: true };

  setBusy(true);
  setStatus("Working hard to qualify your opportunity. Back in a couple of minutes…");
  if (diag) diag.open = false;
  if (diagJson) diagJson.textContent = "";

  function two(n) { return (n < 10 ? "0" : "") + n; }
  function handleData(data) {
    const md =
      (data?.report && (data.report.md || data.report.text)) ||
      data?.text || data?.markdown || "";
    const citations =
      (data?.report && data.report.citations) ||
      data?.citations || data?.sources || [];
    const tipsFromModel = Array.isArray(data?.tips) ? data.tips : [];

    if (!String(md).trim()) throw new Error("Empty report returned from API.");

    const html = mdToHtml(md);
    outputEl && (outputEl.innerHTML = html);
    outputEl && outputEl.setAttribute("data-citations", JSON.stringify(citations || []));

    outputMeta && (outputMeta.textContent =
      `${values.prospect_company || "—"} · ${values.product_service || "—"} · ${values.call_type || "—"}`);

    renderSources(citations);

    // Tips: prefer model → else bank; then merge with AUTO_TIPS
    const buyerType = (form.elements.buyer_type?.value || "").trim();
    const mode = (form.elements.call_type?.value || "").trim();
    BASE_TIPS = tipsFromModel.length ? tipsFromModel : pickBankTips(buyerType, mode);
    renderTips();

    // Enable actions
    copyBtn && (copyBtn.disabled = false);
    dlBtn && (dlBtn.disabled = false);
    emailBtn && (emailBtn.disabled = false);
    docxBtn && (docxBtn.disabled = false);
    prioBtn && (prioBtn.disabled = false);

    // Delta & activity
    const old = localStorage.getItem(OUTPUT_KEY) || "";
    const delta = computeDelta(old, md);
    renderDelta(delta);
    localStorage.setItem(OUTPUT_KEY, md);

    const now = new Date();
    const time = two(now.getHours()) + ":" + two(now.getMinutes());
    addActivity({ time, product: values.product_service || "—", buyer: values.buyer_type || "—" });
    // --- Build and render SWOT from the generated report markdown ---
    try {
      const swot = QUAL_buildSwotFromMd(md);
      QUAL_renderSwot(swot);
      QUAL_wireSwotClicks();
    } catch { /* non-fatal */ }
    setStatus("Done. (Generated from API)");
    document.getElementById("output-title")?.focus();
  }

  function handleError(err) {
    const msg = String(err?.message || err);
    if (/401/.test(msg)) {
      setStatus("Unauthorized (401). Your client didn’t send the QUAL_API_KEY Bearer token.", "error");
    } else {
      setStatus("Could not generate the report. See Diagnostics for details.", "error");
    }
    if (diag && diagJson) { diag.open = true; diagJson.textContent = msg; }
  }

  if (files.length) {
    const fd = new FormData();
    fd.append("kind", "lead-qualification");
    fd.append("basePrefix", basePrefix);
    fd.append("variables", JSON.stringify(values));
    fd.append("ixbrlSummary", JSON.stringify(ixbrlSummary || {}));
    fd.append("policy", JSON.stringify(policy));
    files.forEach((f, i) => fd.append("files", f, f?.name || `report-${i + 1}.pdf`));

    fetch("/api/qualification-generate", { method: "POST", body: fd })
      .then(resp => resp.json().catch(() => ({})).then(data => { if (!resp.ok) throw new Error(data?.error || ("API " + resp.status)); return data; }))
      .then(handleData)
      .catch(handleError)
      .finally(() => setBusy(false));
  } else {
    const body = {
      kind: "lead-qualification",
      basePrefix,
      variables: values,
      ixbrlSummary: ixbrlSummary || {},
      policy
    };
    const injected = safeJson(form.dataset.pdfTexts || "[]") || [];
    body.pdfTexts = injected;

    fetch("/api/qualification-generate", {
      method: "POST",
      credentials: 'include',
      headers: { "Content-Type": "application/json", ...authHeaders() },
      body: JSON.stringify(body)
    })
      .then(resp => resp.json().catch(() => ({})).then(data => { if (!resp.ok) throw new Error(data?.error || ("API " + resp.status)); return data; }))
      .then(handleData)
      .catch(handleError)
      .finally(() => setBusy(false));
  }
});

// Reset handler
form?.addEventListener("reset", () => {
  queueMicrotask(() => {
    try {
      localStorage.removeItem(FORM_KEY);
      localStorage.removeItem(ACTIVITY_KEY);
      localStorage.removeItem(OUTPUT_KEY);
      localStorage.removeItem(notesKey());
    } catch { }
    delete form.dataset.pdfTexts;
    outputEl && (outputEl.innerHTML = "");
    sourceList && (sourceList.innerHTML = "");
    sourcesCard && (sourcesCard.style.display = "none");
    flagsList && (flagsList.innerHTML = "");
    tipsList && (tipsList.innerHTML = "");
    tipsEmpty && (tipsEmpty.style.display = "");
    ixbrlChips && (ixbrlChips.innerHTML = "");
    statusEl && (statusEl.textContent = "");
    copyBtn && (copyBtn.disabled = true);
    dlBtn && (dlBtn.disabled = true);
    emailBtn && (emailBtn.disabled = true);
    docxBtn && (docxBtn.disabled = true);
    prioBtn && (prioBtn.disabled = true);
    document.getElementById("swot-s")?.replaceChildren();
    document.getElementById("swot-w")?.replaceChildren();
    document.getElementById("swot-o")?.replaceChildren();
    document.getElementById("swot-t")?.replaceChildren();
    try { prioModal?.close(); } catch { }
    LAST_PRIO_PAYLOAD = null;
    document.querySelector(".panel.left")?.scrollTo({ top: 0, behavior: "auto" });
    document.getElementById("seller_name")?.focus();
    BASE_TIPS = []; AUTO_TIPS = []; renderTips();
  });
});

// ==== Init ====
// lead-qualification init
(async function init() {
  // 1) Populate user chip if signed in
  try {
    const me = await fetch("/.auth/me", { cache: "no-store" });
    const j = await me.json();
    const email = j?.clientPrincipal?.userDetails || j?.clientPrincipal?.identityProvider || "";
    const userBadge = document.getElementById("user-badge");
    const userEmail = document.getElementById("user-email");
    if (email && userBadge && userEmail) {
      userEmail.textContent = email;
      userBadge.classList.remove("is-hidden");
    }
  } catch {/* ignore auth probe errors */ }

  // 2) Cache DOM
  const form = document.getElementById("qual-form");
  const submitBtn = document.getElementById("submit");
  const callTypeSel = document.getElementById("call_type");       // desktop selector
  const callTypeSelXs = document.getElementById("call_type_xs");   // mobile mirror

  // Guard: if the form or button isn’t present, stop cleanly
  if (!form || !submitBtn) return;

  // 3) Busy helper toggles CSS spinner visibility (.btn.primary.busy .spinner)
  function setBusy(on) {
    submitBtn.disabled = on;
    submitBtn.classList.toggle("busy", on);
  }
  let inFlight = false;
  // 4) Wire submit
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    setBusy(true);
    try {
    } finally {
      setBusy(false);
      inFlight = false;
    }
  });

  // 4) Highlighter buttons (analysis panel)
  const hiBtn = document.getElementById("toggle-highlighter");
  const clrBtn = document.getElementById("clear-highlights");
  if (hiBtn && outputEl) {
    hiBtn.addEventListener("click", () => {
      QUAL_highlightOn = !QUAL_highlightOn;
      hiBtn.setAttribute("aria-pressed", String(QUAL_highlightOn));
    });
    outputEl.addEventListener("mouseup", () => {
      if (QUAL_highlightOn) QUAL_wrapSelectionInMark(outputEl);
    });
  }
  if (clrBtn && outputEl) {
    clrBtn.addEventListener("click", () => QUAL_clearHighlights(outputEl));
  }

  // 5) One source of truth for enabling submit
  function syncSubmitEnabled() {
    submitBtn.disabled = !form.checkValidity();
  }
  form.addEventListener("input", syncSubmitEnabled);
  syncSubmitEnabled();
  if (callTypeSel && callTypeSelXs) {
    // initial sync to keep them matching on load
    callTypeSelXs.value = callTypeSel.value || "";

    const syncXS = () => { callTypeSelXs.value = callTypeSel.value; };
    const syncDesk = () => { callTypeSel.value = callTypeSelXs.value; };

    callTypeSel.addEventListener("change", syncXS);
    callTypeSelXs.addEventListener("change", syncDesk);
  }


  // 6) Your existing bootstraps (safe-guarded)
  try { loadForm && loadForm(); } catch { }
  try { loadCallPref && loadCallPref(); } catch { }

  try { updateMapIndicator && updateMapIndicator(); } catch { }

  try { await (loadTipsBank && loadTipsBank()); } catch { }

  const chHintEl = document.getElementById("company_number_hint");
  const chVal = (form.elements?.company_number?.value || "").trim();
  if (!chVal && chHintEl) chHintEl.style.display = "";

  // If you keep these helpers, they run; otherwise the try/catch avoids errors.
  try { enableSubmitIfValid && enableSubmitIfValid(); } catch { }
  try { renderActivity && renderActivity(); } catch { }
  try { renderTips && renderTips(); } catch { }
  // Theme toggle (top bar) — uses data-theme + localStorage("it-theme")
  const themeBtn = document.getElementById("themeToggle");
  function applyTheme(t) {
    const theme = (t === "dark") ? "dark" : "light";
    document.documentElement.setAttribute("data-theme", theme);
    try { localStorage.setItem("it-theme", theme); } catch { }
    syncThemeBtn();
  }
  function syncThemeBtn() {
    if (!themeBtn) return;
    const current = document.documentElement.getAttribute("data-theme") || "light";
    const isDark = current === "dark";
    themeBtn.textContent = isDark ? "Light mode" : "Night mode";
    themeBtn.setAttribute("aria-pressed", String(isDark));
  }
  if (themeBtn) {
    themeBtn.addEventListener("click", () => {
      const cur = document.documentElement.getAttribute("data-theme") || "light";
      applyTheme(cur === "dark" ? "light" : "dark");
    });
    // Initial label/state
    syncThemeBtn();
  }
  // Keep label in sync if theme changes elsewhere
  window.addEventListener("storage", (e) => {
    if (e.key === "it-theme" || e.key === "theme") {
      const v = (e.newValue || "").toLowerCase();
      if (v === "dark" || v === "light") {
        document.documentElement.setAttribute("data-theme", v);
        syncThemeBtn();
      }
    }
  });
})();
