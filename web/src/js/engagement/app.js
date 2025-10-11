// /web/src/js/engagement/app.js ---- 11-10-2025 v5.6 -----//
// Works with: web/engagement/index.html (v3) + styles.css
// Endpoint: /api/engagement-generate  ✅

// Imports
import { getIndex, loadTemplate, canonicalBuyerId } from "/src/lib/contentLoader.js";
import { renderScriptFromJson, buildPreface } from "./render.js";

// ---------- tiny utils ----------
const MODE_KEY = "engagement.mode";
const DIAG = {
  set(obj) {
    const el = document.getElementById("diag-json");
    if (!el) return;
    try { el.textContent = JSON.stringify(obj, null, 2); } catch { el.textContent = String(obj); }
  }
};
const norm = (s) => String(s || "").toLowerCase().trim();

// Canonicalise "Direct"/"Partner" (any case) -> "direct"|"partner"
function canonicalMode(v) {
  return norm(v) === "partner" ? "partner" : "direct";
}

// Extract number from "~450 words" etc.
function parseLength(val) {
  const m = String(val || "").match(/\d+/);
  return m ? parseInt(m[0], 10) : 450;
}

// Make “Professional (corporate)” → “Professional”, “Warm (professional)” → “Warm”, etc.
function canonicalTone(val) {
  const s = norm(val);
  if (s.startsWith("warm")) return "Warm";
  if (s.startsWith("straight")) return "Straightforward";
  return "Professional";
}

// Split textarea lines/CSV/semicolon into array
function splitPoints(s) {
  return String(s || "")
    .split(/\r?\n|[,;]+/)
    .map(t => t.trim())
    .filter(Boolean);
}

// ---------- DOM cache (IDs exactly as per index.html) ----------
const els = {
  // header
  headerCallType: document.getElementById("call_type"),
  headerRemember: document.getElementById("remember_call_type"),
  headerChipText: document.getElementById("map-label"),
  userBadge: document.getElementById("user-badge"),
  userEmail: document.getElementById("user-email"),

  // form + fields
  form: document.getElementById("script-form"),
  submit: document.getElementById("submit"),
  product: document.getElementById("product"),
  buyer: document.getElementById("buyer_behaviour"),
  tone: document.getElementById("tone"),
  length: document.getElementById("script_length"),

  seller_name: document.getElementById("seller_name"),
  seller_company: document.getElementById("seller_company"),
  prospect_name: document.getElementById("prospect_name"),
  prospect_role: document.getElementById("prospect_role"),
  prospect_company: document.getElementById("prospect_company"),

  usps: document.getElementById("value_proposition"),
  other: document.getElementById("context"),
  next_step: document.getElementById("next_step"),

  // mobile fallback selector (optional)
  call_type_xs: document.getElementById("call_type_xs"),

  // output & actions
  status: document.getElementById("status"),
  output: document.getElementById("output"),
  btnCopy: document.getElementById("copy-script"),
  btnDownload: document.getElementById("download-script"),
  btnPopout: document.getElementById("popout"),

  // diagnostics
  diag: document.getElementById("diagnostics"),
  diagJson: document.getElementById("diag-json"),

  // helpers / intel (present in layout; content is optional)
  intelBody: document.getElementById("intel-body"),
};

// ---------- Status helper ----------
function setStatus(msg) {
  if (els.status) els.status.textContent = msg || "";
}

// ---------- User badge ----------
async function loadUserBadge() {
  if (!els.userBadge && !els.userEmail) return;
  try {
    const r = await fetch("/.auth/me", { cache: "no-store" });
    const j = await r.json().catch(() => ({}));
    const cp = j?.clientPrincipal;
    const label = cp?.userDetails || cp?.identityProvider || "Guest";
    if (els.userEmail) els.userEmail.textContent = label;
    if (els.userBadge) els.userBadge.classList.remove("is-hidden");
  } catch {
    if (els.userEmail) els.userEmail.textContent = "Guest";
    if (els.userBadge) els.userBadge.classList.remove("is-hidden");
  }
}

// ---------- Mode (Sales model) state & sync ----------
function getSavedMode() {
  return localStorage.getItem(MODE_KEY);
}

function getHeaderMode() {
  const raw = els.headerCallType?.value || "";
  // header options are capitalised text (no value attr)
  return canonicalMode(raw);
}

function getFormModeXS() {
  const raw = els.call_type_xs?.value || "";
  return canonicalMode(raw);
}

function reflectModeToHeader(mode) {
  const label = mode === "partner" ? "Partner" : "Direct";
  if (els.headerChipText) els.headerChipText.textContent = label;
  if (els.headerCallType && els.headerCallType.value !== label) {
    // match the visible text option
    const want = label.toLowerCase();
    for (const opt of els.headerCallType.options) {
      if (norm(opt.textContent) === want) { els.headerCallType.value = opt.textContent; break; }
    }
  }
  if (els.call_type_xs && els.call_type_xs.value !== label) {
    const want = label.toLowerCase();
    for (const opt of els.call_type_xs.options) {
      if (norm(opt.textContent) === want) { els.call_type_xs.value = opt.textContent; break; }
    }
  }
}

function persistModeIfAllowed(mode) {
  // Persist only if Remember in header is checked (or header remember not present)
  if (!els.headerRemember || els.headerRemember.checked) {
    localStorage.setItem(MODE_KEY, mode);
  }
}

function initialMode() {
  // Priority: saved ⇒ header select (if user pre-changed before JS) ⇒ XS select ⇒ default "direct"
  return canonicalMode(getSavedMode() || els.headerCallType?.value || els.call_type_xs?.value || "direct");
}

// Single source of truth
let currentMode = initialMode();
let productsLoadSeq = 0; // for race control

async function setMode(mode, { persist = true } = {}) {
  const m = canonicalMode(mode);
  if (m === currentMode) {
    // Still ensure header reflects & products list is consistent
    reflectModeToHeader(m);
  } else {
    currentMode = m;
    reflectModeToHeader(m);
    if (persist) persistModeIfAllowed(m);
  }
  // Repopulate products; only apply latest fetch (race-safe)
  await populateProductsRaceSafe(m);
  updateGenerateState();
}

async function populateProductsRaceSafe(mode) {
  const seq = ++productsLoadSeq;
  try {
    setStatus("Loading products…");
    let products = [];
    try {
      // (5) Robust error handling around getIndex
      const idx = await getIndex(mode);
      products = Array.isArray(idx?.products) ? idx.products : [];
    } catch (e) {
      if (seq !== productsLoadSeq) return; // superseded
      setStatus("Couldn’t load product list.");
      DIAG.set({ kind: "index-error", message: String(e?.message || e), mode });
      if (els.product) {
        els.product.innerHTML = `<option value="">No products found</option>`;
      }
      return;
    }
    if (seq !== productsLoadSeq) return; // superseded

    if (!els.product) { setStatus("No product control found."); return; }
    els.product.innerHTML = "";

    if (!products.length) {
      els.product.innerHTML = `<option value="">No products found</option>`;
      setStatus(`No products listed for “${mode}”`);
      updateGenerateState();
      return;
    }

    // Do NOT auto-select a product; user must choose (per your UX)
    const ph = document.createElement("option");
    ph.value = "";
    ph.textContent = "Select…";
    els.product.appendChild(ph);

    for (const p of products) {
      const opt = document.createElement("option");
      opt.value = p.id;
      opt.textContent = p.label || p.id;
      els.product.appendChild(opt);
    }
    setStatus("");
  } finally {
    if (seq === productsLoadSeq) updateGenerateState();
  }
}

// ---------- Remember (USPs / Other / Next step) with inline buttons ----------
// We inject a small button row under “Next step” using existing CSS token classes.
// (6) Ensure the feature exists even if HTML didn’t include buttons.
const REM_KEYS = { usps: "eng.usps", other: "eng.other", next_step: "eng.next_step" };

function loadRememberedFields() {
  if (els.usps) els.usps.value = localStorage.getItem(REM_KEYS.usps) || els.usps.value || "";
  if (els.other) els.other.value = localStorage.getItem(REM_KEYS.other) || els.other.value || "";
  if (els.next_step) els.next_step.value = localStorage.getItem(REM_KEYS.next_step) || els.next_step.value || "";
}

function injectRememberButtons() {
  if (!els.next_step) return;
  // If already injected, skip
  if (document.getElementById("remember-hint")) return;

  const row = document.createElement("div");
  row.className = "remember-row"; // styled in styles.css (chip-like, subtle)

  const save = document.createElement("button");
  save.type = "button";
  save.className = "btn small";
  save.id = "remember-save";
  save.textContent = "Remember USP/Other/Next";

  const clear = document.createElement("button");
  clear.type = "button";
  clear.className = "btn small";
  clear.id = "remember-clear";
  clear.textContent = "Clear remembered";

  const hint = document.createElement("span");
  hint.id = "remember-hint";
  hint.className = "muted";

  row.appendChild(save);
  row.appendChild(clear);
  row.appendChild(hint);

  // Place directly after next_step input’s field container
  const field = els.next_step.closest(".field") || els.next_step.parentElement;
  field?.appendChild(row);

  // Wire
  save.addEventListener("click", () => {
    if (els.usps) localStorage.setItem(REM_KEYS.usps, els.usps.value ?? "");
    if (els.other) localStorage.setItem(REM_KEYS.other, els.other.value ?? "");
    if (els.next_step) localStorage.setItem(REM_KEYS.next_step, els.next_step.value ?? "");
    hint.textContent = "Defaults saved.";
    setTimeout(() => { hint.textContent = ""; }, 2000);
  });
  clear.addEventListener("click", () => {
    Object.values(REM_KEYS).forEach(k => localStorage.removeItem(k));
    hint.textContent = "Saved defaults cleared.";
    setTimeout(() => { hint.textContent = ""; }, 2000);
  });
}

// ---------- Buyer intel (right rail) ----------
// Reads optional buyer_intel from the library index. No extra HTTPs to /intel/*.json.
async function loadBuyerIntel(buyerId) {
  const b = (buyerId || "").toLowerCase();
  const intelBody = document.getElementById("intel-body");
  if (!intelBody) return;

  try {
    const r = await fetch("/content/call-library/v1/index.json", { cache: "no-store" });
    if (!r.ok) { intelBody.hidden = true; return; }

    const data = await r.json().catch(() => ({}));
    // Expected optional shape:
    // { buyer_intel: { "innovator": { priorities:[], pains:[], triggers:[], value_proof:[], objections:[], ctas:[] }, ... } }
    const pack = data?.buyer_intel?.[b];
    if (!pack) { intelBody.hidden = true; return; }

    const map = {
      "intel-priorities": ["priorities", "buyer_priorities"],
      "intel-pains": ["pains", "typical_pains"],
      "intel-triggers": ["triggers"],
      "intel-proof": ["value_proof", "proof"],
      "intel-objections": ["objections"],
      "intel-ctas": ["ctas"],
    };

    Object.entries(map).forEach(([id, keys]) => {
      const el = document.getElementById(id);
      if (!el) return;
      const arr = keys.map(k => pack?.[k]).find(v => Array.isArray(v)) || [];
      el.innerHTML = arr.map(s => `<li>${String(s)}</li>`).join("") || "<li>(none)</li>";
      el.parentElement?.classList?.add("loaded");
    });

    intelBody.hidden = false;
  } catch {
    intelBody.hidden = true;
  }
}

// ---------- Validation & button enabling ----------
function computeValidity() {
  // Respect HTML5 required rules + ensure selects have non-empty value
  if (!els.form) return false;
  const htmlValid = els.form.checkValidity();
  const productOk = !!els.product?.value;
  const buyerOk = !!els.buyer?.value;
  return htmlValid && productOk && buyerOk;
}

function updateGenerateState() {
  const ok = computeValidity();
  if (els.submit) {
    els.submit.disabled = !ok;
    els.submit.setAttribute("aria-disabled", String(!ok));
  }
  // Copy/Download remain disabled until a script is produced
}

async function onMakeFollowupEmail() {
  if (!els.output) return;
  const scriptMdText = els.output.textContent || "";

  const payload = {
    op: "email",
    tone: els.tone?.value || "Professional (corporate)",
    seller: { name: els.seller_name?.value || "", company: els.seller_company?.value || "" },
    prospect: { name: els.prospect_name?.value || "", role: els.prospect_role?.value || "", company: els.prospect_company?.value || "" },
    scriptMdText: (els.output?.textContent || "").trim(),
    callNotes: (document.getElementById("notes")?.value || "").trim()
  };

  setStatus("Building follow-up email…");
  const res = await fetch("/api/engagement-generate", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    setStatus(`API ${res.status}: ${txt.slice(0, 800)}`);
    return;
  }

  const data = await res.json().catch(() => ({}));
  const emailText = String(data?.email || "").trim() || "(No email returned)";

  // Show in pop-out window (consistent with your Pop-out handler)
  const w = window.open("", "followup_email", "width=820,height=900");
  const safe = emailText.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  w.document.write(`<!doctype html><meta charset="utf-8"><title>Conversation Guide</title>...`);
  w.document.close();

  setStatus("");
}

// ---------- Generate pipeline ----------
async function onGenerate() {
  try {
    if (els.submit) { els.submit.disabled = true; els.submit.classList.add("busy"); }
    setStatus("Preparing…");

    const mode = canonicalMode(
      getHeaderMode() || getFormModeXS() || currentMode || "direct"
    );

    const productId = els.product?.value || "";
    if (!productId) { setStatus("Please choose a product"); return; }

    const buyerId = canonicalBuyerId(els.buyer?.value || "early-majority");

    // 1) Load Markdown template (fail-fast; no silent fallback)
    let templateMd;
    try {
      templateMd = await loadTemplate({ mode, productId, buyerId });
    } catch (err) {
      const message = String(err?.message || err);
      setStatus(message);
      DIAG.set({ kind: "template-error", message, mode, productId, buyerId });
      return;
    }

    // 2) Build variables (tone canonicalised)
    const variables = {
      seller: {
        name: els.seller_name?.value || "",
        company: els.seller_company?.value || ""
      },
      prospect: {
        name: els.prospect_name?.value || "",
        role: els.prospect_role?.value || "",
        company: els.prospect_company?.value || ""
      },
      tone: canonicalTone(els.tone?.value || "Professional"),
      length: parseLength(els.length?.value),
      usps: splitPoints(els.usps?.value),
      other_points: splitPoints(els.other?.value),
      chosen_next_step: (els.next_step?.value || "").trim() || null,
      mode,
      productId,
      buyerId
    };

    // 3) POST to the endpoint (1)
    setStatus("Generating…");
    let data;
    try {
      DIAG.set({ kind: "request", variables, mode, productId, buyerId }); // keeps PII-light; omit templateMd if you prefer
      const res = await fetch("/api/engagement-generate", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          templateMd,
          variables,
          policy: {
            language: "en-GB",
            nonAssumptiveClose: true,
          }
        })
      });

      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        const body = txt?.slice(0, 800);
        setStatus(`API ${res.status}: ${body}`);
        DIAG.set({ kind: "api-error", status: res.status, body, variables: { mode, productId, buyerId } });
        return;
      }
      data = await res.json();
    } catch (e) {
      const message = String(e?.message || e);
      setStatus("Network error talking to API.");
      DIAG.set({ kind: "api-network-error", message });
      return;
    }

    // 4) Render response
    const html = renderScriptFromJson(data);
    const preface = buildPreface({ sellerName: els.seller_name?.value || "" });
    const prefaceHTML = preface?.html || "";
    if (els.output) {
      els.output.innerHTML = prefaceHTML + (html || "<p>(No content returned)</p>");
      DIAG.set({ kind: "ok", productId, buyerId, length: variables.length, time: new Date().toISOString() });
    }
    // Enable actions
    if (els.btnCopy) els.btnCopy.disabled = !html;
    if (els.btnDownload) els.btnDownload.disabled = !html;

    setStatus("");
  } catch (e) {
    const message = String(e?.message || e);
    setStatus(message);
    DIAG.set({ kind: "unhandled", message, stack: e?.stack });
  } finally {
    if (els.submit) { els.submit.disabled = !computeValidity(); els.submit.classList.remove("busy"); }
  }
}

// ---------- Highlighter ----------
let highlightOn = false;
function wrapSelectionInMark(container) {
  const sel = window.getSelection();
  if (!sel || sel.rangeCount === 0) return;
  const range = sel.getRangeAt(0);
  if (!container.contains(range.commonAncestorContainer)) return;
  const mark = document.createElement("mark");
  try { range.surroundContents(mark); } catch { /* ignore impossible selection */ }
  sel.removeAllRanges();
}
function clearHighlights(container) {
  container.querySelectorAll("mark").forEach(m => {
    const p = m.parentNode;
    while (m.firstChild) p.insertBefore(m.firstChild, m);
    p.removeChild(m);
  });
}

// ---------- Wiring ----------
function wire() {
  // Header -> mode
  if (els.buyer) {
    els.buyer.addEventListener("change", () => {
      const buyerId = canonicalBuyerId(els.buyer.value || "");
      loadBuyerIntel(buyerId);             // populate right-rail
      updateGenerateState();
    });
  }
  if (els.headerCallType) {
    els.headerCallType.addEventListener("change", async () => {
      const label = els.headerCallType.value; // "Direct"|"Partner"|"" from <option>
      await setMode(label || "Direct");
    });
  }
  // XS form fallback -> mode
  if (els.call_type_xs) {
    els.call_type_xs.addEventListener("change", async () => {
      const label = els.call_type_xs.value;
      await setMode(label || "Direct");
    });
  }
  // Remember mode toggle (7)
  if (els.headerRemember) {
    els.headerRemember.addEventListener("change", () => {
      if (els.headerRemember.checked) localStorage.setItem(MODE_KEY, currentMode);
      else localStorage.removeItem(MODE_KEY);
    });
  }

  // Highlighter buttons
  const btnHi = document.getElementById("toggle-highlighter");
  const btnClear = document.getElementById("clear-highlights");
  if (btnHi && els.output) {
    btnHi.addEventListener("click", () => {
      highlightOn = !highlightOn;
      btnHi.setAttribute("aria-pressed", String(highlightOn));
    });
    els.output.addEventListener("mouseup", () => {
      if (highlightOn) wrapSelectionInMark(els.output);
    });
  }
  if (btnClear && els.output) {
    btnClear.addEventListener("click", () => clearHighlights(els.output));
  }

  // Form submit
  if (els.form) {
    els.form.addEventListener("submit", (e) => {
      e.preventDefault();
      if (computeValidity()) onGenerate();
      else updateGenerateState();
    });
  }

  // Follow-up email
  const btnFU = document.getElementById("make-followup");
  if (btnFU) {
    btnFU.addEventListener("click", onMakeFollowupEmail);
  }

  // Field changes -> recompute validity
  const watch = [
    els.product, els.buyer, els.tone, els.length,
    els.seller_name, els.seller_company,
    els.prospect_name, els.prospect_role, els.prospect_company,
    els.usps, els.other, els.next_step
  ].filter(Boolean);

  for (const el of watch) {
    el.addEventListener("input", updateGenerateState);
    el.addEventListener("change", updateGenerateState);
  }

  // Copy / Download / Popout
  if (els.btnCopy && els.output) {
    els.btnCopy.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(els.output.textContent || "");
      } catch { }
    });
  }
  if (els.btnDownload && els.output) {
    els.btnDownload.addEventListener("click", () => {
      const blob = new Blob([els.output.textContent || ""], { type: "text/plain;charset=utf-8" });
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = "conversation-guide.txt";
      a.click();
      URL.revokeObjectURL(a.href);
    });
  }
  if (els.btnPopout && els.output) {
    els.btnPopout.addEventListener("click", () => {
      const w = window.open("", "engagement_pop", "width=880,height=900");
      const body = (els.output.textContent || "")
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
      w.document.write(`<!doctype html><title>Conversation Guide</title><pre style="white-space:pre-wrap;padding:16px;">${body}</pre>`);
      w.document.close();
    });
  }
}

// ---------- Bootstrap ----------
(async function bootstrap() {
  // 1) Badge
  await loadUserBadge();

  // 2) Mode initialisation + products (race-safe)
  reflectModeToHeader(currentMode);
  await setMode(currentMode); // populates product list and updates chip; persists if Remember checked

  // 3) Keep helpful tips in synch
  if (els.buyer) loadBuyerIntel(canonicalBuyerId(els.buyer.value || ""));

  // 4) Remembered fields row & restore
  injectRememberButtons();
  loadRememberedFields();

  // 5) Wire controls & set initial button state
  wire();
  updateGenerateState();

  // Initial diagnostics snapshot
  DIAG.set({
    ready: true,
    mode: currentMode,
    hooks: Object.fromEntries(Object.entries(els).map(([k, v]) => [k, !!v]))
  });
})();
