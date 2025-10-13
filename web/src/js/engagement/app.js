// /web/src/js/engagement/app.js ---- 11-10-2025 v5.6 -----//
// Works with: web/engagement/index.html (v3) + styles.css
// Endpoint: /api/engagement-generate  ✅

// Imports
import { getIndex, loadTemplate, canonicalBuyerId } from "/src/lib/contentLoader.js";
import { renderScriptFromJson, buildPreface, enhanceRenderedGuide } from "./render.js";

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

// Generic show/hide binder for a button + a body element
function bindToggle({ buttonId, bodyId, defaultExpanded = false }) {
  const btn = document.getElementById(buttonId);
  const body = document.getElementById(bodyId);
  if (!btn || !body) return;

  // Initial state
  const initExpanded = (btn.getAttribute("aria-expanded") || "").toLowerCase() === "true"
    ? true
    : defaultExpanded;
  apply(initExpanded);

  btn.addEventListener("click", () => {
    const expanded = btn.getAttribute("aria-expanded") === "true";
    apply(!expanded);
  });

  function apply(expanded) {
    btn.setAttribute("aria-expanded", String(expanded));
    btn.textContent = expanded ? "Hide" : "Show";
    body.hidden = !expanded;
  }
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

// log activity helpers 
let _lastRender = null;

function logActivity({ productId, buyerId, length }) {
  const ol = document.getElementById("activity-log");
  if (!ol) return;
  const li = document.createElement("li");
  const when = new Date().toLocaleString();
  li.textContent = `${when} – Product: ${productId} · Buyer: ${buyerId} · ~${length} words`;
  ol.prepend(li);
}

function logDelta(prev, curr) {
  const ul = document.getElementById("delta-log");
  if (!ul || !prev) return;
  const sectionKeys = ["opening", "buyer_pain", "buyer_desire", "example_illustration", "handling_objections", "next_step"];
  const diffs = [];
  for (const k of sectionKeys) {
    const a = String(prev.sections?.[k] || "");
    const b = String(curr.sections?.[k] || "");
    if (a !== b) {
      const d = b.length - a.length;
      const dir = d > 0 ? "↑" : "↓";
      diffs.push(`${k.replace(/_/g, " ")} ${dir}${Math.abs(d)}`);
    }
  }
  if (!diffs.length) return;
  const li = document.createElement("li");
  li.textContent = diffs.join(" · ");
  ul.prepend(li);
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
  const productId = els.product?.value || "";
  const buyerId = canonicalBuyerId(els.buyer?.value || "");
  if (productId && buyerId) {
    loadBuyerIntelFromTemplate({ mode: m, productId, buyerId });
  }
  updateGenerateState();
}

function populateTipsFromJson(json) {
  const list = document.getElementById("tips-list");
  if (!list) return;
  const tips = Array.isArray(json?.tips) ? json.tips.filter(Boolean) : [];
  list.innerHTML = tips.length
    ? tips.map(t => `<li>${String(t)}</li>`).join("")
    : "<li>(no tips provided)</li>";
  const btn = document.getElementById("toggle-tips");
  if (btn) {
    // ensure tips panel is visible when we have content
    btn.setAttribute("aria-expanded", "true");
    const body = document.getElementById("tips-body");
    if (body) body.hidden = false;
  }
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
async function loadBuyerIntelFromTemplate({ mode, productId, buyerId }) {
  const intelBody = document.getElementById("intel-body");
  if (!intelBody) return;
  if (!mode || !productId || !buyerId) { intelBody.hidden = true; return; }

  // Helpers
  const setList = (slotId, items) => {
    const host = document.getElementById(slotId);
    if (!host) return;
    const arr = (items || []).filter(Boolean);
    host.innerHTML = arr.length
      ? `<ul>${arr.map(s => `<li class="intel-chip" tabindex="0" title="Click to add to notes">${String(s)}</li>`).join("")}</ul>`
      : `<ul><li class="muted">(none)</li></ul>`;
    host.parentElement?.classList?.add("loaded");

    // Make each bullet clickable to append to call notes
    const notes = document.getElementById("notes");
    if (notes) {
      host.querySelectorAll("li.intel-chip").forEach(li => {
        const put = () => {
          const val = (notes.value || "").trim();
          const add = String(li.textContent || "").trim();
          notes.value = val ? `${val}\n${add}` : add;
          notes.dispatchEvent(new Event("input", { bubbles: true }));
        };
        li.addEventListener("click", put);
        li.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); put(); } });
      });
    }
  };

  const mdSection = (md, name) => {
    // Grab text under "## <name>" until the next "##"
    const re = new RegExp(`^##\\s*${name}\\s*\\n([\\s\\S]*?)(?=^##\\s|\\Z)`, "mi");
    const m = md.match(re);
    return m ? m[1].trim() : "";
  };

  const bulletise = (txt, max = 5) => {
    const src = String(txt || "").trim();
    if (!src) return [];
    const lines = src.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

    // Prefer existing bullets in the template
    const bulletLines = lines.filter(l => /^[-*•]\s+/.test(l));
    if (bulletLines.length >= 2) {
      return bulletLines
        .map(l => l.replace(/^[-*•]\s+/, "").trim())
        .filter(Boolean)
        .slice(0, max);
    }

    // Fall back: sentence chunks
    return src
      .replace(/\n+/g, " ")
      .split(/(?<=[.!?])\s+/)
      .map(s => s.trim())
      .filter(Boolean)
      .slice(0, max);
  };

  try {
    // Pull from the same template source used for generation
    const templateMd = await loadTemplate({ mode, productId, buyerId });

    // Map your panel’s six slots from closest sections
    const prioritiesTxt = mdSection(templateMd, "Overview");
    const painsTxt = mdSection(templateMd, "Buyer Pain");
    const triggersTxt = mdSection(templateMd, "Buyer Desire");
    const proofTxt = mdSection(templateMd, "Example Illustration");
    const objectionsTxt = mdSection(templateMd, "Handling Objections");
    const ctasTxt = mdSection(templateMd, "Next Step");

    setList("intel-priorities", bulletise(prioritiesTxt));
    setList("intel-pains", bulletise(painsTxt));
    setList("intel-triggers", bulletise(triggersTxt));
    setList("intel-proof", bulletise(proofTxt));
    setList("intel-objections", bulletise(objectionsTxt));
    setList("intel-ctas", bulletise(ctasTxt));

    intelBody.hidden = false;
  } catch {
    ["intel-priorities", "intel-pains", "intel-triggers", "intel-proof", "intel-objections", "intel-ctas"].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.innerHTML = `<ul><li class="muted">(unavailable)</li></ul>`;
    });
    intelBody.hidden = false;
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
  setStatus("Building follow-up email…");

  const scriptMdText = (els.output?.textContent || "").trim();
  const payload = {
    op: "email",
    tone: els.tone?.value || "Professional (corporate)",
    seller: { name: els.seller_name?.value || "", company: els.seller_company?.value || "" },
    prospect: { name: els.prospect_name?.value || "", role: els.prospect_role?.value || "", company: els.prospect_company?.value || "" },
    scriptMdText,
    callNotes: (document.getElementById("notes")?.value || "")
  };

  try {
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
    const email = String(data?.email || "").trim() || "(No email returned)";

    // ✅ Use the same modal/ids that render.js wires
    const dlg = document.getElementById("email-modal");
    const box = document.getElementById("email-text");
    if (box) box.value = email;
    dlg?.showModal();
    setStatus("");
  } catch (err) {
    setStatus(`Network error: ${String(err?.message || err)}`);
  }
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
    // Wire the render-level tools without changing the page HTML
    const html = renderScriptFromJson(data, { sellerName: variables?.seller?.name });
    if (els.output) {
      els.output.innerHTML = html || "<p>(No content returned)</p>";
    }
    
        // Enable actions (copy/download read from the rendered card)
    const hasContent = !!html;
    if (els.btnCopy) els.btnCopy.disabled = !hasContent;
    if (els.btnDownload) els.btnDownload.disabled = !hasContent;
    enhanceRenderedGuide(els.output, {
      getContext: () => ({
        tone: els.tone?.value || "Professional (corporate)",
        seller: { name: els.seller_name?.value || "", company: els.seller_company?.value || "" },
        prospect: { name: els.prospect_name?.value || "", role: els.prospect_role?.value || "", company: els.prospect_company?.value || "" },
        scriptMdText: (els.output?.textContent || "").trim(),
        callNotes: (document.getElementById("notes")?.value || ""),
        usps: (els.usps?.value || "").split(/\r?\n|[,;]+/).map(s => s.trim()).filter(Boolean),
        nextStep: (els.next_step?.value || "").trim(),
        productLabel: els.product?.options?.[els.product.selectedIndex || 0]?.textContent || "",
        buyerType: els.buyer?.value || ""
      })
    });

    logDelta(_lastRender, data);
    logActivity({ productId, buyerId, length: variables.length });
    _lastRender = data;

    // Also populate the tips panel
    populateTipsFromJson(data);
    setStatus("");
  } catch (e) {
    const message = String(e?.message || e);
    setStatus(message);
    DIAG.set({ kind: "unhandled", message, stack: e?.stack });
  } finally {
    if (els.submit) { els.submit.disabled = !computeValidity(); els.submit.classList.remove("busy"); }
  }
}

function wireTogglePanels() {
  // ---------- Buyer needs (intel) ----------
  const btnIntel =
    document.getElementById("toggle-intel") ||
    document.querySelector('[data-toggle="intel"]');
  const intelBody =
    document.getElementById("intel-body") ||
    document.querySelector('[data-panel="intel"]');

  if (btnIntel && intelBody) {
    // Ensure initial ARIA state
    if (!btnIntel.hasAttribute("aria-expanded")) {
      btnIntel.setAttribute("aria-expanded", intelBody.hidden ? "false" : "true");
      btnIntel.textContent = intelBody.hidden ? "Show" : "Hide";
    }

    btnIntel.addEventListener("click", async (e) => {
      // If the control is an <a>, don’t navigate
      if (e && typeof e.preventDefault === "function") e.preventDefault();

      const expanded = btnIntel.getAttribute("aria-expanded") === "true";

      if (expanded) {
        // Hide
        intelBody.hidden = true;
        btnIntel.textContent = "Show";
        btnIntel.setAttribute("aria-expanded", "false");
        return;
      }

      // Show (and try to load intel once)
      try {
        const buyerId = typeof canonicalBuyerId === "function" ? canonicalBuyerId(els?.buyer?.value || "") : (els?.buyer?.value || "");
        const mode =
          (typeof canonicalMode === "function"
            ? canonicalMode(getHeaderMode?.() || getFormModeXS?.() || getSavedMode?.() || "direct")
            : (getHeaderMode?.() || getFormModeXS?.() || getSavedMode?.() || "direct"));
        const productId = els?.product?.value || "";

        const needsFetch =
          intelBody.getAttribute("data-loaded") !== "1" ||
          intelBody.textContent.trim() === "" ||
          intelBody.querySelectorAll("*").length === 0;

        if (productId && buyerId && typeof loadBuyerIntelFromTemplate === "function" && needsFetch) {
          await loadBuyerIntelFromTemplate({ mode, productId, buyerId });
          intelBody.setAttribute("data-loaded", "1");
        }
      } catch (err) {
        console.error("[toggle-intel] load error (showing panel anyway):", err);
      } finally {
        intelBody.hidden = false;
        btnIntel.textContent = "Hide";
        btnIntel.setAttribute("aria-expanded", "true");
      }
    });
  } else {
    // Helpful console hint if wiring failed
    if (!btnIntel) console.warn("[toggle-intel] button not found (expected #toggle-intel or [data-toggle='intel'])");
    if (!intelBody) console.warn("[toggle-intel] panel not found (expected #intel-body or [data-panel='intel'])");
  }

  // ---------- Sales tips ----------
  const btnTips =
    document.getElementById("toggle-tips") ||
    document.querySelector('[data-toggle="tips"]');
  const tipsBody =
    document.getElementById("tips-body") ||
    document.querySelector('[data-panel="tips"]');

  if (btnTips && tipsBody) {
    if (!btnTips.hasAttribute("aria-expanded")) {
      btnTips.setAttribute("aria-expanded", tipsBody.hidden ? "false" : "true");
      btnTips.textContent = tipsBody.hidden ? "Show" : "Hide";
    }

    btnTips.addEventListener("click", (e) => {
      if (e && typeof e.preventDefault === "function") e.preventDefault();
      const expanded = btnTips.getAttribute("aria-expanded") === "true";
      tipsBody.hidden = expanded;
      btnTips.textContent = expanded ? "Show" : "Hide";
      btnTips.setAttribute("aria-expanded", expanded ? "false" : "true");
    });
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
      const mode = canonicalMode(getHeaderMode() || getFormModeXS() || currentMode || "direct");
      const productId = els.product?.value || "";
      loadBuyerIntelFromTemplate({ mode, productId, buyerId });
      updateGenerateState();
    });
  }
  if (els.product) {
    els.product.addEventListener("change", () => {
      const buyerId = canonicalBuyerId(els.buyer?.value || "");
      const mode = canonicalMode(getHeaderMode() || getFormModeXS() || currentMode || "direct");
      const productId = els.product?.value || "";
      loadBuyerIntelFromTemplate({ mode, productId, buyerId });
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
  if (els.buyer) {
    const mode = currentMode;
    const productId = els.product?.value || "";
    const buyerId = canonicalBuyerId(els.buyer.value || "");
    if (productId && buyerId) {
      loadBuyerIntelFromTemplate({ mode, productId, buyerId });
    }
  }
  if (els.product) {
    els.product.addEventListener("change", () => {
      const mode = canonicalMode(getHeaderMode() || getFormModeXS() || currentMode || "direct");
      const buyerId = canonicalBuyerId(els.buyer?.value || "");
      const productId = els.product?.value || "";
      if (productId && buyerId) {
        loadBuyerIntelFromTemplate({ mode, productId, buyerId });
      }
    });
  }
  wireTogglePanels();

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
