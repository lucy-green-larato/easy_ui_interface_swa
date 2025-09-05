// first-call-script-v2.js
import { getIndex, loadTemplate, canonical } from "../lib/callLibrary.js?v=fix6";

// Shape-agnostic, absolute-only, works with split or unified indexes.
async function loadProductIndex(mode) {
  const m = String(mode || '').toLowerCase() === 'partner' ? 'partner' : 'direct';

  // 1) Try legacy mode-specific index (fine if 404)
  try {
    const r = await fetch(`/content/call-library/v1/${m}/index.json`, { cache: 'no-store' });
    if (r.ok) {
      console.info('[Product] using mode index:', m);
      return await r.json(); // may be array or {products:[...]}
    }
  } catch (_) { }

  // 2) Fallback: unified v1 index
  const u = await fetch(`/content/call-library/v1/index.json`, { cache: 'no-store' });
  if (!u.ok) throw new Error(`HTTP ${u.status} for /content/call-library/v1/index.json`);
  const data = await u.json();

  // ---- normalize any shape to a flat array of product-like objects ----
  const arr = extractProducts(data);

  // Filter by mode using "path" when available; otherwise leave as-is
  const byMode = arr.filter(it => {
    const p = String(it?.path || '').toLowerCase();
    if (p.includes('/partner/')) return m === 'partner';
    if (p.includes('/direct/')) return m === 'direct';
    return true; // no path: include in both
  });

  // If filtering removed everything (e.g., only partner paths in file and we asked for direct), fall back to "all"
  const out = byMode.length ? byMode : arr;

  console.info(`[Product] mode=${m} total=${arr.length} filtered=${byMode.length} using=${out.length}`);
  return { products: out };
}

// Heuristic extractor: handles array root, {products:[...]}, {items:[...]},
// {products:{direct:[...], partner:[...]}} and other nested arrays with product-like items.
function extractProducts(data) {
  if (!data) return [];

  if (Array.isArray(data)) return data;

  if (Array.isArray(data.products)) return data.products;
  if (Array.isArray(data.items)) return data.items;

  // products is an object keyed by mode (e.g., { direct:[...], partner:[...] })
  if (data.products && typeof data.products === 'object' && !Array.isArray(data.products)) {
    const flat = Object.values(data.products).flatMap(v => Array.isArray(v) ? v : []);
    if (flat.length) return flat;
  }

  // last resort: deep-scan for arrays of objects that look like products
  const out = [];
  const stack = [data];
  const seen = new Set();
  while (stack.length) {
    const node = stack.pop();
    if (!node || typeof node !== 'object' || seen.has(node)) continue;
    seen.add(node);

    if (Array.isArray(node)) {
      if (node.length && typeof node[0] === 'object') {
        const candidates = node.filter(o => o && typeof o === 'object' && ('id' in o || 'label' in o || 'name' in o));
        if (candidates.length) out.push(...candidates);
      }
      for (const v of node) if (v && typeof v === 'object') stack.push(v);
    } else {
      for (const v of Object.values(node)) if (v && typeof v === 'object') stack.push(v);
    }
  }
  return out;
}


/* basePrefix for subpath hosting */
const basePrefix = (() => {
  const parts = (location.pathname || "").split("/");
  const last = parts[parts.length - 1] || "";
  if (last.includes(".")) return "";
  return parts.length > 1 && parts[1] ? `/${parts[1]}` : "";
})();

// State shared by views (keep on window for pop-out)
let bulletMode = false;
let highlightMode = false;
let lastRender = { html: "", bulletsHtml: "" };
window.bulletMode = bulletMode;
window.lastRender = lastRender;

// Small helpers
const esc = s => String(s || "").replace(/[&<>]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
const strongify = s => String(s || "").replace(/\*\*([^\*\n][\s\S]*?)\*\*/g, "<strong>$1</strong>");

// Derive a nice display name from a login/email
function deriveNameFromLogin(login) {
  const s = String(login || "").trim();
  if (!s) return "";
  const raw = s.includes("@") ? s.split("@")[0] : s;
  const parts = raw.replace(/[_.-]+/g, " ").split(" ").filter(Boolean);
  return parts.map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
}

// Will hold the friendly name for welcome copy
let currentUserName = "";

// Canon/aliases
const REQUIRED_CANON = ["Opening", "Buyer Pain", "Buyer Desire", "Example Illustration", "Handling Objections", "Next Step"];
const SECTION_ALIASES = {
  "Opening": ["opening", "introduction", "overview"],
  "Buyer Pain": ["buyer pain", "pains", "pain", "challenges", "problems"],
  "Buyer Desire": ["buyer desire", "desire", "goals", "objectives", "what they want"],
  "Example Illustration": ["example illustration", "example", "case study", "illustration"],
  "Handling Objections": ["handling objections", "objections", "objection handling"],
  "Next Step": ["next step", "next steps", "call to action", "cta"]
};
// UI-only heading overrides
const DISPLAY_LABELS = { "Opening": "Overview" };

function buildPreface(values = {}) {
  const formName = String(values.seller_name || "").trim();
  const who = formName || currentUserName || "there";
  const title = "About this guide";
  const body = [
    `Hi ${esc(who)} 👋`,
    `You’ve got this. This guide gives you concise, practical coaching for your next call — what to focus on, how to steer the conversation, and how to land a clear next step.`,
    `Pick the ideas that fit your style. Keep it simple, be curious, and use the buyer insights on the right to back your judgement. Good selling! 🚀`
  ].map(p => `<p>${p}</p>`).join("");
  return { title, html: body };
}

function normaliseMd(raw) {
  return String(raw || "")
    .replace(/\r/g, "")
    .replace(/\s*##\s+/g, "\n\n## ")
    .replace(/(?:^|[^\n])\s-\s(?=\S)/g, m => (m.endsWith("- ") ? m : m.replace(/\s-\s/, "\n- ")))
    .replace(/[ \t]+\n/g, "\n")
    .trim();
}
function parseSections(raw) {
  const s = normaliseMd(raw);
  const lines = s.split("\n");
  const sections = [];
  let current = null;
  const pushCurrent = () => { if (current && (current.paras.length || current.list.length)) sections.push(current); };
  const start = t => { if (current) pushCurrent(); current = { title: t, paras: [], list: [] }; };

  const CANON = new Map([
    ["opening", "Opening"],
    ["buyer pain", "Buyer Pain"], ["pains", "Buyer Pain"], ["pain", "Buyer Pain"],
    ["buyer desire", "Buyer Desire"], ["desire", "Buyer Desire"], ["goals", "Buyer Desire"],
    ["example illustration", "Example Illustration"], ["example", "Example Illustration"], ["case study", "Example Illustration"],
    ["handling objections", "Handling Objections"], ["objections", "Handling Objections"],
    ["next step", "Next Step"], ["next steps", "Next Step"], ["call to action", "Next Step"], ["cta", "Next Step"],
  ]);
  const HEADING_RX = /^(?:#{2,}\s*|\*\*\s*|__\s*)?(opening|buyer pain|buyer desire|example illustration|handling objections|objections|next step|next steps|call to action|cta)\b[:\s-]*?(.*)$/i;

  const firstNonEmpty = lines.find(l => l.trim());
  if (firstNonEmpty) {
    const m0 = firstNonEmpty.trim().match(HEADING_RX);
    const canon0 = m0 ? CANON.get((m0[1] || "").toLowerCase().trim()) : null;
    if (!canon0) start("Opening");
  }

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    const m = line.match(HEADING_RX);
    if (m) {
      const canon = CANON.get((m[1] || "").toLowerCase().trim());
      if (canon) {
        pushCurrent();
        current = { title: canon, paras: [], list: [] };
        const rest = (m[2] || "").trim();
        if (rest) current.paras.push(rest);
        continue;
      }
    }
    if (line.startsWith("- ")) { if (!current) start("Opening"); current.list.push(line.slice(2).trim()); continue; }
    if (!current) start("Opening");
    current.paras.push(line);
  }
  pushCurrent();
  return sections;
}

function commonBlock(node) {
  const el = node?.nodeType === 3 ? node.parentElement : node;
  return el?.closest?.('p, li') || null;
}

function highlightCurrentSelection() {
  const sel = window.getSelection();
  if (!sel || sel.isCollapsed || sel.rangeCount === 0) return;

  const range = sel.getRangeAt(0);
  if (!outputEl.contains(range.commonAncestorContainer)) return;

  const a = commonBlock(sel.anchorNode);
  const b = commonBlock(sel.focusNode);
  if (!a || !b || a !== b) {
    setStatus('Select within a single paragraph or bullet to highlight.', 'error');
    sel.removeAllRanges();
    return;
  }

  const r = range.cloneRange();
  const frag = r.extractContents();
  const mark = document.createElement('mark');
  mark.className = 'hl';
  mark.appendChild(frag);
  r.insertNode(mark);

  sel.removeAllRanges();
  setStatus('Highlighted.');
}

function clearHighlights() {
  outputEl.querySelectorAll('mark.hl').forEach(m => {
    const parent = m.parentNode;
    while (m.firstChild) parent.insertBefore(m.firstChild, m);
    parent.removeChild(m);
    parent.normalize();
  });
  setStatus('Highlights cleared.');
}

function renderSectionsToHtml(rawMd, valuesForFallback) {
  const escapeRxLocal = (typeof escapeRx === "function")
    ? escapeRx
    : (s => String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const labelForFn = (typeof labelFor === "function") ? labelFor : (s => s);
  const PREF = buildPreface(valuesForFallback || {});

  const sections = parseSections(rawMd);
  const byKey = new Map(sections.map(s => [String(s.title).toLowerCase(), s]));

  const v = valuesForFallback || {};
  const greetRx = (() => {
    const pn = (v.prospect_name || "").trim();
    const sn = (v.seller_name || "").trim();
    const sc = (v.seller_company || "").trim();
    if (!pn || !sn || !sc) return null;
    return new RegExp(
      `^hello\\s+${escapeRxLocal(pn)}\\s*,?\\s+it[’']?s\\s+${escapeRxLocal(sn)}\\s+from\\s+${escapeRxLocal(sc)}\\.?$`,
      "i"
    );
  })();

  const getByAliases = (canonName) => {
    const aliases = SECTION_ALIASES[canonName] || [canonName];
    for (const a of aliases) {
      const key = String(a).toLowerCase();
      if (byKey.has(key)) return byKey.get(key);
      for (const [k, v] of byKey.entries()) {
        if (k.includes(key) || key.includes(k)) return v;
      }
    }
    return null;
  };

  const blocks = [];
  const found = [];
  const missing = [];

  for (const canonName of REQUIRED_CANON) {
    const sec = getByAliases(canonName);

    if (sec) {
      if (/^opening$/i.test(sec.title) && greetRx) {
        sec.paras = (sec.paras || []).filter(p => !greetRx.test(String(p).trim()));
      }

      const body = [
        ...(sec.paras || []).map(p => `<p>${strongify(esc(p))}</p>`),
        ...(sec.list && sec.list.length
          ? [`<ul>${sec.list.map(li => `<li>${strongify(esc(li))}</li>`).join("")}</ul>`]
          : [])
      ].join("");

      found.push(canonName);
      blocks.push(
        `<section class="script-sec"><h3>${esc(labelForFn(canonName))}</h3>${body}</section>`
      );
    } else {
      missing.push(canonName);
      if (canonName === "Next Step") {
        const nx = v?.next_step || "";
        const pHtml = nx
          ? `<p>${strongify(esc(nx))}</p>`
          : `<p class="muted">Not provided in the generated script.</p>`;
        blocks.push(
          `<section class="script-sec is-missing"><h3>${esc(labelForFn("Next Step"))}</h3>${pHtml}</section>`
        );
      } else {
        blocks.push(
          `<section class="script-sec is-missing"><h3>${esc(labelForFn(canonName))}</h3><p class="muted">Not provided in the generated script.</p></section>`
        );
      }
    }
  }

  const prefaceHtml = `<section class="script-sec preface">
    <h3>${esc(PREF.title || "About this guide")}</h3>
    ${PREF.html}
  </section>`;

  return {
    html: `<div class="script-body">${prefaceHtml}${blocks.join("")}</div>`,
    found,
    missing,
    sections
  };
}

const labelFor = (canon) => (DISPLAY_LABELS[canon] || canon);

// Bullet view helpers
function firstSentence(s, max = 220) {
  const txt = String(s || "").replace(/\s+/g, " ").trim();
  if (!txt) return "";
  const m = txt.match(/^(.{1,220}?[.!?])(\s|$)/);
  return (m ? m[1] : txt.slice(0, max)).trim();
}
function sentencesWithNumbers(s, limit = 2) {
  const parts = String(s || "").split(/(?<=[.!?])\s+/);
  const hits = parts.filter(x => /\b(\d{1,3}(,\d{3})*|\d+%|£\d+)/.test(x));
  return hits.slice(0, limit).map(x => x.trim());
}
function renderBulletScriptFromSections(sections) {
  const by = Object.create(null); for (const s of sections) by[(s.title || "").toLowerCase()] = s;
  const bullets = [];
  if (by["opening"]) { const t = firstSentence(by["opening"].paras.join(" ").trim() || by["opening"].list[0] || ""); if (t) bullets.push(t); }
  if (by["buyer pain"]) { if (by["buyer pain"].list.length) bullets.push(...by["buyer pain"].list.slice(0, 3)); else { const t = firstSentence(by["buyer pain"].paras.join(" ").trim()); if (t) bullets.push(t); } }
  if (by["buyer desire"]) { if (by["buyer desire"].list.length) bullets.push(...by["buyer desire"].list.slice(0, 3)); else { const t = firstSentence(by["buyer desire"].paras.join(" ").trim()); if (t) bullets.push(t); } }
  if (by["example illustration"]) { const para = by["example illustration"].paras.join(" ").trim(); const metricLines = sentencesWithNumbers(para, 2); if (metricLines.length) bullets.push(...metricLines); else { const t = firstSentence(para); if (t) bullets.push("Example: " + t); } }
  if (by["handling objections"]) { if (by["handling objections"].list.length) bullets.push(...by["handling objections"].list.slice(0, 3)); else { const t = firstSentence(by["handling objections"].paras.join(" ").trim()); if (t) bullets.push("Objections: " + t); } }
  if (by["next step"]) { const t = firstSentence((by["next step"].paras.join(" ") || by["next step"].list.join("; ")).trim()); if (t) bullets.push("Next step: " + t); }
  const items = bullets.map(b => `<li>${esc(b)}</li>`).join("");
  return `<div class="script-body"><h3>Bullet point script</h3><ul>${items || "<li>No bulletable content found.</li>"}</ul></div>`;
}

/* Keys */
const FORM_ID = 'script-form';
const STORAGE_KEY = 'first_call_script_v2.form';
const ACTIVITY_KEY = 'first_call_script_v2.activity';
const OUTPUT_KEY = 'first_call_script_v2.last_output';
const CALL_PREF_KEY = 'first_call_script_v2.call_pref';
const TIPS_HIDDEN_KEY = 'first_call_script_v2.tips_hidden';
const OPENERS_HIDDEN_KEY = 'first_call_script_v2.openers_hidden';
const OPENERS_PREF_VERSION = '2';
try {
  const vkey = 'first_call_script_v2.openers_pref_version';
  if (localStorage.getItem(vkey) !== OPENERS_PREF_VERSION) {
    localStorage.setItem(OPENERS_HIDDEN_KEY, '1');
    localStorage.setItem(vkey, OPENERS_PREF_VERSION);
  }
} catch { }

const NOTES_KEY_PREFIX = 'first_call_script_v2.notes';
const INTEL_URL = './intel.json';

const REQUIRED = ['call_type', 'seller_name', 'seller_company', 'prospect_name', 'prospect_role', 'prospect_company', 'buyer_behaviour', 'product'];

// Refs
const form = document.getElementById(FORM_ID);
const submitBtn = document.getElementById('submit');
const resetBtn = document.getElementById('resetBtn');
const outputEl = document.getElementById('output');
const copyScriptBtn = document.getElementById('copy-script');
const downloadScriptBtn = document.getElementById('download-script');
const toggleBulletsBtn = document.getElementById('toggle-bullets');
const tipsList = document.getElementById('tips-list');
const toggleTipsBtn = document.getElementById('toggle-tips');
const openersList = document.getElementById('openers-list');
const toggleOpenersBtn = document.getElementById('toggle-openers');
const tipsBody = document.getElementById('tips-body');
const popoutBtn = document.getElementById('popout');
const notesArea = document.getElementById('notes');
const saveNotesBtn = document.getElementById('save-notes');
const downloadNotesBtn = document.getElementById('download-notes');
const outputMeta = document.getElementById('output-meta');
const statusEl = document.getElementById('status');
const deltaLog = document.getElementById('delta-log');
const activityLog = document.getElementById('activity-log');
const activityTpl = document.getElementById('activity-item');
const diag = document.getElementById('diagnostics');
const diagJson = document.getElementById('diag-json');
const modal = document.getElementById('script-modal');
const closeModal = document.getElementById('close-modal');
const modalScript = document.getElementById('modal-script');
const productSel = document.getElementById('product');
const buyerBehSel = document.getElementById('buyer_behaviour');
const toggleIntelBtn = document.getElementById('toggle-intel');
const intelBody = document.getElementById('intel-body');
const callTypeSel = document.getElementById('call_type');
const callTypeSelXs = document.getElementById('call_type_xs');
const rememberCallType = document.getElementById('remember_call_type');
const mapLabel = document.getElementById('map-label');
const helpBtn = document.getElementById('help');
const helpModal = document.getElementById('help-modal');
const helpClose = document.getElementById('help-close');
const userBadge = document.getElementById('user-badge');
const userEmail = document.getElementById('user-email');
const toggleHighlighterBtn = document.getElementById('toggle-highlighter');
const clearHighlightsBtn = document.getElementById('clear-highlights');
const modeFromCallType = v => canonical.mode(v);
const buyerCanon = v => canonical.buyer(v);
const nowTime = () => new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });

// Default Buyer needs collapsed on first load
if (toggleIntelBtn && intelBody) {
  toggleIntelBtn.setAttribute('aria-expanded', 'false');
  toggleIntelBtn.textContent = 'Show';
  intelBody.hidden = true;
}

if (closeModal) closeModal.addEventListener("click", () => modal.close());
if (helpBtn) helpBtn.addEventListener('click', () => helpModal?.showModal());
if (helpClose) helpClose.addEventListener('click', () => helpModal?.close());

toggleHighlighterBtn?.addEventListener('click', () => {
  highlightMode = !highlightMode;
  toggleHighlighterBtn.setAttribute('aria-pressed', String(highlightMode));
  setStatus(highlightMode ? 'Highlighter on — select text in the guide.' : 'Highlighter off.');
});

clearHighlightsBtn?.addEventListener('click', clearHighlights);

// Toggle between full guide and bullet point view (single, de-duped handler)
toggleBulletsBtn?.addEventListener('click', () => {
  bulletMode = !bulletMode;
  window.bulletMode = bulletMode;

  toggleBulletsBtn.setAttribute('aria-pressed', String(bulletMode));
  toggleBulletsBtn.textContent = bulletMode ? 'Show full guide' : 'Show bullet point script';

  const htmlToShow = bulletMode ? (lastRender.bulletsHtml || '') : (lastRender.html || '');
  if (htmlToShow) {
    outputEl.innerHTML = htmlToShow;
    modalScript.innerHTML = htmlToShow;
  }
});

// Create a highlight when user selects text and releases the mouse
outputEl?.addEventListener('mouseup', () => {
  if (highlightMode) highlightCurrentSelection();
});

// ESC turns the highlighter off
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && highlightMode) {
    highlightMode = false;
    toggleHighlighterBtn?.setAttribute('aria-pressed', 'false');
    setStatus('Highlighter off.');
  }
});

function setBusy(isBusy) {
  if (submitBtn) submitBtn.disabled = isBusy || !allRequiredFilled(form);
  if (resetBtn) resetBtn.disabled = isBusy;
  const hasText = !!(outputEl?.textContent?.trim());
  if (copyScriptBtn) copyScriptBtn.disabled = isBusy || !hasText;
  if (downloadScriptBtn) downloadScriptBtn.disabled = isBusy || !hasText;
  if (submitBtn) submitBtn.classList.toggle('busy', isBusy);
}
function setStatus(msg, kind = 'info') { if (!statusEl) return; statusEl.textContent = msg; statusEl.dataset.kind = kind; }

function allRequiredFilled(formEl) {
  if (!formEl) return false;
  const labels = { call_type: 'Direct or partner sales', seller_name: 'Your name', seller_company: 'Your company', prospect_name: 'Prospect name', prospect_role: 'Prospect role', prospect_company: 'Prospect company', buyer_behaviour: 'Buyer behaviour', product: 'Product' };
  const missing = [];
  const getVal = (name) => {
    if (name === 'product') return (productSel?.value || '').trim();
    if (name === 'buyer_behaviour') return (buyerBehSel?.value || '').trim();
    if (name === 'call_type') return (callTypeSel?.value || '').trim();
    return (formEl.elements[name]?.value || '').trim();
  };
  for (const name of REQUIRED) { const val = getVal(name); if (!val) missing.push(labels[name] || name); }
  if (missing.length) setStatus(`Please complete: ${missing.join(', ')}.`, 'error'); else setStatus('');
  return missing.length === 0;
}
function refreshSubmitState() { if (submitBtn) submitBtn.disabled = !allRequiredFilled(form); }

function validateField(input) {
  if (!input) return;
  const el = (typeof input === 'string') ? (form.elements[input] || document.getElementById(input)) : input;
  if (!el) return;
  const id = el.id || el.name || '';
  const errorEl = document.getElementById(`${id}_error`) || (id === 'call_type' ? document.getElementById('call_type_error') : null);
  let value = '';
  if (id === 'product') value = (productSel.value || '').trim();
  else if (id === 'buyer_behaviour') value = (buyerBehSel.value || '').trim();
  else if (id === 'call_type') value = (callTypeSel?.value || '').trim();
  else value = (el.value || '').trim();
  const valid = !!value;
  if (!errorEl) return;
  if (valid) { errorEl.textContent = ''; el.removeAttribute('aria-invalid'); el.classList.remove('invalid'); }
  else { errorEl.textContent = id === 'call_type' ? 'Please choose a call type.' : 'This field is required.'; el.setAttribute('aria-invalid', 'true'); el.classList.add('invalid'); }
}

function saveForm() { if (!form) return; const fd = Object.fromEntries(new FormData(form).entries()); localStorage.setItem(STORAGE_KEY, JSON.stringify(fd)); }
function loadForm() {
  const raw = localStorage.getItem(STORAGE_KEY); if (!raw) return;
  try { const data = JSON.parse(raw); Object.entries(data).forEach(([k, v]) => { const el = form.elements[k] || document.getElementById(k); if (el) el.value = v; }); } catch { }
}

// Notes
const notesKey = () => {
  const pn = (form?.elements?.prospect_name?.value || '').trim().toLowerCase();
  const pc = (form?.elements?.prospect_company?.value || '').trim().toLowerCase();
  return `${NOTES_KEY_PREFIX}::${pc}::${pn}` || NOTES_KEY_PREFIX;
};
function loadNotes() { try { notesArea.value = localStorage.getItem(notesKey()) || ''; } catch { } }
function saveNotes() { try { localStorage.setItem(notesKey(), notesArea.value || ''); setStatus('Notes saved.'); } catch (e) { setStatus('Could not save notes.', 'error'); } }
function downloadText(filename, text) { const blob = new Blob([text], { type: 'text/plain' }); const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = filename; document.body.appendChild(a); a.click(); URL.revokeObjectURL(url); a.remove(); }

// Buyer needs helpers
const norm = s => String(s || '').toLowerCase().trim();
const toArr = v => Array.isArray(v) ? v.filter(Boolean).map(String) : (v ? [String(v)] : []);
function getKeyInsensitive(obj, requested) {
  if (!obj || typeof obj !== 'object') return null;
  const want = norm(requested).replace(/\s+/g, '-');
  for (const k of Object.keys(obj)) if (norm(k).replace(/\s+/g, '-') === want) return k;
  for (const k of Object.keys(obj)) if (norm(k).includes(norm(requested))) return k;
  return null;
}
const pEl = t => { const el = document.createElement('p'); el.textContent = t; return el; };
const setSlot = (id, arr = []) => { const slot = document.getElementById(id); const items = (arr.length ? arr : ['No data available for this selection.']); if (slot) slot.replaceChildren(...items.map(pEl)); };
const fillAll = arr => { setSlot('intel-priorities', arr); setSlot('intel-pains', arr); setSlot('intel-triggers', arr); setSlot('intel-proof', arr); setSlot('intel-objections', arr); setSlot('intel-ctas', arr); };

function normaliseIntel(raw) {
  if (!raw || typeof raw !== 'object') return { products: {} };
  if (raw.products && typeof raw.products === 'object') return raw;
  const out = { products: {} };
  Object.entries(raw).forEach(([product, buyersObj]) => {
    const behaviours = {};
    if (buyersObj && typeof buyersObj === 'object') {
      Object.entries(buyersObj).forEach(([behaviour, arr]) => {
        const [priorities, pains, triggers, proof, objections, ctas] = Array.isArray(arr) ? arr : [];
        behaviours[behaviour] = { priorities: toArr(priorities), pains: toArr(pains), triggers: toArr(triggers), proof: toArr(proof), objections: toArr(objections), ctas: toArr(ctas) };
      });
    }
    out.products[product] = { behaviours };
  });
  return out;
}

let intel = null;
function renderIntel(productLabel, behaviourLabel) {
  if (!intel) return;
  const products = intel.products || {};
  const productKey = getKeyInsensitive(products, productLabel);
  const productNode = productKey ? products[productKey] : null;
  if (!productNode) { fillAll(['No data available (unknown product).']); return; }
  const pool = productNode.behaviours || productNode.buyers || {};
  const key = getKeyInsensitive(pool, behaviourLabel);
  const node = key ? pool[key] : (productNode.needs || null);
  if (!node) {
    const behavioursList = [...Object.keys(productNode.behaviours || {}), ...Object.keys(productNode.buyers || {})].join(', ') || '—';
    fillAll([`No data for this behaviour. Available: ${behavioursList}`]); return;
  }
  const sections = {
    priorities: toArr(node?.priorities ?? node?.Priorities),
    pains: toArr(node?.pains ?? node?.Pains),
    triggers: toArr(node?.triggers ?? node?.Triggers),
    proof: toArr(node?.proof ?? node?.Proof ?? node?.value ?? node?.Value),
    objections: toArr(node?.objections ?? node?.Objections),
    ctas: toArr(node?.ctas ?? node?.CTAs ?? node?.callsToAction ?? node?.CallsToAction)
  };
  const allEmpty = Object.values(sections).every(a => a.length === 0);
  if (allEmpty) { fillAll(['No data available for this selection.']); return; }
  setSlot('intel-priorities', sections.priorities);
  setSlot('intel-pains', sections.pains);
  setSlot('intel-triggers', sections.triggers);
  setSlot('intel-proof', sections.proof);
  setSlot('intel-objections', sections.objections);
  setSlot('intel-ctas', sections.ctas);
}

// Activity / delta
function addActivity(entry) {
  const list = JSON.parse(localStorage.getItem(ACTIVITY_KEY) || '[]');
  list.unshift(entry); localStorage.setItem(ACTIVITY_KEY, JSON.stringify(list.slice(0, 3))); renderActivity();
}
function renderActivity() {
  const list = JSON.parse(localStorage.getItem(ACTIVITY_KEY) || '[]');
  if (!activityLog) return; activityLog.innerHTML = '';
  list.forEach(item => {
    const node = activityTpl.content.cloneNode(true);
    node.querySelector('.time').textContent = item.time;
    node.querySelector('.product').textContent = item.product;
    node.querySelector('.buyer').textContent = item.buyer_behaviour || item.buyer || '';
    node.querySelector('.length').textContent = item.length || '';
    activityLog.appendChild(node);
  });
}
function computeDelta(oldText, newText) {
  const oldLines = (oldText || '').split('\n'), newLines = (newText || '').split('\n');
  let added = 0, removed = 0;
  const oldSet = new Set(oldLines), newSet = new Set(newLines);
  newLines.forEach(l => { if (!oldSet.has(l) && l.trim()) added++; });
  oldLines.forEach(l => { if (!newSet.has(l) && l.trim()) removed++; });
  return { added, removed };
}
function renderDelta(delta) {
  if (!deltaLog) return;
  deltaLog.innerHTML = '';
  const a = document.createElement('li'); a.textContent = `+${delta.added} new line${delta.added === 1 ? '' : 's'}`;
  const r = document.createElement('li'); r.textContent = `−${delta.removed} removed line${delta.removed === 1 ? '' : 's'}`;
  deltaLog.appendChild(a); deltaLog.appendChild(r);
}

// Header chip / remember
function loadCallPref() {
  try {
    const pref = JSON.parse(localStorage.getItem(CALL_PREF_KEY) || 'null');
    if (pref?.call_type && callTypeSel && !callTypeSel.value) callTypeSel.value = pref.call_type;
    if (pref?.remember === true && rememberCallType) rememberCallType.checked = true;
  } catch { }
  updateMapIndicator();
}
function saveCallPrefIfRequested() {
  if (rememberCallType && rememberCallType.checked && callTypeSel && callTypeSel.value) {
    localStorage.setItem(CALL_PREF_KEY, JSON.stringify({ remember: true, call_type: callTypeSel.value }));
  } else if (rememberCallType && !rememberCallType.checked) {
    localStorage.removeItem(CALL_PREF_KEY);
  }
  updateMapIndicator();
}
function updateMapIndicator() {
  const pref = JSON.parse(localStorage.getItem(CALL_PREF_KEY) || 'null');
  if (mapLabel) mapLabel.textContent = pref?.call_type || (callTypeSel?.value || 'Not set');
}

// Assets for Call opener ideas
const OPENERS_URL_JSON = './assets/call-opener-ideas.json';
const OPENERS_URL_TXT = './assets/call-opener-ideas.txt';

async function loadOpenersFromAssets() {
  try {
    const rj = await fetch(OPENERS_URL_JSON, { cache: 'no-store' });
    if (rj.ok) {
      const arr = await rj.json();
      const tips = (Array.isArray(arr) ? arr : []).map(s => String(s).trim()).filter(Boolean);
      if (tips.length) return tips.slice(0, 3);
    }
  } catch { }

  try {
    const rt = await fetch(OPENERS_URL_TXT, { cache: 'no-store' });
    if (rt.ok) {
      const t = await rt.text();
      const tips = t.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
      if (tips.length) return tips.slice(0, 3);
    }
  } catch { }

  return [];
}

function renderOpeners(list) {
  if (!openersList) return;
  openersList.innerHTML = '';

  const items = (list && list.length) ? list.slice(0, 3) : [
    'Add up to 3 ideas in ./assets/call-opener-ideas.json or .txt'
  ];

  items.forEach(t => {
    const li = document.createElement('li');
    li.textContent = t;
    if (!list || !list.length) li.className = 'muted';
    openersList.appendChild(li);
  });
}

function applyOpenersVisibility() {
  const openersBody = document.getElementById('openers-body');
  const btn = document.getElementById('toggle-openers');
  if (!openersBody || !btn) return;

  const stored = localStorage.getItem(OPENERS_HIDDEN_KEY);
  const hidden = (stored === null) ? true : stored === '1';

  openersBody.hidden = hidden;
  btn.textContent = hidden ? 'Show' : 'Hide';
  btn.setAttribute('aria-expanded', String(!hidden));
}

// Tips toggle + auto-hide
function applyTipsVisibility() {
  const body = document.getElementById('tips-body');
  if (!body || !tipsList || !toggleTipsBtn) return;

  const hidden = localStorage.getItem(TIPS_HIDDEN_KEY) === '1';
  body.hidden = hidden;
  toggleTipsBtn.textContent = hidden ? 'Show' : 'Hide';
  toggleTipsBtn.setAttribute('aria-expanded', String(!hidden));

  const hasTips = tipsList.children.length > 0;
  let empty = document.getElementById('tips-empty');

  if (!hasTips) {
    if (!empty) {
      empty = document.createElement('p');
      empty.id = 'tips-empty';
      empty.className = 'muted';
      empty.textContent = 'More tips for this conversation will be offered after you generate your guide.';
      tipsList.insertAdjacentElement('afterend', empty);
    }
  } else if (empty) {
    empty.remove();
  }
}

// Events
const onChangeRecalcIntel = () => {
  const label = productSel?.options?.[productSel.selectedIndex]?.text ?? productSel?.value ?? '';
  renderIntel(label, buyerBehSel?.value || '');
};

callTypeSel?.addEventListener('change', async () => {
  if (callTypeSelXs) callTypeSelXs.value = callTypeSel.value;
  validateField(callTypeSel);
  saveForm(); updateMapIndicator(); refreshSubmitState();
  const mode = modeFromCallType(callTypeSel.value) || 'direct';
  await populateProductsForMode(mode);
  validateField('product');
});

productSel?.addEventListener('change', () => {
  validateField('product');
  saveForm();
  onChangeRecalcIntel();
});

buyerBehSel?.addEventListener('change', () => {
  validateField('buyer_behaviour');
  saveForm();
  onChangeRecalcIntel();
});

callTypeSelXs?.addEventListener('change', async () => {
  if (!callTypeSel) return;
  callTypeSel.value = callTypeSelXs.value;
  validateField(callTypeSel);
  saveForm(); updateMapIndicator(); refreshSubmitState();
  const mode = modeFromCallType(callTypeSel.value) || 'direct';
  await populateProductsForMode(mode);
  callTypeSel.dispatchEvent(new Event('change', { bubbles: true }));
});

document.getElementById('toggle-intel')?.addEventListener('click', () => {
  const expanded = toggleIntelBtn.getAttribute('aria-expanded') === 'true';
  toggleIntelBtn.setAttribute('aria-expanded', String(!expanded));
  if (intelBody) intelBody.hidden = expanded;
  toggleIntelBtn.textContent = expanded ? 'Show' : 'Hide';
});

toggleTipsBtn?.addEventListener('click', () => {
  const hidden = localStorage.getItem(TIPS_HIDDEN_KEY) === '1';
  localStorage.setItem(TIPS_HIDDEN_KEY, hidden ? '0' : '1');
  applyTipsVisibility();
});

saveNotesBtn?.addEventListener('click', saveNotes);
downloadNotesBtn?.addEventListener('click', () => {
  const who = (form?.elements?.prospect_name?.value || 'prospect').trim().replace(/\s+/g, '-');
  downloadText(`call-notes_${who}_${new Date().toISOString().slice(0, 10)}.txt`, notesArea.value || '');
});

copyScriptBtn?.addEventListener('click', async () => {
  const txt = outputEl?.innerText || ''; if (!txt.trim()) return;
  await navigator.clipboard.writeText(txt); setStatus('Copied script to clipboard.');
});
downloadScriptBtn?.addEventListener('click', () => {
  const txt = outputEl?.innerText || ''; if (!txt.trim()) return;
  const who = (form?.elements?.prospect_name?.value || 'script').trim().replace(/\s+/g, '-');
  downloadText(`conversation-guide_${who}_${new Date().toISOString().slice(0, 10)}.txt`, txt);
});

// Product registry
async function populateProductsForMode(mode) {
  const sel = document.getElementById('product');
  if (!sel) return;
  sel.innerHTML = `<option value="">Loading…</option>`;
  try {
    const idx = await loadProductIndex(mode);
    const items = Array.isArray(idx?.products) ? idx.products : Array.isArray(idx?.items) ? idx.items : Array.isArray(idx) ? idx : [];
    if (!items.length) { sel.innerHTML = `<option value="">No products found</option>`; console.warn('[Product] index.json loaded but empty', idx); return; }
    const saved = (form?.elements?.product?.value || '').trim().toLowerCase();
    sel.innerHTML = '<option value="" disabled>Select…</option>';
    for (const item of items) {
      const idLike = item.id ?? item.slug ?? item.value ?? item.key ?? item.name ?? item.label;
      const label = item.label ?? item.name ?? String(idLike ?? '');
      const val = String(idLike || '').trim().toLowerCase().replace(/\s+/g, '-');
      if (!val) continue;
      const opt = document.createElement('option'); opt.value = val; opt.textContent = label;
      if (saved && (saved === val || saved === String(label).toLowerCase())) opt.selected = true;
      sel.appendChild(opt);
    }
    if (!sel.value) sel.options[0].selected = true;
    validateField(sel);
    if (buyerBehSel?.value) {
      const label = sel?.options?.[sel.selectedIndex]?.text || sel.value || '';
      renderIntel(label, buyerBehSel.value);
    }
    refreshSubmitState();
  } catch (err) {
    console.error('[Product] Failed to load index.json', err);
    sel.innerHTML = `<option value="">Error loading products</option>`;
    setStatus('Could not load products index. See console for details.', 'error');
  }
}

// Submit: API-first
form && form.addEventListener("submit", async (e) => {
  e.preventDefault();
  for (const name of REQUIRED) {
    const el = form.elements[name] || document.getElementById(name);
    if (!el || !String(el.value || "").trim()) { validateField(el); el?.focus(); setStatus("Please complete required fields.", "error"); return; }
  }

  const values = {
    call_type: (callTypeSel?.value || "").trim(),
    seller_name: (form.elements.seller_name?.value || "").trim(),
    seller_company: (form.elements.seller_company?.value || "").trim(),
    prospect_name: (form.elements.prospect_name?.value || "").trim(),
    prospect_role: (form.elements.prospect_role?.value || "").trim(),
    prospect_company: (form.elements.prospect_company?.value || "").trim(),
    product: (productSel?.value || form.elements.product?.value || "").trim(),
    buyer_behaviour: (buyerBehSel?.value || form.elements.buyer_behaviour?.value || "").trim(),
    tone: (form.elements.tone?.value || "").trim(),
    script_length: (form.elements.script_length?.value || "").trim(),
    value_proposition: (form.elements.value_proposition?.value || "").trim(),
    context: (form.elements.context?.value || "").trim(),
    next_step: (form.elements.next_step?.value || "").trim(),
  };

  const mode = modeFromCallType(values.call_type);
  const productId = String(values.product).toLowerCase().trim();
  const buyerId = buyerCanon(values.buyer_behaviour);
  if (!productId) { validateField("product"); setStatus("Please choose a product.", "error"); return; }

  setBusy(true); setStatus("Generating call script…");
  if (diag) diag.open = false; if (diagJson) diagJson.textContent = "";

  try {
    const mdUrl = `/content/call-library/v1/${mode}/${productId}/${buyerId}.md`;
    let templateText = "";
    try { const res = await fetch(mdUrl, { cache: "no-store" }); if (res.ok) templateText = await res.text(); } catch { }
    const body = {
      kind: "call-script",
      basePrefix,
      variables: { ...values, mode, product: productId, buyerType: buyerId },
      templateMdText: templateText
    };

    const resp = await fetch("/api/generate", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    if (!resp.ok) { const errTxt = await resp.text(); throw new Error(`API ${resp.status}: ${errTxt}`); }

    const data = await resp.json();
    const script = data?.script?.text || "";
    const tips = data?.script?.tips || [];
    if (!script || !script.trim()) throw new Error("Empty script returned from API");

    const escapeRx = s => String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const rendered = renderSectionsToHtml(script, values);
    const bulletsHtml = renderBulletScriptFromSections(rendered.sections);
    lastRender = { html: rendered.html, bulletsHtml }; window.lastRender = lastRender;

    const htmlToShow = bulletMode ? bulletsHtml : rendered.html;
    outputEl.innerHTML = htmlToShow;
    modalScript.innerHTML = htmlToShow;
    if (toggleBulletsBtn) {
      toggleBulletsBtn.setAttribute('aria-pressed', String(bulletMode));
      toggleBulletsBtn.textContent = bulletMode
        ? 'Show full guide'
        : 'Show bullet point script';
    }

    outputMeta.textContent = `Library · ${productSel?.options?.[productSel.selectedIndex]?.text || productId} · ${values.buyer_behaviour || "—"} · ${values.call_type || "—"} · Tone: ${values.tone || "—"}`;

    tipsList.innerHTML = ""; (tips || []).forEach(t => { const li = document.createElement("li"); li.textContent = t; tipsList.appendChild(li); });
    applyTipsVisibility();

    try {
      const openerTips = await loadOpenersFromAssets();
      renderOpeners(openerTips);
    } catch { }
    applyOpenersVisibility();

    copyScriptBtn.disabled = false; downloadScriptBtn.disabled = false;

    const old = localStorage.getItem(OUTPUT_KEY) || "";
    const delta = computeDelta(old, script);
    renderDelta(delta);
    localStorage.setItem(OUTPUT_KEY, script);

    addActivity({ time: nowTime(), product: productId, buyer_behaviour: values.buyer_behaviour || "—", length: values.length || "—" });

    setStatus("Done. (Generated from API)");
    setBusy(false);
    document.getElementById("output-title")?.focus();
  } catch (err) {
    setStatus("Could not generate a call script. See Diagnostics for details.", "error");
    if (diag) { diag.open = true; diagJson.textContent = String(err?.message || err); }
    setBusy(false);
  }
});

// Follow-up email
document.getElementById('make-followup')?.addEventListener('click', async () => {
  try {
    const payload = {
      kind: 'call-followup',
      basePrefix,
      variables: {
        seller_name: (form.elements.seller_name?.value || "").trim(),
        seller_company: (form.elements.seller_company?.value || "").trim(),
        prospect_name: (form.elements.prospect_name?.value || "").trim(),
        prospect_role: (form.elements.prospect_role?.value || "").trim(),
        prospect_company: (form.elements.prospect_company?.value || "").trim(),
        tone: (form.elements.tone?.value || "").trim(),
      },
      scriptMdText: localStorage.getItem('first_call_script_v2.last_output') || "",
      callNotes: (notesArea?.value || "")
    };
    setStatus('Building follow-up email…');
    const r = await fetch('/api/generate', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    const data = await r.json();
    if (!r.ok) throw new Error(data?.error || 'Could not build follow-up');
    modalScript.innerText = data?.followup?.email || 'No email produced.';
    modal.showModal();
    setStatus('Follow-up ready.');
  } catch (e) {
    setStatus('Could not build follow-up email', 'error');
    diag.open = true; diagJson.textContent = String(e?.message || e);
  }
});

// Pop-out
if (popoutBtn) {
  popoutBtn.onclick = () => {
    const meta = (document.getElementById('output-meta')?.textContent || '').trim();
    const html = (window.bulletMode && window.lastRender?.bulletsHtml) || (window.lastRender?.html) || (outputEl?.innerHTML || "");
    if (!html || !html.trim()) { setStatus("No script to pop out yet.", "error"); return; }
    const w = window.open("", "_blank", "noopener,noreferrer");
    if (!w) { setStatus("Pop-out was blocked by the browser.", "error"); return; }
    w.document.open();
    w.document.write(`<!doctype html>
<meta charset="utf-8"><title>Conversation Guide</title>
<style>
  body{font:16.5px/1.65 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica Neue,Arial;padding:20px;background:#fff;color:#111}
  .meta{color:#6b7280;font-size:.9rem;margin-bottom:10px}
  .bar{display:flex;gap:8px;align-items:center;justify-content:flex-end;margin-bottom:8px}
  button{padding:.35rem .55rem;border:1px solid #e5e7eb;border-radius:8px;background:#f9fafb;cursor:pointer}
</style>
<div class="bar"><button onclick="window.print()">Print</button>
<button onclick="navigator.clipboard.writeText(document.body.innerText)">Copy</button></div>
<div class="meta">${esc(meta)}</div>
<div>${html}</div>`);
    w.document.close();
  };
}

// Unified reset handler
form?.addEventListener('reset', () => {
  queueMicrotask(() => {
    try {
      if (typeof callTypeSelXs !== 'undefined' && typeof callTypeSel !== 'undefined') {
        callTypeSelXs.value = callTypeSel.value || '';
      }

      try { localStorage.removeItem(STORAGE_KEY); } catch { }
      try { localStorage.removeItem(ACTIVITY_KEY); } catch { }
      try { localStorage.removeItem(OUTPUT_KEY); } catch { }
      try { localStorage.removeItem(notesKey?.() ?? ''); } catch { }

      outputEl && (outputEl.innerHTML = '');
      tipsList && (tipsList.innerHTML = '');
      activityLog && (activityLog.innerHTML = '');
      deltaLog && (deltaLog.innerHTML = '');
      statusEl && (statusEl.textContent = '');
      copyScriptBtn && (copyScriptBtn.disabled = true);
      downloadScriptBtn && (downloadScriptBtn.disabled = true);

      if (notesArea) {
        if ('value' in notesArea) notesArea.value = '';
        else notesArea.textContent = '';
      }

      form.querySelectorAll('.invalid')?.forEach(el => el.classList.remove('invalid'));

      typeof fillAll === 'function' && fillAll(['—']);
      typeof refreshSubmitState === 'function' && refreshSubmitState();

      document.querySelector('.panel.left')?.scrollTo({ top: 0, behavior: 'instant' });
      document.getElementById('seller_name')?.focus();

      typeof applyTipsVisibility === 'function' && applyTipsVisibility();
    } catch (e) {
      typeof setStatus === 'function' && setStatus('Could not reset form.', 'error');
    }
  });
});

// Init
(async function init() {
  try {
    const me = await fetch('/.auth/me', { cache: 'no-store' });
    const j = await me.json();
    const email = j?.clientPrincipal?.userDetails || j?.clientPrincipal?.identityProvider || '';
    if (email && userBadge && userEmail) { userEmail.textContent = email; userBadge.classList.remove('is-hidden'); }
  } catch { }

  try { const res = await fetch(INTEL_URL, { cache: 'no-store' }); intel = normaliseIntel(await res.json()); }
  catch { intel = { products: {} }; }

  loadForm(); loadCallPref();
  if (callTypeSelXs && callTypeSel) { callTypeSelXs.value = callTypeSel.value || ''; }

  const currentMode = modeFromCallType(callTypeSel?.value || 'direct');
  await populateProductsForMode(currentMode);
  if (productSel?.value && buyerBehSel?.value) {
    const label = productSel?.options?.[productSel.selectedIndex]?.text ?? productSel?.value ?? '';
    renderIntel(label, buyerBehSel.value);
  }

  applyTipsVisibility();
  const hasText = !!(outputEl?.textContent?.trim());
  copyScriptBtn.disabled = !hasText;
  downloadScriptBtn.disabled = !hasText;
  submitBtn.disabled = !allRequiredFilled(form);
  loadNotes();

  try {
    const openerTips = await loadOpenersFromAssets();
    renderOpeners(openerTips);
  } catch { }
  applyOpenersVisibility();

  document.addEventListener('input', (e) => {
    if (e.target.closest('#' + FORM_ID)) { validateField(e.target); saveForm(); refreshSubmitState(); }
    if (e.target === callTypeSel) saveCallPrefIfRequested();
  });
})();
