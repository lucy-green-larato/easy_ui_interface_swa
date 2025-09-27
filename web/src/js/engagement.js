// /web/src/js/engagement.js
// Vanilla browser module. No external deps.

const API_BASE = "/api"; // via SWA reverse proxy
const CALL_LIB_BASE = (window.CALL_LIB_BASE || "https://sales-tools.larato.co.uk").replace(/\/+$/, "");

async function loadCallLibraryIndex() {
  try {
    const r = await fetch(`${CALL_LIB_BASE}/content/call-library/v1/index.json`, { cache: "no-store" });
    if (!r.ok) return null;
    return await r.json();
  } catch { return null; }
}

function $(sel) { return document.querySelector(sel); }

function setBusy(isBusy) {
  const btn = $("#eng-generate");
  const status = $("#eng-status");
  if (btn) { btn.disabled = !!isBusy; btn.classList.toggle("is-loading", !!isBusy); }
  if (status) { status.textContent = isBusy ? "Generating…" : ""; }
}

async function generate() {
  const topic = $("#eng-topic")?.value?.trim() || "";
  const audience = $("#eng-audience")?.value?.trim() || "";
  const tone = $("#eng-tone")?.value || "Professional (corporate)";
  const length = $("#eng-length")?.value || "300";
  const templateId = $("#eng-template")?.value?.trim() || "";

  const outEl = $("#eng-output");
  const status = $("#eng-status");

  if (!topic || !audience) {
    if (status) status.textContent = "Please provide topic and audience.";
    return;
  }

  setBusy(true);
  try {
    const r = await fetch(`${API_BASE}/engagement-generate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ topic, audience, tone, length, templateId })
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) {
      if (status) status.textContent = data?.message || "Request failed";
      return;
    }
    const metaLine = data?.meta ? `\n\n---\n_model: ${data.meta.model} · ${data.meta.durationMs}ms_` : "";
    if (outEl) outEl.textContent = (data.content || "").trim() + metaLine;
    if (status) status.textContent = "Done.";
  } catch (e) {
    if (status) status.textContent = "Unexpected error.";
  } finally {
    setBusy(false);
  }
}

(async function init() {
  // best-effort template index load (optional)
  try {
    const idx = await loadCallLibraryIndex();
    if (idx && Array.isArray(idx.templates) && $("#eng-template") && !$("#eng-template").value) {
      // If your select is empty, we could populate it—BUT per brief we only rely on hooks, not DOM structure.
      // We'll just store it globally for any custom UI.
      window._callLibIndex = idx;
    }
  } catch {}
  $("#eng-generate")?.addEventListener("click", generate);
})();
