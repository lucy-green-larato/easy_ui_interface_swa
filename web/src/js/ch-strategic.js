/* web/src/js/ch-strategic.js */
/* eslint-disable no-console */
(() => {
  "use strict";

  // ---------- Utilities ----------
  const qs = (sel, root = document) => root.querySelector(sel);
  const byId = (id) => document.getElementById(id);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const text = (el, value) => { if (el) el.textContent = value ?? ""; };
  const show = (el) => el && el.removeAttribute("hidden");
  const hide = (el) => el && el.setAttribute("hidden", "hidden");
  const setAriaExpanded = (btn, expanded) => btn?.setAttribute("aria-expanded", String(expanded));

  // CSV helpers (client-side preview/counters only; server is source of truth)
  const stripBom = (s) => (s && s.charCodeAt(0) === 0xfeff ? s.slice(1) : s);
  const parseCsvRows = (textContent) => {
    const s = stripBom(textContent ?? "");
    const rows = [];
    let i = 0, q = false, field = "", row = [];
    while (i < s.length) {
      const c = s[i];
      if (q) {
        if (c === '"') { if (s[i + 1] === '"') { field += '"'; i += 2; continue; } q = false; i++; continue; }
        field += c; i++; continue;
      }
      if (c === '"') { q = true; i++; continue; }
      if (c === ",") { row.push(field); field = ""; i++; continue; }
      if (c === "\n" || c === "\r") { if (c === "\r" && s[i + 1] === "\n") i++; row.push(field); rows.push(row); row = []; field = ""; i++; continue; }
      field += c; i++;
    }
    row.push(field); rows.push(row);
    while (rows.length && rows.at(-1).every((c) => (c ?? "").trim() === "")) rows.pop();
    return rows;
  };

  // --- Server limits (read from /api/ch-strategic/health) -----------------------
  let SERVER_LIMITS = { maxUploadBytes: 10 * 1024 * 1024, maxRows: 5000, chunkSize: 100 }; // safe defaults

  async function loadServerLimits() {
    try {
      const r = await fetch('/api/ch-strategic/health', { cache: 'no-store' });
      if (!r.ok) return;
      const j = await r.json();
      if (j?.limits?.maxUploadBytes) SERVER_LIMITS.maxUploadBytes = Number(j.limits.maxUploadBytes);
      if (j?.limits?.maxRows) SERVER_LIMITS.maxRows = Number(j.limits.maxRows);
      if (j?.limits?.chunkSize) SERVER_LIMITS.chunkSize = Number(j.limits.chunkSize);
    } catch { /* use defaults */ }
  }

  function maxUploadLabelClient() {
    return `${Math.max(1, Math.floor((SERVER_LIMITS.maxUploadBytes || 0) / 1048576))} MB`;
  }

  // --- resilient fetch helper ---------------------------------------------------
  function withTimeout(promise, ms, aborter) {
    let to;
    const timeout = new Promise((_, rej) => { to = setTimeout(() => { aborter?.abort(); rej(new Error("Request timeout")); }, ms); });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(to));
  }

  async function getJSON(url, { timeout = 12000, retries = 2, signal } = {}) {
    let lastErr;
    for (let i = 0; i <= retries; i++) {
      const controller = new AbortController();
      if (signal) signal.addEventListener("abort", () => controller.abort(), { once: true });
      try {
        const res = await withTimeout(fetch(url, { cache: "no-store", signal: controller.signal }), timeout, controller);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return await res.json().catch(() => ({}));
      } catch (err) {
        lastErr = err;
        if (i === retries) break;
        await sleep(300 * Math.pow(2, i));
      }
    }
    throw lastErr || new Error("Network error");
  }

  // Client-side limits (mirror server)
  const CLIENT_LIMITS = {
    MAX_BYTES: 10 * 1024 * 1024,            // 10 MB
    EVIDENCE_MAX: 50,
    MAX_TERMS: 10,
    TERM_RE: /^[A-Za-z0-9 _-]{1,50}$/
  };

  function looksLikeCsv(file) {
    if (!file) return false;
    const t = (file.type || "").toLowerCase();
    if (t.includes("text/csv") || t === "text/plain" || t === "application/vnd.ms-excel" ||
      t === "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") return true;
    return /\.csv$/i.test(file.name || "");
  }

  // ---------- State ----------
  const state = {
    callType: "Direct",
    csvFile: /** @type {File|null} */ (null),
    evidence: "",
    run: null // { runId, polling, pollAbort, downloadEnabled }
  };

  // ---------- Elements ----------
  const el = {
    // header
    callType: byId("call_type"),
    remember: byId("remember_call_type"),
    mapLabel: byId("map-label"),
    helpBtn: byId("help"),
    userBadge: byId("user-badge"),
    userEmail: byId("user-email"),

    // form
    form: byId("ch-strategic-form"),
    evidence: byId("evidence_tag"),
    evidenceError: byId("evidence_error"),
    csv: byId("csv_file"),
    csvError: byId("csv_error"),
    analyze: byId("analyzeBtn"),         // uses worker flow now
    startLarge: byId("startLargeBtn"),   // alias to same flow
    reset: byId("resetBtn"),
    status: byId("status"),
    diag: byId("diag-json"),

    // right rail toggles + skipped details
    intelToggle: byId("toggle-intel"),
    intelBody: byId("intel-body"),
    openersBtn: qs('button[aria-controls="openers-body"]'),
    openersBody: byId("openers-body"),
    tipsBtn: qs('button[aria-controls="tips-body"]'),
    tipsBody: byId("tips-body"),
    skippedDetails: byId("skippedDetails"),

    // progress/counters
    progress: byId("progress"),
    progressFill: byId("progressFill"),
    countTotal: byId("count-total"),
    countProcessed: byId("count-processed"),
    countMatched: byId("count-matched"),
    countSkipped: byId("count-skipped"),

    // results & downloads
    resultsMatchesTitle: byId("matches-title"),
    resultsMatches: byId("results-matches"),
    resultsSkippedTitle: byId("skipped-title"),
    resultsSkipped: byId("results-skipped"),
    downloadContainer: byId("downloadContainer"),
    btnDlResults: byId("chs-download-results"),
    btnDlLog: byId("chs-download-log"),

    // feedback
    fbCard: byId("feedbackCard"),
    fbDetails: byId("fbDetails"),
    fbUp: byId("fbUp"),
    fbDown: byId("fbDown"),
    fbComment: byId("fbComment"),
    fbSample: byId("fbSample"),
    fbStatus: byId("fbStatus")
  };

  // ---------- Storage (remember call type) ----------
  const STORAGE_KEYS = { CALL_TYPE: "itt.call_type" };
  function loadCallType() {
    const remembered = localStorage.getItem(STORAGE_KEYS.CALL_TYPE);
    if (remembered && el.callType) {
      state.callType = remembered;
      el.callType.value = remembered;
      text(el.mapLabel, remembered);
      if (el.remember) el.remember.checked = true;
    }
  }
  function persistCallType() {
    if (el.remember?.checked) localStorage.setItem(STORAGE_KEYS.CALL_TYPE, state.callType);
  }

  // ---------- SWA badge ----------
  async function loadUserBadge() {
    try {
      const res = await fetch("/.auth/me", { redirect: "manual", cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();
      const principal = data?.clientPrincipal;
      const email = principal?.userDetails || principal?.userId;
      if (email && el.userBadge && el.userEmail) {
        text(el.userEmail, email);
        el.userBadge.classList.remove("is-hidden");
      }
    } catch { /* no-op */ }
  }

  // ---------- UI bits ----------
  function toggleSection(btn, body) {
    if (!btn || !body) return;
    btn.addEventListener("click", () => {
      const showIt = body.hasAttribute("hidden");
      if (showIt) show(body); else hide(body);
      setAriaExpanded(btn, showIt);
    });
  }

  function renderStatus(msg, type = "info", { openSkipped = false } = {}) {
    if (!el.status) return;
    el.status.className = `status ${type}`;
    while (el.status.firstChild) el.status.removeChild(el.status.firstChild);

    const textNode = document.createElement("span");
    textNode.textContent = String(msg ?? "");
    el.status.appendChild(textNode);

    if (openSkipped) {
      const space = document.createTextNode(" ");
      const btn = document.createElement("button");
      btn.type = "button";
      btn.id = "status-open-skipped";
      btn.className = "btn inline";
      btn.textContent = "View details";
      btn.addEventListener("click", () => {
        const d = el.skippedDetails;
        if (!d) return;
        try { d.open = true; } catch { }
        d.removeAttribute("hidden");
        d.scrollIntoView({ behavior: "smooth", block: "start" });
      });
      el.status.appendChild(space);
      el.status.appendChild(btn);
    }
  }

  function setCounters({ total = 0, processed = 0, matched = 0, skipped = 0 }) {
    text(el.countTotal, `Total: ${total}`); el.countTotal && (el.countTotal.dataset.count = String(total));
    text(el.countProcessed, `Processed: ${processed}`); el.countProcessed && (el.countProcessed.dataset.count = String(processed));
    text(el.countMatched, `Matches: ${matched}`); el.countMatched && (el.countMatched.dataset.count = String(matched));
    text(el.countSkipped, `Skipped: ${skipped}`); el.countSkipped && (el.countSkipped.dataset.count = String(skipped));
  }

  function setProgressPercent(percent) {
    if (!el.progress || !el.progressFill) return;
    if (percent == null) {
      hide(el.progress); el.progress.setAttribute("aria-hidden", "true"); el.progressFill.style.width = "0%"; return;
    }
    show(el.progress); el.progress.removeAttribute("aria-hidden");
    el.progressFill.style.width = `${Math.max(0, Math.min(100, percent))}%`;
  }

  function clearResults() {
    if (el.resultsMatches) el.resultsMatches.innerHTML = "";
    if (el.resultsSkipped) el.resultsSkipped.innerHTML = "";
    if (el.resultsMatchesTitle) el.resultsMatchesTitle.setAttribute("hidden", "hidden");
    if (el.resultsSkippedTitle) el.resultsSkippedTitle.setAttribute("hidden", "hidden");
    if (el.downloadContainer) el.downloadContainer.innerHTML = "";
    if (el.btnDlResults) el.btnDlResults.setAttribute("disabled", "disabled");
    if (el.btnDlLog) el.btnDlLog.setAttribute("disabled", "disabled");
    setCounters({ total: 0, processed: 0, matched: 0, skipped: 0 });
    setProgressPercent(null);
  }

  function appendResultItem({ type, companyNumber, companyName, message }) {
    const line = document.createElement("div");
    line.className = `result ${type || "info"}`;
    const parts = [];
    if (type) parts.push(`${type}:`);
    if (companyName) parts.push(`Name=${companyName}`);
    if (companyNumber) parts.push(`• Number=${companyNumber}`);
    if (message) parts.push(`• ${message}`);
    line.textContent = parts.join(" ");

    if (type === "match") {
      if (el.resultsMatchesTitle) el.resultsMatchesTitle.removeAttribute("hidden");
      el.resultsMatches?.appendChild(line);
    } else {
      if (el.resultsSkippedTitle) el.resultsSkippedTitle.removeAttribute("hidden");
      el.resultsSkipped?.appendChild(line);
    }
  }

  async function renderResultsPanel(runId) {
    try {
      const resp = await fetch(`/api/ch-strategic/download?runId=${encodeURIComponent(runId)}&file=results`);
      if (!resp.ok) return;
      const json = await resp.json();
      const items = Array.isArray(json?.items) ? json.items : [];
      const total = Number(json?.summary?.rows || items.length || 0);

      setCounters({ total, processed: total, matched: total, skipped: 0 });
      setProgressPercent(100);

      if (el.resultsMatches) el.resultsMatches.innerHTML = "";
      if (el.resultsSkipped) el.resultsSkipped.innerHTML = "";
      el.resultsMatchesTitle?.removeAttribute("hidden");
      el.resultsSkippedTitle?.setAttribute("hidden", "hidden");

      const sample = items.slice(0, 200);
      for (const m of sample) {
        appendResultItem({
          type: "match",
          companyNumber: m["Company Number"] || m.companyNumber || m.number || "",
          companyName: m["Company Name"] || m.companyName || m.name || ""
        });
      }
      if (items.length > sample.length) {
        appendResultItem({ type: "match", message: `…and ${items.length - sample.length} more` });
      }
    } catch { /* ignore preview errors */ }
  }

  function wireDownloadButtons(runId) {
    const enable = () => {
      if (el.btnDlResults) {
        el.btnDlResults.removeAttribute("disabled");
        el.btnDlResults.onclick = () => {
          window.location.href = `/api/ch-strategic/download?runId=${encodeURIComponent(runId)}&file=results`;
        };
      }
      if (el.btnDlLog) {
        el.btnDlLog.removeAttribute("disabled");
        el.btnDlLog.onclick = () => {
          window.location.href = `/api/ch-strategic/download?runId=${encodeURIComponent(runId)}&file=log`;
        };
      }
    };

    // If you don't have the two fixed buttons, render a fallback button
    if (!el.btnDlResults && !el.btnDlLog && el.downloadContainer) {
      el.downloadContainer.innerHTML = "";
      const btn = document.createElement("button");
      btn.type = "button"; btn.className = "btn"; btn.textContent = "Download Results";
      btn.addEventListener("click", () => window.location.href = `/api/ch-strategic/download?runId=${encodeURIComponent(runId)}&file=results`);
      el.downloadContainer.appendChild(btn);
      return;
    }

    enable();
  }

  function showFeedbackCard(showDetails = false) {
    if (!el.fbCard) return;
    el.fbCard.hidden = false;
    if (el.fbDetails) el.fbDetails.hidden = !showDetails;
  }

  function wireFeedbackUI() {
    if (!el.fbUp || !el.fbDown || !el.fbStatus) return;

    el.fbUp.addEventListener("click", () => {
      if (el.fbDetails) el.fbDetails.hidden = true;
      el.fbUp.setAttribute("aria-pressed", "true");
      el.fbDown.setAttribute("aria-pressed", "false");
    });
    el.fbDown.addEventListener("click", () => {
      if (el.fbDetails) el.fbDetails.hidden = false;
      el.fbDown.setAttribute("aria-pressed", "true");
      el.fbUp.setAttribute("aria-pressed", "false");
    });

    const submit = byId("fbSubmit");
    submit?.addEventListener("click", async () => {
      const runId = state.run?.runId || "";
      const useful = el.fbDown.getAttribute("aria-pressed") === "true" ? "down" : "up";
      const comment = (el.fbComment?.value || "").trim().slice(0, 500);
      const includeSample = !!el.fbSample?.checked;
      const tags = Array.from(document.querySelectorAll('#fbDetails input[type="checkbox"]'))
        .filter(x => x.checked).map(x => x.value);

      const totals = {
        total: Number(el.countTotal?.dataset.count || 0),
        matched: Number(el.countMatched?.dataset.count || 0),
        skipped: Number(el.countSkipped?.dataset.count || 0)
      };
      const evidenceTag = (el.evidence?.value || "").trim().slice(0, 50);

      if (!runId) { el.fbStatus.textContent = "Run not found."; return; }
      el.fbStatus.textContent = "Sending…";

      // Server expects { runId, note }, so we serialize details into "note"
      const noteObj = { useful, tags, comment, includeSample, evidenceTag, totals };
      try {
        const r = await fetch("/api/ch-strategic/feedback", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ runId, note: JSON.stringify(noteObj) })
        });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        el.fbStatus.textContent = "Thanks for the feedback!";
      } catch {
        el.fbStatus.textContent = "Failed to send feedback. Please try again later.";
      }
    });
  }

  // --- Global error visibility (no silent failures) ---
  window.addEventListener("unhandledrejection", (e) => {
    const msg = (e && e.reason && e.reason.message) ? e.reason.message : "Unexpected error";
    appendResultItem({ type: "error", message: msg });
  });
  window.addEventListener("error", (e) => {
    appendResultItem({ type: "error", message: e.message || "Unexpected error" });
  });

  // ---------- API ----------
  async function apiStartRun({ file, evidenceTag }) {
    const fd = new FormData();
    // Server reads the file (field name is not enforced but we use "file")
    fd.append("file", file, file.name);
    if (evidenceTag) fd.append("evidenceTag", evidenceTag); // server ignores extra fields safely
    const res = await fetch("/api/ch-strategic/start", { method: "POST", body: fd });
    if (!res.ok) {
      let msg = "";
      try { msg = await res.text(); } catch { }
      throw new Error(`Start failed (${res.status})${msg ? `: ${msg}` : ""}`);
    }
    return res.json(); // { runId }
  }

  async function apiPollStatus(runId, { signal } = {}) {
    const s = await getJSON(`/api/ch-strategic/status?runId=${encodeURIComponent(runId)}`, { signal });
    const total = Number(s.totalRows || 0);
    const processed = Number(s.processedRows || 0);
    const completed = s.state === "Completed" || s.state === "Failed";
    return {
      total,
      processed,
      matched: Number(s.matched || 0), // optional, default 0
      skipped: Number(s.skipped || 0), // optional, default 0
      messages: s.messages || null,
      completed,
      state: s.state
    };
  }

  // ---------- Flows ----------
  async function startAndPollWorkerFlow({ file, evidence }) {
    // Start
    renderStatus("Submitting job…");
    const statusEl = el.status;
    if (statusEl) { statusEl.setAttribute("aria-busy", "true"); statusEl.focus?.(); }

    const { runId } = await apiStartRun({ file, evidenceTag: evidence });
    if (!runId) throw new Error("No runId returned.");

    state.run = { runId, polling: true, pollAbort: new AbortController(), downloadEnabled: false };

    // Wire download buttons (enabled on completion)
    wireDownloadButtons(runId);

    // Poll
    const startedAt = Date.now();
    while (state.run.polling) {
      const status = await apiPollStatus(runId, { signal: state.run.pollAbort.signal });

      // Counters & progress
      setCounters({
        total: status.total,
        processed: status.processed,
        matched: status.matched,
        skipped: status.skipped
      });
      const pct = status.total > 0 ? Math.round((status.processed / status.total) * 100) : (status.completed ? 100 : 0);
      setProgressPercent(pct);
      renderStatus(`Processing… ${status.processed}/${status.total}`, "info");

      // Messages (if any)
      if (Array.isArray(status.messages)) status.messages.forEach((m) => appendResultItem(m));

      if (status.completed) {
        state.run.polling = false;
        setProgressPercent(100);

        // Enable the fixed download buttons (href already wired)
        if (el.btnDlResults) el.btnDlResults.removeAttribute("disabled");
        if (el.btnDlLog) el.btnDlLog.removeAttribute("disabled");

        const hadIssues = status.skipped > 0;
        renderStatus(
          status.state === "Failed"
            ? "Job failed. See log for details."
            : "Job completed. You can download results.",
          status.state === "Failed" ? "error" : (hadIssues ? "warn" : "info"),
          { openSkipped: hadIssues }
        );

        // Show feedback card; open details if no matches or issues occurred
        const showDetails = (status.state === "Failed") || (status.matched === 0) || hadIssues;
        showFeedbackCard(showDetails);

        if (statusEl) statusEl.setAttribute("aria-busy", "false");
        break;
      }

      // Backoff: 0–20s @1s, 20–60s @1500ms, 60s+ @3000ms
      const ageSec = (Date.now() - startedAt) / 1000;
      const delay = ageSec < 20 ? 1000 : ageSec < 60 ? 1500 : 3000;
      await sleep(delay);
    }

    state.run?.pollAbort?.abort();
    if (statusEl) statusEl.setAttribute("aria-busy", "false");
    await renderResultsPanel(state.run.runId);
  }

  // ---------- Form wiring & UX ----------
  function validateClientInputs(file, evidence) {
    let fileErr = "", evidenceErr = "";

    // File checks
    if (!file) {
      fileErr = "Provide a CSV file.";
    } else if (!looksLikeCsv(file)) {
      fileErr = "Please upload a CSV file.";
    } else if (typeof SERVER_LIMITS?.maxUploadBytes === "number" && file.size > SERVER_LIMITS.maxUploadBytes) {
      // Use live server limit (from /health); falls back to helper label
      fileErr = `File too large. Max ${typeof maxUploadLabelClient === "function" ? maxUploadLabelClient() : "configured size"}.`;
    }

    // Evidence checks (comma-separated terms)
    const ev = (evidence || "").trim();
    if (!ev) {
      evidenceErr = "Enter a keyword or phrase.";
    } else {
      const terms = Array.from(new Set(ev.split(",").map(t => t.trim()).filter(Boolean)));
      if (terms.length === 0) {
        evidenceErr = "Enter a keyword or phrase.";
      } else if (terms.length > CLIENT_LIMITS.MAX_TERMS) {
        evidenceErr = `Too many evidence terms (max ${CLIENT_LIMITS.MAX_TERMS}).`;
      } else if (!terms.every(t => CLIENT_LIMITS.TERM_RE.test(t))) {
        evidenceErr = `Each term up to ${CLIENT_LIMITS.EVIDENCE_MAX} chars (A–Z a–z 0–9 space _ -).`;
      }
    }

    if (el.csvError) el.csvError.textContent = fileErr;
    if (el.evidenceError) el.evidenceError.textContent = evidenceErr;

    return { ok: !fileErr && !evidenceErr };
  }

  function updateAnalyzeState() {
    const ev = (el.evidence?.value || "").trim();
    const f = state.csvFile;

    let evidenceLooksOk = false;
    if (ev) {
      const terms = Array.from(new Set(ev.split(",").map(t => t.trim()).filter(Boolean)));
      evidenceLooksOk = terms.length > 0 &&
        terms.length <= CLIENT_LIMITS.MAX_TERMS &&
        terms.every(t => CLIENT_LIMITS.TERM_RE.test(t));
    }
    const fileLooksOk = !!f && looksLikeCsv(f) && f.size <= CLIENT_LIMITS.MAX_BYTES;

    if (el.analyze) el.analyze.disabled = !(evidenceLooksOk && fileLooksOk);
    if (el.startLarge) el.startLarge.disabled = !(evidenceLooksOk && fileLooksOk);
  }

  function wireRightRail() {
    toggleSection(el.intelToggle, el.intelBody);
    toggleSection(el.openersBtn, el.openersBody);
    toggleSection(el.tipsBtn, el.tipsBody);
  }

  function wireHelp() {
    el.helpBtn?.addEventListener("click", () => {
      const msg = [
        "This tool scans the Strategic Report section for your evidence tag.",
        "• Evidence tag is case-insensitive and limited to letters, digits, space, underscore, hyphen (max 50 chars).",
        "• CSV must include Company Name and Company Number.",
        "• Jobs run in the background; downloads enable when results/log are ready."
      ].join("\n");
      alert(msg);
    });
  }

  function bindPicker() {
    const fileEl = byId("csv_file");
    const btn = byId("pick-file");
    if (!fileEl || !btn) return;
    fileEl.disabled = false;
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      try {
        if (typeof fileEl.showPicker === "function") fileEl.showPicker();
        else fileEl.click();
      } catch { fileEl.click(); }
    }, { passive: false });
  }

  function wireForm() {
    // Call type + remember
    el.callType?.addEventListener("change", () => {
      state.callType = el.callType.value;
      text(el.mapLabel, state.callType);
      persistCallType();
    });
    el.remember?.addEventListener("change", persistCallType);

    // Inputs
    el.evidence?.addEventListener("input", () => {
      state.evidence = (el.evidence.value || "").trim();
      updateAnalyzeState();
    });
    el.csv?.addEventListener("change", () => {
      state.csvFile = el.csv.files?.[0] || null;
      updateAnalyzeState();
    });

    // Start (Analyze == Large run now)
    const startHandler = async (evt) => {
      evt.preventDefault();
      clearResults();
      const { ok } = validateClientInputs(state.csvFile, state.evidence);
      if (!ok) return;
      try {
        await startAndPollWorkerFlow({ file: state.csvFile, evidence: state.evidence });
      } catch (err) {
        renderStatus("Start failed.", "error");
        appendResultItem({ type: "error", message: err.message || String(err) });
      }
    };
    el.analyze?.addEventListener("click", startHandler);
    el.startLarge?.addEventListener("click", startHandler);

    // Reset
    el.reset?.addEventListener("click", (evt) => {
      evt.preventDefault();
      el.form?.reset?.();
      state.csvFile = null;
      state.evidence = "";
      clearResults();
      updateAnalyzeState();
    });

    // Enter key on evidence triggers Analyze
    el.evidence?.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !el.analyze?.disabled) el.analyze.click();
    });
  }

  // Pause polling when tab hidden
  document.addEventListener("visibilitychange", () => {
    if (document.hidden && state.run?.pollAbort) {
      try { state.run.pollAbort.abort(); } catch { }
    }
  });
  window.addEventListener("beforeunload", () => {
    if (state.run?.pollAbort) { try { state.run.pollAbort.abort(); } catch { } }
  });

  // ---------- Init ----------
  async function init() {
    if (el.status) { el.status.setAttribute("aria-live", "polite"); el.status.setAttribute("tabindex", "-1"); }
    await loadServerLimits();
    loadCallType();
    updateAnalyzeState();
    wireForm();
    bindPicker();
    wireRightRail();
    wireHelp();
    wireFeedbackUI();
    loadUserBadge();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
