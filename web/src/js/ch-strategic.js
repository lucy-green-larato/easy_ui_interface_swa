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

  // CSV helpers (for counts/preview/PBI parsing client-side; server is source of truth)
  const stripBom = (s) => (s && s.charCodeAt(0) === 0xfeff ? s.slice(1) : s);
  const parseCsvRows = (textContent) => {
    // Minimal robust CSV (quotes, commas, CRLF)
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
  const normaliseHeader = (s) => (s ?? "").trim().toLowerCase();

  function setDownloadAnchor(href) {
    if (!href) return;
    let a = document.getElementById('download-link');
    if (!a) {
      const fileInput = document.getElementById('csv_file');
      a = document.createElement('a');
      a.id = 'download-link';
      a.download = 'ch-strategic.csv';
      a.textContent = 'Download CSV';
      a.href = href;
      a.setAttribute('hidden', 'hidden');
      if (fileInput && fileInput.parentNode) {
        fileInput.parentNode.appendChild(a);
      } else {
        document.body.appendChild(a);
      }
    } else {
      a.href = href;
    }
    a.removeAttribute('hidden');
  }

  // --- resilient fetch helper ---------------------------------------------------
  function withTimeout(promise, ms, aborter) {
    let to;
    const timeout = new Promise((_, rej) => { to = setTimeout(() => { aborter?.abort(); rej(new Error('Request timeout')); }, ms); });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(to));
  }

  // Basic retry (idempotent GETs only)
  async function getJSON(url, { timeout = 12000, retries = 2, signal } = {}) {
    let lastErr;
    for (let i = 0; i <= retries; i++) {
      const controller = new AbortController();
      const chained = signal ? (signal.addEventListener('abort', () => controller.abort()), controller.signal) : controller.signal;
      try {
        const res = await withTimeout(fetch(url, { cache: 'no-store', signal: chained }), timeout, controller);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return await res.json().catch(() => ({})); // never throw on JSON parse
      } catch (err) {
        lastErr = err;
        if (i === retries) break;
        await sleep(300 * Math.pow(2, i)); // 300ms, 600ms backoff
      }
    }
    throw lastErr || new Error('Network error');
  }

  // Build tiny CSV from matches (fallback for small inline result)
  function toCsv(rows, header) {
    const esc = (v) => {
      const s = String(v ?? "");
      return /[",\n\r]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
    };
    const out = [];
    if (header?.length) out.push(header.map(esc).join(","));
    for (const r of rows) out.push(r.map(esc).join(","));
    return out.join("\r\n");
  }

  // Blob download helper
  function downloadBlob({ name, type = "text/csv;charset=utf-8", content }) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = name; document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
  }
  const nowStamp = () => {
    const d = new Date(), pad = (n) => `${n}`.padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}`;
  };

  // --- Client-side limits (keep in sync with server) ---------------------------
  // Server validates: length <= 50 and /^[A-Za-z0-9 _-]*$/
  const CLIENT_LIMITS = {
    MAX_BYTES: 20 * 1024 * 1024,          // 20 MB (match server MAX_SIZE)
    EVIDENCE_MAX: 50,                      // max chars
    EVIDENCE_RE: /^[A-Za-z0-9 _-]{1,50}$/, // allowed chars
  };

  // Quick file-type check (MIME or extension)
  function looksLikeCsv(file) {
    if (!file) return false;
    const t = (file.type || '').toLowerCase();
    if (t.includes('text/csv') || t === 'text/plain' || t === 'application/vnd.ms-excel') return true;
    return /\.csv$/i.test(file.name || '');
  }

  // Validate inputs; returns { ok, errs } and fills UI error slots
  function validateClientInputs(file, evidence) {
    const errs = { file: '', evidence: '' };

    if (!file) errs.file = 'Provide a CSV file.';
    else {
      if (!looksLikeCsv(file)) errs.file = 'Please upload a CSV file.';
      else if (file.size > CLIENT_LIMITS.MAX_BYTES) {
        const mb = (CLIENT_LIMITS.MAX_BYTES / (1024 * 1024)) | 0;
        errs.file = `File too large. Max ${mb} MB.`;
      }
    }

    const ev = (evidence || '').trim();
    if (!ev) errs.evidence = 'Enter a keyword or phrase.';
    else if (!CLIENT_LIMITS.EVIDENCE_RE.test(ev)) {
      errs.evidence = `Evidence may be up to ${CLIENT_LIMITS.EVIDENCE_MAX} chars (A–Z a–z 0–9 space _ -).`;
    }

    // Paint errors (XSS-safe)
    if (el.csvError) el.csvError.textContent = errs.file;
    if (el.evidenceError) el.evidenceError.textContent = errs.evidence;

    return { ok: !errs.file && !errs.evidence, errs };
  }

  // ---------- State ----------
  const state = {
    callType: "Direct",
    csvFile: /** @type {File|null} */ (null),  // Source CSV file (upload or synthesized from PBI)
    evidence: "",
    largeRun: null, // { jobId, polling, pollAbort?, downloadShown }
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
    analyze: byId("analyzeBtn"),
    startLarge: byId("startLargeBtn"),
    reset: byId("resetBtn"),
    status: byId("status"),
    diag: byId("diag-json"),

    // cancel button
    btnCancel: byId("btnCancel"),

    // right rail toggles + skipped details
    intelToggle: byId("toggle-intel"),
    intelBody: byId("intel-body"),
    openersBtn: qs('button[aria-controls="openers-body"]'),
    openersBody: byId("openers-body"),
    tipsBtn: qs('button[aria-controls="tips-body"]'),
    tipsBody: byId("tips-body"),
    skippedDetails: byId("skippedDetails"),

    // results & progress
    progress: byId("progress"),
    progressFill: byId("progressFill"),
    counters: byId("counters"),
    countTotal: byId("count-total"),
    countProcessed: byId("count-processed"),
    countMatched: byId("count-matched"),
    countSkipped: byId("count-skipped"),
    results: byId("results"),
    downloadContainer: byId("downloadContainer"),

    // PBI dialog
    pbiBtn: byId("importPbiBtn"),
    pbiDialog: byId("pbi-dialog"),
    pbiForm: byId("pbi-form"),
    pbiReportId: byId("pbi-report-id"),
    pbiVisual: byId("pbi-visual"),
    pbiStatus: byId("pbi-status"),
  };

  // Single alias for the primary start button (Analyze by default)
  let btnStart = el.analyze;

  // Enable/disable both start buttons together
  function setStartsDisabled(disabled) {
    if (el.analyze) el.analyze.disabled = !!disabled;
    if (el.startLarge) el.startLarge.disabled = !!disabled;
  }

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

  // ---------- Auth badge (SWA) ----------
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

  function updateAnalyzeState() {
    const ev = (el.evidence?.value || '').trim();
    const f = state.csvFile;
    const evidenceLooksOk = ev.length > 0 && ev.length <= CLIENT_LIMITS.EVIDENCE_MAX && CLIENT_LIMITS.EVIDENCE_RE.test(ev);
    const fileLooksOk = !!f && looksLikeCsv(f) && f.size <= CLIENT_LIMITS.MAX_BYTES;

    if (el.analyze) el.analyze.disabled = !(evidenceLooksOk && fileLooksOk);
    if (el.startLarge) el.startLarge.disabled = !(evidenceLooksOk && fileLooksOk);
  }

  function renderStatus(msg, type = "info", { openSkipped = false } = {}) {
    if (!el.status) return;
    el.status.className = `status ${type}`;
    // clear existing content
    while (el.status.firstChild) el.status.removeChild(el.status.firstChild);

    // add message safely
    const textNode = document.createElement('span');
    textNode.textContent = String(msg ?? '');
    el.status.appendChild(textNode);

    // optional "View details" button (fixed markup, no user content)
    if (openSkipped) {
      const space = document.createTextNode(' ');
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.id = 'status-open-skipped';
      btn.className = 'btn inline';
      btn.textContent = 'View details';
      btn.addEventListener('click', () => {
        const d = el.skippedDetails;
        if (!d) return;
        try { d.open = true; } catch { /* older browsers */ }
        d.removeAttribute('hidden');
        d.scrollIntoView({ behavior: 'smooth', block: 'start' });
      });
      el.status.appendChild(space);
      el.status.appendChild(btn);
    }
  }

  function setCounters({ total = 0, processed = 0, matched = 0, skipped = 0 }) {
    text(el.countTotal, `Total: ${total}`); el.countTotal && (el.countTotal.dataset.count = total);
    text(el.countProcessed, `Processed: ${processed}`); el.countProcessed && (el.countProcessed.dataset.count = processed);
    text(el.countMatched, `Matches: ${matched}`); el.countMatched && (el.countMatched.dataset.count = matched);
    text(el.countSkipped, `Skipped: ${skipped}`); el.countSkipped && (el.countSkipped.dataset.count = skipped);
  }

  function setProgress(percent) {
    if (!el.progress || !el.progressFill) return;
    if (percent == null) {
      hide(el.progress); el.progress.setAttribute("aria-hidden", "true"); el.progressFill.style.width = "0%"; return;
    }
    show(el.progress); el.progress.removeAttribute("aria-hidden");
    el.progressFill.style.width = `${Math.max(0, Math.min(100, percent))}%`;
  }

  function clearResults() {
    if (el.results) el.results.innerHTML = "";
    if (el.downloadContainer) el.downloadContainer.innerHTML = "";
    setCounters({ total: 0, processed: 0, matched: 0, skipped: 0 });
    setProgress(null);
  }

  // Safe result/status line (accepts string or object with details)
  function appendResultItem(item) {
    const container = el.results || el.status;
    if (!container) return;

    const div = document.createElement('div');
    div.className = 'status-item';

    if (item && typeof item === 'object') {
      const badge = document.createElement('strong');
      badge.textContent = (item.type || 'info') + ': ';
      const msg = document.createElement('span');
      const parts = [];
      if (item.companyName) parts.push(`Name=${item.companyName}`);
      if (item.companyNumber) parts.push(`Number=${item.companyNumber}`);
      if (item.message) parts.push(`Msg=${item.message}`);
      msg.textContent = parts.join(' • ') || '';
      div.appendChild(badge);
      div.appendChild(msg);
    } else {
      const msg = document.createElement('span');
      msg.className = 'msg';
      msg.textContent = String(item ?? '');
      div.appendChild(msg);
    }
    container.appendChild(div);
  }

  function renderDownloadButton({ filename, href, blobContent }) {
    if (!el.downloadContainer) return;
    el.downloadContainer.innerHTML = "";
    const btn = document.createElement("button");
    btn.type = "button"; btn.className = "btn"; btn.textContent = "Download CSV";
    btn.addEventListener("click", () => {
      if (href) { window.location.href = href; return; }
      if (blobContent) downloadBlob({ name: filename, content: blobContent });
    });
    el.downloadContainer.appendChild(btn);
  }

  function showFeedbackCard(showDetails = false) {
    const card = document.getElementById('feedbackCard');
    if (!card) return;
    card.hidden = false;
    const details = document.getElementById('fbDetails');
    if (details) details.hidden = !showDetails;
  }

  function wireFeedbackUI() {
    const up = document.getElementById('fbUp');
    const down = document.getElementById('fbDown');
    const details = document.getElementById('fbDetails');
    const submit = document.getElementById('fbSubmit');
    const status = document.getElementById('fbStatus');

    if (!up || !down || !submit) return;

    up.addEventListener('click', () => {
      details.hidden = true;
      up.setAttribute('aria-pressed', 'true'); down.setAttribute('aria-pressed', 'false');
    });
    down.addEventListener('click', () => {
      details.hidden = false;
      down.setAttribute('aria-pressed', 'true'); up.setAttribute('aria-pressed', 'false');
    });

    submit.addEventListener('click', async () => {
      const jobId = (state.largeRun && state.largeRun.jobId) || document.getElementById('job-id-badge')?.textContent || '';
      const useful = down.getAttribute('aria-pressed') === 'true' ? 'down' : 'up';
      const comment = (document.getElementById('fbComment')?.value || '').trim().slice(0, 500);
      const includeSample = !!document.getElementById('fbSample')?.checked;
      const tagEls = Array.from(document.querySelectorAll('#fbDetails input[type="checkbox"]'));
      const tags = tagEls.filter(x => x.checked).map(x => x.value);

      // totals from the counters
      const totals = {
        total: Number(el.countTotal?.dataset.count || 0),
        matched: Number(el.countMatched?.dataset.count || 0),
        skipped: Number(el.countSkipped?.dataset.count || 0)
      };

      const evidenceTag = (el.evidence?.value || '').trim().slice(0, 50);

      status.textContent = 'Sending…';
      try {
        const r = await fetch('/api/ch-strategic/feedback', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ jobId, useful, tags, comment, includeSample, evidenceTag, totals })
        });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        status.textContent = 'Thanks for the feedback!';
      } catch (e) {
        status.textContent = 'Failed to send feedback. Please try again later.';
      }
    });
  }

  // --- Global error visibility (no silent failures) --------------------------
  window.addEventListener('unhandledrejection', (e) => {
    const msg = (e && e.reason && e.reason.message) ? e.reason.message : 'Unexpected error';
    appendResultItem(`Error: ${msg}`);
  });
  window.addEventListener('error', (e) => {
    appendResultItem(`Error: ${e.message || 'Unexpected error'}`);
  });

  // ---------- File helpers ----------
  async function fileFromCsvString(csvText, name = "companies.csv") {
    return new File([csvText], name, { type: "text/csv;charset=utf-8" });
  }

  // ---------- API (spec-compliant endpoints) ----------
  async function apiSmallRunMultipart({ file, evidenceTag }) {
    const fd = new FormData();
    fd.append("csv_file", file);                // <-- field name MUST be csv_file
    if (evidenceTag) fd.append("evidenceTag", evidenceTag);
    return fetch("/api/ch-strategic", { method: "POST", body: fd });
  }

  // Large run start: POST /api/ch-strategic/start
  async function apiStartLargeRun({ file, evidenceTag }) {
    const fd = new FormData();
    fd.append("csv_file", file);               // <-- must be csv_file
    if (evidenceTag) fd.append("evidenceTag", evidenceTag);

    const res = await fetch("/api/ch-strategic/start", { method: "POST", body: fd });
    if (!res.ok) {
      let msg = '';
      try { msg = await res.text(); } catch { }
      throw new Error(`Start failed (${res.status})${msg ? `: ${msg}` : ''}`);
    }
    return res.json(); // { jobId, statusUrl, downloadUrl }
  }

  document.getElementById('download-link').href = downloadUrl;


  // Status uses PATH PARAM with jobId (normalized to UI shape)
  async function apiPollStatus(jobId, { signal } = {}) {
    const s = await getJSON(`/api/ch-strategic/status/${encodeURIComponent(jobId)}`, { signal });
    const total = Number(s.totalChunks || 0);
    const processed = Number(s.completedChunks || 0);
    const completed = s.state === 'done' || s.state === 'cancelled' || s.state === 'error';
    const canDownload = s.state === 'done' && !!s.outputBlob;
    return {
      total, processed,
      matched: Number(s.matched || 0),
      skipped: Number(s.skipped || 0),
      errorsByReason: s.errorsByReason || null,
      completed, canDownload,
      firstRowEmitted: canDownload,
      rows: processed,
      state: s.state
    };
  }

  // PBI export returns CSV (server handles MSAL + allowlist); we parse client-side
  async function apiPbiExport(payload) {
    const res = await fetch("/api/ch-strategic/pbi-export", { // <-- fixed path
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {}),
    });
    if (!res.ok) throw new Error(`Power BI export failed (${res.status})`);
    const csv = await res.text(); // CSV string
    if (!csv?.trim()) throw new Error("Empty CSV returned from Power BI export.");
    return csv;
  }

  // ---------- Flows ----------
  async function handleSmallOrUpgrade({ file, evidence }) {
    // Try small-run first (server decides). If >50, spec says server returns 400 with guidance to batch.
    renderStatus("Submitting for inline processing…");
    const statusEl = document.getElementById('status');
    if (statusEl) { statusEl.setAttribute('aria-busy', 'true'); statusEl.focus?.(); }
    const res = await apiSmallRunMultipart({ file, evidenceTag: evidence });

    if (res.status === 400) {
      renderStatus("List too large for inline processing. Starting background job…");
      return { upgraded: true };
    }
    if (!res.ok) throw new Error(`Request failed (${res.status})`);

    // Expect a compact inline result. Accept either a CSV body or JSON with matches.
    const contentType = res.headers.get("content-type") || "";
    if (contentType.includes("text/csv")) {
      // Server returned final CSV directly
      const csv = await res.text();
      const filename = `strategic-review_matches_${nowStamp()}.csv`;
      renderDownloadButton({ filename, blobContent: csv });
      setProgress(100);
      renderStatus("Job completed. You can download results.", "info");
      showFeedbackCard(false);
      return { upgraded: false, done: true };
    } else {
      const data = await res.json();
      const counts = data?.counts || {};
      setCounters(counts);
      setProgress(100);

      // Skipped/errors listing (if server provided detail)
      (data?.skipped || []).forEach((s) =>
        appendResultItem({ type: "skip", companyNumber: s.companyNumber, companyName: s.companyName, message: s.reason })
      );
      (data?.errors || []).forEach((e) =>
        appendResultItem({ type: "error", companyNumber: e.companyNumber, companyName: e.companyName, message: e.message })
      );

      // Build/download CSV if provided or construct minimal from matches
      const filename = `strategic-review_matches_${nowStamp()}.csv`;
      if (data?.csv) {
        renderDownloadButton({ filename, blobContent: data.csv });
      } else if (Array.isArray(data?.matches)) {
        const header = ["Company Name", "Company Number", "Evidence"];
        const rows = data.matches.map((m) => [m.companyName ?? "", m.companyNumber ?? "", m.evidence ?? ""]);
        renderDownloadButton({ filename, blobContent: toCsv(rows, header) });
      }

      const hasIssues = (data?.skipped?.length || 0) > 0 || (data?.errors?.length || 0) > 0;
      renderStatus(
        hasIssues ? "Job completed. Some companies were skipped or errored." : "Job completed.",
        hasIssues ? "warn" : "info",
        { openSkipped: hasIssues }
      );
      const matched = Number(data?.counts?.matched || 0);
      const showDetails = (matched === 0) || hasIssues;
      showFeedbackCard(showDetails);
      setTimeout(() => setProgress(null), 1200);
      return { upgraded: false, done: true };
    }
  }

  async function handleLargeRun({ file, evidence }) {
    // Start orchestration
    const { jobId, downloadUrl } = await apiStartLargeRun({ file, evidenceTag: evidence });
    setDownloadAnchor(downloadUrl || `/api/ch-strategic/download/${encodeURIComponent(jobId)}`);
    if (!jobId) throw new Error("No jobId returned.");

    // Initialize run state + announce start
    state.largeRun = { jobId, polling: true, pollAbort: null, downloadShown: false };
    setProgress(5);
    renderStatus(`Background job started: ${jobId.slice(0, 8)}…`, "info");
    // Populate the Job ID badge in the retention note
    const badge = document.getElementById('job-id-badge');
    if (badge) badge.textContent = jobId;

    // accessibility: mark busy and focus status region
    {
      const statusEl = document.getElementById('status');
      if (statusEl) { statusEl.setAttribute('aria-busy', 'true'); statusEl.focus?.(); }
    }

    // Wire the Cancel button for THIS jobId
    if (el.btnCancel) {
      el.btnCancel.disabled = false;

      const onCancel = async () => {
        el.btnCancel.disabled = true;          // prevent double-clicks
        renderStatus('Cancelling…', 'info');
        try {
          await fetch(`/api/ch-strategic/cancel/${encodeURIComponent(jobId)}`, { method: 'POST' });
          // Stop local polling immediately; the server will flip to "cancelled" shortly
          if (state.largeRun && state.largeRun.jobId === jobId) {
            if (state.largeRun.pollAbort) state.largeRun.pollAbort.abort();
            state.largeRun.polling = false;
          }
        } catch (e) {
          renderStatus('Cancel failed. Please retry.', 'error');
          el.btnCancel.disabled = false;
          setStartsDisabled(false);
          return;
        }
        renderStatus('Job cancellation requested.', 'info');
        setStartsDisabled(false);
      };

      // Ensure a clean handler per run (no stacking from previous jobs)
      el.btnCancel.replaceWith(el.btnCancel.cloneNode(true));
      // Re-select (since replaceWith returns void)
      const freshBtn = document.getElementById('btnCancel');
      freshBtn.disabled = false;
      freshBtn.addEventListener('click', onCancel, { once: true });
      // Update reference
      el.btnCancel = freshBtn;
    }

    // --- Poll with AbortController + backoff ---
    const pollAbort = new AbortController();
    state.largeRun.pollAbort = pollAbort;
    const startedAt = Date.now();

    while (state.largeRun.polling) {
      // fetch status with abort support
      const status = await apiPollStatus(jobId, { signal: pollAbort.signal });

      // Counters
      setCounters({
        total: Number(status?.total) || 0,
        processed: Number(status?.processed) || 0,
        matched: Number(status?.matched) || 0,
        skipped: Number(status?.skipped) || 0,
      });

      // Compute % locally if possible
      const total = Number(status?.total) || 0;
      const processed = Number(status?.processed) || 0;
      if (total > 0) setProgress(Math.round((processed / total) * 100));

      // Optional stream messages (if backend provides)
      if (Array.isArray(status?.messages)) {
        status.messages.forEach((m) => appendResultItem(m));
      }

      // Show download only after at least one row exists or server says OK
      const canEnableDownload =
        !state.largeRun.downloadShown &&
        (status?.canDownload === true ||
          status?.firstRowEmitted === true ||
          Number(status?.rows) > 0 ||
          Number(status?.matched) > 0);

      if (canEnableDownload) {
        renderDownloadButton({
          filename: "",
          href: `/api/ch-strategic/download/${encodeURIComponent(jobId)}`,
        });
        state.largeRun.downloadShown = true;
      }

      // Show errorsByReason in diagnostics if provided
      if (status?.errorsByReason && el.diag) {
        el.diag.textContent = JSON.stringify({ errorsByReason: status.errorsByReason }, null, 2);
      }

      // Completion (done or cancelled)
      if (status?.completed || status?.state === 'cancelled') {
        state.largeRun.polling = false;
        setProgress(100);

        // Disable Cancel button now that we're finished
        if (el.btnCancel) el.btnCancel.disabled = true;

        // Safety: enable download if finished with rows but button not shown yet
        const hasRows =
          status?.canDownload === true ||
          status?.firstRowEmitted === true ||
          Number(status?.rows) > 0 ||
          Number(status?.matched) > 0;

        if (!state.largeRun.downloadShown && hasRows) {
          renderDownloadButton({
            filename: "",
            href: `/api/ch-strategic/download/${encodeURIComponent(jobId)}`,
          });
          state.largeRun.downloadShown = true;
        }

        const wasCancelled = status?.state === 'cancelled';
        const hadIssues = Number(status?.skipped || 0) > 0 || (Array.isArray(status?.errors) && status.errors.length > 0);

        renderStatus(
          wasCancelled
            ? "Job was cancelled."
            : (state.largeRun.downloadShown
              ? "Job completed. You can download results."
              : "Job completed. No matches were found."),
          "info",
          { openSkipped: hadIssues }
        );

        const matched = Number(status?.matched || 0);
        const showDetails = wasCancelled || matched === 0 || hadIssues;
        showFeedbackCard(showDetails);

        // accessibility: mark not busy on completion
        const statusEl2 = document.getElementById('status');
        if (statusEl2) statusEl2.setAttribute('aria-busy', 'false');

        setStartsDisabled(false);
        break;
      }

      // Backoff: 0–20s @750ms, 20–60s @1500ms, 60s+ @3000ms
      const ageSec = (Date.now() - startedAt) / 1000;
      const delay = ageSec < 20 ? 750 : ageSec < 60 ? 1500 : 3000;
      await sleep(delay);
    }

    // ensure any in-flight fetch is cancelled when loop exits
    pollAbort.abort();

    // safety: also mark not busy here in case we exited outside the completion block
    {
      const statusEl = document.getElementById('status');
      if (statusEl) statusEl.setAttribute('aria-busy', 'false');
    }

    setStartsDisabled(false);
  }

  function wireRightRail() {
    toggleSection(el.intelToggle, el.intelBody);
    toggleSection(el.openersBtn, el.openersBody);
    toggleSection(el.tipsBtn, el.tipsBody);
  }

  function wirePbiDialog() {
    if (!el.pbiBtn || !el.pbiDialog) return;

    // Open dialog
    el.pbiBtn.addEventListener("click", () => {
      try { el.pbiDialog.showModal(); } catch { el.pbiDialog.setAttribute("open", "open"); }
      el.pbiStatus && (el.pbiStatus.textContent = "");
    });

    // Submit (import)
    el.pbiForm?.addEventListener("submit", async (evt) => {
      evt.preventDefault();
      const submitter = /** @type {HTMLButtonElement} */ (evt.submitter);
      if (submitter?.value !== "import") return; // cancel path

      try {
        renderStatus("Importing from Power BI…");
        const reportId = el.pbiReportId?.value?.trim();
        const visual = el.pbiVisual?.value?.trim();
        // Backend returns CSV (server MSAL + allowlist)
        const csv = await apiPbiExport({ reportId, visual });
        const rows = parseCsvRows(csv);
        const total = Math.max(0, rows.length - 1);
        state.csvFile = await fileFromCsvString(csv, "pbi-export.csv");
        setCounters({ total });
        renderStatus(`Imported ${total} companies from Power BI.`, "info");
        updateAnalyzeState();
      } catch (err) {
        console.error(err);
        el.pbiStatus && (el.pbiStatus.textContent = err.message || "Import failed.");
        renderStatus("Power BI import failed.", "error");
      } finally {
        try { el.pbiDialog.close(); } catch { el.pbiDialog.removeAttribute("open"); }
      }
    });
  }

  function wireHelp() {
    el.helpBtn?.addEventListener("click", () => {
      const msg = [
        "This tool scans only the Strategic Report section for your evidence tag.",
        "• Evidence tag is case-insensitive and limited to letters, digits, space, underscore, hyphen (max 50 chars).",
        "• CSV must include Company Name and Company Number.",
        "• Large jobs run as a background orchestration; Download becomes available once the first row is written."
      ].join("\n");
      alert(msg);
    });
  }

  // Pause network when tab is hidden (aborts in-flight fetch safely)
  document.addEventListener('visibilitychange', () => {
    if (document.hidden && state.largeRun?.pollAbort) {
      try { state.largeRun.pollAbort.abort(); } catch { }
    }
  });

  window.addEventListener('beforeunload', () => {
    if (state.largeRun?.pollAbort) {
      try { state.largeRun.pollAbort.abort(); } catch { }
    }
  });

  function wireForm() {
    // 1) Call type + remember
    el.callType?.addEventListener("change", () => {
      state.callType = el.callType.value;
      text(el.mapLabel, state.callType);
      persistCallType();
    });
    el.remember?.addEventListener("change", persistCallType);

    // 2) Evidence + CSV inputs
    el.evidence?.addEventListener("input", () => {
      state.evidence = (el.evidence.value || "").trim();
      updateAnalyzeState();
    });
    el.csv?.addEventListener("change", () => {
      state.csvFile = el.csv.files?.[0] || null;
      updateAnalyzeState();
    });

    // 3) Analyze (small run, with server-side upgrade if >50)
    el.analyze?.addEventListener("click", async (evt) => {
      evt.preventDefault();
      clearResults();

      const { ok } = validateClientInputs(state.csvFile, state.evidence);
      if (!ok) return;

      setStartsDisabled(true);
      try {
        const res = await handleSmallOrUpgrade({ file: state.csvFile, evidence: state.evidence });
        if (res?.upgraded) {
          await handleLargeRun({ file: state.csvFile, evidence: state.evidence });
        }
      } catch (err) {
        renderStatus("Request failed.", "error");
        appendResultItem(`Error: ${err.message || err}`);
      } finally {
        setStartsDisabled(false);
      }
    });

    // 4) Start large run explicitly
    el.startLarge?.addEventListener("click", async (evt) => {
      evt.preventDefault();
      clearResults();

      const { ok } = validateClientInputs(state.csvFile, state.evidence);
      if (!ok) return;

      setStartsDisabled(true);
      try {
        await handleLargeRun({ file: state.csvFile, evidence: state.evidence });
      } catch (err) {
        renderStatus("Start failed.", "error");
        appendResultItem(`Error: ${err.message || err}`);
      } finally {
        setStartsDisabled(false);
      }
    });

    // 5) Reset
    el.reset?.addEventListener("click", (evt) => {
      evt.preventDefault();
      el.form?.reset?.();
      state.csvFile = null;
      state.evidence = "";
      clearResults();
      updateAnalyzeState();
      setStartsDisabled(false);
    });

    // Optional: Enter key on evidence triggers Analyze
    el.evidence?.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !el.analyze?.disabled) el.analyze.click();
    });

    // Keep our primary start alias synced
    btnStart = el.analyze || btnStart;
  }

  function bindPicker() {
    const fileEl = document.getElementById('csv_file');
    const btn = document.getElementById('pick-file');
    if (!fileEl || !btn) return;
    fileEl.disabled = false;
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      try {
        if (typeof fileEl.showPicker === 'function') fileEl.showPicker();
        else fileEl.click();
      } catch {
        fileEl.click();
      }
    }, { passive: false });
  }

  // ---------- Init ----------
  function init() {
    // status region accessibility
    if (el.status) { el.status.setAttribute('aria-live', 'polite'); el.status.setAttribute('tabindex', '-1'); }
    loadCallType();
    updateAnalyzeState();
    wireForm();
    bindPicker();
    wireRightRail();
    wirePbiDialog();
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
