/* web/src/js/ch-strategic.js */
/* eslint-disable no-console */
(() => {
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

  // ---------- State ----------
  const state = {
    callType: "Direct",
    csvFile: /** @type {File|null} */ (null),  // Source CSV file (upload or synthesized from PBI)
    evidence: "",
    // Large run state
    largeRun: null, // { instanceId, polling, downloadShown }
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
    const evidenceOK = (el.evidence?.value || "").trim().length > 0;
    const fileOK = !!state.csvFile;
    el.analyze.disabled = !(evidenceOK && fileOK);
  }

  function renderStatus(msg, type = "info", { openSkipped = false } = {}) {
    if (!el.status) return;
    el.status.className = `status ${type}`;
    if (openSkipped) {
      el.status.innerHTML = `${msg} <button type="button" id="status-open-skipped" class="btn inline">View details</button>`;
      byId("status-open-skipped")?.addEventListener("click", () => {
        const d = el.skippedDetails;
        if (!d) return;
        try { d.open = true; } catch { /* older browsers */ }
        d.removeAttribute("hidden");
        d.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    } else {
      el.status.textContent = msg;
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

  function appendResultItem({ type = "info", companyNumber, companyName, message }) {
    if (!el.results) return;
    const item = document.createElement("div");
    item.className = `result ${type}`;
    item.innerHTML = `
      <div class="result__head">
        <strong>${companyName || "(no name)"} </strong>
        <span class="muted">· ${companyNumber || "(no number)"} </span>
      </div>
      <div class="result__body">${message || ""}</div>
    `;
    el.results.appendChild(item);
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

  // ---------- File helpers ----------
  async function fileFromCsvString(csvText, name = "companies.csv") {
    return new File([csvText], name, { type: "text/csv;charset=utf-8" });
  }

  // ---------- API (spec-compliant endpoints) ----------
  // Small run: POST /api/ch-strategic (multipart: file, evidenceTag)
  async function apiSmallRunMultipart({ file, evidenceTag }) {
    const fd = new FormData();
    fd.append("file", file);
    fd.append("evidenceTag", evidenceTag);
    const res = await fetch("/api/ch-strategic", { method: "POST", body: fd });
    return res;
  }

  // Large run start: POST /api/ch-strategic/start (we send same multipart; server can persist to blob)
  async function apiStartLargeRun({ file, evidenceTag }) {
    const fd = new FormData();
    fd.append("file", file);
    fd.append("evidenceTag", evidenceTag);
    const res = await fetch("/api/ch-strategic/start", { method: "POST", body: fd });
    if (!res.ok) throw new Error(`Start failed (${res.status})`);
    return res.json(); // { instanceId }
  }

  // Status + download use PATH PARAMS with instanceId
  async function apiPollStatus(instanceId) {
    const res = await fetch(`/api/ch-strategic/status/${encodeURIComponent(instanceId)}`, { cache: "no-store" });
    if (!res.ok) throw new Error(`Status failed (${res.status})`);
    return res.json(); // { total, processed, matched, skipped, errorsByReason, ...optional flags }
  }

  // PBI export returns CSV (server handles MSAL + allowlist); we parse client-side
  async function apiPbiExport(payload) {
    const res = await fetch("/api/pbi-export", {
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
    const res = await apiSmallRunMultipart({ file, evidenceTag: evidence });

    if (res.status === 400) {
      renderStatus("List too large for inline processing. Starting background job…");
      return { upgraded: true };
    }
    if (!res.ok) throw new Error(`Request failed (${res.status})`);

    // Expect a compact inline result. We’ll accept either a CSV body or JSON with matches.
    const contentType = res.headers.get("content-type") || "";
    if (contentType.includes("text/csv")) {
      // Server returned final CSV directly
      const csv = await res.text();
      const filename = `strategic-review_matches_${nowStamp()}.csv`;
      renderDownloadButton({ filename, blobContent: csv });
      setProgress(100);
      renderStatus("Job completed. You can download results.", "info");
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
      setTimeout(() => setProgress(null), 1200);
      return { upgraded: false, done: true };
    }
  }

  async function handleLargeRun({ file, evidence }) {
    // Start orchestration
    const { instanceId } = await apiStartLargeRun({ file, evidenceTag: evidence });
    if (!instanceId) throw new Error("No instanceId returned.");

    state.largeRun = { instanceId, polling: true, downloadShown: false };
    setProgress(5);
    renderStatus(`Background job started: ${instanceId.slice(0, 8)}…`);

    // Poll
    while (state.largeRun.polling) {
      const status = await apiPollStatus(instanceId);

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

      // Show download only after at least one row exists
      const canEnableDownload =
        !state.largeRun.downloadShown &&
        (
          Number(status?.matched) > 0 ||
          Number(status?.rows) > 0 ||
          status?.firstRowEmitted === true ||
          status?.canDownload === true
        );

      if (canEnableDownload) {
        renderDownloadButton({
          filename: "",
          href: `/api/ch-strategic/download/${encodeURIComponent(instanceId)}`,
        });
        state.largeRun.downloadShown = true;
      }

      // Show errorsByReason in diagnostics if provided
      if (status?.errorsByReason && el.diag) {
        el.diag.textContent = JSON.stringify({ errorsByReason: status.errorsByReason }, null, 2);
      }

      // Completion
      if (status?.completed) {
        state.largeRun.polling = false;
        setProgress(100);

        // Safety: if completed with matches but we haven't enabled download yet, enable now
        const hasRows =
          Number(status?.matched) > 0 ||
          Number(status?.rows) > 0 ||
          status?.firstRowEmitted === true ||
          status?.canDownload === true;

        if (!state.largeRun.downloadShown && hasRows) {
          renderDownloadButton({
            filename: "",
            href: `/api/ch-strategic/download/${encodeURIComponent(instanceId)}`,
          });
          state.largeRun.downloadShown = true;
        }

        renderStatus(
          state.largeRun.downloadShown
            ? "Job completed. You can download results."
            : "Job completed. No matches were found.",
          "info",
          { openSkipped: Number(status?.skipped) > 0 }
        );
        break;
      }

      await sleep(1500);
    }

    setTimeout(() => setProgress(null), 1500);
  }

  // ---------- Event wiring ----------
  function wireForm() {
    if (!el.form) return;

    // Call type
    el.callType?.addEventListener("change", (e) => {
      state.callType = e.target.value || "Direct";
      text(el.mapLabel, state.callType);
      persistCallType();
    });
    el.remember?.addEventListener("change", () => {
      if (el.remember.checked) persistCallType();
      else localStorage.removeItem(STORAGE_KEYS.CALL_TYPE);
    });

    // Evidence
    el.evidence?.addEventListener("input", () => {
      state.evidence = el.evidence.value.trim();
      if (el.evidenceError) el.evidenceError.textContent = "";
      updateAnalyzeState();
    });

    // CSV file selection (we keep the File for server-side processing)
    el.csv?.addEventListener("change", () => {
      if (el.csvError) el.csvError.textContent = "";
      clearResults();
      const f = el.csv.files?.[0] || null;
      state.csvFile = f;
      // Optional: estimate total by parsing first to update counters (not required)
      if (f) {
        f.text().then((t) => {
          const rows = parseCsvRows(t);
          const header = rows[0]?.map(normaliseHeader) || [];
          const hasCN = header.includes("company name");
          const hasCNo = header.includes("company number");
          if (!hasCN || !hasCNo) {
            el.csvError && (el.csvError.textContent = 'CSV must include headers "Company Name" and "Company Number".');
          }
          const total = Math.max(0, rows.length - 1);
          setCounters({ total });
          renderStatus(`Loaded ${total} companies from CSV.`, "info");
        }).catch(() => { /* ignore estimation errors */ });
      }
      updateAnalyzeState();
    });

    // Analyze click
    el.analyze?.addEventListener("click", async () => {
      clearResults();
      el.analyze.disabled = true;
      try {
        state.evidence = el.evidence.value.trim();
        if (!state.evidence) { el.evidenceError && (el.evidenceError.textContent = "Enter a keyword or phrase."); return; }
        if (!state.csvFile) { el.csvError && (el.csvError.textContent = "Provide a CSV or import from Power BI."); return; }

        // Try small run, upgrade if 400 per spec
        const small = await handleSmallOrUpgrade({ file: state.csvFile, evidence: state.evidence });
        if (small.upgraded) {
          await handleLargeRun({ file: state.csvFile, evidence: state.evidence });
        }
      } catch (err) {
        console.error(err);
        renderStatus(err.message || "An unexpected error occurred.", "error");
        if (el.diag) el.diag.textContent = JSON.stringify({ error: String(err) }, null, 2);
      } finally {
        el.analyze.disabled = false;
      }
    });

    // Reset
    el.reset?.addEventListener("click", () => {
      state.csvFile = null;
      state.evidence = "";
      clearResults();
      if (el.status) el.status.textContent = "";
      if (el.csvError) el.csvError.textContent = "";
      if (el.evidenceError) el.evidenceError.textContent = "";
      updateAnalyzeState();
    });
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
        "• Evidence tag is case-insensitive and tolerant of common OCR typos/synonyms.",
        "• CSV must include Company Name and Company Number.",
        "• Large jobs run as a background orchestration; Download CSV becomes available once the first row is written."
      ].join("\n");
      alert(msg);
    });
  }

  // ---------- Init ----------
  function init() {
    loadCallType();
    updateAnalyzeState();
    wireForm();
    wireRightRail();
    wirePbiDialog();
    wireHelp();
    loadUserBadge();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
