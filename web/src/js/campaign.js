/* campaignjs.txt — corrected to work with your existing HTML and /api/generate.
   Field names, IDs, and payload shape exactly match your page. No renames. */

(function () {
  // ---------- DOM helpers ----------
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  // ---------- Elements ----------
  const els = {
    // inputs
    csvUpload: $("#csvUpload"),
    goBtn: $("#goBtn"),
    csvBadge: $("#csvBadge"),
    companyName: $("#companyName"),
    companyWebsite: $("#companyWebsite"),
    companyLinkedIn: $("#companyLinkedIn"),
    companyUsps: $("#companyUsps"),
    tone: $("#tone"),
    evidenceWindow: $("#evidenceWindow"),
    includeCompliance: $("#includeCompliance"),
    // status / stage
    statusDot: $("#statusDot"),
    statusText: $("#statusText"),
    currentRunId: $("#currentRunId"),
    runBadgeId: $("#runBadgeId"),
    // tabs + panels
    tabs: $$(".tab"),
    contentRoot: $("#campaign-content"),
    tabOverview: $("#tab-overview"),
    tabLanding: $("#tab-landing"),
    tabEmails: $("#tab-emails"),
    tabEvidence: $("#tab-evidence"),
    tabSales: $("#tab-sales"),
    // other
    debugLog: $("#debugLog"),
    toast: $("#campaign-toast"),
    newRunBtn: $("#newRunBtn"),
  };

  // ---------- State ----------
  let selectedFile = null;
  let csvText = "";
  let csvSha = "";
  let rowCount = 0;

  // ---------- Utils ----------
  const log = (msg) => {
    if (!els.debugLog) return;
    const ts = new Date().toISOString().replace("T", " ").replace("Z", "");
    els.debugLog.textContent += `[${ts}] ${String(msg)}\n`;
    els.debugLog.scrollTop = els.debugLog.scrollHeight;
  };

  const setStatus = (txt, mode) => {
    if (els.statusText) els.statusText.textContent = txt || "";
    if (!els.statusDot) return;
    els.statusDot.className = "status-dot";
    if (mode === "ok") els.statusDot.classList.add("ok");
    else if (mode === "err") els.statusDot.classList.add("err");
  };

  const updateStage = (state) => {
    const order = ["ValidatingInput", "EvidenceBuilder", "DraftCampaign", "QualityGate", "Completed"];
    order.forEach((s) => {
      const el = document.getElementById("step-" + s);
      if (!el) return;
      el.classList.remove("active", "done");
      if (state === s) el.classList.add("active");
      if (order.indexOf(s) < order.indexOf(state)) el.classList.add("done");
    });
  };

  const setRunId = (id) => {
    const v = id || "–";
    if (els.currentRunId) els.currentRunId.textContent = v;
    if (els.runBadgeId) els.runBadgeId.textContent = v;
  };

  const setActiveTab = (panelId) => {
    els.tabs.forEach((t) => t.classList.toggle("active", t.dataset.tabTarget === panelId));
    if (!els.contentRoot) return;
    $$("#campaign-content > div").forEach((p) => (p.style.display = p.id === panelId ? "" : "none"));
  };

  const toast = (m) => {
    if (!els.toast) return;
    els.toast.textContent = String(m || "");
    els.toast.classList.add("show");
    setTimeout(() => els.toast.classList.remove("show"), 1600);
  };

  async function sha256Hex(buf) {
    const hash = await crypto.subtle.digest("SHA-256", buf);
    return Array.from(new Uint8Array(hash)).map((b) => b.toString(16).padStart(2, "0")).join("");
  }
  function countCsvRows(text) {
    const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);
    return Math.max(0, lines.length - 1);
  }

  // ---------- Tabs ----------
  els.tabs.forEach((btn) => btn.addEventListener("click", () => setActiveTab(btn.dataset.tabTarget)));
  setActiveTab("tab-overview");

  // ---------- CSV selection ----------
  els.csvUpload?.addEventListener("change", async (e) => {
    const f = e.target.files && e.target.files[0];
    if (!f) {
      selectedFile = null;
      csvText = "";
      csvSha = "";
      rowCount = 0;
      if (els.csvBadge) els.csvBadge.textContent = "(no file)";
      if (els.goBtn) els.goBtn.disabled = true;
      return;
    }
    selectedFile = f;
    const [text, buf] = await Promise.all([f.text(), f.arrayBuffer()]);
    csvText = text;
    rowCount = countCsvRows(text);
    csvSha = await sha256Hex(buf);
    if (els.csvBadge) els.csvBadge.textContent = `${f.name} · ${rowCount} rows · sha256=${csvSha.slice(0, 12)}…`;
    if (els.goBtn) els.goBtn.disabled = false;
    setStatus("Ready", "ok");
    log(`CSV ready: rows=${rowCount}, sha256=${csvSha}`);
  });

  // ---------- Renderers (IDs/fields unchanged) ----------
  function renderOverview(c) {
    if (!els.tabOverview) return;
    els.tabOverview.innerHTML = "";
    const ex = document.createElement("div");
    ex.className = "pre";
    ex.textContent = c?.executive_summary || "(no executive summary)";
    els.tabOverview.appendChild(ex);
  }

  function renderLanding(c) {
    if (!els.tabLanding) return;
    const lp = c?.landing_page || {};
    els.tabLanding.style.display = "";
    els.tabLanding.innerHTML = `
      <h3>${lp.headline || ""}</h3>
      <p>${lp.subheadline || ""}</p>
      <ul>${(lp.sections || [])
        .map((s) => `<li><strong>${s.title || ""}</strong>: ${s.content || ""}</li>`)
        .join("")}</ul>
      <p><strong>CTA:</strong> ${lp.cta || ""}</p>
    `;
  }

  function renderEmails(c) {
    if (!els.tabEmails) return;
    const emails = Array.isArray(c?.emails) ? c.emails : [];
    els.tabEmails.style.display = "";
    els.tabEmails.innerHTML =
      emails
        .map(
          (e, i) => `
        <div class="card" style="padding:10px; margin:8px 0">
          <div class="email-head"><span class="email-subject">${e.subject || ""}</span><span class="muted">#${i + 1}</span></div>
          <div class="email-preview muted">${e.preview || ""}</div>
          ${e.html ? `<div>${e.html}</div>` : `<pre class="pre">${e.body || e.text || ""}</pre>`}
        </div>`
        )
        .join("") || '<div class="muted">No emails.</div>';
  }

  function renderEvidence(c) {
    if (!els.tabEvidence) return;
    const ev = Array.isArray(c?.evidence_log) ? c.evidence_log : [];
    els.tabEvidence.style.display = "";
    els.tabEvidence.innerHTML = `
      <table class="table">
        <thead><tr><th>Publisher</th><th>Title</th><th>Date</th><th>URL</th><th>Excerpt</th></tr></thead>
        <tbody>
          ${ev
            .map(
              (x) => `
            <tr>
              <td>${x.publisher || ""}</td>
              <td>${x.title || ""}</td>
              <td>${x.date || ""}</td>
              <td>${x.url ? `<a href="${x.url}" target="_blank" rel="noopener">${x.url}</a>` : ""}</td>
              <td>${x.excerpt || ""}</td>
            </tr>`
            )
            .join("")}
        </tbody>
      </table>
    `;
  }

  function renderSales(c) {
    if (!els.tabSales) return;
    const s = c?.sales_enablement || {};
    els.tabSales.style.display = "";
    els.tabSales.innerHTML = `
      <h4>Call script</h4><pre class="pre">${s.call_script || ""}</pre>
      <h4>One pager</h4><pre class="pre">${s.one_pager || ""}</pre>
    `;
  }

  function renderAll(c) {
    renderOverview(c);
    renderLanding(c);
    renderEmails(c);
    renderEvidence(c);
    renderSales(c);
  }

  // ---------- API ----------
  async function generate(payload) {
    const res = await fetch("/api/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const text = await res.text();
    let data = {};
    try {
      data = JSON.parse(text);
    } catch {}
    if (!res.ok) throw new Error(`generate ${res.status}: ${text.slice(0, 400)}`);
    return data;
  }

  // ---------- Actions ----------
  els.newRunBtn?.addEventListener("click", () => {
    if (els.csvUpload) els.csvUpload.click();
  });

  els.goBtn?.addEventListener("click", async () => {
    if (!selectedFile) {
      toast("Choose a CSV first");
      return;
    }
    try {
      els.goBtn.disabled = true;
      setStatus("Generating…");
      updateStage("ValidatingInput");
      setRunId("–");

      const csv_text = await selectedFile.text();

      // EXACT payload field names
      const payload = {
        kind: "campaign",
        csv_text,
        source: "upload",
        page: "campaign",
        rowCount,
        csv_sha256: csvSha,
        company: {
          name: els.companyName?.value || undefined,
          website: els.companyWebsite?.value || undefined,
          linkedin: els.companyLinkedIn?.value || undefined,
          usps: els.companyUsps?.value || undefined,
        },
        tone: els.tone?.value || undefined,
        evidenceWindowMonths: parseInt(els.evidenceWindow?.value || "6", 10),
        complianceFooter: !!els.includeCompliance?.checked,
      };

      log("Submitting to /api/generate (kind=campaign)");
      const t0 = Date.now();
      const result = await generate(payload);
      const ms = Date.now() - t0;
      log(`Received response in ${ms}ms`);
      if (result && result._debug_prompt) log("---- PROMPT SENT TO LLM ----\n" + result._debug_prompt);

      updateStage("DraftCampaign");
      updateStage("QualityGate");
      updateStage("Completed");
      renderAll(result);
      setStatus("Completed", "ok");
      setActiveTab("tab-overview");
    } catch (e) {
      log(`Error: ${e && e.message ? e.message : e}`);
      setStatus("Error", "err");
      alert(`Error: ${e.message || e}`);
    } finally {
      els.goBtn.disabled = false;
    }
  });

  // ---------- Expose minimal globals expected by your page ----------
  window.CampaignPage = window.CampaignPage || { setActiveTab, updateStage, setRunId };
  window.Campaign = window.Campaign || {
    startNewRun: () => (els.csvUpload ? els.csvUpload.click() : undefined),
    updateStage,
    setRunId,
  };
})();
