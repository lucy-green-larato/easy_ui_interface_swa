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
    } catch { }
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

      // NEW: normalise envelope and use contract if available
      const body = (result && result.body) ? result.body : result;
      if (body && (body._debug_prompt || result._debug_prompt)) {
        log("---- PROMPT SENT TO LLM ----\n" + (body._debug_prompt || result._debug_prompt));
      }

      updateStage("DraftCampaign");
      updateStage("QualityGate");
      updateStage("Completed");

      // Contract-only renderer: API returns the bare contract (no contract_v1 wrapper)
      const hasSetter = window.CampaignUI && typeof window.CampaignUI.setContract === "function";
      window.lastResult = body;                 // keep for debugging
      window.lastContract = body;               // the body IS the contract
      if (hasSetter) window.CampaignUI.setContract(window.lastContract);
      setStatus("Completed (contract)", "ok");

      setActiveTab("tab-overview");
    } catch (e) {
      log(`Error: ${e && e.message ? e.message : e}`);
      setStatus("Error", "err");
      alert(`Error: ${e.message || e}`);
    } finally {
      els.goBtn.disabled = false;
    }
  });

  // ---------- Contract-aware renderer ----------
  window.CampaignUI = window.CampaignUI || {};
  window.CampaignUI.setContract = function (contract) {
    try {
      if (!contract || typeof contract !== "object") {
        setStatus("No contract to render", "err");
        return;
      }

      // Top-level numbered sections (bracket notation)
      const wf = contract["2_workflow"] || {};
      const out = contract["3_campaign_output"] || {};

      // Dotted subsections (bracket notation)
      const exec = out["3.1_executive_summary"] || {};
      const offer = out["3.4_offer_strategy_and_assets"] || {};
      const chan = out["3.5_channel_plan_and_orchestration"] || {};
      const sales = out["3.6_sales_enablement_alignment"] || {};

      // Evidence entries live under workflow 2C
      const evidenceLog = (wf["2C_evidence_log"] && wf["2C_evidence_log"].entries) || [];

      // -------- Overview tab
      if (els.tabOverview) {
        const html = exec.draft ? `<div class="pre">${String(exec.draft)}</div>` : `<div class="muted">(no executive summary)</div>`;
        els.tabOverview.innerHTML = html;
        els.tabOverview.style.display = "";
      }

      // -------- Landing tab
      if (els.tabLanding) {
        const lp = (offer.landing_page_wire_copy || {});
        const list = Array.isArray(lp.how_it_works_steps) ? lp.how_it_works_steps : [];
        const grid = Array.isArray(lp.outcomes_grid) ? lp.outcomes_grid : [];
        els.tabLanding.innerHTML = `
          <h3>${lp.outcome_header || "Achieve [result] in [timeframe]"}</h3>
          <p class="muted">${lp.proof_line || "Backed by [CaseID] and [ClaimID]"}</p>
          <h4>How it works</h4>
          <ul>${list.map(s => `<li>${String(s)}</li>`).join("")}</ul>
          <h4>Outcomes</h4>
          <ul>${grid.map(g => `<li>${String(g.metric || g.result || "")}</li>`).join("")}</ul>
          <p><strong>CTA:</strong> ${lp.cta || "Get your [offer]"}</p>
        `;
        els.tabLanding.style.display = "";
      }

      // -------- Emails tab
      if (els.tabEmails) {
        const emails = Array.isArray(chan.email_sequence) ? chan.email_sequence : [];
        els.tabEmails.innerHTML = emails.length
          ? emails.map((e, i) => `
              <div class="card" style="padding:10px; margin:8px 0">
                <div class="email-head">
                  <span class="email-subject">${String(e.subject || "")}</span>
                  <span class="muted">#${i + 1}</span>
                </div>
                <pre class="pre">${String(e.body_90_120_words || e.body || "")}</pre>
              </div>
            `).join("")
          : '<div class="muted">No emails.</div>';
        els.tabEmails.style.display = "";
      }

      // -------- Evidence tab
      if (els.tabEvidence) {
        els.tabEvidence.innerHTML = evidenceLog.length
          ? `
            <table class="table">
              <thead>
                <tr><th>Publisher</th><th>Title</th><th>Date</th><th>URL</th><th>Excerpt</th></tr>
              </thead>
              <tbody>
                ${evidenceLog.map(x => `
                  <tr>
                    <td>${String(x.publisher || "")}</td>
                    <td>${String(x.title || "")}</td>
                    <td>${String(x.date || "")}</td>
                    <td>${x.url ? `<a href="${x.url}" target="_blank" rel="noreferrer">link</a>` : ""}</td>
                    <td>${String(x.excerpt_max_2_lines || x.excerpt || "")}</td>
                  </tr>
                `).join("")}
              </tbody>
            </table>`
          : '<div class="muted">No evidence entries.</div>';
        els.tabEvidence.style.display = "";
      }

      // -------- Sales tab
      if (els.tabSales) {
        const qs = Array.isArray(sales.discovery_questions_5_to_7) ? sales.discovery_questions_5_to_7 : [];
        const cards = Array.isArray(sales.objection_cards) ? sales.objection_cards : [];
        els.tabSales.innerHTML = `
          <h4>Discovery questions</h4>
          <ul>${qs.map(q => `<li>${String(q)}</li>`).join("")}</ul>
          <h4>Objection cards</h4>
          ${cards.length ? cards.map(c => `
            <div class="card" style="padding:10px; margin:8px 0">
              <div><strong>Blocker:</strong> ${String(c.blocker || "")}</div>
              <div><strong>Reframe:</strong> ${String(c.reframe_with_evidence || "")}</div>
              <div><strong>Proof:</strong> ${String(c.proof_case_metric || "")}</div>
              <div><strong>Risk reversal:</strong> ${String(c.risk_reversal_mechanism || "")}</div>
            </div>
          `).join("") : '<div class="muted">No objections.</div>'}
        `;
        els.tabSales.style.display = "";
      }
    } catch (err) {
      log(`render error: ${err && err.message ? err.message : err}`);
      setStatus("Render error", "err");
    }
  };
  
  // ---------- Expose minimal globals expected by your page ----------
  window.CampaignPage = window.CampaignPage || { setActiveTab, updateStage, setRunId };
  window.Campaign = window.Campaign || {
    startNewRun: () => (els.csvUpload ? els.csvUpload.click() : undefined),
    updateStage,
    setRunId,
  };
})();
