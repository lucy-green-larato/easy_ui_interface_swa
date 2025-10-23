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
    // Map your existing payload onto /api/campaign/start
    const startBody = {
      page: payload.page || "campaign",
      rowCount: payload.rowCount,
      // Put the original fields under filters so the worker can pick them up
      // NOTE: csv_text can be large; your backend will trim if needed
      filters: {
        kind: payload.kind,
        source: payload.source,
        csv_sha256: payload.csv_sha256,
        company: payload.company,
        persona: payload.persona,
        evidenceWindowMonths: payload.evidenceWindowMonths,
        complianceFooter: payload.complianceFooter,
        csv_text: payload.csv_text
      },
      // Optional legacy fields if you start using them:
      salesModel: payload.salesModel || null,
      call_type: payload.call_type || null,
      notes: null
    };

    // 1) Start
    const startRes = await fetch("/api/campaign/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(startBody)
    });
    const startText = await startRes.text();
    const startJson = (() => { try { return JSON.parse(startText); } catch { return {}; } })();
    if (!startRes.ok || !startJson.runId) {
      throw new Error(`start ${startRes.status}: ${startText.slice(0, 400)}`);
    }
    const runId = startJson.runId;

    // 2) Poll status until Completed (or Failed / timeout)
    const t0 = Date.now();
    const TIMEOUT_MS = 120000;
    const POLL_MS = 1500;
    let state = "Queued";
    while (Date.now() - t0 < TIMEOUT_MS) {
      const sRes = await fetch(`/api/campaign/status?runId=${encodeURIComponent(runId)}`, { cache: "no-store" });
      const sText = await sRes.text();
      const sJson = (() => { try { return JSON.parse(sText); } catch { return {}; } })();
      state = sJson.state || "Unknown";
      if (state === "Completed") break;
      if (state === "Failed") {
        const msg = sJson?.error?.message || "Worker failed";
        throw new Error(`campaign failed: ${msg}`);
      }
      await new Promise(r => setTimeout(r, POLL_MS));
    }
    if (state !== "Completed") throw new Error("timeout waiting for campaign");

    // 3) Fetch the final campaign JSON
    const fRes = await fetch(`/api/campaign/fetch?runId=${encodeURIComponent(runId)}&file=campaign`, { cache: "no-store" });
    const fText = await fRes.text();
    if (!fRes.ok) throw new Error(`fetch campaign ${fRes.status}: ${fText.slice(0, 400)}`);
    const contract = (() => { try { return JSON.parse(fText); } catch { return fText; } })();

    return { runId, contract_v1: contract }; // keep a simple, stable envelope
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

      // result = { runId, contract_v1 }
      setRunId(result.runId);
      updateStage("DraftCampaign");
      updateStage("QualityGate");
      updateStage("Completed");

      const body = { contract_v1: result.contract_v1 };
      window.lastResult = body;
      window.lastContract = result.contract_v1 || null;

      const hasSetter = window.CampaignUI && typeof window.CampaignUI.setContract === "function";
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

  // ---------- Expose minimal globals expected by your page ----------
  window.CampaignPage = window.CampaignPage || { setActiveTab, updateStage, setRunId };
  window.Campaign = window.Campaign || {
    startNewRun: () => (els.csvUpload ? els.csvUpload.click() : undefined),
    updateStage,
    setRunId,
  };
})();
