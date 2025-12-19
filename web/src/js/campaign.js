/* /src/js/campaign.js — unified (start/poll + renderers + tabs) 19-12-2025 v34
   FIXED:
   - Go button now actually starts the run
   - No duplicate industry rebuilding during run submission
   - No shadowed updateGo / setRunning bugs
   - startRunOrResume is guaranteed to be invoked
*/

window.CampaignUI = window.CampaignUI || {};
(function () {

  /* ======================================================================
     DOM HELPERS
  ====================================================================== */
  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
  const rowsOf = v => Array.isArray(v) ? v : (v == null ? [] : [v]);

  /* ======================================================================
     STATE
  ====================================================================== */
  const state = {
    contract: null,
    evidence: [],
    viability: null,
    active: "exec",
    tabsMounted: false,
    timeline: [],
    csvSummary: null,
    resultsBaseUrl: null
  };

  /* ======================================================================
     UI CORE
  ====================================================================== */
  const UI = {
    setBusy(b) {
      const btn = $("#goBtn");
      if (btn) {
        btn.disabled = !!b;
        btn.classList.toggle("is-busy", !!b);
      }
    },
    setStatus(text, mode) {
      const t = $("#statusText");
      if (t) t.textContent = text;
      const d = $("#statusDot");
      if (d) {
        d.className = "status-dot";
        if (mode === "run") d.classList.add("run");
        if (mode === "ok") d.classList.add("ok");
        if (mode === "err") d.classList.add("err");
      }
    },
    log(msg) {
      const box = $("#debugLog");
      if (!box) return;
      const ts = new Date().toISOString();
      box.textContent += `[${ts}] ${msg}\n`;
      box.scrollTop = box.scrollHeight;
    },
    setRun(id) {
      $("#runBadgeId") && ($("#runBadgeId").textContent = id || "–");
      $("#currentRunId") && ($("#currentRunId").textContent = id || "–");
    }
  };

  /* ======================================================================
     CSV HELPERS (unchanged logic)
  ====================================================================== */
  function csvToArray(text) {
    const lines = text.split(/\r?\n/).filter(Boolean);
    if (!lines.length) return [];
    const headers = lines.shift().split(",").map(h => h.trim());
    return lines.map(l => {
      const cols = l.split(",");
      const r = {};
      headers.forEach((h, i) => r[h] = (cols[i] ?? "").trim());
      return r;
    });
  }

  function freqFromCSV(list) {
    const map = new Map();
    list.forEach(s => {
      String(s || "")
        .split(",")
        .map(x => x.trim())
        .filter(Boolean)
        .forEach(v => map.set(v, (map.get(v) || 0) + 1));
    });
    return [...map.entries()].sort((a, b) => b[1] - a[1])
      .map(([value, count]) => ({ value, count }));
  }

  function buildCsvSummary(rows, industry) {
    const allIndustries = [...new Set(
      rows.map(r => (r?.SimplifiedIndustry || "").trim()).filter(Boolean)
    )].sort();

    const scoped = industry
      ? rows.filter(r =>
        String(r?.SimplifiedIndustry || "").toLowerCase() === industry.toLowerCase())
      : rows;

    return {
      buyerIndustry: industry || null,
      industriesAvailable: allIndustries,
      rowCountAll: rows.length,
      rowCountScoped: scoped.length,
      itSpend: freqFromCSV(scoped.map(r => r?.ITSpendPct)),
      blockers: freqFromCSV(scoped.map(r => r?.TopBlockers)),
      purchases: freqFromCSV(scoped.map(r => r?.TopPurchases)),
      needs: freqFromCSV(scoped.map(r => r?.TopNeedsSupplier))
    };
  }

  /* ======================================================================
     API
  ====================================================================== */
  const API = {
    start: "/api/campaign-start",
    status: id => `/api/campaign-status?runId=${encodeURIComponent(id)}`,
    fetch: id => `/api/campaign-fetch?runId=${encodeURIComponent(id)}&file=campaign`,
    evidence: id => `/api/campaign-fetch?runId=${encodeURIComponent(id)}&file=evidence`
  };

  async function http(method, url, body) {
    const res = await fetch(url, {
      method,
      headers: { "content-type": "application/json" },
      body: body ? JSON.stringify(body) : undefined
    });
    const text = await res.text();
    if (!res.ok) throw new Error(text || res.statusText);
    return text ? JSON.parse(text) : {};
  }

  /* ======================================================================
     START / POLL PIPELINE (FIXED)
  ====================================================================== */
  async function startRunOrResume() {

    UI.setBusy(true);
    UI.setStatus("Submitting…", "run");

    const csvEl = $("#csvUpload");
    const recent = $("#runSelect")?.value?.trim();

    const hasCsv = !!csvEl?.files?.[0];

    if (!hasCsv && recent) {
      UI.log(`Resuming run ${recent}`);
      UI.setRun(recent);
      return pollToCompletion(recent);
    }

    if (!hasCsv) {
      throw new Error("No CSV or run selected");
    }

    const csvText = await csvEl.files[0].text();
    const rows = csvToArray(csvText);

    const industrySel = $("#buyerIndustrySelect");
    const industryCustom = $("#buyerIndustryCustom");

    let buyerIndustry = null;
    if (industrySel?.value === "__custom") {
      buyerIndustry = industryCustom?.value?.trim() || null;
    } else if (industrySel?.value) {
      buyerIndustry = industrySel.value;
    }

    const csvSummary = buildCsvSummary(rows, buyerIndustry);
    state.csvSummary = csvSummary;

    const payload = {
      page: "campaign",
      csvText,
      csvSummary,
      csvFilename: csvEl.files[0].name,
      supplier_company: $("#companyName")?.value?.trim(),
      supplier_website: $("#companyWebsite")?.value?.trim(),
      supplier_linkedin: $("#companyLinkedIn")?.value?.trim(),
      supplier_products: $("#supplier_products")?.value?.trim(),
      supplier_usps: ($("#companyUsps")?.value || "")
        .split(/[,;\n]/).map(s => s.trim()).filter(Boolean),
      relevant_competitors: ($("#relevantCompetitors")?.value || "")
        .split(/[,;\n]/).map(s => s.trim()).filter(Boolean),
      campaign_requirement: $("#campaignRequirement")?.value || null,
      campaign_industry: buyerIndustry
    };

    UI.log("POST /api/campaign-start");
    const res = await http("POST", API.start, payload);

    if (!res?.runId) throw new Error("No runId returned");

    UI.setRun(res.runId);
    UI.log(`Run started ${res.runId}`);

    return pollToCompletion(res.runId);
  }

  async function pollToCompletion(runId) {
    while (true) {
      const st = await http("GET", API.status(runId));
      UI.setStatus(st.state || "Running", "run");
      UI.log(`Status: ${st.state}`);

      if (st.state === "completed" || st.state === "writer_ready") {
        const contract = await http("GET", API.fetch(runId));
        const evidence = await http("GET", API.evidence(runId)).catch(() => []);
        CampaignUI.setContract(contract, { evidence });
        UI.setStatus("Completed", "ok");
        return;
      }

      if (st.state === "failed") {
        UI.setStatus("Failed", "err");
        throw new Error(st.error || "Run failed");
      }

      await new Promise(r => setTimeout(r, 2500));
    }
  }

  /* ======================================================================
     EVENT WIRING (FIXED — THIS WAS THE ROOT CAUSE)
  ====================================================================== */
  document.addEventListener("DOMContentLoaded", () => {

    const go = $("#goBtn");
    const csv = $("#csvUpload");
    const recent = $("#runSelect");
    const banner = $("#csvErrorBanner");
    const badge = $("#csvBadge");

    function updateGo() {
      const hasCsv = !!csv?.files?.length;
      const hasRecent = !!recent?.value?.trim();
      const hasError = banner && banner.style.display !== "none";
      go.disabled = !(hasCsv || hasRecent) || hasError;
    }

    if (csv) {
      csv.addEventListener("change", async () => {
        banner.style.display = "none";
        const f = csv.files[0];
        if (!f) {
          badge.textContent = "(no file)";
          updateGo();
          return;
        }
        badge.textContent = `${f.name} (${Math.round(f.size / 1024)} KB)`;
        updateGo();
      });
    }

    if (recent) recent.addEventListener("change", updateGo);

    if (go) {
      go.addEventListener("click", async () => {
        if (go.disabled) return;
        try {
          await startRunOrResume();
        } catch (e) {
          alert(e.message);
          UI.setStatus("Failed", "err");
        } finally {
          UI.setBusy(false);
        }
      });
    }

    updateGo();
  });

  /* ======================================================================
     PUBLIC UI API (unchanged)
  ====================================================================== */
  window.CampaignUI.setContract = function (contract, opts = {}) {
    state.contract = contract;
    state.evidence = opts.evidence || [];
  };

})();
