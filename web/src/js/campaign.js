/* /src/js/campaign.js — Gold-only UI (v2.0) 19-12-2025. 
   Purpose:
   - Render campaign.json written by Phase 3 writer
   - No legacy support
   - No strategy_v2 rendering
   - Deterministic tabs
*/

(function () {
  // ---------------------------------------------------------------------------
  // DOM helpers
  // ---------------------------------------------------------------------------
  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
  const rowsOf = (v) => Array.isArray(v) ? v : (v == null ? [] : [v]);

  function clear(node) {
    if (node) node.innerHTML = "";
  }

  function setPanel(node) {
    const host = $("#centerPanel");
    if (!host) return;
    clear(host);
    if (node) host.appendChild(node);
  }

  function h(text, level = 3) {
    const el = document.createElement(`h${level}`);
    el.textContent = text || "";
    return el;
  }

  function pre(text) {
    const el = document.createElement("pre");
    el.textContent = (text ?? "").toString();
    return el;
  }

  function list(items) {
    const ul = document.createElement("ul");
    rowsOf(items).forEach(v => {
      const li = document.createElement("li");
      li.textContent = (v ?? "").toString();
      ul.appendChild(li);
    });
    return ul;
  }

  function field(label, value) {
    const wrap = document.createElement("div");
    wrap.appendChild(h(label, 4));

    if (Array.isArray(value)) {
      wrap.appendChild(list(value));
    } else if (typeof value === "object" && value !== null) {
      Object.entries(value).forEach(([k, v]) => {
        wrap.appendChild(field(k, v));
      });
    } else {
      wrap.appendChild(pre(value));
    }

    return wrap;
  }

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  const state = {
    contract: null,
    active: "exec",
    tabsMounted: false
  };

  // ---------------------------------------------------------------------------
  // Renderers (STRICT: Gold sections only)
  // ---------------------------------------------------------------------------
  function renderExecutiveSummary() {
    const es = state.contract?.sections?.executive_summary;
    if (!es) return setPanel(pre("Executive summary not available."));

    const wrap = document.createElement("div");

    if (Array.isArray(es.paragraphs)) {
      es.paragraphs.forEach(p => {
        const el = document.createElement("p");
        el.textContent = p;
        wrap.appendChild(el);
      });
    }

    if (Array.isArray(es.citations) && es.citations.length) {
      wrap.appendChild(h("Citations", 4));
      wrap.appendChild(list(es.citations));
    }

    setPanel(wrap);
  }

  function renderGoToMarket() {
    const gtm = state.contract?.sections?.go_to_market;
    if (!gtm) return setPanel(pre("Go-to-market section not available."));

    const wrap = document.createElement("div");
    Object.entries(gtm).forEach(([k, v]) => {
      if (k !== "citations") wrap.appendChild(field(k, v));
    });

    if (Array.isArray(gtm.citations)) {
      wrap.appendChild(h("Citations", 4));
      wrap.appendChild(list(gtm.citations));
    }

    setPanel(wrap);
  }

  function renderOffering() {
    const off = state.contract?.sections?.offering;
    if (!off) return setPanel(pre("Offering section not available."));

    const wrap = document.createElement("div");
    Object.entries(off).forEach(([k, v]) => {
      if (k !== "citations") wrap.appendChild(field(k, v));
    });

    if (Array.isArray(off.citations)) {
      wrap.appendChild(h("Citations", 4));
      wrap.appendChild(list(off.citations));
    }

    setPanel(wrap);
  }

  function renderSalesEnablement() {
    const se = state.contract?.sections?.sales_enablement;
    if (!se) return setPanel(pre("Sales enablement section not available."));

    const wrap = document.createElement("div");
    Object.entries(se).forEach(([k, v]) => {
      if (k !== "citations") wrap.appendChild(field(k, v));
    });

    if (Array.isArray(se.citations)) {
      wrap.appendChild(h("Citations", 4));
      wrap.appendChild(list(se.citations));
    }

    setPanel(wrap);
  }

  function renderProofPoints() {
    const pp = state.contract?.sections?.proof_points;
    if (!pp || !Array.isArray(pp.points)) {
      return setPanel(pre("No proof points available."));
    }

    const wrap = document.createElement("div");
    wrap.appendChild(list(pp.points));

    if (Array.isArray(pp.citations) && pp.citations.length) {
      wrap.appendChild(h("Citations", 4));
      wrap.appendChild(list(pp.citations));
    }

    setPanel(wrap);
  }

  function renderViability() {
    const v = state.contract?.viability;
    if (!v) return setPanel(pre("No viability data available."));

    const wrap = document.createElement("div");
    wrap.appendChild(h(`Viability: ${v.grade || "Unknown"}`));

    Object.entries(v.dimensions || {}).forEach(([k, val]) => {
      wrap.appendChild(field(k, val));
    });

    setPanel(wrap);
  }

  // ---------------------------------------------------------------------------
  // Tabs
  // ---------------------------------------------------------------------------
  const SECTIONS = [
    { id: "exec", label: "Executive summary", render: renderExecutiveSummary },
    { id: "gtm", label: "Go-to-market", render: renderGoToMarket },
    { id: "off", label: "Offering", render: renderOffering },
    { id: "se", label: "Sales enablement", render: renderSalesEnablement },
    { id: "pp", label: "Proof points", render: renderProofPoints },
    { id: "viab", label: "Viability", render: renderViability }
  ];

  function mountTabs() {
    const host = $("#sectionTabs");
    if (!host) return;

    clear(host);
    SECTIONS.forEach(sec => {
      const btn = document.createElement("button");
      btn.textContent = sec.label;
      btn.className = "tab" + (sec.id === state.active ? " active" : "");
      btn.onclick = () => {
        state.active = sec.id;
        $$(".tab", host).forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        sec.render();
      };
      host.appendChild(btn);
    });

    state.tabsMounted = true;
  }

  // ---------------------------------------------------------------------------
  // Public API (called by poller)
  // ---------------------------------------------------------------------------
  window.CampaignUI = {
    setContract(contract) {
      state.contract = contract;
      state.active = "exec";
      mountTabs();
      renderExecutiveSummary();
    }
  };

  // ---------------------------------------------------------------------------
  // Boot
  // ---------------------------------------------------------------------------
  document.addEventListener("DOMContentLoaded", () => {
    // -----------------------------
    // Initial UI mount (safe)
    // -----------------------------
    mountTabs();
    renderExecutiveSummary();

    // -----------------------------
    // CSV error banner MUST start hidden
    // -----------------------------
    const csvErrorBanner = document.getElementById("csvErrorBanner");
    if (csvErrorBanner) {
      csvErrorBanner.style.display = "none";
      csvErrorBanner.textContent = "";
    }

    // --------------------------------------------------
    // Buyer industry selector — explicit custom handling
    // --------------------------------------------------
    const industrySelect = document.getElementById("buyerIndustrySelect");
    const industryCustom = document.getElementById("buyerIndustryCustom");

    if (industrySelect && industryCustom) {
      // Initial state
      const syncIndustryUI = () => {
        if (industrySelect.value === "__custom") {
          industryCustom.style.display = "block";
          industryCustom.focus();
        } else {
          industryCustom.style.display = "none";
          industryCustom.value = "";
        }
      };

      syncIndustryUI();
      industrySelect.addEventListener("change", syncIndustryUI);
    }

    // --------------------------------------------------
    // CSV upload handling — FULLY RESTORED + UNLOCK SAFE
    // --------------------------------------------------
    const csvInput = document.getElementById("csvUpload");
    const csvBadge = document.getElementById("csvBadge");

    if (csvInput) {
      csvInput.addEventListener("change", async () => {
        const file = csvInput.files && csvInput.files[0];

        // Reset error state FIRST (this unlocks Go)
        if (csvErrorBanner) {
          csvErrorBanner.style.display = "none";
          csvErrorBanner.textContent = "";
        }

        if (!file) {
          if (csvBadge) csvBadge.textContent = "(no file)";
          updateGo?.();
          return;
        }

        // Badge update immediately (fixes UI confusion)
        if (csvBadge) {
          const kb = Math.round(file.size / 1024);
          csvBadge.textContent = `${file.name} (${kb} KB)`;
        }

        try {
          const text = await file.text();

          // Minimal validation ONLY
          if (!text.trim()) {
            throw new Error("CSV file is empty.");
          }

          const lines = text.split(/\r?\n/).filter(Boolean);
          if (lines.length < 2) {
            throw new Error("CSV must contain a header row and at least one data row.");
          }

          // SUCCESS: explicitly clear any error state
          if (csvErrorBanner) {
            csvErrorBanner.style.display = "none";
            csvErrorBanner.textContent = "";
          }

        } catch (err) {
          if (csvErrorBanner) {
            csvErrorBanner.textContent = "CSV error: " + (err.message || err);
            csvErrorBanner.style.display = "block";
          }
          if (csvBadge) {
            csvBadge.textContent = "(invalid CSV)";
          }
        }

        // CRITICAL: re-evaluate Go button state
        updateGo?.();
      });
    }

    // -----------------------------
    // Initial Go button evaluation
    // -----------------------------
    updateGo?.();
  });
})();
