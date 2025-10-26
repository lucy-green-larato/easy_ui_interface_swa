/* /src/js/campaign.js — unified (start/poll + renderers + tabs) 26-10-2025 v2
   Works with your existing markup:
   - #sectionTabs (left tabs)
   - #centerPanel  (middle panel)
   - #statusText, #statusDot, #debugLog, #runBadgeId, #currentRunId, #goBtn, #csvUpload, etc.
*/

(function () {
  // ---------- DOM helpers ----------
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const rowsOf = (x) => Array.isArray(x) ? x : [];

  // ---------- Minimal state ----------
  const state = { contract: null, active: "exec" };

  // ---------- Generic UI helpers ----------
  function setPanelContent(node) {
    const mount = $("#centerPanel");
    if (!mount) return;
    mount.innerHTML = "";
    if (node) mount.appendChild(node);
  }
  function makePre(text) {
    const pre = document.createElement("div");
    pre.className = "pre";
    pre.textContent = String(text || "").trim() || "(no content)";
    return pre;
  }
  function makeList(items) {
    const ul = document.createElement("ul");
    ul.className = "list";
    (items || []).forEach(x => {
      const li = document.createElement("li");
      li.textContent = String(x || "").trim();
      ul.appendChild(li);
    });
    if (!ul.children.length) {
      const p = document.createElement("p");
      p.className = "muted";
      p.textContent = "(none)";
      return p;
    }
    return ul;
  }
  function makeTable(headers, rows) {
    const table = document.createElement("table");
    table.className = "table";
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    headers.forEach(h => {
      const th = document.createElement("th");
      th.textContent = h;
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    const tbody = document.createElement("tbody");
    (rows || []).forEach(r => {
      const tr = document.createElement("tr");
      r.forEach(cell => {
        const td = document.createElement("td");
        if (cell && typeof cell === "object" && cell.__link) {
          const a = document.createElement("a");
          a.href = cell.href; a.textContent = cell.text || cell.href;
          a.target = "_blank"; a.rel = "noopener";
          td.appendChild(a);
        } else if (Array.isArray(cell)) {
          td.textContent = cell.join(", ");
        } else {
          td.textContent = String(cell ?? "");
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(thead);
    table.appendChild(tbody);
    return table;
  }

  // ---------- RENDERERS (flat schema keys) ----------
  function renderExecutiveSummary() {
    const c = state.contract || {};
    const lines = rowsOf(c.executive_summary);
    setPanelContent(lines.length ? makeList(lines) : makePre("(no executive summary)"));
  }
  function renderEvidenceLog() {
    const c = state.contract || {};
    const entries = rowsOf(c.evidence_log);
    const headers = ["ClaimID", "Summary", "Source type", "Title", "URL", "Quote"];
    const rows = entries.map(e => [
      // Wrap claim_id as an in-page link like #CLM-001
      e.claim_id ? { __link: true, href: `#${e.claim_id}`, text: e.claim_id } : "",
      e.summary || "",
      e.source_type || "",
      e.title || "",
      e.url ? { __link: true, href: e.url, text: e.url } : "",
      e.quote || ""
    ]);

    const table = makeTable(headers, rows);

    // Cross-navigation: click CLM-xxx → switch to ICP tab and highlight rows
    table.addEventListener("click", (ev) => {
      const a = ev.target.closest('a[href^="#CLM-"]');
      if (!a) return;
      ev.preventDefault();
      const claimId = a.getAttribute("href").slice(1); // e.g., CLM-001

      // Switch to ICP tab
      const icpBtn = document.querySelector('[data-section="icp"]');
      if (icpBtn) icpBtn.click();

      // After ICP renders, highlight the row(s) that reference this claim id
      setTimeout(() => {
        // Mark all rows that include the claim in their data-claims attribute
        const matches = Array.from(document.querySelectorAll('tr[data-claims]'))
          .filter(tr => (tr.getAttribute('data-claims') || '').split(' ').includes(claimId));
        matches.forEach(tr => tr.classList.add('hl'));
        if (matches[0]) matches[0].scrollIntoView({ block: 'center', behavior: 'smooth' });

        // Remove highlight after a moment
        setTimeout(() => matches.forEach(tr => tr.classList.remove('hl')), 2500);
      }, 0);
    });

    setPanelContent(table);
  }

  function renderCaseLibrary() {
    const c = state.contract || {};
    const cases = rowsOf(c.case_studies);
    const headers = ["Customer", "Industry", "Headline", "Bullets", "Link", "Source"];
    const rows = cases.map(k => [
      k.customer || "",
      k.industry || "",
      k.headline || "",
      rowsOf(k.bullets),
      k.link ? { __link: true, href: k.link, text: k.link } : "",
      k.source || ""
    ]);
    setPanelContent(makeTable(headers, rows));
  }
  function renderPositioning() {
    const c = state.contract || {};
    const pos = c.positioning_and_differentiation || {};
    const wrap = document.createElement("div");

    const h1 = document.createElement("h3"); h1.textContent = "Value Proposition";
    wrap.appendChild(h1); wrap.appendChild(makePre(pos.value_prop || ""));

    const sw = pos.swot || {};
    if (sw) {
      const h2 = document.createElement("h3"); h2.textContent = "SWOT";
      wrap.appendChild(h2);
      const t = makeTable(
        ["Strengths", "Weaknesses", "Opportunities", "Threats"],
        [[rowsOf(sw.strengths), rowsOf(sw.weaknesses), rowsOf(sw.opportunities), rowsOf(sw.threats)]]
      );
      wrap.appendChild(t);
    }

    if (Array.isArray(pos.differentiators)) {
      const h3 = document.createElement("h3"); h3.textContent = "Differentiators";
      wrap.appendChild(h3); wrap.appendChild(makeList(rowsOf(pos.differentiators)));
    }

    const comp = rowsOf(pos.competitor_set);
    if (comp.length) {
      const h4 = document.createElement("h3"); h4.textContent = "Competitor Set";
      wrap.appendChild(h4);
      const headers = ["Vendor", "Reason in set", "URL"];
      const rows = comp.map(v => [v.vendor || "", v.reason_in_set || "", v.url ? { __link: true, href: v.url, text: v.url } : ""]);
      wrap.appendChild(makeTable(headers, rows));
    }

    setPanelContent(wrap);
  }
  function renderICPMatrix() {
    const c = state.contract || {};
    const mm = c.messaging_matrix || {};
    const wrap = document.createElement("div");

    const h1 = document.createElement("h3"); h1.textContent = "Non-negotiables";
    wrap.appendChild(h1); wrap.appendChild(makeList(rowsOf(mm.nonnegotiables)));

    const h2 = document.createElement("h3"); h2.textContent = "Messaging Matrix";
    wrap.appendChild(h2);

    const headers = ["Persona", "Pain", "Value statement", "Proof", "CTA"];
    const rows = rowsOf(mm.matrix).map(r => [r.persona || "", r.pain || "", r.value_statement || "", r.proof || "", r.cta || ""]);
    const table = makeTable(headers, rows);

    // Tag each body row with the CLM ids it mentions
    const TRS = table.querySelectorAll("tbody tr");
    const claimRe = /\bCLM-\d{3}\b/g;
    rowsOf(mm.matrix).forEach((r, i) => {
      const ids = new Set(
        ((r.value_statement || "").match(claimRe) || [])
          .concat((r.proof || "").match(claimRe) || [])
      );
      if (ids.size) {
        TRS[i].setAttribute("data-claims", Array.from(ids).join(" "));
      }
    });

    wrap.appendChild(table);
    setPanelContent(wrap);
  }
  function renderOffer() {
    const c = state.contract || {};
    const offer = c.offer_strategy || {};
    const lp = offer.landing_page || {};
    const wrap = document.createElement("div");
    const rows = [
      ["Hero", lp.hero || ""],
      ["Why it matters", rowsOf(lp.why_it_matters)],
      ["What you get", rowsOf(lp.what_you_get)],
      ["How it works", rowsOf(lp.how_it_works)],
      ["Outcomes", rowsOf(lp.outcomes)],
      ["Proof", rowsOf(lp.proof)],
      ["CTA", lp.cta || ""]
    ];
    wrap.appendChild(makeTable(["Field", "Value"], rows));
    const h2 = document.createElement("h3"); h2.textContent = "Assets checklist";
    wrap.appendChild(h2); wrap.appendChild(makeList(rowsOf(offer.assets_checklist)));
    setPanelContent(wrap);
  }
  function renderChannel() {
    const c = state.contract || {};
    const plan = c.channel_plan || {};
    const wrap = document.createElement("div");

    const h1 = document.createElement("h3"); h1.textContent = "Emails";
    wrap.appendChild(h1);
    const emailHeaders = ["Subject", "Preview", "Body"];
    const emailRows = rowsOf(plan.emails).map(e => [e.subject || "", e.preview || "", e.body || ""]);
    const emailTbl = makeTable(emailHeaders, emailRows);
    $$("td", emailTbl).forEach(td => td.style.whiteSpace = "pre-wrap");
    wrap.appendChild(emailTbl);

    const h2 = document.createElement("h3"); h2.textContent = "LinkedIn";
    wrap.appendChild(h2);
    const li = plan.linkedin || {};
    wrap.appendChild(makeTable(["Field", "Value"], [
      ["Connect note", li.connect_note || ""],
      ["Insight post", li.insight_post || ""],
      ["DM", li.dm || ""],
      ["Comment strategy", li.comment_strategy || ""]
    ]));

    if (Array.isArray(plan.paid) && plan.paid.length) {
      const h3 = document.createElement("h3"); h3.textContent = "Paid";
      wrap.appendChild(h3);
      wrap.appendChild(makeTable(["Variant", "Proof (cited)", "CTA"],
        plan.paid.map(p => [p.variant || "", p.proof || "", p.cta || ""])));
    }

    if (plan.event) {
      const ev = plan.event;
      const h4 = document.createElement("h3"); h4.textContent = "Event / Webinar";
      wrap.appendChild(h4);
      wrap.appendChild(makeTable(["Field", "Value"], [
        ["Concept", ev.concept || ""],
        ["Agenda", ev.agenda || ""],
        ["Speakers", ev.speakers || ""],
        ["CTA", ev.cta || ""]
      ]));
    }

    setPanelContent(wrap);
  }
  function renderSalesEnablement() {
    const c = state.contract || {};
    const se = c.sales_enablement || {};
    const wrap = document.createElement("div");

    const h1 = document.createElement("h3"); h1.textContent = "Discovery Questions";
    wrap.appendChild(h1); wrap.appendChild(makeList(rowsOf(se.discovery_questions)));

    const h2 = document.createElement("h3"); h2.textContent = "Objection Cards";
    wrap.appendChild(h2);
    wrap.appendChild(makeTable(["Blocker", "Reframe (ClaimID)", "Proof", "Risk reversal"],
      rowsOf(se.objection_cards).map(o => [o.blocker || "", o.reframe_with_claimid || "", o.proof || "", o.risk_reversal || ""])));

    const h3 = document.createElement("h3"); h3.textContent = "Proof Pack Outline";
    wrap.appendChild(h3); wrap.appendChild(makeList(rowsOf(se.proof_pack_outline)));

    const h4 = document.createElement("h3"); h4.textContent = "Handoff Rules";
    wrap.appendChild(h4); wrap.appendChild(makePre(se.handoff_rules || ""));

    setPanelContent(wrap);
  }
  function renderMeasurement() {
    const c = state.contract || {};
    const m = c.measurement_and_learning || {};
    const wrap = document.createElement("div");
    wrap.appendChild(makeTable(["Field", "Value"], [
      ["KPIs", rowsOf(m.kpis)],
      ["Weekly test plan", m.weekly_test_plan || ""],
      ["UTM & CRM", m.utm_and_crm || ""],
      ["Evidence freshness rule", m.evidence_freshness_rule || ""]
    ]));
    setPanelContent(wrap);
  }
  function renderCompliance() {
    const c = state.contract || {};
    const cg = c.compliance_and_governance || {};
    const wrap = document.createElement("div");
    wrap.appendChild(makeTable(["Field", "Value"], [
      ["Substantiation file", cg.substantiation_file || ""],
      ["GDPR/PECR checklist", cg.gdpr_pecr_checklist || ""],
      ["Brand & accessibility checks", cg.brand_accessibility_checks || ""],
      ["Approval log note", cg.approval_log_note || ""]
    ]));
    setPanelContent(wrap);
  }
  function renderRisks() {
    const c = state.contract || {};
    setPanelContent(makeList(rowsOf(c.risks_and_contingencies)));
  }
  function renderOnePager() {
    const c = state.contract || {};
    const wrap = document.createElement("div");
    const h = document.createElement("h3"); h.textContent = "One-pager bullets";
    wrap.appendChild(h);
    wrap.appendChild(makeList(rowsOf(c.one_pager_summary || [])));
    setPanelContent(wrap);
  }

  // ---------- Best-practice start → poll → fetch wiring ----------
  (function () {
    const $ = (sel, root = document) => root.querySelector(sel);

    const UI = {
      setBusy(b) {
        const btn = $("#goBtn");
        if (btn) { btn.disabled = !!b; btn.classList.toggle("is-busy", !!b); }
      },
      setStatus(txt, mode) {
        const s = $("#statusText"); if (s) s.textContent = txt;
        const dot = $("#statusDot");
        if (dot) {
          dot.className = "status-dot";
          if (mode === "ok") dot.classList.add("ok");
          if (mode === "err") dot.classList.add("err");
          if (mode === "run") dot.classList.add("run");
        }
      },
      log(line) {
        const box = $("#debugLog");
        if (!box) return;
        const t = new Date().toISOString();
        box.textContent += `[${t}] ${line}\n`;
        box.scrollTop = box.scrollHeight;
      },
      setRun(runId) {
        const b = $("#runBadgeId"); if (b) b.textContent = runId || "";
        const h = $("#currentRunId"); if (h) h.value = runId || "";
      }
    };

    // ---- small fetch helper with timeout + correlation id ----
    function withTimeout(ms, signal) {
      const ctrl = new AbortController();
      const id = setTimeout(() => ctrl.abort("timeout"), ms);
      const compose = signal
        ? new AbortController()
        : ctrl;
      if (signal) {
        signal.addEventListener("abort", () => compose.abort(signal.reason));
        compose.signal.addEventListener("abort", () => clearTimeout(id), { once: true });
      }
      return { signal: (signal ? compose.signal : ctrl.signal), clear: () => clearTimeout(id) };
    }

    async function http(method, url, { headers = {}, body, timeoutMs = 20000 } = {}) {
      const cid = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
      const t = withTimeout(timeoutMs);
      try {
        const res = await fetch(url, {
          method,
          headers: { "content-type": "application/json", "x-correlation-id": cid, ...headers },
          body: body ? JSON.stringify(body) : undefined,
          signal: t.signal
        });
        const text = await res.text();
        let json; try { json = text ? JSON.parse(text) : null; } catch { json = null; }
        if (!res.ok) {
          const msg = json?.message || res.statusText || "HTTP error";
          throw new Error(`${res.status} ${msg}`);
        }
        return json ?? {};
      } finally {
        t.clear();
      }
    }

    // ---- API endpoints (adjust if you use different routes) ----
    const API = {
      start: "/api/campaign-start",                      // POST
      status: (runId) => `/api/campaign-status?runId=${encodeURIComponent(runId)}`, // GET
      fetchContract: (runId) => `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=campaign` // GET
    };

    // ---- Start a run ----
    async function startRun() {
      UI.setBusy(true);
      UI.setStatus("Submitting…", "run");
      UI.log("Submitting job to /api/campaign-start");

      // collect inputs (defensive)
      const salesModel = ($("#salesModel")?.value || "").trim().toLowerCase() || null; // "partner" | "direct"
      const notes = ($("#notes")?.value || "").trim() || null;
      const csv = $("#csvUpload")?.files?.[0] || null;

      // optional rowCount from CSV (if you already uploaded elsewhere, pass provided count)
      let rowCount = null;
      if (csv && typeof csv.size === "number") {
        // lightweight guess: lines ≈ size/40; better is server-side after upload; pass null if unsure
        rowCount = null;
      }

      const payload = {
        page: "campaign",
        salesModel,
        notes,
        rowCount
      };

      const startResp = await http("POST", API.start, { body: payload, timeoutMs: 25000 });
      const runId = startResp?.runId;
      if (!runId) throw new Error("No runId returned from /api/campaign-start");

      UI.setRun(runId);
      UI.log(`Run started: ${runId}`);
      UI.setStatus("Queued", "run");

      const contract = await pollToCompletion(runId);
      UI.log("Contract fetched; rendering…");
      window.CampaignUI?.setContract?.(contract);
      UI.setStatus("Completed", "ok");
    }

    // ---- Poll status until terminal state; then fetch contract ----
    async function pollToCompletion(runId) {
      const started = Date.now();
      const MAX_MS = 8 * 60 * 1000; // 8 minutes cap
      let attempt = 0;

      while (true) {
        if (Date.now() - started > MAX_MS) throw new Error("Timed out waiting for completion");

        const st = await http("GET", API.status(runId), { timeoutMs: 15000 });
        const state = (st?.state || "Unknown");
        UI.setStatus(state, state === "Failed" ? "err" : "run");
        UI.log(`Status: ${state}`);

        if (state === "Completed") {
          // fetch final JSON
          const contract = await http("GET", API.fetchContract(runId), { timeoutMs: 30000 });
          if (!contract || typeof contract !== "object") throw new Error("Empty or invalid contract JSON");
          return contract;
        }
        if (state === "Failed" || state === "Unknown") {
          const msg = st?.error?.message || `Run ended with state: ${state}`;
          throw new Error(msg);
        }

        // backoff: 1s→2s→3s (cap 5s)
        attempt += 1;
        const sleepMs = Math.min(1000 + attempt * 500, 5000);
        await new Promise(r => setTimeout(r, sleepMs));
      }
    }

    // ---- Wire the Go button ----
    document.addEventListener("DOMContentLoaded", () => {
      const go = $("#goBtn");
      if (!go) return;
      go.addEventListener("click", async () => {
        try {
          await startRun();
        } catch (err) {
          console.error(err);
          UI.log("Error: " + (err?.message || err));
          UI.setStatus("Failed", "err");
          alert("Campaign run failed: " + (err?.message || err));
        } finally {
          UI.setBusy(false);
        }
      });
    });
  })();

  // ---------- Tabs (file-divider style) ----------
  const SECTIONS = [
    { id: "exec", label: "Executive Summary", render: renderExecutiveSummary },
    { id: "elog", label: "Evidence Log (table)", render: renderEvidenceLog },
    { id: "cases", label: "Case Study Library (table)", render: renderCaseLibrary },
    { id: "pos", label: "Positioning & Differentiation", render: renderPositioning },
    { id: "icp", label: "ICP & Messaging Matrix (table)", render: renderICPMatrix },
    { id: "offer", label: "Offer Strategy & Assets", render: renderOffer },
    { id: "chan", label: "Channel Plan & Orchestration", render: renderChannel },
    { id: "se", label: "Sales Enablement Alignment", render: renderSalesEnablement },
    { id: "ml", label: "Measurement & Learning Plan", render: renderMeasurement },
    { id: "comp", label: "Compliance & Governance", render: renderCompliance },
    { id: "risk", label: "Risks & Contingencies", render: renderRisks },
    { id: "one", label: "One Page Campaign Summary", render: renderOnePager }
  ];

  function mountTabs() {
    const host = $("#sectionTabs");
    if (!host) return;
    host.innerHTML = "";
    SECTIONS.forEach(s => {
      const btn = document.createElement("button");
      btn.className = "tab" + (s.id === state.active ? " active" : "");
      btn.type = "button";
      btn.textContent = s.label;
      btn.dataset.section = s.id;
      btn.addEventListener("click", () => {
        $$(".tab", host).forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        state.active = s.id;
        s.render();
      });
      host.appendChild(btn);
    });
  }

  // ---------- Public API used by the polling code ----------
  window.CampaignUI = {
    setContract(contract_v1) {
      state.contract = contract_v1 || null;
      state.active = "exec";
      mountTabs();
      renderExecutiveSummary();
    }
  };

  // ---------- Existing start/poll code (kept) ----------
  const debug = $("#debugLog");
  const setStatus = (txt, mode) => {
    $("#statusText") && ($("#statusText").textContent = txt);
    const dot = $("#statusDot"); if (!dot) return;
    dot.className = "status-dot";
    if (mode === "ok") dot.classList.add("ok");
    if (mode === "err") dot.classList.add("err");
  };
  const log = (m) => { if (!debug) return; const t = new Date().toISOString(); debug.textContent += `[${t}] ${m}\n`; debug.scrollTop = debug.scrollHeight; };

  // Wire up your existing Go flow (unchanged endpoints)
  document.addEventListener("DOMContentLoaded", () => {
    // if your page already calls CampaignUI.setContract(lastResult), leave as-is
  });
})();
