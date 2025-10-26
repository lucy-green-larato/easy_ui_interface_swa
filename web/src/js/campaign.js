/* /src/js/campaign.js — unified (start/poll + renderers + tabs)
   26-10-2025 v3 (drop-in)

   Expects these elements in the page:
   - #sectionTabs (left tabs, role=tablist in HTML)
   - #centerPanel  (middle panel)
   - #statusText, #statusDot, #debugLog, #runBadgeId, #currentRunId
   - #goBtn, #csvUpload (optional), #salesModel (optional), #notes (optional)
*/

(function () {
  // ---------- DOM helpers ----------
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const rowsOf = (v) => Array.isArray(v) ? v : (v == null ? [] : [v]);

  // ---------- App state ----------
  const state = { contract: null, active: "exec", tabsMounted: false };

  // ---------- Generic UI helpers ----------
  function setPanelContent(node) {
    const mount = $("#centerPanel");
    if (!mount) return;
    mount.innerHTML = "";
    if (node) mount.appendChild(node);
  }
  function makePre(text) {
    const div = document.createElement("div");
    div.className = "pre";
    div.textContent = (text ?? "").toString().trim() || "(no content)";
    return div;
  }
  function makeList(items) {
    const list = rowsOf(items);
    if (!list.length) return makePre("(none)");
    const ul = document.createElement("ul");
    ul.className = "list";
    list.forEach(x => {
      const li = document.createElement("li");
      li.textContent = (x ?? "").toString();
      ul.appendChild(li);
    });
    return ul;
  }
  function makeTable(headers, rows) {
    const table = document.createElement("table");
    table.className = "table";
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    headers.forEach(h => { const th = document.createElement("th"); th.textContent = h; trh.appendChild(th); });
    thead.appendChild(trh);
    const tbody = document.createElement("tbody");
    (rows || []).forEach(r => {
      const tr = document.createElement("tr");
      (r || []).forEach(cell => {
        const td = document.createElement("td");
        if (cell && typeof cell === "object" && cell.__link) {
          const a = document.createElement("a");
          a.href = cell.href;
          a.textContent = cell.text || cell.href;
          a.target = "_blank";
          a.rel = "noopener";
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

  // ---------- Renderers (match your schema keys) ----------
  function renderExecutiveSummary() {
    const lines = rowsOf(state.contract?.executive_summary);
    setPanelContent(lines.length ? makeList(lines) : makePre("When a run completes, the executive summary will render here."));
  }

  function renderEvidenceLog() {
    const entries = rowsOf(state.contract?.evidence_log);
    const headers = ["ClaimID", "Summary", "Source type", "Title", "URL", "Quote"];
    const rows = entries.map(e => [
      e.claim_id ? { __link: true, href: `#${e.claim_id}`, text: e.claim_id } : "",
      e.summary || "",
      e.source_type || "",
      e.title || "",
      e.url ? { __link: true, href: e.url, text: e.url } : "",
      e.quote || ""
    ]);
    const table = makeTable(headers, rows);

    // Cross-nav: click CLM-xxx → switch to ICP tab + highlight
    table.addEventListener("click", (ev) => {
      const a = ev.target.closest('a[href^="#CLM-"]');
      if (!a) return;
      ev.preventDefault();
      const claimId = a.getAttribute("href").slice(1); // CLM-001
      const icpBtn = document.querySelector('[data-section="icp"]');
      if (icpBtn) icpBtn.click();
      setTimeout(() => {
        const matches = Array.from(document.querySelectorAll('tr[data-claims]'))
          .filter(tr => (tr.getAttribute('data-claims') || '').split(' ').includes(claimId));
        matches.forEach(tr => tr.classList.add('hl'));
        if (matches[0]) matches[0].scrollIntoView({ block: 'center', behavior: 'smooth' });
        setTimeout(() => matches.forEach(tr => tr.classList.remove('hl')), 2500);
      }, 0);
    });

    setPanelContent(table);
  }

  function renderCaseLibrary() {
    const cases = rowsOf(state.contract?.case_studies);
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
    const pos = state.contract?.positioning_and_differentiation || {};
    const wrap = document.createElement("div");

    const h1 = document.createElement("h3"); h1.textContent = "Value Proposition";
    wrap.appendChild(h1); wrap.appendChild(makePre(pos.value_prop || ""));

    // SWOT
    const sw = pos.swot || {};
    const hasSw = [sw.strengths, sw.weaknesses, sw.opportunities, sw.threats]
      .some(a => Array.isArray(a) && a.length);

    const h2 = document.createElement("h3"); h2.textContent = "SWOT";
    wrap.appendChild(h2);

    if (hasSw) {
      wrap.appendChild(makeTable(
        ["Strengths", "Weaknesses", "Opportunities", "Threats"],
        [[rowsOf(sw.strengths), rowsOf(sw.weaknesses), rowsOf(sw.opportunities), rowsOf(sw.threats)]]
      ));
    } else {
      const p = document.createElement("p");
      p.className = "muted";
      p.textContent = "No evidence found";
      wrap.appendChild(p);
    }

    if (Array.isArray(pos.differentiators) && pos.differentiators.length) {
      const h3 = document.createElement("h3"); h3.textContent = "Differentiators";
      wrap.appendChild(h3); wrap.appendChild(makeList(pos.differentiators));
    }

    const comp = rowsOf(pos.competitor_set);
    if (comp.length) {
      const h4 = document.createElement("h3"); h4.textContent = "Competitor Set";
      wrap.appendChild(h4);
      wrap.appendChild(makeTable(
        ["Vendor", "Reason in set", "URL"],
        comp.map(v => [v.vendor || "", v.reason_in_set || "", v.url ? { __link: true, href: v.url, text: v.url } : ""])
      ));
    }

    setPanelContent(wrap);
  }
  function renderICPMatrix() {
    const mm = state.contract?.messaging_matrix || {};
    const wrap = document.createElement("div");

    const h1 = document.createElement("h3"); h1.textContent = "Non-negotiables";
    wrap.appendChild(h1); wrap.appendChild(makeList(mm.nonnegotiables || []));

    const h2 = document.createElement("h3"); h2.textContent = "Messaging Matrix";
    wrap.appendChild(h2);

    const headers = ["Persona", "Pain", "Value statement", "Proof", "CTA"];
    const rows = rowsOf(mm.matrix).map(r => [r.persona || "", r.pain || "", r.value_statement || "", r.proof || "", r.cta || ""]);
    const table = makeTable(headers, rows);

    // Tag each row with claim ids from value_statement/proof so Evidence Log can jump here
    const TRS = table.querySelectorAll("tbody tr");
    const claimRe = /\bCLM-\d{3}\b/g;
    rowsOf(mm.matrix).forEach((r, i) => {
      const ids = new Set(
        ((r.value_statement || "").match(claimRe) || []).concat((r.proof || "").match(claimRe) || [])
      );
      if (ids.size) TRS[i].setAttribute("data-claims", Array.from(ids).join(" "));
    });

    wrap.appendChild(table);
    setPanelContent(wrap);
  }

  function renderOffer() {
    const offer = state.contract?.offer_strategy || {};
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
    const plan = state.contract?.channel_plan || {};
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
    const se = state.contract?.sales_enablement || {};
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
    const m = state.contract?.measurement_and_learning || {};
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
    const cg = state.contract?.compliance_and_governance || {};
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
    setPanelContent(makeList(rowsOf(state.contract?.risks_and_contingencies)));
  }

  function renderOnePager() {
    const wrap = document.createElement("div");
    const h = document.createElement("h3"); h.textContent = "One-pager bullets";
    wrap.appendChild(h);
    wrap.appendChild(makeList(rowsOf(state.contract?.one_pager_summary)));
    setPanelContent(wrap);
  }

  function renderActive() {
    const sec = SECTIONS.find(s => s.id === state.active) || SECTIONS[0];
    (sec?.render || renderExecutiveSummary)();
  }

  function mountTabs() {
    const host = $("#sectionTabs");
    if (!host) return;
    host.innerHTML = "";
    host.setAttribute("role", "tablist");
    host.setAttribute("aria-label", "Campaign sections");

    SECTIONS.forEach(s => {
      const btn = document.createElement("button");
      btn.className = "tab" + (s.id === state.active ? " active" : "");
      btn.type = "button";
      btn.textContent = s.label;
      btn.dataset.section = s.id;
      btn.setAttribute("role", "tab");
      btn.setAttribute("aria-selected", String(s.id === state.active));
      btn.addEventListener("click", () => {
        state.active = s.id;
        $$(".tab", host).forEach(b => { b.classList.remove("active"); b.setAttribute("aria-selected", "false"); });
        btn.classList.add("active");
        btn.setAttribute("aria-selected", "true");
        renderActive();
      });
      host.appendChild(btn);
    });

    state.tabsMounted = true;
  }

  // ---------- Public API for the poller ----------
  window.CampaignUI = {
    setContract(contract_v1) {
      state.contract = contract_v1 || null;
      state.active = "exec";
      if (!state.tabsMounted) mountTabs();
      renderActive();
    }
  };

  // ---------- Start → poll → fetch wiring ----------
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

  function withTimeout(ms, signal) {
    const ctrl = new AbortController();
    const id = setTimeout(() => ctrl.abort("timeout"), ms);
    if (!signal) return { signal: ctrl.signal, clear: () => clearTimeout(id) };
    const compose = new AbortController();
    signal.addEventListener("abort", () => compose.abort(signal.reason));
    compose.signal.addEventListener("abort", () => clearTimeout(id), { once: true });
    return { signal: compose.signal, clear: () => clearTimeout(id) };
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

  const API = {
    start: "/api/campaign-start",
    status: (runId) => `/api/campaign-status?runId=${encodeURIComponent(runId)}`,
    fetchContract: (runId) => `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=campaign`
  };

  async function startRun() {
    UI.setBusy(true);
    UI.setStatus("Submitting…", "run");
    UI.log("Submitting job to /api/campaign-start");

    const salesModel = ($("#salesModel")?.value || "").trim().toLowerCase() || null; // "partner" | "direct" | null
    const notes = ($("#notes")?.value || "").trim() || null;

    // CSV is optional; rowCount left null (worker computes true count)
    const payload = { page: "campaign", salesModel, notes, rowCount: null };

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

  async function pollToCompletion(runId) {
    const started = Date.now();
    const MAX_MS = 8 * 60 * 1000; // 8 minutes
    let attempt = 0;

    while (true) {
      if (Date.now() - started > MAX_MS) throw new Error("Timed out waiting for completion");

      const st = await http("GET", API.status(runId), { timeoutMs: 15000 });
      const stateName = st?.state || "Unknown";
      UI.setStatus(stateName, stateName === "Failed" ? "err" : "run");
      UI.log(`Status: ${stateName}`);

      if (stateName === "Completed") {
        const contract = await http("GET", API.fetchContract(runId), { timeoutMs: 30000 });
        if (!contract || typeof contract !== "object") throw new Error("Empty or invalid contract JSON");
        return contract;
      }
      if (stateName === "Failed" || stateName === "Unknown") {
        const msg = st?.error?.message || `Run ended with state: ${stateName}`;
        throw new Error(msg);
      }

      attempt += 1;
      const sleepMs = Math.min(1000 + attempt * 500, 5000); // 1s → 5s
      await new Promise(r => setTimeout(r, sleepMs));
    }
  }

  // ---------- Boot ----------
  document.addEventListener("DOMContentLoaded", () => {
    // 1) Mount tabs immediately so the UI isn’t blank before data arrives
    mountTabs();
    renderExecutiveSummary();

    // 2) Elements we care about
    const go = $("#goBtn");
    const csv = $("#csvUpload");
    const recent = $("#runSelect");
    const csvBadge = $("#csvBadge");
    const leftRail = $("#inputs");

    // 3) Running guard + unified enable/disable
    let isRunning = false;
    function setRunning(b) {
      isRunning = !!b;
      UI.setBusy(isRunning);
      updateGo();
    }

    // 4) Button enable policy: enabled if CSV chosen OR a recent run selected
    function updateGo() {
      if (!go) return;
      const hasCsv = !!(csv && csv.files && csv.files.length > 0);
      const hasRecent = !!(recent && recent.value && recent.value.trim() !== "");
      go.disabled = isRunning || !(hasCsv || hasRecent);
    }

    // 5) CSV badge + enable/disable wiring
    function formatBytes(n) {
      if (!Number.isFinite(n)) return "";
      const units = ["B", "KB", "MB", "GB"];
      let i = 0, v = n;
      while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
      return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
    }

    if (csv) {
      csv.addEventListener("change", () => {
        const f = csv.files && csv.files[0];
        if (csvBadge) {
          csvBadge.textContent = f ? `${f.name} (${formatBytes(f.size)})` : "(no file)";
        }
        updateGo();
      });
    }

    if (recent) {
      recent.addEventListener("change", updateGo);
    }

    // 6) Go handler with double-click protection and clean teardown
    if (go) {
      updateGo(); // set initial state
      go.addEventListener("click", async () => {
        if (go.disabled || isRunning) return;
        try {
          setRunning(true);
          await startRun();                 // will update status, poll, fetch, and call CampaignUI.setContract()
        } catch (err) {
          console.error(err);
          UI.log("Error: " + (err?.message || err));
          UI.setStatus("Failed", "err");
          alert("Campaign run failed: " + (err?.message || err));
        } finally {
          setRunning(false);
        }
      });
    }

    // 7) Pressing Enter in the left rail triggers Go (if enabled)
    if (leftRail && go) {
      leftRail.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" && !go.disabled && !isRunning) {
          ev.preventDefault();
          go.click();
        }
      });
    }
  });

  // Section registry lives near tabs so it’s visible here
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
})();
