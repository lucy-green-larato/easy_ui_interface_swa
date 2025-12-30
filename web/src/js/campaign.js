/* /src/js/campaign.js — unified (start/poll + renderers + tabs) 29-12-2025 v38
   ROLE/SCOPE (hard boundaries):
   - Deterministic transport + rendering layer only
   - No inference, no “helpful” guesses, no CSV interpretation/summarisation
   - Backend artefacts are authoritative; UI renders verbatim (including empty-but-valid)
*/

window.CampaignUI = window.CampaignUI || {};
(function () {
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  const state = {
    contract: null,
    evidence: [],
    viability: null,
    strategy_v2: null,
    run: null,
    active: "exec",
    tabsMounted: false,
    timeline: [],
    resultsBaseUrl:
      window.CONFIG?.resultsBaseUrl ||
      "https://<YOUR-STORAGE-ACCOUNT>.blob.core.windows.net/results/"
  };

  function getSection(name) {
    // Canonical: UI renders writer output only
    if (state.contract?.sections?.[name] !== undefined) {
      return state.contract.sections[name];
    }

    // Transitional tolerance: very old writers flattened sections at top level
    if (state.contract?.[name] !== undefined) {
      return state.contract[name];
    }

    return null;
  }

  function setPanelContent(node) {
    const mount = $("#centerPanel");
    if (!mount) return;
    mount.innerHTML = "";
    if (node) mount.appendChild(node);
  }

  function makeHeading(text, level = 3) {
    const lvl = Number.isInteger(level) && level >= 1 && level <= 6 ? level : 3;
    const h = document.createElement("h" + String(lvl));
    h.textContent = String(text ?? "");
    return h;
  }

  function makePre(text, className = "pre") {
    const div = document.createElement("div");
    div.className = className;
    div.textContent = (text ?? "").toString();
    return div;
  }

  function makeList(items) {
    const ul = document.createElement("ul");
    ul.className = "list";
    (Array.isArray(items) ? items : []).forEach((x) => {
      const li = document.createElement("li");
      li.textContent = (x ?? "").toString();
      ul.appendChild(li);
    });
    return ul;
  }

  function makeKVTable(obj) {
    const table = document.createElement("table");
    table.className = "table";
    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    ["Field", "Value"].forEach((h) => {
      const th = document.createElement("th");
      th.textContent = h;
      trh.appendChild(th);
    });
    thead.appendChild(trh);

    const tbody = document.createElement("tbody");
    Object.entries(obj || {}).forEach(([k, v]) => {
      const tr = document.createElement("tr");
      const td1 = document.createElement("td");
      td1.textContent = k;

      const td2 = document.createElement("td");
      td2.appendChild(renderValue(v));

      tr.appendChild(td1);
      tr.appendChild(td2);
      tbody.appendChild(tr);
    });

    table.appendChild(thead);
    table.appendChild(tbody);
    return table;
  }

  function renderValue(value) {
    // Render verbatim. No ranking, no filtering, no heuristics.
    if (value === null) return makePre("null");
    if (value === undefined) return makePre("undefined");

    if (typeof value === "string") {
      // Preserve empties as empty-but-valid
      if (value === "") return makePre("(empty)");
      const p = document.createElement("p");
      p.textContent = value;
      return p;
    }

    if (typeof value === "number" || typeof value === "boolean") {
      return makePre(String(value));
    }

    if (Array.isArray(value)) {
      // If array of scalars, list them; otherwise JSON.
      const allScalar = value.every(
        (v) => v === null || ["string", "number", "boolean"].includes(typeof v)
      );
      if (allScalar) return value.length ? makeList(value.map(String)) : makePre("(empty)");
      return makePre(JSON.stringify(value, null, 2));
    }

    if (typeof value === "object") {
      // If it's a simple object, render as KV table; else JSON.
      const keys = Object.keys(value);
      if (!keys.length) return makePre("(empty)");
      // Prefer a KV table for readability without “interpreting”.
      return makeKVTable(value);
    }

    return makePre(String(value));
  }

  function renderSectionObject(title, obj) {
    const wrap = document.createElement("div");
    if (title) wrap.appendChild(makeHeading(title, 3));

    if (obj === null || obj === undefined) {
      wrap.appendChild(makePre("No data."));
      return wrap;
    }

    // If the backend already supplied paragraphs/citations patterns, render them plainly,
    // but do not merge, reorder, or infer.
    if (typeof obj === "object" && !Array.isArray(obj)) {
      // Common Gold-style
      if (Array.isArray(obj.paragraphs)) {
        obj.paragraphs.forEach((pTxt) => {
          const p = document.createElement("p");
          p.textContent = (pTxt ?? "").toString();
          wrap.appendChild(p);
        });
        if (Array.isArray(obj.citations)) {
          wrap.appendChild(makeHeading("Citations", 4));
          wrap.appendChild(obj.citations.length ? makeList(obj.citations) : makePre("(empty)"));
        }
        // Render remaining keys verbatim (excluding paragraphs/citations/title already shown)
        const rest = { ...obj };
        delete rest.paragraphs;
        delete rest.citations;
        delete rest.title;
        if (Object.keys(rest).length) {
          wrap.appendChild(makeHeading("Fields", 4));
          wrap.appendChild(makeKVTable(rest));
        }
        return wrap;
      }
    }

    // Fallback: show verbatim structure
    wrap.appendChild(renderValue(obj));
    return wrap;
  }

  // ---------------------------------------------------------------------------
  // Tabs (deterministic; no “auto” selection logic)
  // ---------------------------------------------------------------------------
  const SECTIONS = [
    { id: "exec", label: "Executive summary", render: renderExecutiveSummary },
    { id: "gtm", label: "Go-to-market", render: renderGoToMarket },
    { id: "off", label: "Offering", render: renderOffering },
    { id: "se", label: "Sales enablement", render: renderSalesEnablement },
    { id: "pp", label: "Proof points", render: renderProofPoints },
    { id: "elog", label: "Evidence log", render: renderEvidenceLog },
    { id: "viab", label: "Viability", render: renderViability }
  ];

  function mountTabs(force = false) {
    const host = $("#sectionTabs");
    if (!host) return false;
    if (state.tabsMounted && !force && host.childElementCount) return true;

    host.replaceChildren();
    host.setAttribute("role", "tablist");
    host.setAttribute("aria-label", "Campaign sections");

    SECTIONS.forEach((s) => {
      const btn = document.createElement("button");
      btn.className = "tab" + (s.id === state.active ? " active" : "");
      btn.type = "button";
      btn.textContent = s.label;
      btn.dataset.section = s.id;
      btn.setAttribute("role", "tab");
      btn.setAttribute("aria-selected", String(s.id === state.active));
      btn.addEventListener("click", () => {
        state.active = s.id;
        $$(".tab", host).forEach((b) => {
          b.classList.remove("active");
          b.setAttribute("aria-selected", "false");
        });
        btn.classList.add("active");
        btn.setAttribute("aria-selected", "true");
        renderActive();
      });
      host.appendChild(btn);
    });

    state.tabsMounted = true;
    return true;
  }

  function renderActive() {
    if (!state.tabsMounted) mountTabs(true);
    const sec = SECTIONS.find((s) => s.id === state.active) || SECTIONS[0];
    (sec?.render || renderExecutiveSummary)();
  }

  // ---------------------------------------------------------------------------
  // Renderers (truth-preserving; backend is source of truth)
  // ---------------------------------------------------------------------------
  function renderExecutiveSummary() {
    const data = getSection("executive_summary");
    if (data === null) {
      setPanelContent(makePre("Executive summary not available yet."));
      return;
    }
    setPanelContent(renderSectionObject(null, data));
  }

  function renderGoToMarket() {
    const data = getSection("go_to_market");

    if (data === null) {
      setPanelContent(makePre("Go-to-market section not available yet."));
      return;
    }
    setPanelContent(renderSectionObject(null, data));
  }

  function renderOffering() {
    const data = getSection("offering");

    if (data === null) {
      setPanelContent(makePre("Offering section not available yet."));
      return;
    }
    setPanelContent(renderSectionObject(null, data));
  }

  function renderSalesEnablement() {
    const data = getSection("sales_enablement");

    if (data === null) {
      setPanelContent(makePre("Sales enablement section not available yet."));
      return;
    }
    setPanelContent(renderSectionObject(null, data));
  }

  function renderProofPoints() {
    const data = getSection("proof_points");

    if (data === null) {
      setPanelContent(makePre("Proof points not available yet."));
      return;
    }
    setPanelContent(renderSectionObject(null, data));
  }

  function renderEvidenceLog() {
    // HARD RULE: no filtering, no dedup, no ranking, no “quality” heuristics.
    const entries =
      (Array.isArray(state.evidence) && state.evidence.length ? state.evidence : null) ||
      (Array.isArray(state.contract?.evidence_log) ? state.contract.evidence_log : []);

    const wrap = document.createElement("div");
    wrap.appendChild(makeHeading("Evidence log", 3));

    if (!entries || !entries.length) {
      wrap.appendChild(makePre("No evidence items returned by backend."));
      setPanelContent(wrap);
      return;
    }

    // Render verbatim table with common fields, plus raw JSON fallback per row.
    const table = document.createElement("table");
    table.className = "table";
    table.style.width = "100%";
    table.style.tableLayout = "fixed";

    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    ["claim_id", "source_type", "title", "url", "summary", "quote"].forEach((h) => {
      const th = document.createElement("th");
      th.textContent = h;
      trh.appendChild(th);
    });
    thead.appendChild(trh);

    const tbody = document.createElement("tbody");

    entries.forEach((e) => {
      const tr = document.createElement("tr");

      const claimId = e?.claim_id ?? "";
      const sourceType = e?.source_type ?? "";
      const title = e?.title ?? "";
      const url = e?.url ?? "";
      const summary = e?.summary ?? "";
      const quote = e?.quote ?? "";

      function tdText(v) {
        const td = document.createElement("td");
        td.textContent = (v ?? "").toString();
        td.style.verticalAlign = "top";
        td.style.whiteSpace = "pre-wrap";
        return td;
      }

      tr.appendChild(tdText(claimId));
      tr.appendChild(tdText(sourceType));
      tr.appendChild(tdText(title));

      // URL as link if present
      {
        const td = document.createElement("td");
        td.style.verticalAlign = "top";
        td.style.whiteSpace = "pre-wrap";
        if (url) {
          const a = document.createElement("a");
          a.href = url;
          a.target = "_blank";
          a.rel = "noopener";
          a.textContent = url;
          td.appendChild(a);
        } else {
          td.textContent = "";
        }
        tr.appendChild(td);
      }

      tr.appendChild(tdText(summary));
      tr.appendChild(tdText(quote));

      tbody.appendChild(tr);
    });

    table.appendChild(thead);
    table.appendChild(tbody);
    wrap.appendChild(table);

    setPanelContent(wrap);
  }

  function renderViability() {
    // Viability is an upstream artefact (file). UI loads and renders verbatim if present.
    const wrap = document.createElement("div");
    wrap.appendChild(makeHeading("Viability", 3));

    if (!state.viability) {
      wrap.appendChild(makePre("No viability artefact returned."));
      setPanelContent(wrap);
      return;
    }

    wrap.appendChild(renderValue(state.viability));
    setPanelContent(wrap);
  }

  // ---------------------------------------------------------------------------
  // Public API (poller calls setContract)
  // ---------------------------------------------------------------------------
  window.CampaignUI = Object.assign(window.CampaignUI || {}, {
    setContract(contract_v1, opts = {}) {
      state.contract = contract_v1 || null;
      state.evidence = Array.isArray(opts.evidence) ? opts.evidence : [];
      state.active = "exec"; // deterministic default
      mountTabs(true);
      renderActive();
    }
  });

  // ---------------------------------------------------------------------------
  // UI: status/log/timeline (transparent diagnostics; no interpretation)
  // ---------------------------------------------------------------------------
  const UI = {
    setBusy(b) {
      const btn = $("#goBtn");
      if (btn) {
        btn.disabled = !!b;
        btn.classList.toggle("is-busy", !!b);
      }
    },
    setStatus(txt, mode) {
      const s = $("#statusText");
      if (s) s.textContent = txt;

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
      const b = $("#runBadgeId");
      if (b) b.textContent = runId || "–";
      const h = $("#currentRunId");
      if (h) h.textContent = runId || "–";
    },
    resetTimeline() {
      state.timeline = [];
      const tl = $("#runTimeline");
      if (tl) tl.textContent = "(no events yet)";
    },
    pushTimeline(phase, note) {
      if (!phase) return;
      const at = new Date().toISOString();
      const entry = { at, phase, note: note || "" };
      state.timeline.push(entry);

      const box = $("#runTimeline");
      if (!box) return;

      const autoScroll = (box.scrollTop + box.clientHeight + 40) >= box.scrollHeight;
      const line = `[${at}] ${phase}${note ? " — " + note : ""}\n`;

      if (box.textContent.trim() === "(no events yet)") box.textContent = line;
      else box.textContent += line;

      if (autoScroll) box.scrollTop = box.scrollHeight;
    }
  };

  // ---------------------------------------------------------------------------
  // Transport helpers (no silent coercion; surface errors)
  // ---------------------------------------------------------------------------
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
    url = String(url || "").trim().replace(/^`|`$/g, "");
    if (!/^https?:\/\//i.test(url) && url[0] !== "/") url = "/" + url;

    const cid = `${Date.now().toString(36)}-${Math.random().toString(16).slice(2)}`;
    const t = withTimeout(timeoutMs);

    try {
      const res = await fetch(url, {
        method,
        headers: { "content-type": "application/json", "x-correlation-id": cid, ...headers },
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: t.signal
      });

      const text = await res.text();
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch {
        json = null;
      }

      if (!res.ok) {
        const msg = (json && (json.message || json.error)) || res.statusText || "HTTP error";
        throw new Error(`${res.status} ${msg}`);
      }

      return json ?? {};
    } finally {
      t.clear();
    }
  }

  const API = {
    start: () => `/api/campaign-start`,
    status: (runId) => `/api/campaign-status?runId=${encodeURIComponent(runId)}`,
    fetchContract: (runId) => `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=campaign`,
    fetchStrategyV2: (runId) => `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=campaign_strategy`,
    fetchEvidence: (runId) => `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=evidence`,
    fetchEvidenceLog: (runId) => `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=evidence_log`
  };

  function withPrefix(url) {
    const prefix = state.run?.prefix;
    if (!prefix) return url;
    return `${url}&prefix=${encodeURIComponent(prefix)}`;
  }

  // ---------------------------------------------------------------------------
  // Transitional adapter (structure-only). No inference; no multi-schema bridging.
  // ---------------------------------------------------------------------------
  function normaliseStrategyV2(raw) {
    if (!raw || typeof raw !== "object") return raw;

    // Unwrap common wrappers only.
    let core = raw;
    if (core.strategy_v2 && typeof core.strategy_v2 === "object") core = core.strategy_v2;
    else if (core.campaign_strategy && typeof core.campaign_strategy === "object") core = core.campaign_strategy;
    else if (core.data && typeof core.data === "object") core = core.data;

    // Return shallow copy (structure-preserving)
    return { ...core };
  }

  // ---------------------------------------------------------------------------
  // Optional viability artefact loader (renders verbatim if present)
  // ---------------------------------------------------------------------------
  async function loadViability(prefix) {
    if (!prefix) {
      state.viability = null;
      return;
    }
    try {
      let p = String(prefix).trim();
      if (!p.endsWith("/")) p += "/";

      const base = String(state.resultsBaseUrl || "");
      const url = `${base}${p}strategy_v2/viability.json`;

      const res = await fetch(url, { method: "GET" });
      if (!res.ok) {
        state.viability = null;
        return;
      }
      const text = await res.text();
      try {
        state.viability = text ? JSON.parse(text) : null;
      } catch {
        state.viability = null;
      }
    } catch {
      state.viability = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Start → poll → fetch (backend is source of truth)
  // ---------------------------------------------------------------------------
  async function startRunOrResume() {
    UI.setBusy(true);
    UI.setStatus("Preparing…", "run");

    // Collect explicit user inputs. Deterministic normalisation only (trim + list split where UI specifies list input).
    const salesModel = ($("#salesModel")?.value ?? "");
    const notes = ($("#notes")?.value ?? "");

    const supplier_company = ($("#companyName")?.value ?? "");
    const supplier_website = ($("#companyWebsite")?.value ?? "");
    const supplier_linkedin = ($("#companyLinkedIn")?.value ?? "");
    const supplier_products_raw = ($("#supplier_products")?.value ?? "");

    const uspsText = ($("#companyUsps")?.value ?? "");
    const supplier_usps = uspsText
      ? String(uspsText).split(/\r?\n|;|,/).map((s) => s.trim()).filter((s) => s.length > 0)
      : [];

    const compText = ($("#relevantCompetitors")?.value ?? "");
    const relevant_competitors = compText
      ? String(compText).split(/[,;\n]/).map((s) => s.trim()).filter((s) => s.length > 0).slice(0, 8)
      : [];

    const campaign_requirement = ($("#campaignRequirement")?.value ?? "");

    // Industry selector (explicit only). No CSV-derived rebuilding; no inference.
    let buyer_industry = null;
    const industrySelect = $("#buyerIndustrySelect");
    const industryCustom = $("#buyerIndustryCustom");
    if (industrySelect) {
      const v = (industrySelect.value ?? "");
      if (v === "__custom") {
        const custom = (industryCustom?.value ?? "");
        buyer_industry = custom === "" ? "" : custom; // preserve empty-but-valid if user chose custom but left empty
      } else if (v !== "") {
        buyer_industry = v;
      } else {
        buyer_industry = null; // user did not decide / left as auto-detect option
      }
    }

    // Data source selection
    const fileEl = $("#csvUpload");
    const hasCsv = !!(fileEl?.files?.[0]);
    const recent = $("#runSelect");
    const selectedRunId = (recent?.value ?? "").trim();

    // Resume path: no CSV uploaded, but a recent run is selected.
    if (!hasCsv && selectedRunId) {
      UI.log(`Resuming existing run: ${selectedRunId}`);
      UI.setRun(selectedRunId);
      return await fetchCompleteRun(selectedRunId, true);
    }

    // Start new run path: CSV required (per current UI state gating).
    UI.setStatus("Submitting…", "run");
    UI.log("Submitting job to /api/campaign-start");

    let csvTextRaw = null;
    let csvFilename = null;
    let rowCount = null;

    if (hasCsv) {
      const file = fileEl.files[0];
      csvFilename = file?.name || null;
      csvTextRaw = await file.text();

      // IMPORTANT: no CSV interpretation. We do NOT parse, summarise, count, infer.
      // Optional diagnostics only: row count via newline count is still “interpretation”.
      // We therefore leave rowCount null unless backend explicitly needs it.
      rowCount = null;
    }

    // Payload must preserve null vs string. No “smart defaults”.
    const payload = {
      page: "campaign",
      salesModel: salesModel === "" ? null : salesModel,
      notes: notes === "" ? null : notes,

      csvText: csvTextRaw,              // null if none
      csvFilename: csvFilename,         // null if none
      rowCount: rowCount,               // null (UI does not interpret CSV)

      supplier_company: supplier_company,   // pass verbatim (backend may validate)
      supplier_website: supplier_website,
      supplier_linkedin: supplier_linkedin,

      supplier_products: supplier_products_raw, // pass verbatim string (backend decides)
      supplier_usps: supplier_usps,             // deterministic split based on UI instruction

      campaign_industry: buyer_industry,        // null or explicit string (or empty string if user chose custom but left empty)
      relevant_competitors: relevant_competitors,
      campaign_requirement: campaign_requirement === "" ? null : campaign_requirement
    };

    const startResp = await http("POST", API.start(), { body: payload, timeoutMs: 25000 });
    const runId = startResp?.runId;
    if (!runId) throw new Error("No runId returned from /api/campaign-start");

    UI.setRun(runId);
    UI.log(`Run started: ${runId}`);
    UI.setStatus("Queued", "run");
    UI.resetTimeline();

    return await fetchCompleteRun(runId, true);
  }

  async function fetchCompleteRun(runId, allowPoll) {
    const contract = await pollToCompletion(runId, allowPoll);

    // Fetch strategy_v2 (optional) and attach verbatim-ish under contract.strategy_v2
    try {
      const strategyV2 = await http("GET", withPrefix(API.fetchStrategyV2(runId)), { timeoutMs: 20000 });
      if (strategyV2 && typeof strategyV2 === "object") {
        state.strategy_v2 = normaliseStrategyV2(strategyV2);
      }
    } catch (e) {
      UI.log("strategy_v2 fetch skipped: " + (e?.message || e));
    }

    // Fetch evidence (optional). Preserve verbatim.
    let evidenceItems = [];
    try {
      const evCanon = await http("GET", withPrefix(API.fetchEvidence(runId)), { timeoutMs: 20000 });
      if (evCanon && Array.isArray(evCanon.claims)) evidenceItems = evCanon.claims;
      else if (Array.isArray(evCanon)) evidenceItems = evCanon;
    } catch (e1) {
      try {
        const evLegacy = await http("GET", withPrefix(API.fetchEvidenceLog(runId)), { timeoutMs: 20000 });
        if (Array.isArray(evLegacy)) evidenceItems = evLegacy;
      } catch (e2) {
        UI.log("Evidence load failed: " + (e2?.message || e2));
      }
    }

    const prefix = state.run?.prefix || null;
    if (prefix) await loadViability(prefix);

    window.CampaignUI?.setContract?.(contract, { evidence: evidenceItems });

    UI.setStatus("Completed", "ok");
    return contract;
  }

  async function pollToCompletion(runId, allowPoll = true) {
    const normState = (s) => String(s || "Unknown");
    const stateKey = (s) => normState(s).toLowerCase();

    async function fetchCampaignContract() {
      const prefix = state.run?.prefix;
      const url =
        prefix
          ? `${API.fetchContract(runId)}&prefix=${encodeURIComponent(prefix)}`
          : API.fetchContract(runId);

      const contract = await http("GET", url, { timeoutMs: 30000 });

      if (!contract || typeof contract !== "object")
        throw new Error("Empty or invalid contract JSON");

      return contract;
    }

    function isTerminalSuccess(statusObj) {
      if (!statusObj || typeof statusObj !== "object") return false;

      // Primary authoritative completion signal
      if (String(statusObj.state).toLowerCase() === "completed") return true;

      // Secondary authoritative completion signal (writer finished)
      if (statusObj.markers && statusObj.markers.writerCompleted === true)
        return true;

      return false;
    }

    // Buttons/banners (transparent; do not “decide” outcomes)
    const errorBanner = $("#runErrorBanner");
    const retryBtn = $("#retryBtn");
    const restartBtn = $("#restartRunBtn");
    const started = Date.now();

    // Allow override via window.CONFIG (set from your hosting layer if desired)
    const MAX_MS =
      (Number.isFinite(Number(window.CONFIG?.campaignRunMaxMs)) && Number(window.CONFIG.campaignRunMaxMs) > 0)
        ? Number(window.CONFIG.campaignRunMaxMs)
        : 8 * 60 * 1000;

    // Optional: tiny pad to avoid “near-boundary” failures
    const MAX_MS_PAD = 30 * 1000;
    const MAX_MS_TOTAL = MAX_MS + MAX_MS_PAD;

    let attempt = 0;

    while (true) {
      if (Date.now() - started > MAX_MS_TOTAL) throw new Error("Timed out waiting for completion");

      const st = await http("GET", API.status(runId), { timeoutMs: 15000 });
      if (st?.prefix) {
        state.run = state.run || {};
        state.run.prefix = st.prefix;
      }
      const stateName = normState(st?.state);
      const k = stateKey(st?.state);

      UI.setStatus(stateName, k === "failed" ? "err" : "run");
      UI.log(`Status: ${stateName}`);

      // Timeline: show backend history verbatim if present.
      if (Array.isArray(st?.history)) {
        const known = new Set(state.timeline.map((e) => e.at + "|" + e.phase + "|" + (e.note || "")));
        st.history.forEach((h) => {
          const sig = (h.at || "") + "|" + (h.phase || "") + "|" + (h.note || "");
          if (!known.has(sig)) UI.pushTimeline(h.phase || "?", h.note || "");
        });
      }
      UI.pushTimeline("status", stateName);

      if (k === "failed") {
        const msg = st?.error?.message || st?.error || "This run failed.";
        if (errorBanner) {
          errorBanner.textContent = "Run failed: " + msg;
          errorBanner.style.display = "block";
        }
        if (retryBtn) {
          retryBtn.dataset.runId = runId;
          retryBtn.style.display = "inline-block";
        }
        if (restartBtn) {
          restartBtn.dataset.runId = runId;
          restartBtn.style.display = "inline-block";
        }
        throw new Error(msg);
      } else {
        if (errorBanner) errorBanner.style.display = "none";
        if (retryBtn) retryBtn.style.display = "none";
        if (restartBtn) restartBtn.style.display = "none";
      }

      if (isTerminalSuccess(st)) {
        // Fetch contract (writer artefact) with a small retry loop.
        let lastErr;
        for (let i = 0; i < 4; i++) {
          try {
            return await fetchCampaignContract();
          } catch (e) {
            lastErr = e;
          }
          await new Promise((r) => setTimeout(r, 300 + i * 400));
        }
        throw lastErr || new Error("Contract fetch failed");
      }

      if (!allowPoll) throw new Error(`Run is not completed (state: ${stateName})`);

      attempt += 1;
      const sleepMs = Math.min(1000 + attempt * 500, 5000);
      await new Promise((r) => setTimeout(r, sleepMs));
    }
  }

  // ---------------------------------------------------------------------------
  // Input-side logic (UI owns visibility + simple constraints; no inference)
  // ---------------------------------------------------------------------------
  let isRunning = false;

  function setRunning(b) {
    isRunning = !!b;
    UI.setBusy(isRunning);
    updateGo();
  }

  function updateGo() {
    const go = $("#goBtn");
    if (!go) return;

    const csv = $("#csvUpload");
    const recent = $("#runSelect");
    const csvErrorBanner = $("#csvErrorBanner");

    const hasCsv = !!(csv?.files?.length);
    const hasRecent = !!(recent?.value && String(recent.value).trim() !== "");
    const csvInvalid = !!(csvErrorBanner && csvErrorBanner.style.display !== "none" && csvErrorBanner.textContent.trim() !== "");

    go.disabled = isRunning || csvInvalid || !(hasCsv || hasRecent);
  }

  function formatBytes(n) {
    if (!Number.isFinite(n)) return "";
    const units = ["B", "KB", "MB", "GB"];
    let i = 0, v = n;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
  }

  function syncIndustryUI() {
    const industrySelect = $("#buyerIndustrySelect");
    const industryCustom = $("#buyerIndustryCustom");
    if (!industrySelect || !industryCustom) return;

    if (industrySelect.value === "__custom") {
      industryCustom.style.display = "block";
      industryCustom.focus();
    } else {
      industryCustom.style.display = "none";
      industryCustom.value = ""; // UI-owned clearing when toggle deselected
    }
  }

  // ---------------------------------------------------------------------------
  // Boot
  // ---------------------------------------------------------------------------
  document.addEventListener("DOMContentLoaded", () => {
    mountTabs(true);
    renderExecutiveSummary();

    // Ensure CSV error banner starts hidden.
    const csvErrorBanner = $("#csvErrorBanner");
    if (csvErrorBanner) {
      csvErrorBanner.style.display = "none";
      csvErrorBanner.textContent = "";
    }

    // Industry selector toggle (explicit only)
    const industrySelect = $("#buyerIndustrySelect");
    if (industrySelect) {
      syncIndustryUI();
      industrySelect.addEventListener("change", () => {
        syncIndustryUI();
        updateGo();
      });
    }

    const industryCustom = $("#buyerIndustryCustom");
    if (industryCustom) {
      // Custom text edits may affect “user intent”, but Go enablement doesn’t depend on it.
      industryCustom.addEventListener("input", () => { /* no-op by design */ });
    }

    // CSV upload: UI validation only. No parsing, no summarising, no inferring.
    const csv = $("#csvUpload");
    const csvBadge = $("#csvBadge");

    if (csv) {
      csv.addEventListener("change", async () => {
        const f = csv.files && csv.files[0];

        if (csvErrorBanner) {
          csvErrorBanner.style.display = "none";
          csvErrorBanner.textContent = "";
        }

        if (!f) {
          if (csvBadge) csvBadge.textContent = "(no file)";
          updateGo();
          return;
        }

        if (csvBadge) csvBadge.textContent = `${f.name} (${formatBytes(f.size)})`;

        try {
          const raw = await f.text();

          // Minimal validity checks only (no semantic interpretation).
          if (!raw.trim()) throw new Error("CSV file is empty.");

          const lines = raw.split(/\r?\n/).filter(Boolean);
          if (lines.length < 2) throw new Error("CSV must contain a header row and at least 1 data row.");

          if (lines.length > 50000) throw new Error("CSV is too large. Please upload fewer than 50,000 rows.");

          if (/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/.test(raw))
            throw new Error("CSV contains unreadable characters. Please re-export it.");

          if (csvErrorBanner) {
            csvErrorBanner.style.display = "none";
            csvErrorBanner.textContent = "";
          }
        } catch (err) {
          if (csvErrorBanner) {
            csvErrorBanner.textContent = "CSV error: " + (err?.message || err);
            csvErrorBanner.style.display = "block";
          }
          if (csvBadge) csvBadge.textContent = "(invalid CSV)";
        }

        updateGo();
      });
    }

    // Recent run selector affects Go enablement only
    const recent = $("#runSelect");
    if (recent) recent.addEventListener("change", updateGo);

    // Go button starts/resumes run
    const go = $("#goBtn");
    if (go) {
      updateGo();
      go.addEventListener("click", async () => {
        if (go.disabled || isRunning) return;
        try {
          setRunning(true);
          await startRunOrResume();
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

    // Retry: re-poll same runId
    const retryBtn = $("#retryBtn");
    if (retryBtn) {
      retryBtn.addEventListener("click", async () => {
        const runId = retryBtn.dataset.runId;
        if (!runId) return;

        UI.log(`Retrying run: ${runId}`);
        UI.setStatus("Retrying…", "run");
        const banner = $("#runErrorBanner");
        if (banner) banner.style.display = "none";
        retryBtn.style.display = "none";

        try {
          setRunning(true);
          await fetchCompleteRun(runId, true);
        } catch (err) {
          UI.log("Retry failed: " + (err?.message || err));
          UI.setStatus("Failed", "err");
          if (banner) {
            banner.textContent = "Retry failed: " + (err?.message || err);
            banner.style.display = "block";
          }
        } finally {
          setRunning(false);
        }
      });
    }

    // Restart: start a NEW run with same current inputs (no hidden state)
    const restartBtn = $("#restartRunBtn");
    if (restartBtn) {
      restartBtn.addEventListener("click", async () => {
        UI.log("Restarting with same inputs (new run) …");
        UI.setStatus("Submitting…", "run");
        const banner = $("#runErrorBanner");
        if (banner) banner.style.display = "none";
        restartBtn.style.display = "none";

        try {
          setRunning(true);
          await startRunOrResume();
        } catch (err) {
          UI.log("Restart failed: " + (err?.message || err));
          UI.setStatus("Failed", "err");
          if (banner) {
            banner.textContent = "Restart failed: " + (err?.message || err);
            banner.style.display = "block";
          }
        } finally {
          setRunning(false);
        }
      });
    }

    // New run button: clears UI inputs only (does not infer defaults)
    const newRunBtn = $("#newRunBtn");
    if (newRunBtn) {
      newRunBtn.addEventListener("click", () => {
        // Clear run selection + csv selection (explicit UI reset)
        if (recent) recent.value = "";
        if (csv) csv.value = "";
        if (csvBadge) csvBadge.textContent = "(no file)";
        if (csvErrorBanner) {
          csvErrorBanner.style.display = "none";
          csvErrorBanner.textContent = "";
        }

        // Clear output panes/status (truth-preserving reset)
        state.contract = null;
        state.evidence = [];
        state.viability = null;
        state.active = "exec";
        UI.setRun("");
        UI.setStatus("Idle", "ok");
        UI.resetTimeline();
        mountTabs(true);
        renderExecutiveSummary();

        // Do not clear form fields unless explicitly desired. (User intent preserved.)
        updateGo();
      });
    }

    // Enter-to-run in left rail (deterministic affordance)
    const leftRail = $("#inputs");
    if (leftRail && go) {
      leftRail.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" && !go.disabled && !isRunning) {
          ev.preventDefault();
          go.click();
        }
      });
    }

    // Initial evaluation
    updateGo();
  });
})();
