/* /src/js/campaign.js — unified (start/poll + renderers + tabs) 05-11-2025 v6
   Changes vs v5:
   - Support resuming an existing run selected in #runSelect (no CSV required)
   - Poller: tolerate a single transient status fetch error before failing
   - Clearer logs/status for resume vs new run
*/

window.CampaignUI = window.CampaignUI || {};
(function () {
  // ---------- DOM helpers ----------
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const rowsOf = (v) => Array.isArray(v) ? v : (v == null ? [] : [v]);

  // ---------- App state ----------
  const state = { contract: null, evidence: [], active: "exec", tabsMounted: false };

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

  function csvToArray(text) {
    const lines = text.split(/\r?\n/).filter(Boolean);
    if (!lines.length) return [];
    const headers = (lines.shift() || "").split(",").map(h => h.trim());
    return lines.map(line => {
      const cols = line.split(","); // simple CSV (no quoted commas)
      const row = {};
      headers.forEach((h, i) => row[h] = (cols[i] ?? "").trim());
      return row;
    });
  }

  function freqFromCSV(list) {
    const map = new Map();
    list.forEach(s => {
      const parts = String(s || "")
        .split(",")
        .map(x => x.trim())
        .filter(Boolean);
      parts.forEach(p => map.set(p, (map.get(p) || 0) + 1));
    });
    return Array.from(map.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([value, count]) => ({ value, count }));
  }

  function buildCsvSummary(rows, buyerIndustryInput) {
    const allIndustries = Array.from(new Set(rows.map(r => r.SimplifiedIndustry).filter(Boolean))).sort();
    const buyerIndustry = (buyerIndustryInput || "").trim();
    const rowsScoped = buyerIndustry
      ? rows.filter(r => String(r.SimplifiedIndustry || "").toLowerCase() === buyerIndustry.toLowerCase())
      : rows;

    // Fill datalist options for convenience (UI nicety; harmless if duplicates)
    const dl = document.getElementById("industryOptions");
    if (dl && !dl.childElementCount) {
      allIndustries.forEach(ind => {
        const opt = document.createElement("option");
        opt.value = ind;
        dl.appendChild(opt);
      });
    }

    // Frequencies
    const itSpend = freqFromCSV(rowsScoped.map(r => r.ITSpendPct));
    const blockers = freqFromCSV(rowsScoped.map(r => r.TopBlockers));
    const purchases = freqFromCSV(rowsScoped.map(r => r.TopPurchases));
    const needs = freqFromCSV(rowsScoped.map(r => r.TopNeedsSupplier));
    const sampleCompanies = rowsScoped.slice(0, 10).map(r => r.CompanyName).filter(Boolean);

    return {
      schema: `csv-summary-v1:${buyerIndustry ? buyerIndustry.toLowerCase() : "all"}`,
      buyerIndustry: buyerIndustry || null,
      industriesAvailable: allIndustries,
      rowCountAll: rows.length,
      rowCountScoped: rowsScoped.length,
      itSpend, blockers, purchases, needs,
      sampleCompanies
    };
  }

  // ---------- Renderers (unchanged logic) ----------
  function renderExecutiveSummary() {
    const es = state.contract?.executive_summary;

    // Helper to coerce any value to readable text (keeps your style)
    const toText = (item) => {
      if (item == null) return "";
      if (typeof item === "string") return item;
      if (typeof item === "number") return String(item);
      if (typeof item === "object") {
        // Prefer common paragraph keys; then fall back to concatenating string fields
        const prefer = item.lead_paragraph || item.paragraph || item.text || item.value || item.content || item.headline;
        if (typeof prefer === "string") return prefer;
        const vals = Object.values(item).filter(v => typeof v === "string");
        return (vals.join(" ").trim()) || JSON.stringify(item);
      }
      return String(item);
    };

    // Nothing to render?
    if (!es || (Array.isArray(es) && es.length === 0) || (typeof es === "object" && !Object.keys(es).length)) {
      setPanelContent(makePre("The executive summary will show here when your campaign has been created."));
      return;
    }

    // Normalise into { para, bullets[] }
    let para = "";
    let bullets = [];

    if (Array.isArray(es)) {
      // Legacy/array shape: [paragraph, bullet, bullet...]
      const list = es.map(toText).filter(s => s && s.trim());
      para = list[0] || "";
      bullets = (list.length > 1) ? list.slice(1) : [];
    } else if (typeof es === "object") {
      // Object shape: { headline?, lead_paragraph?, bullets?[] }
      para = (
        (typeof es.lead_paragraph === "string" && es.lead_paragraph) ||
        (typeof es.paragraph === "string" && es.paragraph) ||
        (typeof es.text === "string" && es.text) ||
        (typeof es.content === "string" && es.content) ||
        (typeof es.headline === "string" && es.headline) ||
        ""
      );
      if (Array.isArray(es.bullets)) {
        bullets = es.bullets.map(toText).filter(Boolean);
      } else if (Array.isArray(es.points)) {
        bullets = es.points.map(toText).filter(Boolean);
      } else if (Array.isArray(es.items)) {
        bullets = es.items.map(toText).filter(Boolean);
      }
    }

    // Render
    const wrap = document.createElement("div");

    const p = document.createElement("p");
    p.textContent = String(para || "");
    wrap.appendChild(p);

    if (bullets && bullets.length) {
      const ul = document.createElement("ul");
      ul.className = "list";
      bullets.forEach(b => {
        const li = document.createElement("li");
        li.textContent = String(b || "");
        ul.appendChild(li);
      });
      wrap.appendChild(ul);
    }

    setPanelContent(wrap);
  }

  function renderEvidenceLog() {
    const entries = Array.isArray(state.evidence) && state.evidence.length
      ? state.evidence
      : rowsOf(state.contract?.evidence_log);

    // --- helpers ---------------------------------------------------------------
    const hostOf = (u) => { try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return ""; } };
    const clip = (s, n = 180) => (typeof s === "string" && s.length > n) ? (s.slice(0, n - 1) + "…") : (s || "");
    const isSpecific = (e) => {
      if (!e || !e.url || !e.title) return false;
      const summary = String(e.summary || "").trim();
      const quote = String(e.quote || "").trim();
      return (summary.length >= 8 || quote.length >= 8);
    };
    const srcRank = (t) => {
      const k = String(t || "").toLowerCase();
      if (k.includes("ofcom")) return 1;
      if (k.includes("ons")) return 2;
      if (k.includes("dsit")) return 3;
      if (k.includes("company")) return 4;
      if (k.includes("pdf")) return 5;
      if (k.includes("trade")) return 6;
      if (k.includes("directory")) return 7;
      return 9;
    };
    const badge = (label) => {
      const span = document.createElement("span");
      span.textContent = String(label || "Source");
      span.className = "badge";
      // Subtle, inline-safe styles (no global CSS dependency)
      span.style.display = "inline-block";
      span.style.padding = "2px 6px";
      span.style.borderRadius = "8px";
      span.style.fontSize = "12px";
      span.style.lineHeight = "16px";
      span.style.background = "#eef1f5";
      span.style.color = "#333";
      return span;
    };

    // --- de-dup + filter + sort ------------------------------------------------
    const seen = new Set();
    const deduped = (entries || []).filter(e => {
      const key = e?.claim_id || `${e?.title || ""}|${e?.url || ""}`;
      if (!key) return false;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    const strong = deduped.filter(isSpecific);
    const removedCount = deduped.length - strong.length;

    strong.sort((a, b) => {
      const r = srcRank(a.source_type) - srcRank(b.source_type);
      if (r !== 0) return r;
      const ha = hostOf(a.url), hb = hostOf(b.url);
      if (ha !== hb) return ha.localeCompare(hb);
      return String(a.title || "").localeCompare(String(b.title || ""));
    });

    // --- container + header note ----------------------------------------------
    const wrap = document.createElement("div");
    const note = document.createElement("div");
    note.className = "muted";
    note.style.marginBottom = ".5rem";
    note.textContent = removedCount > 0
      ? `Showing ${strong.length} specific items. Filtered ${removedCount} generic entries with missing URL/title or weak evidence.`
      : `Showing ${strong.length} specific items.`;
    wrap.appendChild(note);

    if (!strong.length) {
      wrap.appendChild(makePre("No specific evidence found. Try widening the evidence window or ensure product/regulator pages are reachable."));
      setPanelContent(wrap);
      return;
    }

    // --- build nicer table -----------------------------------------------------
    const table = document.createElement("table");
    table.className = "table";
    table.style.tableLayout = "fixed";
    table.style.width = "100%";

    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    const headers = [
      { h: "Claim ID", w: "9ch" },
      { h: "Source", w: "22ch" },
      { h: "Title & Link", w: "38%" },
      { h: "Summary / Quote", w: "auto" },
    ];
    headers.forEach(({ h, w }) => {
      const th = document.createElement("th");
      th.textContent = h;
      if (w) th.style.width = w;
      trh.appendChild(th);
    });
    thead.appendChild(trh);

    const tbody = document.createElement("tbody");

    for (const e of strong) {
      const tr = document.createElement("tr");

      // Col 1: Claim ID (non-wrapping)
      {
        const td = document.createElement("td");
        td.textContent = String(e.claim_id || "");
        td.style.whiteSpace = "nowrap";
        tr.appendChild(td);
      }

      // Col 2: Source (badge + host)
      {
        const td = document.createElement("td");
        td.style.whiteSpace = "nowrap";
        const b = badge(e.source_type || "Source");
        const host = hostOf(e.url);
        const small = document.createElement("small");
        small.className = "muted";
        small.style.marginLeft = "6px";
        small.textContent = host || "";
        td.appendChild(b);
        if (host) td.appendChild(small);
        tr.appendChild(td);
      }

      // Col 3: Title & Link (title is the clickable link)
      {
        const td = document.createElement("td");
        td.style.overflow = "hidden";
        td.style.textOverflow = "ellipsis";
        td.style.whiteSpace = "nowrap";
        if (e.url) {
          const a = document.createElement("a");
          a.href = e.url;
          a.target = "_blank";
          a.rel = "noopener";
          a.textContent = String(e.title || e.url);
          td.appendChild(a);

          const ext = document.createElement("span");
          ext.textContent = " ↗";
          ext.className = "muted";
          td.appendChild(ext);
        } else {
          td.textContent = String(e.title || "");
        }
        tr.appendChild(td);
      }

      // Col 4: Summary / Quote (expandable if long)
      {
        const td = document.createElement("td");
        const summary = String(e.summary || "").trim();
        const quote = String(e.quote || "").trim();
        const combined = summary && quote ? `${summary}\n\n“${quote}”` : (summary || quote);

        if (!combined) {
          td.textContent = "";
          tr.appendChild(td);
        } else if (combined.length <= 220) {
          // short: show as-is
          const p = document.createElement("p");
          p.textContent = combined;
          td.appendChild(p);
          tr.appendChild(td);
        } else {
          // long: collapsed details
          const details = document.createElement("details");
          const sm = document.createElement("summary");
          sm.textContent = clip(combined, 220);
          details.appendChild(sm);

          const full = document.createElement("div");
          full.style.marginTop = ".4rem";
          if (quote && !summary) {
            const q = document.createElement("blockquote");
            q.textContent = quote;
            full.appendChild(q);
          } else {
            const pre = document.createElement("pre");
            pre.textContent = combined;
            pre.style.whiteSpace = "pre-wrap";
            pre.style.margin = 0;
            full.appendChild(pre);
          }
          details.appendChild(full);
          td.appendChild(details);
          tr.appendChild(td);
        }
      }

      tbody.appendChild(tr);
    }

    table.appendChild(thead);
    table.appendChild(tbody);
    wrap.appendChild(table);
    setPanelContent(wrap);
  }

  function renderCaseLibrary() {
    const listRaw =
      rowsOf(state.contract?.case_study_library).length
        ? rowsOf(state.contract?.case_study_library)
        : rowsOf(state.contract?.case_studies);

    if (!listRaw.length) {
      setPanelContent(makePre("No verified case studies found on the company website."));
      return;
    }

    const hasVerifiedFlag = listRaw.some(k => k && k.verified === true);

    const headers = hasVerifiedFlag
      ? ["Customer", "Industry", "Headline", "Bullets", "Link", "Source", "Verified"]
      : ["Customer", "Industry", "Headline", "Bullets", "Link", "Source"];

    const rows = listRaw.map(k => {
      const customer = k?.customer || "";
      const industry = k?.industry || "";
      const headline = k?.headline || "";
      const bullets = rowsOf(k?.bullets);
      const href = k?.link || k?.url || "";
      const source = k?.source || k?.source_type || "";
      const base = [customer, industry, headline, bullets, href ? { __link: true, href, text: href } : "", source];
      if (hasVerifiedFlag) base.push(k?.verified ? "✓" : "");
      return base;
    });

    const table = makeTable(headers, rows);
    setPanelContent(table);
  }

  function renderPositioning() {
    const pos = state.contract?.positioning_and_differentiation || {};
    const wrap = document.createElement("div");

    const h1 = document.createElement("h3"); h1.textContent = "Value Proposition";
    wrap.appendChild(h1);

    if (pos.value_prop_moore && (pos.value_prop_moore.paragraph || pos.value_prop_moore.fields)) {
      // Paragraph
      const pre = document.createElement("pre");
      pre.textContent = String(pos.value_prop_moore.paragraph || pos.value_prop || "");
      wrap.appendChild(pre);

      // Structured Moore table (read-only, falls back cleanly)
      const f = pos.value_prop_moore.fields || {};
      wrap.appendChild(makeTable(
        ["For", "Who need", "The", "Is a", "That", "Unlike", "Provides", "Proof points"],
        [[
          f.for_who || "",
          f.who_need || "",
          f.the || "",
          f.is_a || "",
          f.that || "",
          f.unlike || "",
          f.provides || "",
          rowsOf(f.proof_points)
        ]]
      ));
    } else {
      // Legacy one-liner path
      wrap.appendChild(makePre(pos.value_prop || ""));
    }

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
    if (!state.tabsMounted || !$("#sectionTabs")?.children?.length) {
      mountTabs(true);
    }
    const sec = SECTIONS.find(s => s.id === state.active) || SECTIONS[0];
    (sec?.render || renderExecutiveSummary)();
  }

  function mountTabs(force = false) {
    const host = $("#sectionTabs");
    if (!host) return false;
    if (state.tabsMounted && !force && host.childElementCount) return true;

    host.replaceChildren();
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
    return true;
  }

  // ---------- Public API for the poller ----------
  window.CampaignUI = Object.assign(window.CampaignUI || {}, {
    setContract(contract_v1, opts = {}) {
      state.contract = contract_v1 || null;
      state.evidence = Array.isArray(opts.evidence) ? opts.evidence : [];
      state.active = "exec";
      mountTabs(true);
      renderActive();
    },
    _debug: {
      mountTabs,
      renderActive: () => renderActive(),
      getState: () => ({ active: state.active, hasContract: !!state.contract, evidenceCount: state.evidence.length })
    }
  });

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
      const h = $("#currentRunId"); if (h) h.textContent = runId || "";
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
    if (typeof url === 'function') url = url();
    url = String(url || '').trim().replace(/^`|`$/g, '');
    if (!/^https?:\/\//i.test(url) && url[0] !== '/') url = '/' + url;

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
    start: () => `/api/campaign-start`,
    status: (runId) => `/api/campaign-status?runId=${encodeURIComponent(runId)}`,
    fetchContract: (runId) => `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=campaign`,
    fetchEvidenceLog: (runId) => `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=evidence_log`,
  };

  async function startRunOrResume() {
    UI.setBusy(true);
    UI.setStatus("Preparing…", "run");

    // UI inputs
    const salesModel = ($("#salesModel")?.value || "").trim().toLowerCase() || null;
    const notes = ($("#notes")?.value || "").trim() || null;

    const supplier_company = ($("#companyName")?.value || "").trim();
    const supplier_website = ($("#companyWebsite")?.value || "").trim();
    const supplier_linkedin = ($("#companyLinkedIn")?.value || "").trim();

    const uspsText = ($("#companyUsps")?.value || "").trim();
    const supplier_usps = uspsText ? uspsText.split(/\r?\n|;|,/).map(s => s.trim()).filter(Boolean) : [];

    const compText = ($("#relevantCompetitors")?.value || "").trim();
    const relevant_competitors = compText ? compText.split(/[,;\n]/).map(s => s.trim()).filter(Boolean).slice(0, 8) : [];

    const campaign_requirement_raw = ($("#campaignRequirement")?.value || "").trim().toLowerCase();
    const campaign_requirement = ["upsell", "win-back", "growth"].includes(campaign_requirement_raw)
      ? campaign_requirement_raw : null;

    const buyer_industry = ($("#buyerIndustry")?.value || "").trim() || null;

    // CSV presence?
    const fileEl = $("#csvUpload");
    const hasCsv = !!(fileEl?.files?.[0]);

    // Recent run selected?
    const recent = $("#runSelect");
    const selectedRunId = (recent?.value || "").trim();

    // If user selected a recent run and did not attach a CSV, resume it.
    if (!hasCsv && selectedRunId) {
      UI.log(`Resuming existing run: ${selectedRunId}`);
      UI.setRun(selectedRunId);
      return await fetchCompleteRun(selectedRunId, /*allowPoll*/ true);
    }

    // Otherwise, we’re starting a fresh run
    UI.setStatus("Submitting…", "run");
    UI.log("Submitting job to /api/campaign-start");

    // Build optional CSV summary
    let csvSummary = null;
    let csvTextRaw = null;
    let rowCount = null;

    if (hasCsv) {
      const text = await fileEl.files[0].text();
      csvTextRaw = text;
      const rows = csvToArray(text);
      rowCount = rows.length;
      csvSummary = buildCsvSummary(rows, buyer_industry || "");
    }

    const payload = {
      page: "campaign",
      salesModel,
      notes,
      rowCount,
      csvText: csvTextRaw,
      csvSummary,
      csvFilename: fileEl?.files?.[0]?.name || null,
      supplier_company,
      supplier_website,
      supplier_linkedin,
      supplier_usps,
      campaign_industry: buyer_industry,
      relevant_competitors,
      campaign_requirement
    };

    const startResp = await http("POST", API.start(), { body: payload, timeoutMs: 25000 });
    const runId = startResp?.runId;
    if (!runId) throw new Error("No runId returned from /api/campaign-start");

    UI.setRun(runId);
    UI.log(`Run started: ${runId}`);
    UI.setStatus("Queued", "run");

    return await fetchCompleteRun(runId, /*allowPoll*/ true);
  }

  // Fetch a run to completion (or immediately if already done)
  async function fetchCompleteRun(runId, allowPoll) {
    const contract = await pollToCompletion(runId, allowPoll);
    UI.log("Contract fetched; keys=" + Object.keys(contract || {}).join(","));
    // Evidence (non-fatal)
    let evidenceItems = [];
    try {
      const ev = await http("GET", API.fetchEvidenceLog(runId), { timeoutMs: 20000 });
      if (Array.isArray(ev)) evidenceItems = ev;
    } catch (e) {
      UI.log("Evidence fetch skipped: " + (e?.message || e));
    }
    window.CampaignUI?.setContract?.(contract, { evidence: evidenceItems });
    UI.setStatus("Completed", "ok");
    return contract;
  }

  async function pollToCompletion(runId, allowPoll = true) {
    // First peek—if already completed, short-circuit
    try {
      const peek = await http("GET", API.status(runId), { timeoutMs: 12000 });
      const stateName = peek?.state || "Unknown";
      UI.setStatus(stateName, stateName === "Failed" ? "err" : "run");
      UI.log(`Status: ${stateName}`);

      if (stateName === "Completed") {
        // Short bounded retry loop for contract fetch to avoid a just-written race
        let lastErr;
        for (let k = 0; k < 4; k++) { // up to ~2.5s total
          try {
            const contract = await http("GET", API.fetchContract(runId), { timeoutMs: 30000 });
            if (contract && typeof contract === "object") return contract;
            lastErr = new Error("Empty or invalid contract JSON");
          } catch (e) {
            lastErr = e;
          }
          await new Promise(r => setTimeout(r, 300 + k * 400)); // 300ms, 700ms, 1100ms, 1500ms
        }
        throw lastErr || new Error("Contract fetch failed");
      }
      if (!allowPoll) throw new Error(`Run is not completed (state: ${stateName})`);
    } catch (e) {
      // If even the first status fails, let the loop try once (below)
      UI.log("First status check failed, will retry once: " + (e?.message || e));
    }

    const started = Date.now();
    const MAX_MS = 8 * 60 * 1000; // 8 minutes
    let attempt = 0;
    let consecutiveErrors = 0;

    const okDuring = new Set([
      "Queued",
      "DraftCampaign",
      "EvidenceDigest",
      "Outline",
      "SectionWrites",
      "Assemble",
      "Completed"
    ]);

    while (true) {
      if (Date.now() - started > MAX_MS) throw new Error("Timed out waiting for completion");

      try {
        const st = await http("GET", API.status(runId), { timeoutMs: 15000 });
        consecutiveErrors = 0; // success resets the error counter

        const stateName = st?.state || "Unknown";
        UI.setStatus(stateName, stateName === "Failed" ? "err" : "run");
        UI.log(`Status: ${stateName}`);

        if (stateName === "Completed") {
          const contract = await http("GET", API.fetchContract(runId), { timeoutMs: 30000 });
          if (!contract || typeof contract !== "object") throw new Error("Empty or invalid contract JSON");
          return contract;
        }

        if (stateName === "Failed" || stateName === "Unknown" || !okDuring.has(stateName)) {
          const msg = st?.error?.message || `Run ended with unexpected state: ${stateName}`;
          throw new Error(msg);
        }
      } catch (e) {
        consecutiveErrors += 1;
        UI.log("Status poll error: " + (e?.message || e));
        if (consecutiveErrors > 1) throw e; // tolerate one transient error
      }

      attempt += 1;
      const sleepMs = Math.min(1000 + attempt * 500, 5000); // 1s → 5s
      await new Promise(r => setTimeout(r, sleepMs));
    }
  }

  // ---------- Boot ----------
  document.addEventListener("DOMContentLoaded", () => {
    mountTabs();
    requestAnimationFrame(() => {
      if (!state.tabsMounted || !$("#sectionTabs")?.children?.length) mountTabs(true);
    });
    window.addEventListener("pageshow", () => {
      if (!state.tabsMounted || !$("#sectionTabs")?.children?.length) mountTabs(true);
    });
    renderExecutiveSummary();

    const go = $("#goBtn");
    const csv = $("#csvUpload");
    const recent = $("#runSelect");
    const csvBadge = $("#csvBadge");
    const leftRail = $("#inputs");

    let isRunning = false;
    function setRunning(b) {
      isRunning = !!b;
      UI.setBusy(isRunning);
      updateGo();
    }

    function updateGo() {
      if (!go) return;
      const hasCsv = !!(csv && csv.files && csv.files.length > 0);
      const hasRecent = !!(recent && recent.value && recent.value.trim() !== "");
      go.disabled = isRunning || !(hasCsv || hasRecent);
    }

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

    if (leftRail && go) {
      leftRail.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" && !go.disabled && !isRunning) {
          ev.preventDefault();
          go.click();
        }
      });
    }
  });

  const SECTIONS = [
    { id: "exec", label: "Executive Summary", render: renderExecutiveSummary },
    { id: "elog", label: "Evidence Log", render: renderEvidenceLog },
    { id: "cases", label: "Case Studies", render: renderCaseLibrary },
    { id: "pos", label: "Positioning", render: renderPositioning },
    { id: "icp", label: "Messaging", render: renderICPMatrix },
    { id: "offer", label: "Strategy & Assets", render: renderOffer },
    { id: "chan", label: "Go-to-market", render: renderChannel },
    { id: "se", label: "Sales Battle Card", render: renderSalesEnablement },
    { id: "ml", label: "Measurement", render: renderMeasurement },
    { id: "comp", label: "Governance", render: renderCompliance },
    { id: "risk", label: "Contingencies", render: renderRisks },
    { id: "one", label: "One Page Summary", render: renderOnePager }
  ];
})();
