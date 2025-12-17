/* /src/js/campaign.js — unified (start/poll + renderers + tabs) 15-12-2025 v33
   Gold schema aware:
   - Understands "Gold Campaign" contract shape (executive_summary, value_proposition,
     messaging_matrix, sales_enablement, go_to_market_plan, 
     compliance_and_governance, one_pager_summary).
   - Falls back to legacy shapes if Gold fields are absent.
*/

window.CampaignUI = window.CampaignUI || {};
(function () {
  // ---------- DOM helpers ----------
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const rowsOf = (v) => Array.isArray(v) ? v : (v == null ? [] : [v]);

  // ---------- App state ----------
  const state = {
    contract: null,
    evidence: [],
    active: "exec",
    tabsMounted: false,
    viability: null,
    timeline: [],
    csvSummary: null
  };

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

  function makeHeading(text, level = 3) {
    const lvl = Number.isInteger(level) && level >= 1 && level <= 6 ? level : 3;
    const h = document.createElement("h" + String(lvl));
    h.textContent = String(text || "");
    return h;
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

  // === VIABILITY LOADER (top-level helper) ===
  async function loadViability(prefix) {

    if (!prefix) {
      console.warn("[UI] loadViability: missing prefix — viability skipped");
      state.viability = null;
      return;
    }

    try {
      let p = String(prefix).trim();
      if (!p.endsWith("/")) p += "/";

      // Validate that resultsBaseUrl is absolute
      const base = state.resultsBaseUrl || "";
      if (!base.startsWith("http")) {
        console.warn("[UI] resultsBaseUrl is not absolute:", base);
      }

      const url = `${base}${p}strategy_v2/viability.json`;

      console.log("[UI] viability url:", url);

      const res = await fetch(url, { method: "GET" });

      if (!res.ok) {
        console.warn("[UI] viability.json not found or not OK:", res.status, url);
        state.viability = null;
        return;
      }

      const text = await res.text();
      try {
        state.viability = JSON.parse(text);
      } catch (err) {
        console.warn("[UI] viability.json invalid JSON:", text.slice(0, 200));
        state.viability = null;
        return;
      }

      console.log("[UI] Loaded viability", state.viability);

    } catch (err) {
      console.warn("[UI] Failed to load viability.json", err);
      state.viability = null;
    }
  }

  function buildCsvSummary(rows, buyerIndustryInput) {
    // Defensive guards
    if (!Array.isArray(rows)) rows = [];

    // Extract industries safely
    const allIndustries = Array.from(
      new Set(
        rows
          .map(r => (r?.SimplifiedIndustry || "").trim())
          .filter(Boolean)
      )
    ).sort();

    // User-entered industry (may not exist in CSV)
    const buyerIndustry = (buyerIndustryInput || "").trim();

    // Filter rows ONLY if an exact match exists
    const rowsScoped = buyerIndustry
      ? rows.filter(r =>
        String(r?.SimplifiedIndustry || "").trim().toLowerCase() ===
        buyerIndustry.toLowerCase()
      )
      : rows;

    // Frequency buckets (all pure)
    const itSpend = freqFromCSV(rowsScoped.map(r => r?.ITSpendPct || ""));
    const blockers = freqFromCSV(rowsScoped.map(r => r?.TopBlockers || ""));
    const purchases = freqFromCSV(rowsScoped.map(r => r?.TopPurchases || ""));
    const needs = freqFromCSV(rowsScoped.map(r => r?.TopNeedsSupplier || ""));

    const sampleCompanies = rowsScoped
      .slice(0, 10)
      .map(r => r?.CompanyName || "")
      .filter(Boolean);

    return {
      schema: `csv-summary-v1:${buyerIndustry ? buyerIndustry.toLowerCase() : "all"}`,
      buyerIndustry: buyerIndustry || null,
      industriesAvailable: allIndustries,   // pure and safe
      rowCountAll: rows.length,
      rowCountScoped: rowsScoped.length,
      itSpend,
      blockers,
      purchases,
      needs,
      sampleCompanies
    };
  }

  // -- helper: normalise Executive Summary shapes --
  function resolveExecutiveSummaryShapes(es, esLegacy) {
    // Prefer new object shape
    if (es && typeof es === "object" && !Array.isArray(es)) {
      const lead =
        (typeof es.lead_paragraph === "string" && es.lead_paragraph) ||
        (typeof es.paragraph === "string" && es.paragraph) ||
        (typeof es.text === "string" && es.text) ||
        (typeof es.headline === "string" && es.headline) ||
        "";
      const bullets = Array.isArray(es.bullets) ? es.bullets.filter(Boolean) : [];
      return { lead: lead.trim(), bullets };
    }

    // Fallback: legacy array in executive_summary
    if (Array.isArray(es)) {
      const lead = String(es[0] || "").trim();
      const bullets = es.slice(1).map(s => String(s || "").trim()).filter(Boolean);
      return { lead, bullets };
    }

    // Fallback: legacy array in executive_summary_legacy
    if (Array.isArray(esLegacy)) {
      const lead = String(esLegacy[0] || "").trim();
      const bullets = esLegacy.slice(1).map(s => String(s || "").trim()).filter(Boolean);
      return { lead, bullets };
    }

    // Empty default
    return { lead: "", bullets: [] };
  }

  function renderValue(value) {
    const wrap = document.createElement("div");

    if (value == null) {
      wrap.appendChild(makePre("(none)"));
      return wrap;
    }

    // STRING
    if (typeof value === "string") {
      const p = document.createElement("p");
      p.textContent = value;
      wrap.appendChild(p);
      return wrap;
    }

    // ARRAY
    if (Array.isArray(value)) {
      if (value.every(v => typeof v === "string")) {
        wrap.appendChild(makeList(value));
        return wrap;
      }

      // array of objects
      value.forEach((item, i) => {
        const block = document.createElement("div");
        block.style.borderLeft = "3px solid #ddd";
        block.style.paddingLeft = "0.75rem";
        block.style.margin = "0.5rem 0";

        const h = document.createElement("h4");
        h.textContent = item.title || `Item ${i + 1}`;
        block.appendChild(h);

        for (const [k, v] of Object.entries(item)) {
          if (k === "title") continue;
          const sub = document.createElement("div");
          const lab = document.createElement("strong");
          lab.textContent = `${k}: `;
          sub.appendChild(lab);
          sub.appendChild(renderValue(v));
          block.appendChild(sub);
        }
        wrap.appendChild(block);
      });
      return wrap;
    }

    // OBJECT
    if (typeof value === "object") {
      for (const [k, v] of Object.entries(value)) {
        const line = document.createElement("div");
        const label = document.createElement("strong");
        label.textContent = `${k}: `;
        line.appendChild(label);
        line.appendChild(renderValue(v));
        wrap.appendChild(line);
      }
      return wrap;
    }

    // Fallback
    wrap.textContent = String(value);
    return wrap;
  }

  function renderField(label, value) {
    const wrap = document.createElement("div");
    const h = document.createElement("h4");
    h.textContent = label;
    h.style.marginTop = "1rem";
    wrap.appendChild(h);
    wrap.appendChild(renderValue(value));
    return wrap;
  }

  function renderSuccessTarget(value) {
    const wrap = document.createElement("div");
    const h = document.createElement("h4");
    h.textContent = "Success target";
    h.style.marginTop = "1rem";
    wrap.appendChild(h);

    if (value == null) {
      wrap.appendChild(makePre("(none)"));
      return wrap;
    }

    // String mode
    if (typeof value === "string") {
      wrap.appendChild(renderValue(value));
      return wrap;
    }

    // Object mode
    if (typeof value === "object") {
      wrap.appendChild(renderValue(value));
      return wrap;
    }

    // Fallback
    wrap.appendChild(renderValue(String(value)));
    return wrap;
  }

  // ---------- Renderers (Gold-aware) ----------

  function renderExecutiveSummary() {
    const wrap = document.createElement("div");
    const ss = state.contract?.strategy_v2?.story_spine;
    if (ss && typeof ss === "object") {
      const wrap2 = document.createElement("div");

      if (ss.lead) {
        const p = document.createElement("p");
        p.textContent = ss.lead;
        wrap.appendChild(p);
      }

      if (Array.isArray(ss.bullets)) {
        wrap2.appendChild(makeList(ss.bullets.slice(0, 10)));
      }

      if (Array.isArray(ss.citations)) {
        wrap2.appendChild(makeHeading("Citations", 4));
        wrap2.appendChild(makeList(ss.citations));
      }

      setPanelContent(wrap);
      return;
    }
    // GOLD SHAPE: contract.executive_summary { title, paragraphs[], citations[] }
    const esGold = state.contract?.executive_summary;
    if (esGold && typeof esGold === "object" && Array.isArray(esGold.paragraphs)) {
      if (esGold.title) {
        const h = document.createElement("h3");
        h.textContent = esGold.title;
        wrap.appendChild(h);
      }

      esGold.paragraphs.forEach(pTxt => {
        if (!pTxt) return;
        const p = document.createElement("p");
        p.textContent = String(pTxt);
        wrap.appendChild(p);
      });

      if (Array.isArray(esGold.citations) && esGold.citations.length) {
        const h2 = document.createElement("h4");
        h2.textContent = "Citations";
        h2.style.marginTop = "1rem";
        wrap.appendChild(h2);
        wrap.appendChild(makeList(esGold.citations));
      }

      setPanelContent(wrap);
      return;
    }

    // LEGACY NARRATIVE OBJECTS:
    //    - contract.executive_summary (object)
    //    - contract.sections.executive_summary (object)
    //    - contract.executive_summary_narrative (object)
    const esNarr =
      (state.contract && typeof state.contract.executive_summary === "object" && !Array.isArray(state.contract.executive_summary))
        ? state.contract.executive_summary
        : (state.contract && typeof state.contract?.sections?.executive_summary === "object" && !Array.isArray(state.contract.sections.executive_summary))
          ? state.contract.sections.executive_summary
          : (state.contract && typeof state.contract.executive_summary_narrative === "object" && !Array.isArray(state.contract.executive_summary_narrative))
            ? state.contract.executive_summary_narrative
            : null;

    // If we have narrative fields, render them in the required order (no bullets)
    if (esNarr) {
      const order = [
        "environment_paragraph",
        "rationale_paragraph",
        "how_to_win_paragraph",
        "success_paragraph",
        "next_steps_paragraph"
      ];
      const hasAny = order.some(k => typeof esNarr[k] === "string" && esNarr[k].trim());
      if (hasAny) {
        order.forEach(k => {
          const v = typeof esNarr[k] === "string" ? esNarr[k].trim() : "";
          if (!v) return;
          const p = document.createElement("p");
          p.textContent = v;
          wrap.appendChild(p);
        });
        setPanelContent(wrap);
        return;
      }
    }

    // Legacy lead+bullets / array logic
    const esObj = state.contract?.sections?.executive_summary || state.contract?.executive_summary_object || null;
    const esLegacy = Array.isArray(state.contract?.executive_summary_legacy)
      ? state.contract.executive_summary_legacy
      : (Array.isArray(state.contract?.executive_summary) ? state.contract.executive_summary : null);

    const emptyObj = (v) => !v || (typeof v === "object" && !Array.isArray(v) && !Object.keys(v).length);
    const isEmpty = (v) => v == null || (Array.isArray(v) && v.length === 0) || emptyObj(v);

    if (isEmpty(esObj) && isEmpty(esLegacy)) {
      setPanelContent(makePre("The executive summary will show here when your campaign has been created."));
      return;
    }

    // Normalise to { lead, bullets[] } using helper
    const norm = resolveExecutiveSummaryShapes(esObj, esLegacy);
    const para = typeof norm?.lead === "string" ? norm.lead.trim() : "";
    let bullets = Array.isArray(norm?.bullets) ? norm.bullets.filter(Boolean) : [];

    // Prefer Moore VP as the lead if available, and cap bullets ≤ 6
    const mooreObj =
      state.contract?.positioning_and_differentiation?.value_prop_moore ||
      state.contract?.positioning_and_differentiation?.value_proposition_moore ||
      state.contract?.campaign_strategy?.value_proposition_moore ||
      state.contract?.strategy?.value_proposition_moore;

    const mooreLead = (mooreObj && (mooreObj.paragraph || "")) ? String(mooreObj.paragraph).trim() : "";
    const finalLead = mooreLead || para;
    bullets = bullets.slice(0, 6);

    if (finalLead) {
      const p = document.createElement("p");
      p.textContent = finalLead;
      wrap.appendChild(p);
    }
    if (bullets.length) {
      const ul = document.createElement("ul");
      ul.className = "list";
      for (const b of bullets) {
        const li = document.createElement("li");
        li.textContent = String(b || "");
        ul.appendChild(li);
      }
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
      if (!e) return false;
      const source = String(e.source_type || "").toLowerCase();
      const title = String(e.title || "").trim();
      const url = String(e.url || "").trim();
      const summary = String(e.summary || "").trim();
      const quote = String(e.quote || "").trim();

      // Allow Customer profile items even when there is no external URL
      if (source === "customer profile") {
        return !!(title || summary || quote);
      }

      // All other sources must have https URL + a bit of body
      if (!url || !title) return false;
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

    // --- NEW: strategy_v2.proof_points override ---
    const pp2 = state.contract?.strategy_v2?.proof_points;
    if (Array.isArray(pp2) && pp2.length) {
      const wrap = document.createElement("div");
      wrap.appendChild(makeHeading("Proof points (v2)"));
      wrap.appendChild(makeList(pp2));
      setPanelContent(wrap);
      return;
    }

    // Modern case-study containers from writer (curated, website, verified)
    const cs2 = state.contract?.strategy_v2?.case_studies;
    if (cs2 && typeof cs2 === "object") {
      const wrap = document.createElement("div");
      wrap.appendChild(makeHeading("Case studies (v2)"));

      const combined = [
        ...(cs2.curated || []),
        ...(cs2.website || []),
        ...(cs2.verified || [])
      ];

      if (!combined.length) {
        wrap.appendChild(makePre("No case studies available."));
        setPanelContent(wrap);
        return;
      }

      const rows = combined.map(k => {
        const bullets = rowsOf(k?.bullets);
        const href = k?.link || k?.url || "";
        return [
          k?.customer || "",
          k?.industry || "",
          k?.headline || "",
          bullets,
          href ? { __link: true, href, text: href } : "",
          k?.source || ""
        ];
      });

      const headers = ["Customer", "Industry", "Headline", "Bullets", "Link", "Source"];
      wrap.appendChild(makeTable(headers, rows));
      setPanelContent(wrap);
      return;
    }

    // Existing legacy / website case-study logic
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
      const base = [
        customer,
        industry,
        headline,
        bullets,
        href ? { __link: true, href, text: href } : "",
        source
      ];
      if (hasVerifiedFlag) base.push(k?.verified ? "✓" : "");
      return base;
    });

    const table = makeTable(headers, rows);
    setPanelContent(table);
  }

  function renderPositioning() {
    const wrap = document.createElement("div");

    const h1 = document.createElement("h3");
    h1.textContent = "Value Proposition";
    wrap.appendChild(h1);

    // --- NEW: strategy_v2.value_proposition + right_to_play override ---
    const vp2 = state.contract?.strategy_v2?.value_proposition;
    const rtp2 = state.contract?.strategy_v2?.right_to_play;

    if (vp2 && typeof vp2 === "object") {
      wrap.appendChild(makeHeading("Value Proposition (v2)"));

      Object.entries(vp2).forEach(([label, value]) => {
        wrap.appendChild(renderField(label, value));
      });

      if (rtp2) {
        wrap.appendChild(makeHeading("Right to Play"));
        wrap.appendChild(renderField("right_to_play", rtp2));
      }

      setPanelContent(wrap);
      return;
    }

    // GOLD SHAPE
    const vpGold = state.contract?.value_proposition;
    if (vpGold && typeof vpGold === "object") {
      if (vpGold.narrative) {
        const paraBlocks = String(vpGold.narrative)
          .split(/\n{2,}/)
          .map(p => p.trim())
          .filter(Boolean);
        paraBlocks.forEach(pTxt => {
          const p = document.createElement("p");
          p.textContent = pTxt;
          wrap.appendChild(p);
        });
      }

      const moore = vpGold.moore;
      if (moore && typeof moore === "object") {
        const h2 = document.createElement("h3");
        h2.textContent = "Positioning (Moore)";
        h2.style.marginTop = "1rem";
        wrap.appendChild(h2);

        wrap.appendChild(makeTable(
          ["For", "Who", "The", "Is a", "That", "Unlike", "We provide"],
          [[
            moore.for || "",
            moore.who || "",
            moore.the || "",
            moore.is_a || "",
            moore.that || "",
            moore.unlike || "",
            moore.we_provide || ""
          ]]
        ));
      }

      if (vpGold.competitive_position) {
        const h3 = document.createElement("h3");
        h3.textContent = "Competitive position";
        h3.style.marginTop = "1rem";
        wrap.appendChild(h3);
        wrap.appendChild(makePre(vpGold.competitive_position));
      }

      if (Array.isArray(vpGold.proof_points) && vpGold.proof_points.length) {
        const h4 = document.createElement("h3");
        h4.textContent = "Proof points";
        h4.style.marginTop = "1rem";
        wrap.appendChild(h4);
        wrap.appendChild(makeList(vpGold.proof_points));
      }

      setPanelContent(wrap);
      return;
    }

    // LEGACY POSITIONING
    const pos = state.contract?.positioning_and_differentiation || {};

    const vpn = pos.value_prop_narrative;
    if (vpn && typeof vpn === "object") {
      const order = [
        "lead",
        "customer_problem_paragraph",
        "right_to_play_paragraph",
        "differentiation_paragraph",
        "competitor_positions_paragraph",
        "proof_points_paragraph"
      ];
      order.forEach(k => {
        const v = (typeof vpn[k] === "string") ? vpn[k].trim() : "";
        if (!v) return;
        const el = document.createElement(k === "lead" ? "pre" : "p");
        el.textContent = v;
        wrap.appendChild(el);
      });
    }

    const vpm =
      pos.value_prop_moore ||
      pos.value_proposition_moore ||
      state.contract?.campaign_strategy?.value_proposition_moore ||
      state.contract?.strategy?.value_proposition_moore;

    if (vpm && (vpm.paragraph || vpm.fields)) {
      // Narrative paragraph (if present)
      if (vpm.paragraph || pos.value_prop) {
        const pre = document.createElement("pre");
        pre.textContent = String(vpm.paragraph || pos.value_prop || "");
        wrap.appendChild(pre);
      }

      // Structured fields from the Moore template
      const fields = vpm.fields || null;
      if (fields && typeof fields === "object") {
        wrap.appendChild(renderField("Positioning (Moore)", fields));
      }
    } else if (!vpn) {
      // Fallback to simple value_prop string
      wrap.appendChild(makePre(pos.value_prop || ""));
    }

    const sw = pos.swot || {};
    const hasSw = [sw.strengths, sw.weaknesses, sw.opportunities, sw.threats]
      .some(a => Array.isArray(a) && a.length);

    const h2 = document.createElement("h3");
    h2.textContent = "SWOT";
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
      const h3 = document.createElement("h3");
      h3.textContent = "Differentiators";
      wrap.appendChild(h3);
      wrap.appendChild(makeList(pos.differentiators));
    }

    const comp = rowsOf(pos.competitor_set);
    if (comp.length) {
      const h4 = document.createElement("h3");
      h4.textContent = "Competitor Set";
      wrap.appendChild(h4);

      // Prefer a dedicated competitive_battlecard if available in the contract
      const battlecard =
        state.contract?.sales_enablement?.competitive_battlecard ||
        pos.competitive_battlecard ||
        null;

      wrap.appendChild(renderField("Competitive battlecard", battlecard || comp));
    }
    setPanelContent(wrap);
  }


  function renderICPMatrix() {
    const mm = state.contract?.messaging_matrix || {};
    const wrap = document.createElement("div");

    // --- NEW: strategy_v2.buyer_strategy override ---
    const bs2 = state.contract?.strategy_v2?.buyer_strategy;
    if (bs2 && typeof bs2 === "object") {
      wrap.appendChild(makeHeading("Buyer strategy (v2)"));
      const order = [
        "personas",
        "priorities",
        "value_drivers",
        "triggers",
        "objections",
        "messages",
        "segments"
      ];

      order.forEach(k => {
        if (bs2[k] != null) {
          wrap.appendChild(renderField(k, bs2[k]));
        }
      });

      // render any extra fields
      Object.entries(bs2).forEach(([label, value]) => {
        if (!order.includes(label)) {
          wrap.appendChild(renderField(label, value));
        }
      });
      setPanelContent(wrap);
      return;
    }

    // GOLD SHAPE: audiences, pillars, support_points
    const hasGold =
      (Array.isArray(mm.audiences) && mm.audiences.length) ||
      (Array.isArray(mm.pillars) && mm.pillars.length) ||
      (Array.isArray(mm.support_points) && mm.support_points.length);

    if (hasGold) {
      const h1 = document.createElement("h3");
      h1.textContent = "Key audiences";
      wrap.appendChild(h1);
      wrap.appendChild(makeList(mm.audiences || []));

      const h2 = document.createElement("h3");
      h2.textContent = "Messaging pillars";
      h2.style.marginTop = "1rem";
      wrap.appendChild(h2);
      wrap.appendChild(makeList(mm.pillars || []));

      const h3 = document.createElement("h3");
      h3.textContent = "Support points";
      h3.style.marginTop = "1rem";
      wrap.appendChild(h3);
      wrap.appendChild(makeList(mm.support_points || []));

      if (Array.isArray(mm.citations) && mm.citations.length) {
        const h4 = document.createElement("h3");
        h4.textContent = "Citations";
        h4.style.marginTop = "1rem";
        wrap.appendChild(h4);
        wrap.appendChild(makeList(mm.citations));
      }

      setPanelContent(wrap);
      return;
    }

    // LEGACY MATRIX SHAPE
    const h1 = document.createElement("h3"); h1.textContent = "Non-negotiables";
    wrap.appendChild(h1);
    wrap.appendChild(makeList(mm.nonnegotiables || []));

    const h2 = document.createElement("h3"); h2.textContent = "Messaging Matrix";
    wrap.appendChild(h2);

    const headers = ["Persona", "Pain", "Value statement", "Proof", "CTA"];
    const rows = rowsOf(mm.matrix).map(r => [
      r.persona || "",
      r.pain || "",
      r.value_statement || "",
      r.proof || "",
      r.cta || ""
    ]);

    const table = makeTable(headers, rows);

    const TRS = table.querySelectorAll("tbody tr");
    const claimRe = /\bCLM-\d{3}\b/g;
    rowsOf(mm.matrix).forEach((r, i) => {
      const ids = new Set(
        ((r.value_statement || "").match(claimRe) || [])
          .concat((r.proof || "").match(claimRe) || [])
      );
      if (ids.size) TRS[i].setAttribute("data-claims", Array.from(ids).join(" "));
    });

    wrap.appendChild(table);
    setPanelContent(wrap);
  }

  function renderOffer() {
    const wrap = document.createElement("div");

    // --- NEW: strategy_v2.gtm_strategy override ---
    const gtm2 = state.contract?.strategy_v2?.gtm_strategy;
    if (gtm2 && typeof gtm2 === "object") {
      wrap.appendChild(makeHeading("Go-to-market strategy (v2)"));
      Object.entries(gtm2).forEach(([label, value]) => {
        wrap.appendChild(renderField(label, value));
      });
      // ensure WTP/HTW fields appear even if backend uses new names
      if (gtm2.where_to_play)
        wrap.appendChild(renderField("Where to play", gtm2.where_to_play));

      if (gtm2.how_to_win)
        wrap.appendChild(renderField("How to win", gtm2.how_to_win));

      if (gtm2.competitive_context)
        wrap.appendChild(renderField("Competitive context", gtm2.competitive_context));
      setPanelContent(wrap);
      return;
    }

    // GOLD SHAPE
    const gtm = state.contract?.go_to_market_plan || {};
    const hasGold = gtm && Object.keys(gtm).length;

    if (hasGold) {
      wrap.appendChild(makeHeading("Go-to-market strategy"));

      [
        ["Objective", gtm.objective],
        ["Target market", gtm.target_market],
        ["Marketing actions", gtm.marketing_actions],
        ["Sales actions", gtm.sales_actions],
        ["Pipeline model", gtm.pipeline_model],
        ["Recommended CTA", gtm.cta]
      ].forEach(([label, content]) => {
        if (content) wrap.appendChild(renderField(label, content));
      });

      if (Array.isArray(gtm.citations) && gtm.citations.length) {
        wrap.appendChild(makeHeading("Citations"));
        wrap.appendChild(makeList(gtm.citations));
      }
      if (gtm.success_target !== undefined) {
        wrap.appendChild(renderSuccessTarget(gtm.success_target));
      }
      setPanelContent(wrap);
      return;
    }

    // LEGACY SHAPE
    const offer = state.contract?.offer_strategy || {};
    const lp = offer.landing_page || {};

    const rows = [
      ["Hero", lp.hero || ""],
      ["Why it matters", rowsOf(lp.why_it_matters)],
      ["What you get", rowsOf(lp.what_you_get)],
      ["How it works", rowsOf(lp.how_it_works)],
      ["Outcomes", rowsOf(lp.outcomes)],
      ["Proof", rowsOf(lp.proof)],
      ["CTA", lp.cta || ""]
    ];
    if (offer.success_target !== undefined) {
      wrap.appendChild(renderSuccessTarget(offer.success_target));
    }

    if (Array.isArray(offer.assets_checklist)) {
      wrap.appendChild(makeHeading("Assets checklist"));
      wrap.appendChild(makeList(offer.assets_checklist));
    }

    setPanelContent(wrap);
  }

  function renderSalesEnablement() {
    const wrap = document.createElement("div");

    // GOLD SHAPE: contract.sales_enablement
    const seGold = state.contract?.sales_enablement || {};
    const hasGold = seGold && typeof seGold === "object" && (
      seGold.campaign_overview ||
      (Array.isArray(seGold.buyer_outcomes) && seGold.buyer_outcomes.length) ||
      (Array.isArray(seGold.discovery_questions) && seGold.discovery_questions.length) ||
      seGold.master_pitch
    );

    if (hasGold) {
      if (seGold.campaign_overview) {
        const h0 = document.createElement("h3");
        h0.textContent = "Campaign overview";
        wrap.appendChild(h0);
        wrap.appendChild(makePre(seGold.campaign_overview));
      }

      if (Array.isArray(seGold.buyer_outcomes) && seGold.buyer_outcomes.length) {
        const h1 = document.createElement("h3");
        h1.textContent = "Buyer outcomes";
        h1.style.marginTop = "1rem";
        wrap.appendChild(h1);
        wrap.appendChild(makeList(seGold.buyer_outcomes));
      }

      if (Array.isArray(seGold.discovery_questions) && seGold.discovery_questions.length) {
        const h2 = document.createElement("h3");
        h2.textContent = "Discovery questions";
        h2.style.marginTop = "1rem";
        wrap.appendChild(h2);
        wrap.appendChild(makeList(seGold.discovery_questions));
      }

      if (seGold.master_pitch) {
        const h3 = document.createElement("h3");
        h3.textContent = "Master pitch";
        h3.style.marginTop = "1rem";
        wrap.appendChild(h3);
        wrap.appendChild(makePre(seGold.master_pitch));
      }
      setPanelContent(wrap);
      return;
    }

    // LEGACY SHAPE
    const se = state.contract?.sales_enablement || {};

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

  function renderOnePager() {
    const wrap = document.createElement("div");

    // GOLD SHAPE: one_pager_summary { positioning, core_message, quick_facts[] }
    const op = state.contract?.one_pager_summary || {};
    if (op && typeof op === "object" && (op.positioning || op.core_message || Array.isArray(op.quick_facts))) {
      const h = document.createElement("h3");
      h.textContent = "One-page summary";
      wrap.appendChild(h);

      const rows = [];
      if (op.positioning) rows.push(["Positioning", op.positioning]);
      if (op.core_message) rows.push(["Core message", op.core_message]);
      if (Array.isArray(op.quick_facts) && op.quick_facts.length) {
        rows.push(["Quick facts", op.quick_facts]);
      }
      wrap.appendChild(makeTable(["Field", "Value"], rows));
      setPanelContent(wrap);
      return;
    }

    // LEGACY SHAPE
    const h = document.createElement("h3"); h.textContent = "One-pager bullets";
    wrap.appendChild(h);
    wrap.appendChild(makeList(rowsOf(state.contract?.one_pager_summary)));
    setPanelContent(wrap);
  }

  function renderViability() {
    const wrap = document.createElement("div");

    // No viability loaded
    if (!state.viability) {
      wrap.appendChild(makePre("No viability signals found."));
      setPanelContent(wrap);
      return;
    }

    // Title
    const h = document.createElement("h3");
    h.textContent = "Strategy viability (v3 evaluator)";
    wrap.appendChild(h);

    // Render every field in viability.json
    for (const [key, val] of Object.entries(state.viability)) {
      wrap.appendChild(renderField(key, val));
    }

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

  UI.pushTimeline = function (phase, note) {
    if (!phase) return;

    const at = new Date().toISOString();
    const entry = { at, phase, note };

    state.timeline.push(entry);

    const box = document.getElementById("runTimeline");
    if (!box) return;

    const autoScroll = (box.scrollTop + box.clientHeight + 40) >= box.scrollHeight;

    const line = `[${at}] ${phase}${note ? " — " + note : ""}\n`;
    if (box.textContent.trim() === "(no events yet)") {
      box.textContent = line;
    } else {
      box.textContent += line;
    }

    if (autoScroll) {
      box.scrollTop = box.scrollHeight;
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

    status: (runId) =>
      `/api/campaign-status?runId=${encodeURIComponent(runId)}`,

    fetchContract: (runId) =>
      `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=campaign`,

    fetchStrategyV2: (runId) =>
      `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=campaign_strategy`,

    fetchEvidence: (runId) =>
      `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=evidence`,

    fetchEvidenceLog: (runId) =>
      `/api/campaign-fetch?runId=${encodeURIComponent(runId)}&file=evidence_log`,
  };

  // ---------------------------------------------------------------------------
  // Start → fetch → viability loader
  // ---------------------------------------------------------------------------
  async function startRunOrResume() {
    UI.setBusy(true);
    UI.setStatus("Preparing…", "run");

    // UI inputs
    const salesModel = ($("#salesModel")?.value || "").trim().toLowerCase() || null;
    const notes = ($("#notes")?.value || "").trim() || null;

    const supplier_company = ($("#companyName")?.value || "").trim();
    const supplier_website = ($("#companyWebsite")?.value || "").trim();
    const supplier_linkedin = ($("#companyLinkedIn")?.value || "").trim();
    const supplier_products = (document.getElementById("supplier_products")?.value || "").trim();

    const uspsText = ($("#companyUsps")?.value || "").trim();
    const supplier_usps = uspsText
      ? uspsText.split(/\r?\n|;|,/).map(s => s.trim()).filter(Boolean)
      : [];

    const compText = ($("#relevantCompetitors")?.value || "").trim();
    const relevant_competitors = compText
      ? compText.split(/[,;\n]/).map(s => s.trim()).filter(Boolean).slice(0, 8)
      : [];

    const campaign_requirement_raw = ($("#campaignRequirement")?.value || "").trim().toLowerCase();
    const campaign_requirement = ["upsell", "win-back", "growth"].includes(campaign_requirement_raw)
      ? campaign_requirement_raw
      : null;

    // ----------------------
    // Industry selector logic (CSV-backed)
    // ----------------------

    // CASE A: User selects from dropdown
    let buyer_industry = null;

    const industrySelect = $("#buyerIndustrySelect");
    const industryCustom = $("#buyerIndustryCustom");

    if (industrySelect) {
      if (industrySelect.value === "__custom") {
        buyer_industry = (industryCustom?.value || "").trim() || null;
      } else if (industrySelect.value) {
        buyer_industry = industrySelect.value.trim();
      }
    }

    // CSV presence?
    const fileEl = $("#csvUpload");
    const hasCsv = !!(fileEl?.files?.[0]);

    // Recent run selected?
    const recent = $("#runSelect");
    const selectedRunId = (recent?.value || "").trim();

    // Resume if no new CSV
    if (!hasCsv && selectedRunId) {
      UI.log(`Resuming existing run: ${selectedRunId}`);
      UI.setRun(selectedRunId);
      return await fetchCompleteRun(selectedRunId, true);
    }

    // Otherwise start a new run
    UI.setStatus("Submitting…", "run");
    UI.log("Submitting job to /api/campaign-start");

    let csvSummary = null;
    let csvTextRaw = null;
    let rowCount = null;

    if (hasCsv) {
      const text = await fileEl.files[0].text();
      csvTextRaw = text;
      const rows = csvToArray(text);
      rowCount = rows.length;

      // Build CSV summary and store it
      csvSummary = buildCsvSummary(rows, buyer_industry || "");
      state.csvSummary = csvSummary;

      // Rebuild the Industry dropdown from CSV + wire up Custom…
      const sel = document.getElementById("buyerIndustrySelect");
      const customField = document.getElementById("buyerIndustryCustom");

      if (sel && customField) {
        // Clear any existing options
        sel.innerHTML = "";

        // 1) Auto-detect option
        const optAuto = document.createElement("option");
        optAuto.value = "";
        optAuto.textContent = "(auto-detect from CSV)";
        sel.appendChild(optAuto);

        // 2) CSV-driven industries
        const industries = Array.isArray(csvSummary?.industriesAvailable)
          ? csvSummary.industriesAvailable
          : [];

        industries.forEach(ind => {
          const opt = document.createElement("option");
          opt.value = ind;
          opt.textContent = ind;
          sel.appendChild(opt);
        });

        // 3) Custom option
        const optCustom = document.createElement("option");
        optCustom.value = "__custom";
        optCustom.textContent = "Custom…";
        sel.appendChild(optCustom);

        // Reset custom field
        customField.style.display = "none";
        customField.value = "";

        // Toggle custom input on selection
        sel.addEventListener("change", () => {
          if (sel.value === "__custom") {
            customField.style.display = "block";
            customField.focus();
          } else {
            customField.style.display = "none";
          }
        });
      }
    }


    // Populate <select id="buyerIndustrySelect"> with sorted distinct industries
    const sel = document.getElementById("buyerIndustrySelect");
    const customField = document.getElementById("buyerIndustryCustom");

    if (sel) {
      // Remove all before rebuilding (avoid stale UI)
      while (sel.firstChild) sel.removeChild(sel.firstChild);

      const optAuto = document.createElement("option");
      optAuto.value = "";
      optAuto.textContent = "(auto-detect from CSV)";
      sel.appendChild(optAuto);

      const industries =
        (state.csvSummary &&
          Array.isArray(state.csvSummary.industriesAvailable))
          ? state.csvSummary.industriesAvailable
          : [];

      industries.forEach(ind => {
        const opt = document.createElement("option");
        opt.value = ind;
        opt.textContent = ind;
        sel.appendChild(opt);
      });

      const optCustom = document.createElement("option");
      optCustom.value = "__custom";
      optCustom.textContent = "Custom…";
      sel.appendChild(optCustom);

      // show/hide custom input
      sel.addEventListener("change", () => {
        if (sel.value === "__custom") {
          if (customField) customField.style.display = "block";
        } else {
          if (customField) customField.style.display = "none";
        }
      });
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
      supplier_products,
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
    state.timeline = [];
    const tl = document.getElementById("runTimeline");
    if (tl) tl.textContent = "(no events yet)";

    return await fetchCompleteRun(runId, true);
  }

  function normaliseStrategyV2(raw) {
    if (!raw || typeof raw !== "object") return raw;

    // 1) Unwrap common wrappers so we get to the real core object
    //    - current worker: { strategy_v2: { story_spine, ... } }
    //    - older shapes might use campaign_strategy or data
    let core = raw;
    if (core.strategy_v2 && typeof core.strategy_v2 === "object") {
      core = core.strategy_v2;
    } else if (core.campaign_strategy && typeof core.campaign_strategy === "object") {
      core = core.campaign_strategy;
    } else if (core.data && typeof core.data === "object") {
      core = core.data;
    }

    const out = { ...core };

    // 2) Writer/legacy fields → canonical Gold fields

    // Legacy “flat” GTM fields → nested gtm_strategy
    if (core.where_to_play) {
      out.gtm_strategy = {
        ...(out.gtm_strategy || {}),
        where_to_play: core.where_to_play
      };
    }

    if (core.how_to_win) {
      out.gtm_strategy = {
        ...(out.gtm_strategy || {}),
        how_to_win: core.how_to_win
      };
    }

    if (core.competitive_context) {
      out.gtm_strategy = {
        ...(out.gtm_strategy || {}),
        competitive_context: core.competitive_context
      };
    }

    // Legacy VP shapes → value_proposition
    if (core.value_prop_moore && !out.value_proposition) {
      out.value_proposition = { moore: core.value_prop_moore };
    }

    if (core.positioning_and_differentiation && !out.value_proposition) {
      const pod = core.positioning_and_differentiation;
      if (pod && typeof pod === "object") {
        out.value_proposition = pod.value_proposition || null;
      }
    }

    // Messaging legacy → buyer_strategy
    if (core.messaging_matrix && !out.buyer_strategy) {
      out.buyer_strategy = { matrix: core.messaging_matrix };
    }

    // Legacy case-studies block → proof_points section
    if (core.case_studies && !out.proof_points) {
      out.proof_points = core.case_studies;
    }

    // Canonicalise fields expressed under nested "sections"
    if (core.sections && typeof core.sections === "object") {
      const sec = core.sections;

      if (sec.buyer_strategy && !out.buyer_strategy) {
        out.buyer_strategy = sec.buyer_strategy;
      }

      if (sec.value_proposition && !out.value_proposition) {
        out.value_proposition = sec.value_proposition;
      }

      if (sec.messages && !out.buyer_strategy) {
        out.buyer_strategy = { messages: sec.messages };
      }

      if (sec.gtm && !out.gtm_strategy) {
        out.gtm_strategy = sec.gtm;
      }
    }

    return out;
  }

  function detectPreferredTab(contract) {
    if (!contract || typeof contract !== "object") return null;

    // -------------------------
    // 0) WRITER OUTPUT FIRST
    // -------------------------
    // If the writer produced Gold/sections content, default to Executive Summary.
    // This prevents "completed but looks empty" when strategy_v2 is sparse.
    const hasWriterSections =
      (contract.sections && typeof contract.sections === "object" && Object.keys(contract.sections).length > 0) ||
      (contract.executive_summary && typeof contract.executive_summary === "object") ||
      (contract.value_proposition && typeof contract.value_proposition === "object") ||
      (contract.go_to_market_plan && typeof contract.go_to_market_plan === "object") ||
      (contract.sales_enablement && typeof contract.sales_enablement === "object");

    if (hasWriterSections) return "exec";

    const sv2 = contract.strategy_v2 || {};

    // -------------------------
    // 1) STRATEGY V2 (PHASE 2)
    // -------------------------
    // Executive summary (story spine)
    if (sv2.story_spine) return "exec";

    // GTM strategy
    if (
      sv2.gtm_strategy ||
      sv2.where_to_play ||
      sv2.how_to_win ||
      sv2.competitive_context
    ) {
      return "gtm";
    }

    // Buyer strategy / messaging
    if (sv2.buyer_strategy || sv2.personas || sv2.messages) {
      return "msg";
    }

    // Value proposition / differentiation
    if (sv2.value_proposition || sv2.value_prop_moore || sv2.right_to_play) {
      return "off";
    }

    // Case studies / proof points
    if (sv2.proof_points || sv2.case_studies) {
      return "pp";
    }

    // -------------------------
    // 2) SALES ENABLEMENT (ANY SHAPE)
    // -------------------------
    if (contract.sales_enablement) return "se";

    // -------------------------
    // 3) FALLBACK SIGNALS
    // -------------------------
    // If viability was loaded, it's useful to surface it.
    if (state?.viability) return "viab";

    // If we at least have evidence, show evidence log rather than an empty exec panel.
    const hasEvidence =
      (Array.isArray(contract.evidence_log) && contract.evidence_log.length) ||
      (Array.isArray(state?.evidence) && state.evidence.length);

    if (hasEvidence) return "elog";

    return null; // fall back to default ("exec")
  }

  async function fetchCompleteRun(runId, allowPoll) {
    const contract = await pollToCompletion(runId, allowPoll);
    try {
      const strategyV2 = await http(
        "GET",
        API.fetchStrategyV2(runId),
        { timeoutMs: 20000 }
      );
      if (strategyV2 && typeof strategyV2 === "object") {
        contract.strategy_v2 = normaliseStrategyV2(strategyV2);
      }
    } catch (e) {
      UI.log("strategy_v2 fetch skipped: " + (e?.message || e));
    }
    let evidenceItems = [];
    try {
      const evCanon = await http("GET", API.fetchEvidence(runId), { timeoutMs: 20000 });
      if (evCanon && Array.isArray(evCanon.claims)) {
        evidenceItems = evCanon.claims;
      } else if (Array.isArray(evCanon)) {
        evidenceItems = evCanon;
      }
    } catch (e1) {
      try {
        const evLegacy = await http("GET", API.fetchEvidenceLog(runId), { timeoutMs: 20000 });
        if (Array.isArray(evLegacy)) evidenceItems = evLegacy;
      } catch (e2) {
        UI.log("Evidence load failed completely: " + (e2?.message || e2));
      }
    }

    try {
      const prefix =
        contract?._meta?.source_prefix ||
        contract?.source_prefix ||
        contract?.prefix ||
        null;

      if (!prefix) {
        UI.log("[UI] viability skipped: no canonical prefix present");
      } else if (typeof loadViability === "function") {
        await loadViability(prefix);

        if (state.viability) {
          UI.log("[UI] viability loaded OK");
        } else {
          UI.log("[UI] viability not present (allowed)");
        }
      } else {
        UI.log("[UI] viability loader missing");
      }
    } catch (err) {
      console.warn("[UI] viability load failed (non-fatal)", err);
      UI.log("[UI] viability load failed (non-fatal)");
    }

    window.CampaignUI?.setContract?.(contract, { evidence: evidenceItems });
    try {
      const preferred = detectPreferredTab(contract);
      if (preferred) {
        state.active = preferred;
        mountTabs(true);
        renderActive();
        UI.log(`[UI] Auto-selected tab: ${preferred}`);
      }
    } catch (e) {
      UI.log("Tab auto-select failed: " + (e?.message || e));
    }

    UI.setStatus("Completed", "ok");
    return contract;
  }

  async function pollToCompletion(runId, allowPoll = true) {
    const normState = (s) => String(s || "Unknown");
    const stateKey = (s) => normState(s).toLowerCase();

    async function fetchCampaignContract(runId) {
      const contract = await http("GET", API.fetchContract(runId), { timeoutMs: 30000 });
      if (!contract || typeof contract !== "object") {
        throw new Error("Empty or invalid contract JSON");
      }
      return contract;
    }

    function isTerminalSuccess(statusObj) {
      const k = stateKey(statusObj?.state);
      if (k === "writer_ready") return true;
      if (k === "completed") return true;
      return false;
    }

    try {
      const peek = await http("GET", API.status(runId), { timeoutMs: 12000 });
      const stateName = normState(peek?.state);
      UI.setStatus(stateName, stateName === "Failed" ? "err" : "run");

      // ----- NEW: Failure Banner & Retry Button -----
      const errorBanner = document.getElementById("runErrorBanner");
      const retryBtn = document.getElementById("retryBtn");
      const restartBtn = document.getElementById("restartRunBtn");

      if (stateName.toLowerCase() === "failed") {
        const msg =
          peek?.error?.message ||
          peek?.error ||
          "This run failed. Please try again.";

        // Show banner
        if (errorBanner) {
          errorBanner.textContent = "Run failed: " + msg;
          errorBanner.style.display = "block";
        }

        // Show retry button (re-poll same run)
        if (retryBtn) {
          retryBtn.dataset.runId = runId;
          retryBtn.style.display = "inline-block";
        }

        // Show restart button (start a NEW run from same inputs)
        if (restartBtn) {
          restartBtn.dataset.runId = runId;
          restartBtn.style.display = "inline-block";
        }
      } else {
        // Clear banner & buttons once we recover or start polling again
        if (errorBanner) {
          errorBanner.style.display = "none";
        }
        if (retryBtn) {
          retryBtn.style.display = "none";
          retryBtn.dataset.runId = "";
        }
        if (restartBtn) {
          restartBtn.style.display = "none";
          restartBtn.dataset.runId = "";
        }
      }

      UI.log(`Status: ${stateName}`);

      // TIMELINE SYNC — history + live update
      try {
        const hist = Array.isArray(peek?.history) ? peek.history : [];

        // replay items not yet seen
        const known = new Set(state.timeline.map(e => e.at + "|" + e.phase + "|" + (e.note || "")));
        for (const h of hist) {
          const sig = (h.at || "") + "|" + (h.phase || "") + "|" + (h.note || "");
          if (!known.has(sig)) {
            UI.pushTimeline(h.phase || "?", h.note || "");
          }
        }

        // also add the current stateName as a synthetic "status" event
        UI.pushTimeline("status", stateName);
      } catch (_) {
        /* safe fail */
      }

      if (isTerminalSuccess(peek)) {
        let lastErr;
        for (let k = 0; k < 4; k++) {
          try {
            const contract = await fetchCampaignContract(runId);
            return contract;
          } catch (e) {
            lastErr = e;
          }
          await new Promise(r => setTimeout(r, 300 + k * 400));
        }
        throw lastErr || new Error("Contract fetch failed");
      }

      if (!allowPoll) throw new Error(`Run is not completed (state: ${stateName})`);
    } catch (e) {
      UI.log("First status check failed, will retry once: " + (e?.message || e));
    }

    const started = Date.now();
    const MAX_MS = 8 * 60 * 1000; // 8 minutes
    let attempt = 0;
    let consecutiveErrors = 0;
    const okDuring = new Set([
      "queued",
      "validatinginput",
      "ingest",
      "draftcampaign",
      "evidencedigest",
      "outline",
      "strategysynthesis",
      "strategy_working",
      "strategy_ready",
      "sectionwrites",
      "writer_working",
      "assemble",
      "assembled",
      "completed"
    ].map(s => s.toLowerCase()));

    while (true) {
      if (Date.now() - started > MAX_MS) throw new Error("Timed out waiting for completion");

      try {
        const st = await http("GET", API.status(runId), { timeoutMs: 15000 });
        consecutiveErrors = 0;

        const stateName = normState(st?.state);
        const stateK = stateKey(st?.state);

        UI.setStatus(stateName, stateName === "Failed" ? "err" : "run");
        UI.log(`Status: ${stateName}`);

        if (isTerminalSuccess(st)) {
          const contract = await fetchCampaignContract(runId);
          return contract;
        }

        const isInProgress =
          stateK.endsWith("_queued") ||
          stateK.endsWith("_working") ||
          okDuring.has(stateK);

        if (stateK === "failed" || stateK === "unknown" || !isInProgress) {
          const msg =
            st?.error?.message ||
            `Run ended with unexpected state: ${stateName}`;
          throw new Error(msg);
        }

      } catch (e) {
        consecutiveErrors += 1;
        UI.log("Status poll error: " + (e?.message || e));
        if (consecutiveErrors > 1) throw e;
      }

      attempt += 1;
      const sleepMs = Math.min(1000 + attempt * 500, 5000);
      await new Promise(r => setTimeout(r, sleepMs));
    }
  }

  // ---------- Boot ----------
  document.addEventListener("DOMContentLoaded", () => {
    console.log("[UI] resultsBaseUrl =", state.resultsBaseUrl);
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

    // --- Industry selector: show/hide custom input on change (wired at boot) ---
    const industrySelect = document.getElementById("buyerIndustrySelect");
    const industryCustom = document.getElementById("buyerIndustryCustom");

    if (industrySelect && industryCustom) {
      // Initial state on page load
      if (industrySelect.value === "__custom") {
        industryCustom.style.display = "block";
      } else {
        industryCustom.style.display = "none";
      }

      industrySelect.addEventListener("change", () => {
        if (industrySelect.value === "__custom") {
          industryCustom.style.display = "block";
          industryCustom.focus();
        } else {
          industryCustom.style.display = "none";
          industryCustom.value = "";
        }
      });
    }

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
      const banner = document.getElementById("csvErrorBanner");
      const csvInvalid = banner && banner.style.display !== "none";
      go.disabled = isRunning || csvInvalid || !(hasCsv || hasRecent);
    }

    function formatBytes(n) {
      if (!Number.isFinite(n)) return "";
      const units = ["B", "KB", "MB", "GB"];
      let i = 0, v = n;
      while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
      return `${v.toFixed(v >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
    }

    if (csv) {
      csv.addEventListener("change", async () => {
        const f = csv.files && csv.files[0];
        const banner = document.getElementById("csvErrorBanner");

        // Reset UI defaults
        banner.style.display = "none";
        banner.textContent = "";

        if (!f) {
          csvBadge.textContent = "(no file)";
          updateGo();
          return;
        }

        // Update badge
        csvBadge.textContent = `${f.name} (${formatBytes(f.size)})`;

        // --- VALIDATION RULES ---
        try {
          const raw = await f.text();

          // 1) Must not be empty
          if (!raw.trim()) throw new Error("CSV file is empty.");

          const lines = raw.split(/\r?\n/).filter(Boolean);

          if (lines.length < 2)
            throw new Error("CSV must contain a header row and at least 1 data row.");

          // 2) Must contain Company Name + Company Number
          const header = lines[0].toLowerCase();

          if (!header.includes("company") || !header.includes("number"))
            throw new Error("CSV must include 'Company Name' and 'Company Number' columns.");

          // 3) Max size check (protect your function)
          if (lines.length > 50000)
            throw new Error("CSV is too large. Please upload a file with fewer than 50,000 rows.");

          // 4) No unreadable binary chars (common Excel corruption)
          if (/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/.test(raw))
            throw new Error("CSV contains unreadable characters. Please re-export it.");

          // SUCCESS → clear errors
          banner.style.display = "none";
        } catch (err) {
          banner.textContent = "CSV error: " + (err.message || err);
          banner.style.display = "block";
          csvBadge.textContent = "(invalid CSV)";
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
    // -------------------------
    // RETRY BUTTON
    // -------------------------
    const retryBtn = document.getElementById("retryBtn");
    if (retryBtn) {
      retryBtn.addEventListener("click", async () => {
        const runId = retryBtn.dataset.runId;
        if (!runId) return;

        UI.log(`Retrying run: ${runId}`);
        UI.setStatus("Retrying…", "run");

        // Hide banner
        const banner = document.getElementById("runErrorBanner");
        if (banner) banner.style.display = "none";
        retryBtn.style.display = "none";

        try {
          await fetchCompleteRun(runId, true);
        } catch (err) {
          UI.log("Retry failed: " + (err?.message || err));
          UI.setStatus("Failed", "err");
          if (banner) {
            banner.textContent = "Retry failed: " + (err?.message || err);
            banner.style.display = "block";
          }
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
  // --------------------------------------------------------------
  // Set absolute resultsBaseUrl (Blob storage public endpoint)
  // --------------------------------------------------------------
  state.resultsBaseUrl =
    window.CONFIG?.resultsBaseUrl ||
    "https://<YOUR-STORAGE-ACCOUNT>.blob.core.windows.net/results/";

  console.log("[UI] resultsBaseUrl =", state.resultsBaseUrl);

  const SECTIONS = [
    { id: "exec", label: "Executive summary", render: renderExecutiveSummary },
    { id: "gtm", label: "Go-to-market", render: renderOffer },           // Campaign strategy + segments + differentiation
    { id: "msg", label: "Messaging", render: renderICPMatrix },       // GTM messaging
    { id: "off", label: "Offering", render: renderPositioning },     // Portfolio & Capabilities
    { id: "se", label: "Sales enablement", render: renderSalesEnablement }, // Battle card
    { id: "pp", label: "Proof points", render: renderCaseLibrary },     // Case studies
    { id: "elog", label: "Evidence log", render: renderEvidenceLog },
    { id: "viab", label: "Viability", render: renderViability }
  ];

  // === ROUTING MAP (backend → UI tabs) ===
  const TAB_FIELD_MAP = {
    // Executive summary
    "executive_summary": "exec",
    "story_spine": "exec",

    // Go-to-market
    "gtm_strategy": "gtm",
    "where_to_play": "gtm",
    "how_to_win": "gtm",
    "competitive_context": "gtm",

    // Messaging
    "buyer_strategy": "msg",
    "messaging_matrix": "msg",

    // Offer / Value Proposition
    "value_proposition": "off",
    "right_to_play": "off",
    "portfolio_and_tech": "off",

    // Sales enablement
    "sales_enablement": "se",

    // Proof points
    "proof_points": "pp",
    "case_studies": "pp",

    // Evidence
    "evidence_log": "elog"
  };
})();
