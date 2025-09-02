/* /web/js/campaign.js
   Campaign Builder UI — COMPLETE.
   - Renders campaign.json when a run completes
   - Provides a status poller and "New run" wiring
   - Exposes window.CampaignUI.handleCompleted(runId)
   - Exposes window.Campaign.startStatusPoll(runId) and window.Campaign.startNewRun(body)
*/

(() => {
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  // -----------------------------
  // Minimal DOM helpers
  // -----------------------------
  function el(tag, attrs = {}, ...children) {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (k === "class") node.className = v;
      else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.substring(2), v);
      else if (v !== undefined && v !== null) node.setAttribute(k, v);
    }
    for (const child of children) {
      if (child == null) continue;
      if (Array.isArray(child)) node.append(...child);
      else if (child instanceof Node) node.append(child);
      else node.append(document.createTextNode(String(child)));
    }
    return node;
  }

  function toast(msg) {
    let t = $("#campaign-toast");
    if (!t) {
      t = el("div", { id: "campaign-toast", class: "toast" });
      document.body.appendChild(t);
    }
    t.textContent = msg;
    t.classList.add("show");
    setTimeout(() => t.classList.remove("show"), 1400);
  }

  function copyButton(getText) {
    return el(
      "button",
      {
        class: "btn btn-copy",
        type: "button",
        onclick: async () => {
          try {
            await navigator.clipboard.writeText(getText());
            toast("Copied!");
          } catch (e) {
            console.error(e);
            toast("Copy failed");
          }
        },
      },
      "Copy",
    );
  }

  function downloadDocxButton(runId) {
    return el(
      "button",
      {
        class: "btn btn-primary",
        type: "button",
        onclick: () => {
          const url = `/api/campaign/download?runId=${encodeURIComponent(runId)}`;
          window.location.href = url;
        },
      },
      "Download .docx",
    );
  }

  // -----------------------------
  // Renderers
  // -----------------------------
  function renderOverview(container, campaign) {
    container.innerHTML = "";
    const meta = campaign.meta || {};
    const proof = campaign.input_proof || {};

    const header = el(
      "div",
      { class: "section" },
      el("h3", {}, "Executive Summary"),
      el(
        "div",
        { class: "card" },
        el("pre", { class: "pre" }, campaign.executive_summary || "(none)"),
        copyButton(() => campaign.executive_summary || ""),
      ),
    );

    const metaGrid = el(
      "div",
      { class: "grid meta-grid" },
      metaItem("Tone profile", meta.tone_profile),
      metaItem("Persona focus", meta.persona_focus),
      metaItem("Evidence window (months)", meta.evidence_window_months),
      metaItem("Run ID", proof.run_id),
      metaItem("Row count", String(proof.row_count ?? "")),
      metaItem("CSV sha256", proof.csv_sha256),
      metaItem(
        "Ignored columns",
        Array.isArray(proof.ignored_columns_confirmed)
          ? proof.ignored_columns_confirmed.join(", ")
          : String(proof.ignored_columns_confirmed || ""),
      ),
      metaItem("Filters", typeof proof.filters === "object" ? JSON.stringify(proof.filters) : (proof.filters || "")),
    );

    container.append(header, el("h4", {}, "Meta"), metaGrid);
  }

  function metaItem(label, value) {
    return el(
      "div",
      { class: "card meta" },
      el("div", { class: "meta-label" }, label || ""),
      el("div", { class: "meta-value" }, value == null ? "" : String(value)),
    );
  }

  function renderLanding(container, campaign) {
    container.innerHTML = "";
    const lp = campaign.landing_page;

    if (lp == null) {
      container.append(el("div", { class: "muted" }, "No landing page content."));
      return;
    }

    if (typeof lp === "string") {
      container.append(el("div", { class: "card" }, el("pre", { class: "pre" }, lp), copyButton(() => lp)));
      return;
    }

    if (lp.headline) {
      container.append(
        el(
          "div",
          { class: "card" },
          el("h3", {}, lp.headline),
          lp.subheadline ? el("p", { class: "muted" }, lp.subheadline) : null,
          copyButton(() => [lp.headline, lp.subheadline].filter(Boolean).join("\n")),
        ),
      );
    }

    if (Array.isArray(lp.sections)) {
      lp.sections.forEach((s) => {
        const content = Array.isArray(s.bullets)
          ? el(
              "ul",
              {},
              ...s.bullets.map((b) => el("li", {}, b)),
            )
          : el("pre", { class: "pre" }, s.content || "");
        container.append(
          el(
            "div",
            { class: "card" },
            el("h4", {}, s.title || "Section"),
            content,
            copyButton(() => {
              const text = Array.isArray(s.bullets) ? s.bullets.join("\n") : s.content || "";
              return `${s.title || "Section"}\n\n${text}`;
            }),
          ),
        );
      });
    }

    if (lp.cta) {
      container.append(
        el(
          "div",
          { class: "card" },
          el("strong", {}, "CTA"),
          el("p", {}, lp.cta),
          copyButton(() => lp.cta),
        ),
      );
    }
  }

  function renderEmails(container, campaign) {
    container.innerHTML = "";
    const emails = Array.isArray(campaign.emails) ? campaign.emails : [];
    if (emails.length === 0) {
      container.append(el("div", { class: "muted" }, "No emails generated."));
      return;
    }
    emails.forEach((m, i) => {
      const subject = m.subject || `Email ${i + 1}`;
      const preview = m.preview || "";
      const body = m.body || "";
      container.append(
        el(
          "div",
          { class: "card email-card" },
          el(
            "div",
            { class: "email-head" },
            el("div", { class: "email-subject" }, subject),
            copyButton(() => subject + "\n\n" + body),
          ),
          preview ? el("div", { class: "email-preview muted" }, preview) : null,
          el("pre", { class: "pre" }, body),
        ),
      );
    });
  }

  function renderEvidence(container, campaign) {
    container.innerHTML = "";
    const items = Array.isArray(campaign.evidence_log) ? campaign.evidence_log : [];
    if (items.length === 0) {
      container.append(el("div", { class: "muted" }, "No evidence logged."));
      return;
    }

    const table = el("table", { class: "table" });
    const thead = el(
      "thead",
      {},
      el(
        "tr",
        {},
        el("th", {}, "Publisher"),
        el("th", {}, "Title"),
        el("th", {}, "Date"),
        el("th", {}, "URL"),
        el("th", {}, "Excerpt"),
      ),
    );
    const tbody = el("tbody", {});
    items.forEach((r) => {
      tbody.append(
        el(
          "tr",
          {},
          el("td", {}, r.publisher || ""),
          el("td", {}, r.title || ""),
          el("td", {}, r.date || ""),
          el("td", {}, r.url ? el("a", { href: r.url, target: "_blank", rel: "noopener noreferrer" }, "Open") : ""),
          el("td", {}, r.excerpt || ""),
        ),
      );
    });
    table.append(thead, tbody);
    container.append(table);
  }

  function renderSales(container, campaign) {
    container.innerHTML = "";
    const se = campaign.sales_enablement || {};
    const blocks = [];
    if (se.call_script) blocks.push(["Call Script", se.call_script]);
    if (se.one_pager) blocks.push(["One-Pager", se.one_pager]);
    if (blocks.length === 0) {
      container.append(el("div", { class: "muted" }, "No sales enablement content."));
      return;
    }
    blocks.forEach(([title, text]) => {
      container.append(
        el(
          "div",
          { class: "card" },
          el("h4", {}, title),
          el("pre", { class: "pre" }, text || ""),
          copyButton(() => text || ""),
        ),
      );
    });
  }

  // -----------------------------
  // Tab mounts
  // -----------------------------
  function ensure(id) {
    let n = document.getElementById(id);
    if (!n) {
      const parent = $("#campaign-content") || document.body;
      n = el("div", { id, class: "tab-section card" });
      parent.append(n);
    }
    return n;
  }

  function setActiveTab(id) {
    const tabs = $$(".tab");
    const panels = $$("#campaign-content > div");
    if (tabs.length === 0 || panels.length === 0) return;
    tabs.forEach((t) => t.classList.toggle("active", t.dataset.tabTarget === id));
    panels.forEach((p) => (p.style.display = p.id === id ? "" : "none"));
  }

  // -----------------------------
  // Stage / Run badge helpers
  // -----------------------------
  function setRunIdBadge(runId) {
    const n = $("#currentRunId");
    if (n) n.textContent = runId || "–";
  }

  function updateStageBar(state) {
    const order = ["ValidatingInput", "EvidenceBuilder", "DraftCampaign", "QualityGate", "Completed"];
    order.forEach((s) => {
      const elStep = document.getElementById("step-" + s);
      if (!elStep) return;
      elStep.classList.remove("active", "done");
      if (state === s) elStep.classList.add("active");
      if (order.indexOf(s) < order.indexOf(state)) elStep.classList.add("done");
    });
    const statusText = $("#statusText");
    if (statusText) statusText.textContent = state || "Waiting…";
  }

  // -----------------------------
  // Completed hook
  // -----------------------------
  async function handleCompleted(runId) {
    try {
      const res = await fetch(`/api/campaign/fetch?runId=${encodeURIComponent(runId)}&file=campaign`);
      if (!res.ok) throw new Error(`fetch campaign.json failed: ${res.status}`);
      const campaign = await res.json();

      const dlMount = $("#download-docx-mount");
      if (dlMount) {
        dlMount.innerHTML = "";
        dlMount.append(downloadDocxButton(runId));
      }

      renderOverview(ensure("tab-overview"), campaign);
      renderLanding(ensure("tab-landing"), campaign);
      renderEmails(ensure("tab-emails"), campaign);
      renderEvidence(ensure("tab-evidence"), campaign);
      renderSales(ensure("tab-sales"), campaign);

      setActiveTab("tab-overview");
    } catch (e) {
      console.error(e);
      toast("Failed to load campaign");
    }
  }

  // -----------------------------
  // Status poller + Start helpers
  // -----------------------------
  let _activeController = null;
  const _rendered = new Set();

  function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
  }

  async function pollOnce(runId, signal) {
    const res = await fetch(`/api/campaign/status?runId=${encodeURIComponent(runId)}`, { signal });
    if (!res.ok) throw new Error(`status ${res.status}`);
    const status = await res.json();

    // Update UI
    setRunIdBadge(runId);
    updateStageBar(status.state);

    // Render once on Completed
    if (status.state === "Completed" && !_rendered.has(runId)) {
      _rendered.add(runId);
      await handleCompleted(runId);
    }
    return status.state;
  }

  async function pollLoop(runId, controller) {
    let delay = 1200;
    for (;;) {
      if (controller.signal.aborted) return;
      try {
        const state = await pollOnce(runId, controller.signal);
        if (state === "Completed" || state === "Failed") return;
        await sleep(delay);
        if (delay < 2000) delay += 200;
      } catch (err) {
        if (controller.signal.aborted) return;
        console.warn("status poll error:", err);
        await sleep(2500);
      }
    }
  }

  function startStatusPoll(runId) {
    if (_activeController) _activeController.abort();
    const controller = new AbortController();
    _activeController = controller;
    setRunIdBadge(runId);
    pollLoop(runId, controller);
    return () => controller.abort();
  }

  async function startNewRun(body = {}) {
    // Collect a page value if present in UI (optional)
    const pageSel = $("#runSelect");
    const page = body.page || (pageSel && pageSel.value) || "leadgen";
    const payload = { page, rowCount: body.rowCount ?? 0, csv_sha256: body.csv_sha256 || "test" };

    const res = await fetch("/api/campaign/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) throw new Error(`start failed: ${res.status}`);
    const json = await res.json();
    const runId = json.runId;
    if (!runId) throw new Error("No runId returned from /api/campaign/start");

    setRunIdBadge(runId);
    startStatusPoll(runId);
    return runId;
  }

  // -----------------------------
  // Optional auto-wiring of "New run" button (id=newRunBtn)
  // -----------------------------
  document.addEventListener("DOMContentLoaded", () => {
    const btn = $("#newRunBtn");
    if (btn && !btn.dataset.wired) {
      btn.dataset.wired = "1";
      btn.addEventListener("click", async () => {
        try {
          await startNewRun({});
        } catch (e) {
          console.error(e);
          toast("Start failed");
        }
      });
    }

    // Tab click wiring (id/class structure from index.html). Guard against double-binding.
    const tabButtons = $$(".tab");
    tabButtons.forEach((b) => {
      if (b.dataset.wired) return;
      b.dataset.wired = "1";
      b.addEventListener("click", () => setActiveTab(b.dataset.tabTarget));
    });
  });

  // -----------------------------
  // Public API
  // -----------------------------
  window.CampaignUI = { handleCompleted };
  window.Campaign = {
    startStatusPoll,
    startNewRun,
    setRunId: setRunIdBadge,
    updateStage: updateStageBar,
    setActiveTab,
  };
})();
