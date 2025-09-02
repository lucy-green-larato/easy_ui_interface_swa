/* /web/js/campaign.js
   Campaign Builder UI: renders campaign.json when the run completes.
   Assumes your shell already polls /api/campaign/status and holds the current runId + page.
   If you already have a poller, the only new contract is handleCompleted(runId).
*/

(() => {
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  // ---- Minimal templating helpers ----
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

  function copyButton(getText) {
    return el("button", { class: "btn btn-copy", type: "button", onclick: async () => {
      try {
        await navigator.clipboard.writeText(getText());
        toast("Copied!");
      } catch (e) {
        console.error(e);
        toast("Copy failed");
      }
    }}, "Copy");
  }

  function downloadDocxButton(runId) {
    return el("button", {
      class: "btn btn-primary",
      type: "button",
      onclick: () => {
        const url = `/api/campaign/download?runId=${encodeURIComponent(runId)}`;
        window.location.href = url;
      }
    }, "Download .docx");
  }

  function toast(msg) {
    let t = $("#campaign-toast");
    if (!t) {
      t = el("div", { id: "campaign-toast", class: "toast" });
      document.body.appendChild(t);
    }
    t.textContent = msg;
    t.classList.add("show");
    setTimeout(() => t.classList.remove("show"), 1200);
  }

  // ---- Renderers ----
  function renderOverview(container, campaign) {
    container.innerHTML = "";
    const meta = campaign.meta || {};
    const proof = campaign.input_proof || {};

    const header = el("div", { class: "section" },
      el("h3", {}, "Executive Summary"),
      el("div", { class: "card" },
        el("pre", { class: "pre" }, campaign.executive_summary || "(none)"),
        copyButton(() => campaign.executive_summary || "")
      )
    );

    const metaGrid = el("div", { class: "grid meta-grid" },
      metaItem("Tone profile", meta.tone_profile),
      metaItem("Persona focus", meta.persona_focus),
      metaItem("Evidence window (months)", meta.evidence_window_months),
      metaItem("Run ID", proof.run_id),
      metaItem("Row count", String(proof.row_count ?? "")),
      metaItem("CSV sha256", proof.csv_sha256),
      metaItem("Ignored columns", (Array.isArray(proof.ignored_columns_confirmed) ? proof.ignored_columns_confirmed.join(", ") : String(proof.ignored_columns_confirmed || ""))),
      metaItem("Filters", typeof proof.filters === "object" ? JSON.stringify(proof.filters) : (proof.filters || ""))
    );

    container.append(header, el("h4", {}, "Meta"), metaGrid);
  }

  function metaItem(label, value) {
    return el("div", { class: "card meta" },
      el("div", { class: "meta-label" }, label || ""),
      el("div", { class: "meta-value" }, value == null ? "" : String(value))
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
      container.append(
        el("div", { class: "card" },
          el("pre", { class: "pre" }, lp),
          copyButton(() => lp)
        )
      );
      return;
    }

    // Object form
    if (lp.headline) {
      container.append(
        el("div", { class: "card" },
          el("h3", {}, lp.headline),
          lp.subheadline ? el("p", { class: "muted" }, lp.subheadline) : null,
          copyButton(() => [lp.headline, lp.subheadline].filter(Boolean).join("\n"))
        )
      );
    }

    if (Array.isArray(lp.sections)) {
      lp.sections.forEach((s) => {
        const content = Array.isArray(s.bullets)
          ? el("ul", {}, ...s.bullets.map(b => el("li", {}, b)))
          : el("pre", { class: "pre" }, s.content || "");
        container.append(
          el("div", { class: "card" },
            el("h4", {}, s.title || "Section"),
            content,
            copyButton(() => {
              const text = Array.isArray(s.bullets) ? s.bullets.join("\n") : (s.content || "");
              return `${s.title || "Section"}\n\n${text}`;
            })
          )
        );
      });
    }

    if (lp.cta) {
      container.append(
        el("div", { class: "card" },
          el("strong", {}, "CTA"),
          el("p", {}, lp.cta),
          copyButton(() => lp.cta)
        )
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
        el("div", { class: "card email-card" },
          el("div", { class: "email-head" },
            el("div", { class: "email-subject" }, subject),
            copyButton(() => subject + "\n\n" + body)
          ),
          preview ? el("div", { class: "email-preview muted" }, preview) : null,
          el("pre", { class: "pre" }, body)
        )
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
    const thead = el("thead", {}, el("tr", {},
      el("th", {}, "Publisher"),
      el("th", {}, "Title"),
      el("th", {}, "Date"),
      el("th", {}, "URL"),
      el("th", {}, "Excerpt")
    ));
    const tbody = el("tbody", {});
    items.forEach(r => {
      tbody.append(
        el("tr", {},
          el("td", {}, r.publisher || ""),
          el("td", {}, r.title || ""),
          el("td", {}, r.date || ""),
          el("td", {},
            r.url ? el("a", { href: r.url, target: "_blank", rel: "noopener noreferrer" }, "Open") : ""
          ),
          el("td", {}, r.excerpt || "")
        )
      );
    });
    table.append(thead, tbody);
    container.append(table);
  }

  function renderSales(container, campaign) {
    container.innerHTML = "";
    const se = campaign.sales_enablement || {};
    const blocks = [];

    if (se.call_script) {
      blocks.push(["Call Script", se.call_script]);
    }
    if (se.one_pager) {
      blocks.push(["One-Pager", se.one_pager]);
    }
    if (blocks.length === 0) {
      container.append(el("div", { class: "muted" }, "No sales enablement content."));
      return;
    }
    blocks.forEach(([title, text]) => {
      container.append(
        el("div", { class: "card" },
          el("h4", {}, title),
          el("pre", { class: "pre" }, text || ""),
          copyButton(() => text || "")
        )
      );
    });
  }

  // ---- Tab bootstrap ----
  function ensure(id) {
    let n = document.getElementById(id);
    if (!n) {
      // Creates the section if your shell didn't already include it
      const parent = $("#campaign-content") || document.body;
      n = el("div", { id, class: "tab-section" });
      parent.append(n);
    }
    return n;
  }

  // Exposed hook: call this when status.state === "Completed"
  async function handleCompleted(runId) {
    try {
      const res = await fetch(`/api/campaign/fetch?runId=${encodeURIComponent(runId)}&file=campaign`);
      if (!res.ok) throw new Error(`fetch campaign.json failed: ${res.status}`);
      const campaign = await res.json();

      // Optional: if you want to show a primary action near the title:
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
    } catch (e) {
      console.error(e);
      toast("Failed to load campaign");
    }
  }

  // Make available to your shell
  window.CampaignUI = {
    handleCompleted
  };
})();
