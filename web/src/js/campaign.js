/* /src/js/campaign.js — full file (featureful, still minimal deps) */

const $ = (sel, el = document) => el.querySelector(sel);
const $$ = (sel, el = document) => Array.from(el.querySelectorAll(sel));
const h = (tag, attrs = {}, ...children) => {
  const el = document.createElement(tag);
  Object.entries(attrs || {}).forEach(([k, v]) => {
    if (k === "class") el.className = v;
    else if (k.startsWith("on") && typeof v === "function") el.addEventListener(k.slice(2), v);
    else if (v !== undefined && v !== null) el.setAttribute(k, v);
  });
  for (const c of children.flat()) if (c != null) el.append(typeof c === "string" ? document.createTextNode(c) : c);
  return el;
};

function toast(msg) {
  const t = $("#campaign-toast");
  if (!t) return console.log(msg);
  t.textContent = msg;
  t.classList.add("show");
  setTimeout(() => t.classList.remove("show"), 1800);
}

const api = {
  start: async (payload) => {
    const res = await fetch("/api/campaign/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {})
    });
    if (!res.ok) throw new Error(`start ${res.status}`);
    return res.json(); // { runId }
  },
  status: async (runId) => {
    const res = await fetch(`/api/campaign/status?runId=${encodeURIComponent(runId)}`);
    if (!res.ok) throw new Error(`status ${res.status}`);
    return res.json(); // { state, ... }
  },
  fetchCampaign: async (runId) => {
    const res = await fetch(`/api/campaign/fetch?runId=${encodeURIComponent(runId)}&file=campaign`);
    if (!res.ok) throw new Error(`fetch campaign ${res.status}`);
    return res.json();
  },
  fetchEvidence: async (runId) => {
    const res = await fetch(`/api/campaign/fetch?runId=${encodeURIComponent(runId)}&file=evidence`);
    if (!res.ok) return [];
    return res.json();
  },
  regenerate: async (runId, section) => {
    const res = await fetch(`/api/campaign/regenerate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ runId, section })
    });
    if (!res.ok) throw new Error(`regenerate ${res.status}`);
    return res.json();
  },
  runs: async () => {
    const res = await fetch(`/api/runs`);
    if (!res.ok) throw new Error(`runs ${res.status}`);
    return res.json(); // flexible shape (array)
  },
  downloadDocx: (runId) => {
    window.location.href = `/api/campaign/download?runId=${encodeURIComponent(runId)}`;
  }
};

function setRunId(runId) {
  $("#currentRunId") && ($("#currentRunId").textContent = runId || "–");
  try { localStorage.setItem("campaign:lastRunId", runId || ""); } catch { }
  if (window.CampaignPage?.setRunId) window.CampaignPage.setRunId(runId);
}
function updateStage(state) {
  if (window.CampaignPage?.updateStage) return window.CampaignPage.updateStage(state);
  const order = ["ValidatingInput", "EvidenceBuilder", "DraftCampaign", "QualityGate", "Completed"];
  order.forEach(s => {
    const el = $("#step-" + s);
    if (!el) return;
    el.classList.remove("active", "done");
    if (s === state) el.classList.add("active");
    if (order.indexOf(s) < order.indexOf(state)) el.classList.add("done");
  });
  $("#statusText") && ($("#statusText").textContent = state || "Waiting…");
}
const copyBtn = (getText, label = "Copy") =>
  h("button", {
    class: "btn btn-copy", onclick: async () => {
      try { await navigator.clipboard.writeText(getText()); toast("Copied"); } catch (e) { console.log(e); }
    }
  }, label);
const fmtDate = (d) => { try { return new Date(d).toISOString().slice(0, 10); } catch { return d; } };

let CURRENT_RUN_ID = null;

/* ---------- Rendering ---------- */
function renderOverview(data) {
  const node = $("#tab-overview"); if (!node) return; node.innerHTML = "";
  node.append(
    h("div", { class: "workspace" },
      h("h3", {}, "Executive summary"),
      h("p", {}, data.executive_summary || "(empty)"),
      h("div", { class: "btn-row" },
        copyBtn(() => data.executive_summary || "", "Copy summary"),
        h("button", { class: "btn secondary", onclick: () => doRegenerate("executive_summary") }, "Regenerate summary")
      )
    )
  );
}

function renderLanding(data) {
  const node = $("#tab-landing"); if (!node) return; node.innerHTML = "";
  const lp = data.landing_page || {};
  const sections = (lp.sections || []).map(s =>
    h("div", { class: "card", style: "margin:.5rem 0;padding:.75rem" },
      h("div", { style: "font-weight:600;margin-bottom:.25rem" }, s.title || "(untitled)"),
      s.content ? h("p", {}, s.content) :
        Array.isArray(s.bullets) ? h("ul", {}, ...s.bullets.map(b => h("li", {}, b))) :
          h("p", { class: "muted" }, "(empty)")
    )
  );
  const allText = () => {
    const parts = [
      `Headline: ${lp.headline || ""}`,
      `Subheadline: ${lp.subheadline || ""}`,
      "",
      ...(lp.sections || []).map(s => s.content ? `${s.title}\n${s.content}` :
        Array.isArray(s.bullets) ? `${s.title}\n- ${s.bullets.join("\n- ")}` : (s.title || "")),
      "",
      `CTA: ${lp.cta || ""}`
    ];
    return parts.join("\n");
  };
  node.append(
    h("div", { class: "workspace" },
      h("div", {}, h("strong", {}, lp.headline || "(headline)")),
      h("div", { class: "muted", style: "margin:.25rem 0 .5rem" }, lp.subheadline || ""),
      ...sections,
      h("div", { style: "margin-top:.5rem;font-weight:600" }, lp.cta || ""),
      h("div", { class: "btn-row", style: "margin-top:.5rem" },
        copyBtn(allText, "Copy landing page"),
        h("button", { class: "btn secondary", onclick: () => doRegenerate("landing_page") }, "Regenerate landing")
      )
    )
  );
}
function renderEmails(data) {
  const node = $("#tab-emails"); if (!node) return; node.innerHTML = "";
  const emails = data.emails || [];
  const wrap = h("div", { class: "workspace" },
    h("div", { class: "muted", style: "margin-bottom:.5rem" }, `Emails (${emails.length})`),
    h("div", { class: "btn-row", style: "margin-bottom:.5rem" },
      h("button", { class: "btn secondary", onclick: () => doRegenerate("emails") }, "Regenerate emails")
    )
  );
  emails.forEach((em, i) => {
    const text = `Subject: ${em.subject}\nPreview: ${em.preview}\n\n${em.body}`;
    wrap.append(
      h("div", { class: "card email-card", style: "margin:.5rem 0; padding:.75rem" },
        h("div", { class: "email-head" },
          h("div", { class: "email-subject" }, `Email ${i + 1}: ${em.subject || ""}`),
          copyBtn(() => text, "Copy email")
        ),
        h("div", { class: "email-preview" }, em.preview || ""),
        h("pre", { class: "pre" }, em.body || "")
      )
    );
  });
  if (!emails.length) wrap.append(h("div", { class: "muted" }, "(no emails)"));
  node.append(wrap);
}
function renderEvidence(data) {
  const node = $("#tab-evidence"); if (!node) return; node.innerHTML = "";
  const ev = data.evidence_log || [];
  const table = h("table", { class: "table" },
    h("thead", {}, h("tr", {},
      h("th", {}, "Publisher"), h("th", {}, "Title"), h("th", {}, "Date"),
      h("th", {}, "URL"), h("th", {}, "Excerpt"), h("th", {}, "")
    )),
    h("tbody", {})
  );
  const tbody = $("tbody", table);
  ev.forEach(item => {
    const line = `${item.publisher} – ${item.title} (${fmtDate(item.date)})\n${item.url}\n\n${item.excerpt}`;
    tbody.append(
      h("tr", {},
        h("td", {}, item.publisher || ""),
        h("td", {}, item.title || ""),
        h("td", {}, fmtDate(item.date || "")),
        h("td", {}, item.url ? h("a", { href: item.url, target: "_blank", rel: "noopener" }, item.url) : ""),
        h("td", {}, item.excerpt || ""),
        h("td", {}, copyBtn(() => line, "Copy"))
      )
    );
  });
  node.append(
    h("div", { class: "workspace" },
      h("div", { class: "muted", style: "margin-bottom:.5rem" }, `Evidence (${ev.length})`),
      table
    )
  );
}
function renderSales(data) {
  const node = $("#tab-sales"); if (!node) return; node.innerHTML = "";
  const se = data.sales_enablement || {};
  node.append(
    h("div", { class: "workspace" },
      h("h4", {}, "Call script"),
      h("pre", { class: "pre" }, se.call_script || ""),
      h("div", { style: "margin:.25rem 0 .75rem" }, copyBtn(() => se.call_script || "", "Copy call script")),
      h("h4", {}, "One-pager"),
      h("pre", { class: "pre" }, se.one_pager || ""),
      h("div", { class: "btn-row" },
        copyBtn(() => se.one_pager || "", "Copy one-pager"),
        h("button", { class: "btn secondary", onclick: () => doRegenerate("sales_enablement") }, "Regenerate sales")
      )
    )
  );
}
function renderAllTabs(campaign, runId) {
  renderOverview(campaign);
  renderLanding(campaign);
  renderEmails(campaign);
  renderEvidence(campaign);
  renderSales(campaign);
  const mount = $("#download-docx-mount");
  if (mount) {
    mount.innerHTML = "";
    mount.append(h("button", { class: "btn", onclick: () => api.downloadDocx(runId) }, "Download .docx"));
  }
}

/* ---------- Polling with capped backoff ---------- */
let pollTimer = null;
async function pollUntilDone(runId) {
  CURRENT_RUN_ID = runId;
  setRunId(runId);
  updateStage("ValidatingInput");

  let delay = 1200, maxDelay = 4500;
  clearTimeout(pollTimer);

  const tick = async () => {
    try {
      const s = await api.status(runId);
      if (s?.state) updateStage(s.state);

      if (s?.state === "Completed") {
        const data = await api.fetchCampaign(runId);
        renderAllTabs(data, runId);
        toast("Campaign ready");
        return; // stop polling
      }
      if (s?.state === "Failed") {
        toast("Run failed – check logs / blob error.json if present");
        return; // stop
      }
    } catch (e) {
      console.warn("status poll error", e);
    }
    delay = Math.min(maxDelay, Math.floor(delay * 1.4));
    pollTimer = setTimeout(tick, delay);
  };

  await tick();
}

/* ---------- Regenerate ---------- */
async function doRegenerate(sectionKey) {
  if (!CURRENT_RUN_ID) return toast("No active run");
  try {
    await api.regenerate(CURRENT_RUN_ID, sectionKey);
    toast(`Requested regeneration: ${sectionKey}`);
    // Keep polling; server should update campaign.json when ready
  } catch (e) {
    console.error(e);
    toast("Regenerate failed");
  }
}

/* ---------- Runs list ---------- */
async function loadRuns() {
  const select = $("#runSelect"); if (!select) return;
  try {
    select.innerHTML = "<option value=''>Loading…</option>";
    const data = await api.runs();
    const items = Array.isArray(data) ? data : (data?.items || []);
    select.innerHTML = "<option value='leadgen'>(Select page / optional)</option>";
    // Try to populate options with most recent run IDs for convenience
    const runs = items.map(x => ({
      id: x.runId || x.id || x.instanceId || "",
      state: x.state || x.runtimeStatus || "",
      when: x.createdAt || x.created || x.timeCreated || x.timestamp || ""
    })).filter(r => r.id);

    // Add a sub-optgroup style label
    const optHeader = h("option", { value: "", disabled: true }, "— Recent runs —");
    select.append(optHeader);
    runs.slice(0, 30).forEach(r => {
      const label = `${r.id.slice(0, 8)}…  ${r.state || ""}  ${r.when ? `@ ${r.when}` : ""}`;
      select.append(h("option", { value: r.id }, label));
    });

    $("#loadRecentBtn")?.addEventListener("click", () => {
      const latest = runs.find(r => r.state === "Completed") || runs[0];
      if (!latest) return toast("No runs found");
      resumeRun(latest.id);
    });
  } catch (e) {
    console.warn("runs error", e);
    select.innerHTML = "<option value='leadgen'>(leadgen)</option>";
  }
}

/* ---------- Start / Resume ---------- */
async function startNewRun(payload) {
  const body = { page: "leadgen", ...(payload || {}) };
  const { runId } = await api.start(body);
  await resumeRun(runId);
}
async function resumeRun(runId) {
  CURRENT_RUN_ID = runId;
  setRunId(runId);
  pollUntilDone(runId);
}

/* ---------- Restore on load ---------- */
function restoreOnLoad() {
  const u = new URL(window.location.href);
  const qRun = u.searchParams.get("runId");
  if (qRun) return resumeRun(qRun);

  try {
    const last = localStorage.getItem("campaign:lastRunId");
    if (last) return resumeRun(last);
  } catch { }
}

/* ---------- Expose globals expected by your HTML ---------- */
window.Campaign = { startNewRun, setRunId, updateStage, resumeRun };

/* ---------- Init ---------- */
document.addEventListener("DOMContentLoaded", () => {
  loadRuns();
  restoreOnLoad();

  // ensure header "New run" works if no inline onclick is present
  const newBtn = $("#newRunBtn");
  if (newBtn && !newBtn.getAttribute("onclick")) {
    newBtn.addEventListener("click", () => startNewRun({}));
  }
});
