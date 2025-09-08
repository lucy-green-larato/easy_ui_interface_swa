/* campaign.js — external client for /api/generate (kind:"campaign")
   Field names and DOM IDs match the original inline implementation exactly.
*/

(() => {
  // ---------- tiny DOM helpers ----------
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  // ---------- elements ----------
  const els = {
    statusDot: $('#statusDot'),
    statusText: $('#statusText'),
    csvUpload: $('#csvUpload'),
    goBtn: $('#goBtn'),
    csvBadge: $('#csvBadge'),
    runBadgeId: $('#runBadgeId'),
    currentRunId: $('#currentRunId'),
    // company fields
    companyName: $('#companyName'),
    companyWebsite: $('#companyWebsite'),
    companyLinkedIn: $('#companyLinkedIn'),
    companyUsps: $('#companyUsps'),
    // options
    tone: $('#tone'),
    evidenceWindow: $('#evidenceWindow'),
    includeCompliance: $('#includeCompliance'),
    // panels
    panelOverview: $('#tab-overview'),
    panelLanding:  $('#tab-landing'),
    panelEmails:   $('#tab-emails'),
    panelEvidence: $('#tab-evidence'),
    panelSales:    $('#tab-sales'),
    // other UI
    debug: $('#debugLog'),
    toast: $('#campaign-toast'),
    tabs: $$('.tab'),
    contentRoot: $('#campaign-content'),
  };

  // ---------- state ----------
  let selectedFile = null;
  let csvText = '';
  let csvSha = '';
  let rowCount = 0;

  // ---------- utilities ----------
  function log(line) {
    if (!els.debug) return;
    els.debug.textContent += (els.debug.textContent ? '\n' : '') + String(line);
    els.debug.scrollTop = els.debug.scrollHeight;
  }

  function setStatus(text, mode) {
    if (els.statusText) els.statusText.textContent = text || '';
    if (!els.statusDot) return;
    els.statusDot.className = 'status-dot';
    if (mode === 'ok') els.statusDot.classList.add('ok');
    else if (mode === 'warn') els.statusDot.classList.add('warn');
    else if (mode === 'err') els.statusDot.classList.add('err');
  }

  function setActiveTab(id) {
    els.tabs.forEach(t => t.classList.toggle('active', t.dataset.tabTarget === id));
    if (els.contentRoot) {
      $$('#campaign-content > div').forEach(p => {
        p.style.display = (p.id === id ? '' : 'none');
      });
    }
  }

  function updateStage(state) {
    const order = ['ValidatingInput', 'EvidenceBuilder', 'DraftCampaign', 'QualityGate', 'Completed'];
    order.forEach(s => {
      const el = $('#step-' + s);
      if (!el) return;
      el.classList.remove('active', 'done');
      if (state === s) el.classList.add('active');
      if (order.indexOf(s) < order.indexOf(state)) el.classList.add('done');
    });
  }

  function setRunId(id) {
    const val = id || '–';
    if (els.currentRunId) els.currentRunId.textContent = val;
    if (els.runBadgeId) els.runBadgeId.textContent = val;
  }

  async function sha256Hex(buf) {
    const hash = await crypto.subtle.digest('SHA-256', buf);
    return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('');
  }

  function countCsvRows(text) {
    const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
    return Math.max(0, lines.length - 1);
  }

  function showToast(msg) {
    if (!els.toast) return;
    els.toast.textContent = String(msg || '');
    els.toast.classList.add('show');
    setTimeout(() => els.toast.classList.remove('show'), 2500);
  }

  // ---------- tabs ----------
  els.tabs.forEach(btn => btn.addEventListener('click', () => setActiveTab(btn.dataset.tabTarget)));

  // ---------- CSV selection ----------
  els.csvUpload?.addEventListener('change', async (e) => {
    const f = e.target.files && e.target.files[0];
    if (!f) {
      selectedFile = null; csvText = ''; csvSha = ''; rowCount = 0;
      if (els.csvBadge) els.csvBadge.textContent = '(no file)';
      if (els.goBtn) els.goBtn.disabled = true;
      return;
    }
    selectedFile = f;
    const [text, buf] = await Promise.all([f.text(), f.arrayBuffer()]);
    csvText = text;
    csvSha = await sha256Hex(buf);
    rowCount = countCsvRows(text);
    if (els.csvBadge) els.csvBadge.textContent = `${f.name} · ${rowCount} rows`;
    if (els.goBtn) els.goBtn.disabled = false;
    log(`CSV loaded: ${f.name} (${f.size} bytes), rows=${rowCount}, sha256=${csvSha.slice(0,12)}…`);
  });

  // ---------- renderers (IDs/fields match original inline page) ----------
  function renderOverview(c) {
    if (!els.panelOverview) return;
    els.panelOverview.style.display = '';
    els.panelOverview.innerHTML = '';
    const ex = document.createElement('div');
    ex.className = 'pre';
    ex.textContent = c?.executive_summary || '(no executive summary)';
    els.panelOverview.appendChild(ex);
  }

  function renderLanding(c) {
    if (!els.panelLanding) return;
    const lp = c?.landing_page || {};
    els.panelLanding.style.display = '';
    els.panelLanding.innerHTML = `
      <h3>${lp.headline || ''}</h3>
      <p>${lp.subheadline || ''}</p>
      <ul>${(lp.sections || []).map(s => `<li><strong>${s.title || ''}</strong>: ${s.content || ''}</li>`).join('')}</ul>
      <p><strong>CTA:</strong> ${lp.cta || ''}</p>
    `;
  }

  function renderEmails(c) {
    if (!els.panelEmails) return;
    const emails = Array.isArray(c?.emails) ? c.emails : [];
    els.panelEmails.style.display = '';
    els.panelEmails.innerHTML = emails.map(e => `
      <div class="card">
        <div class="body">
          <div><strong>Subject:</strong> ${e.subject || ''}</div>
          <div><strong>Preview:</strong> ${e.preview || ''}</div>
          ${e.html ? `<div>${e.html}</div>` : `<pre class="pre">${e.body || e.text || ''}</pre>`}
        </div>
      </div>
    `).join('') || '<div class="muted">No emails.</div>';
  }

  function renderEvidence(c) {
    if (!els.panelEvidence) return;
    const ev = Array.isArray(c?.evidence_log) ? c.evidence_log : [];
    els.panelEvidence.style.display = '';
    els.panelEvidence.innerHTML = `
      <table class="table">
        <thead><tr><th>Publisher</th><th>Title</th><th>Date</th><th>URL</th><th>Excerpt</th></tr></thead>
        <tbody>
          ${ev.map(x => `
            <tr>
              <td>${x.publisher || ''}</td>
              <td>${x.title || ''}</td>
              <td>${x.date || ''}</td>
              <td>${x.url ? `<a href="${x.url}" target="_blank" rel="noopener">${x.url}</a>` : ''}</td>
              <td>${x.excerpt || ''}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;
  }

  function renderSales(c) {
    if (!els.panelSales) return;
    const s = c?.sales_enablement || {};
    els.panelSales.style.display = '';
    els.panelSales.innerHTML = `
      <h4>Call script</h4><pre class="pre">${s.call_script || ''}</pre>
      <h4>One pager</h4><pre class="pre">${s.one_pager || ''}</pre>
    `;
  }

  function renderAll(c) {
    renderOverview(c);
    renderLanding(c);
    renderEmails(c);
    renderEvidence(c);
    renderSales(c);
  }

  // ---------- API ----------
  async function generate(payload) {
    const res = await fetch('/api/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const text = await res.text();
    let data = {};
    try { data = JSON.parse(text); } catch { /* leave as {} */ }
    if (!res.ok) {
      throw new Error(`generate ${res.status}: ${text.slice(0, 400)}`);
    }
    return data;
  }

  // ---------- main action ----------
  els.goBtn?.addEventListener('click', async () => {
    try {
      if (!selectedFile) { alert('Please choose a CSV first.'); return; }
      setStatus('Generating…', 'warn');
      updateStage('ValidatingInput');
      setRunId('–');

      const csv_text = await selectedFile.text();
      // Build payload with EXACT field names (unchanged)
      const payload = {
        kind: 'campaign',
        csv_text,               // REQUIRED
        source: 'upload',
        page: 'campaign',
        rowCount,
        csv_sha256: csvSha,
        company: {
          name: els.companyName?.value || undefined,
          website: els.companyWebsite?.value || undefined,
          linkedin: els.companyLinkedIn?.value || undefined,
          usps: els.companyUsps?.value || undefined,
        },
        tone: els.tone?.value || undefined,
        evidenceWindowMonths: parseInt(els.evidenceWindow?.value || '6', 10),
        complianceFooter: !!els.includeCompliance?.checked,
      };

      log('---- REQUEST (truncated) ----');
      log(JSON.stringify({ ...payload, csv_text: `[${csv_text.length} chars]` }, null, 2));

      const t0 = Date.now();
      const result = await generate(payload);
      const ms = Date.now() - t0;
      log(`---- RESPONSE in ${ms}ms ----`);
      if (result._debug_prompt) log('---- PROMPT SENT TO LLM ----\n' + result._debug_prompt);

      renderAll(result);
      updateStage('Completed');
      setStatus('Done', 'ok');
      setActiveTab('tab-overview');
      showToast('Campaign ready');
    } catch (e) {
      log(`generate error: ${e && e.message ? e.message : e}`);
      setStatus('Error', 'err');
      updateStage('ValidatingInput');
      alert(`Error: ${e.message || e}`);
    }
  });

  // expose minimal API for other modules (keeps header “New run” working)
  window.CampaignPage = { setActiveTab, updateStage, setRunId };
  window.Campaign = {
    startNewRun: async () => { $('#csvUpload')?.click(); },
    updateStage,
  };

  // default tab
  setActiveTab('tab-overview');
})();
