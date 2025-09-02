// Lightweight client for Campaign Builder · Phase 1
(function () {
  'use strict';

  // ---------- DOM refs ----------
  const runSelect = document.getElementById('runSelect');
  const csvUpload = document.getElementById('csvUpload');
  const generateBtn = document.getElementById('generateBtn');
  const runSummary = document.getElementById('runSummary');
  const helperNeeds = document.getElementById('helperNeeds');
  const integrity = document.getElementById('integrity');
  const qualityState = document.getElementById('qualityState');
  const stageEl = document.getElementById('stage');
  const toast = document.getElementById('toast');

  const companyName = document.getElementById('companyName');
  const companyWebsite = document.getElementById('companyWebsite');
  const companyLinkedIn = document.getElementById('companyLinkedIn');
  const companyUsps = document.getElementById('companyUsps');
  const tone = document.getElementById('tone');
  const evidenceWindow = document.getElementById('evidenceWindow');
  const includeSubstantiation = document.getElementById('includeSubstantiation');

  let currentRunId = null;
  let polling = null;

  // ---------- UI helpers ----------
  function showToast(msg) {
    toast.textContent = msg;
    toast.style.display = 'block';
    setTimeout(() => (toast.style.display = 'none'), 3000);
  }

  function setGenerateEnabled() {
    const hasRun = !!runSelect.value || (csvUpload && csvUpload.files && csvUpload.files.length > 0);
    const hasCompany = companyName.value && companyWebsite.value && companyLinkedIn.value;
    generateBtn.disabled = !(hasRun && hasCompany);
  }

  // Stage tracker
  const steps = ['validate', 'evidence', 'draft', 'quality'];
  function setStage(state) {
    steps.forEach((s) => {
      const el = stageEl.querySelector(`[data-step="${s}"]`);
      if (el) el.classList.remove('active', 'done');
    });
    const idx = steps.indexOf(state);
    if (idx < 0) return;
    steps.slice(0, idx).forEach((s) => {
      const el = stageEl.querySelector(`[data-step="${s}"]`);
      if (el) el.classList.add('done');
    });
    const active = stageEl.querySelector(`[data-step="${state}"]`);
    if (active) active.classList.add('active');
  }

  // Tabs
  const tabs = document.getElementById('tabs');
  tabs.addEventListener('click', (e) => {
    const t = e.target.closest('.tab');
    if (!t) return;
    document.querySelectorAll('.tab').forEach((el) => el.classList.remove('active'));
    t.classList.add('active');
    const p = t.dataset.panel;
    document.querySelectorAll('.panel').forEach((el) => (el.hidden = true));
    document.getElementById(`panel-${p}`).hidden = false;
  });

  // Form enablement
  ['input', 'change'].forEach((evt) => {
    companyName.addEventListener(evt, setGenerateEnabled);
    companyWebsite.addEventListener(evt, setGenerateEnabled);
    companyLinkedIn.addEventListener(evt, setGenerateEnabled);
    csvUpload.addEventListener(evt, setGenerateEnabled);
  });
  runSelect.addEventListener('change', () => {
    const val = runSelect.value;
    setGenerateEnabled();
    if (!val) {
      runSummary.textContent = 'No run selected';
      return;
    }
    const meta = JSON.parse(runSelect.selectedOptions[0].dataset.meta || '{}');
    runSummary.textContent = `${meta.pbi_page || '—'} · ${meta.row_count || '?'} rows · ${meta.timestamp || ''}`;
  });

  // ---------- API helper ----------
  async function api(path, options) {
    const res = await fetch(path, options);
    if (!res.ok) {
      let body = '';
      try { body = await res.text(); } catch {}
      throw new Error(`${res.status} ${res.statusText} ${body}`);
    }
    const ct = res.headers.get('content-type') || '';
    return ct.includes('application/json') ? res.json() : res.text();
  }

  // ---------- Data loaders ----------
  async function loadRuns() {
    try {
      const data = await api('/api/runs');
      runSelect.innerHTML = '<option value="">Select a run…</option>';
      (data.items || []).forEach((item) => {
        const o = document.createElement('option');
        o.value = item.runId;
        o.textContent = `${item.page} · ${item.rowCount} rows · ${new Date(item.timestamp).toLocaleString()}`;
        o.dataset.meta = JSON.stringify({
          pbi_page: item.page,
          row_count: item.rowCount,
          timestamp: item.timestamp,
        });
        runSelect.appendChild(o);
      });
      document.getElementById('recentRuns').innerHTML = (data.items || [])
        .slice(0, 5)
        .map((i) => `<li>${i.page} · ${i.rowCount} · ${new Date(i.timestamp).toLocaleDateString()}</li>`)
        .join('');
    } catch (e) {
      showToast('Could not load runs');
    }
  }

  // ---------- Start + Poll ----------
  async function startGeneration() {
    try {
      generateBtn.disabled = true;
      setStage('validate');
      qualityState.textContent = 'Starting…';

      const fd = new FormData();
      if (runSelect.value) fd.append('runId', runSelect.value);
      if (csvUpload.files && csvUpload.files[0]) fd.append('csv', csvUpload.files[0]);
      fd.append('companyName', companyName.value);
      fd.append('companyWebsite', companyWebsite.value);
      fd.append('companyLinkedIn', companyLinkedIn.value);
      fd.append('usps', companyUsps.value);
      fd.append('tone', tone.value);
      fd.append('evidenceWindow', evidenceWindow.value);
      fd.append('includeSubstantiation', includeSubstantiation.checked ? 'true' : 'false');

      const data = await api('/api/campaign/start', { method: 'POST', body: fd });
      currentRunId = data.runId;
      integrity.textContent = `runId: ${currentRunId}`;
      pollStatus();
    } catch (e) {
      showToast('Start failed');
      generateBtn.disabled = false;
      console.error(e);
    }
  }

  async function pollStatus() {
    if (polling) clearInterval(polling);
    polling = setInterval(async () => {
      try {
        const s = await api(`/api/campaign/status?runId=${encodeURIComponent(currentRunId)}`);
        // expected states: ValidatingInput | EvidenceBuilder | DraftCampaign | QualityGate | Completed | Failed…
        const map = {
          ValidatingInput: 'validate',
          EvidenceBuilder: 'evidence',
          DraftCampaign: 'draft',
          QualityGate: 'quality',
        };
        setStage(map[s.state] || 'validate');
        qualityState.textContent = s.state || '—';

        if (s.input) {
          integrity.textContent = `runId: ${s.runId} · rows: ${s.input.rowCount || '?'} · page: ${s.input.page || '—'}`;
        }

        if (s.state === 'Completed') {
          clearInterval(polling);
          showToast('Campaign ready');
          document.getElementById('panel-overview').innerHTML =
            `<strong>Completed.</strong> Use the tabs to view content.`;
          // Phase 2: fetch and render campaign.json here
          generateBtn.disabled = false;
        }

        if (s.state && String(s.state).startsWith('Failed')) {
          clearInterval(polling);
          showToast('Generation failed');
          generateBtn.disabled = false;
        }
      } catch (e) {
        // ignore transient errors while the orchestration starts
      }
    }, 2000);
  }

  // ---------- Events ----------
  generateBtn.addEventListener('click', startGeneration);
  document.getElementById('newRunBtn').addEventListener('click', () => {
    runSelect.value = '';
    csvUpload.value = '';
    setGenerateEnabled();
    runSummary.textContent = 'No run selected';
  });
  document.getElementById('loadRecentBtn').addEventListener('click', loadRuns);

  // ---------- Init ----------
  setStage('validate');
  setGenerateEnabled();
  loadRuns();
})();
