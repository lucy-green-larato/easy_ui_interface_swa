/* campaign.js — single client for /api/generate (kind:"campaign") */

(() => {
  // ---------- tiny DOM helpers ----------
  const qs = (s, el = document) => el.querySelector(s);
  const qsa = (s, el = document) => Array.from(el.querySelectorAll(s));
  const on = (el, ev, fn) => el && el.addEventListener(ev, fn);

  // ---------- elements ----------
  const el = {
    dot: qs('#statusDot'),
    stage: qs('#stage'),
    csvUpload: qs('#csvUpload'),
    csvName: qs('#csvName'),
    csvHash: qs('#csvHash'),
    client: qs('#client'),
    product: qs('#product'),
    tone: qs('#tone'),
    audience: qs('#audience'),
    windowMonths: qs('#windowMonths'),
    numEmails: qs('#numEmails'),
    notes: qs('#notes'),
    go: qs('#goBtn'),
    debug: qs('#debugLog'),
    tabs: qsa('.tabbtn'),
    copyVisible: qs('#copyVisible'),
    downloadAll: qs('#downloadAll'),
    // outputs
    tabLanding: qs('#tab-landing'),
    tabEmails: qs('#tab-emails'),
    tabMatrix: qs('#tab-matrix'),
    tabEvidence: qs('#tab-evidence'),
    tabSales: qs('#tab-sales'),
    outLanding: qs('#out-landing'),
    outEmails: qs('#out-emails'),
    outMatrix: qs('#out-matrix'),
    outEvidence: qs('#out-evidence'),
    outSales: qs('#out-sales'),
  };

  let csvText = '';
  let lastResult = null;

  // ---------- utils ----------
  const log = (...args) => {
    const line = args.map(a => typeof a === 'string' ? a : JSON.stringify(a, null, 2)).join(' ');
    el.debug.textContent += (el.debug.textContent ? '\n' : '') + line;
    el.debug.scrollTop = el.debug.scrollHeight;
  };

  const setDot = cls => {
    el.dot.classList.remove('ok', 'warn', 'err');
    if (cls) el.dot.classList.add(cls);
  };

  const setStage = s => { el.stage.textContent = s || ''; };

  const hashText = async (text) => {
    if (!window.crypto?.subtle) return '';
    const enc = new TextEncoder().encode(text);
    const buf = await crypto.subtle.digest('SHA-256', enc);
    return Array.from(new Uint8Array(buf)).map(b=>b.toString(16).padStart(2,'0')).join('');
  };

  const copyToClipboard = async (str) => {
    try {
      await navigator.clipboard.writeText(str);
      return true;
    } catch {
      // fallback
      const ta = document.createElement('textarea');
      ta.value = str; document.body.appendChild(ta); ta.select();
      try { document.execCommand('copy'); return true; } finally { ta.remove(); }
    }
  };

  // ---------- CSV handling ----------
  on(el.csvUpload, 'change', async () => {
    const f = el.csvUpload.files?.[0];
    if (!f) return;
    const text = await f.text();
    csvText = text;
    el.csvName.textContent = `${f.name} • ${f.size.toLocaleString()} bytes`;
    const h = await hashText(text);
    el.csvHash.textContent = h ? `sha256:${h.slice(0,12)}…` : '';
    log(`Loaded CSV: ${f.name} (${f.size} bytes)`);
  });

  // ---------- tabs ----------
  const showTab = (name) => {
    const ids = { landing: el.tabLanding, emails: el.tabEmails, matrix: el.tabMatrix, evidence: el.tabEvidence, sales: el.tabSales };
    Object.entries(ids).forEach(([k, section]) => {
      section.style.display = (k === name) ? '' : 'none';
    });
    qsa('.tabbtn').forEach(b => b.classList.toggle('active', b.dataset.tab === name));
  };
  el.tabs.forEach(b => on(b, 'click', () => showTab(b.dataset.tab || 'landing')));
  on(el.copyVisible, 'click', async () => {
    const active = qsa('.tabbtn').find(b => b.classList.contains('active'))?.dataset.tab || 'landing';
    const map = { landing: el.outLanding, emails: el.outEmails, matrix: el.outMatrix, evidence: el.outEvidence, sales: el.outSales };
    const html = map[active]?.innerText || '';
    const ok = await copyToClipboard(html);
    log(ok ? `Copied ${active} to clipboard.` : `Copy failed for ${active}.`);
  });
  on(el.downloadAll, 'click', () => {
    if (!lastResult) return;
    const blob = new Blob([JSON.stringify(lastResult, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = Object.assign(document.createElement('a'), { href: url, download: 'campaign_result.json' });
    document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
  });

  // ---------- renderers ----------
  const renderLanding = (data) => {
    el.outLanding.innerHTML = '';
    const html = data?.landing_page?.html;
    const md = data?.landing_page?.markdown || data?.landing_page?.md;
    if (html) {
      const wrap = document.createElement('div'); wrap.innerHTML = html; el.outLanding.appendChild(wrap);
    } else if (md) {
      const pre = document.createElement('pre'); pre.textContent = md; el.outLanding.appendChild(pre);
    } else {
      el.outLanding.innerHTML = '<small class="muted">No landing page content returned.</small>';
    }
  };

  const renderEmails = (data) => {
    el.outEmails.innerHTML = '';
    const emails = Array.isArray(data?.emails) ? data.emails : [];
    if (!emails.length) { el.outEmails.innerHTML = '<small class="muted">No emails returned.</small>'; return; }
    emails.forEach((e, i) => {
      const card = document.createElement('div'); card.className = 'card';
      const h = document.createElement('h3'); h.textContent = `Email ${i+1}: ${e.subject || '(no subject)'}`;
      const b = document.createElement('div'); b.className = 'body';
      const preview = e.preview || e.teaser || '';
      const html = e.html || e.body_html;
      const text = e.text || e.body || '';
      const pre = document.createElement('pre'); pre.textContent = text || '';
      if (html) {
        const htmlBox = document.createElement('div'); htmlBox.innerHTML = html; b.appendChild(htmlBox);
      }
      if (!html && text) b.appendChild(pre);
      if (preview) {
        const small = document.createElement('small'); small.className='muted'; small.textContent = `Preview: ${preview}`;
        b.appendChild(document.createElement('br')); b.appendChild(small);
      }
      card.appendChild(h); card.appendChild(b); el.outEmails.appendChild(card);
    });
  };

  const renderMatrix = (data) => {
    el.outMatrix.innerHTML = '';
    const matrix = data?.messaging_matrix || data?.matrix || [];
    if (!Array.isArray(matrix) || !matrix.length) { el.outMatrix.innerHTML = '<small class="muted">No matrix returned.</small>'; return; }
    const tbl = document.createElement('table');
    const head = document.createElement('thead');
    const cols = Object.keys(matrix[0]);
    head.innerHTML = `<tr>${cols.map(c=>`<th>${c}</th>`).join('')}</tr>`;
    const body = document.createElement('tbody');
    matrix.forEach(row => {
      const tr = document.createElement('tr');
      cols.forEach(c => {
        const td = document.createElement('td');
        const v = row[c];
        td.innerHTML = (v ?? '') && typeof v === 'string' ? v : JSON.stringify(v ?? '');
        tr.appendChild(td);
      });
      body.appendChild(tr);
    });
    tbl.appendChild(head); tbl.appendChild(body);
    el.outMatrix.appendChild(tbl);
  };

  const renderEvidence = (data) => {
    el.outEvidence.innerHTML = '';
    const ev = data?.evidence || data?.proof_points || [];
    if (!Array.isArray(ev) || !ev.length) { el.outEvidence.innerHTML = '<small class="muted">No evidence returned.</small>'; return; }
    const ul = document.createElement('ul');
    ev.forEach(x => {
      const li = document.createElement('li'); li.textContent = typeof x === 'string' ? x : JSON.stringify(x);
      ul.appendChild(li);
    });
    el.outEvidence.appendChild(ul);
  };

  const renderSales = (data) => {
    el.outSales.innerHTML = '';
    const s = data?.sales_enablement || {};
    if (!Object.keys(s).length) { el.outSales.innerHTML = '<small class="muted">No sales enablement content returned.</small>'; return; }
    const blocks = [
      ['Talk track', s.talk_track || s.talktrack || ''],
      ['Objection handles', s.objection_handles || s.objections || []],
      ['Discovery questions', s.discovery_questions || s.questions || []],
      ['CTAs', s.ctas || s.calls_to_action || []],
    ];
    blocks.forEach(([title, payload]) => {
      const card = document.createElement('div'); card.className='card';
      const h = document.createElement('h3'); h.textContent = title;
      const b = document.createElement('div'); b.className='body';
      if (Array.isArray(payload)) {
        const ul = document.createElement('ul');
        payload.forEach(p => { const li = document.createElement('li'); li.textContent = p; ul.appendChild(li); });
        b.appendChild(ul);
      } else if (typeof payload === 'string') {
        const pre = document.createElement('pre'); pre.textContent = payload; b.appendChild(pre);
      } else {
        const pre = document.createElement('pre'); pre.textContent = JSON.stringify(payload, null, 2); b.appendChild(pre);
      }
      card.appendChild(h); card.appendChild(b); el.outSales.appendChild(card);
    });
  };

  const renderAll = (data) => {
    renderLanding(data);
    renderEmails(data);
    renderMatrix(data);
    renderEvidence(data);
    renderSales(data);
  };

  // ---------- API ----------
  const generate = async (payload) => {
    const res = await fetch('/api/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const txt = await res.text().catch(()=>String(res.status));
      throw new Error(`Generate failed: ${res.status} ${txt}`);
    }
    return res.json();
  };

  // ---------- main action ----------
  on(el.go, 'click', async () => {
    try {
      if (!csvText || !csvText.trim()) { alert('Please upload a CSV first.'); return; }
      setDot('warn'); setStage('Preparing request…'); log('Preparing payload…');

      const payload = {
        kind: 'campaign',
        csv_text: csvText,
        client: (el.client.value || '').trim(),
        product: (el.product.value || '').trim(),
        tone: (el.tone.value || 'professional'),
        audience: (el.audience.value || '').trim(),
        evidenceWindowMonths: Number(el.windowMonths.value) || 6,
        numEmails: Number(el.numEmails.value) || 5,
        notes: (el.notes.value || '').trim(),
      };

      setStage('Generating…'); log('POST /api/generate', { ...payload, csv_text: `[${csvText.length} chars]` });
      const t0 = Date.now();
      const result = await generate(payload);
      const ms = Date.now() - t0;
      lastResult = result;

      log(`Received result in ${ms}ms`);
      setStage('Rendering…'); renderAll(result);
      setStage('Completed'); setDot('ok'); showTab('landing');
    } catch (err) {
      console.error(err);
      log(String(err));
      setStage('Error'); setDot('err');
      alert(`Error: ${err.message || err}`);
    }
  });

  // default tab
  showTab('landing');
})();
