const els = {
  userName: document.getElementById('userName'),
  pbiSection: document.getElementById('pbiSection'),
  reportContainer: document.getElementById('reportContainer'),
  pbiHint: document.getElementById('pbiHint'),
  btnExpandPBI: document.getElementById('btnExpandPBI'),
  themeToggle: document.getElementById('themeToggle'),
};

async function getPrincipal() {
  try {
    const r = await fetch('/.auth/me', { cache: 'no-store' });
    if (!r.ok) return null;
    const j = await r.json();
    return j?.clientPrincipal || null;
  } catch { return null; }
}

(function initTheme(){
  try{
    const saved = localStorage.getItem('theme');
    if (saved) document.documentElement.setAttribute('data-theme', saved);
    els.themeToggle?.addEventListener('click', () => {
      const cur = document.documentElement.getAttribute('data-theme') || '';
      const next = cur === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem('theme', next);
      els.themeToggle.setAttribute('aria-pressed', String(next === 'dark'));
    });
  }catch{}
})();

async function tryEmbedPBI() {
  if (!els.pbiSection || !els.reportContainer) return;
  try {
    const r = await fetch('/api/pbi_token', { cache: 'no-store' });
    if (!r.ok) throw new Error(`pbi_token ${r.status}`);
    const { embedUrl, accessToken, reportId } = await r.json();
    if (!embedUrl || !accessToken || !reportId) throw new Error('missing Power BI token details');

    const models = window['powerbi-client']?.models || window.powerbi?.models;
    const config = {
      type: 'report',
      tokenType: models?.TokenType?.Embed || 1,
      accessToken,
      embedUrl,
      id: reportId,
      settings: {
        panes: { filters: { visible: false }, pageNavigation: { visible: false } },
        navContentPaneEnabled: false,
        layoutType: models?.LayoutType?.Custom || 1
      }
    };
    const powerbi = window.powerbi;
    powerbi.reset(els.reportContainer);
    powerbi.embed(els.reportContainer, config);
    els.pbiSection.classList.remove('hide');
    els.pbiHint?.classList.add('hide');
  } catch (e) {
    // Show card but hint if token missing; avoids “empty page” confusion
    els.pbiSection.classList.remove('hide');
    els.pbiHint?.classList.remove('hide');
    console.warn('PBI embed skipped:', e);
  }
}

async function boot() {
  const principal = await getPrincipal();
  if (!principal) {
    // Not signed in → send to dedicated login page
    window.location.replace('/login.html');
    return;
  }

  // Signed in UI
  const label = principal.userDetails || principal.userId || '';
  if (els.userName) els.userName.textContent = label;

  await tryEmbedPBI();

  els.btnExpandPBI?.addEventListener('click', () => {
    const expanded = els.btnExpandPBI.getAttribute('aria-pressed') === 'true';
    els.btnExpandPBI.setAttribute('aria-pressed', String(!expanded));
    els.reportContainer.style.minHeight = expanded ? '' : '820px';
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', boot, { once: true });
} else {
  boot();
}
