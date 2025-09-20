// Home page boot: CSP-safe auth/dash toggle + optional Power BI embed

const els = {
  welcome: document.getElementById('welcome'),
  dashboard: document.getElementById('dashboard'),
  userName: document.getElementById('userName'),
  signInBtn: document.getElementById('btnSignIn'),
  signInPrimary: document.getElementById('btnSignInPrimary'),
  signOutBtn: document.getElementById('btnSignOut'),
  themeToggle: document.getElementById('themeToggle'),

  pbiSection: document.getElementById('pbiSection'),
  reportContainer: document.getElementById('reportContainer'),
  pbiHint: document.getElementById('pbiHint'),
  btnExpandPBI: document.getElementById('btnExpandPBI'),
};

// ---------- Theme ----------
(function initTheme() {
  try {
    const saved = localStorage.getItem('theme');
    if (saved) document.documentElement.setAttribute('data-theme', saved);
    els.themeToggle?.addEventListener('click', () => {
      const cur = document.documentElement.getAttribute('data-theme') || '';
      const next = cur === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem('theme', next);
      els.themeToggle.setAttribute('aria-pressed', String(next === 'dark'));
    });
  } catch {}
})();

// ---------- Auth + UI state ----------
async function getPrincipal() {
  try {
    const res = await fetch('/.auth/me', { cache: 'no-store' });
    if (!res.ok) return null;
    const data = await res.json();
    return data?.clientPrincipal || null;
  } catch {
    return null;
  }
}

function showSignedOut() {
  els.welcome?.classList.remove('hide');
  els.dashboard?.classList.add('hide');
  els.signOutBtn?.setAttribute('hidden', 'true');
  els.signInBtn?.removeAttribute('hidden');
  els.signInPrimary?.removeAttribute('hidden');
  if (els.pbiSection) els.pbiSection.classList.add('hide');
}

function showSignedIn(user) {
  const label = user?.userDetails || user?.userId || '';
  if (els.userName) els.userName.textContent = label ? ` ${label}` : '';
  els.welcome?.classList.add('hide');
  els.dashboard?.classList.remove('hide');
  els.signInBtn?.setAttribute('hidden', 'true');
  els.signInPrimary?.setAttribute('hidden', 'true');
  els.signOutBtn?.removeAttribute('hidden');
}

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
    // Show the card with a hint if embedding fails (keeps UX clear)
    els.pbiSection.classList.remove('hide');
    els.pbiHint?.classList.remove('hide');
    console.warn('Power BI embed skipped:', e);
  }
}

async function boot() {
  const principal = await getPrincipal();

  if (!principal) {
    showSignedOut();
    return;
  }

  showSignedIn(principal);
  await tryEmbedPBI();

  // Expand/collapse PBI height (optional)
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
