// ─────────────────────────────────────────────────────────────────────────────
// Local/dev always uses the Functions auth shim; cloud uses SWA /.auth/me.
// This removes any dependency on the emulator's /.auth endpoints.
// ─────────────────────────────────────────────────────────────────────────────

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

// Treat localhost, 127.0.0.1 and Codespaces/SWA emulator *.app.github.dev as "dev-like"
const host = location.hostname || "";
const isDevLike =
  host === "localhost" ||
  host === "127.0.0.1" ||
  host.endsWith(".app.github.dev"); // SWA emulator in Codespaces

// ───────── Theme ─────────
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

// ───────── Auth helpers ─────────
async function getPrincipalFromShim() {
  try {
    let r = await fetch('/api/auth', { cache: 'no-store' });
    if (r.status === 404) r = await fetch('/api/auth?dev=1', { cache: 'no-store' });
    if (!r.ok) return null;
    const j = await r.json();
    return j?.clientPrincipal || j?.principal || null;
  } catch { return null; }
}

async function getPrincipalFromSWA() {
  try {
    const r = await fetch('/.auth/me', { cache: 'no-store' });
    if (!r.ok) return null;
    const j = await r.json();
    return j?.clientPrincipal || null;
  } catch { return null; }
}

// Shim-first in dev; SWA-first in cloud
async function getPrincipal() {
  if (isDevLike) {
    // Local/dev: rely solely on the shim (requires AUTH_DEV_ALWAYS=1 in api)
    return await getPrincipalFromShim();
  }
  // Cloud/staging: real SWA auth
  const p = await getPrincipalFromSWA();
  return p || null;
}

// ───────── UI toggles ─────────
function showSignedOut(){
  els.welcome?.classList.remove('hide');
  els.dashboard?.classList.add('hide');
  els.pbiSection?.classList.add('hide');

  if (isDevLike) {
    // In dev we don't use SWA login—hide both auth buttons to avoid confusion
    els.signInBtn?.setAttribute('hidden','true');
    els.signInPrimary?.setAttribute('hidden','true');
    els.signOutBtn?.setAttribute('hidden','true');
  } else {
    // In cloud, show Sign in; hide Sign out
    els.signOutBtn?.setAttribute('hidden','true');
    els.signInBtn?.removeAttribute('hidden');
    els.signInPrimary?.removeAttribute('hidden');
  }
}

function showSignedIn(p){
  const label = p?.userDetails || p?.userId || '';
  if (els.userName) els.userName.textContent = label;

  els.welcome?.classList.add('hide');
  els.dashboard?.classList.remove('hide');

  if (isDevLike) {
    // Dev: neither sign-in nor sign-out applies (shim controls identity)
    els.signInBtn?.setAttribute('hidden','true');
    els.signInPrimary?.setAttribute('hidden','true');
    els.signOutBtn?.setAttribute('hidden','true');
  } else {
    // Cloud: show Sign out only
    els.signInBtn?.setAttribute('hidden','true');
    els.signInPrimary?.setAttribute('hidden','true');
    els.signOutBtn?.removeAttribute('hidden');
  }
}

// ───────── Power BI embed ─────────
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
      tokenType: models?.TokenType?.Embed ?? 1,
      accessToken,
      embedUrl,
      id: reportId,
      settings: {
        panes: { filters: { visible: false }, pageNavigation: { visible: false } },
        navContentPaneEnabled: false,
        layoutType: models?.LayoutType?.Custom ?? 1
      }
    };
    const powerbi = window.powerbi;
    powerbi.reset(els.reportContainer);
    powerbi.embed(els.reportContainer, config);

    els.pbiSection.classList.remove('hide');
    els.pbiHint?.classList.add('hide');
  } catch (e) {
    els.pbiSection.classList.remove('hide');
    els.pbiHint?.classList.remove('hide');
    console.warn('Power BI embed skipped:', e);
  }
}

// ───────── boot ─────────
async function boot(){
  const principal = await getPrincipal();
  if (!principal) { showSignedOut(); return; }

  showSignedIn(principal);
  await tryEmbedPBI();

  els.btnExpandPBI?.addEventListener('click', () => {
    const expanded = els.btnExpandPBI.getAttribute('aria-pressed') === 'true';
    els.btnExpandPBI.setAttribute('aria-pressed', String(!expanded));
    els.reportContainer.style.minHeight = expanded ? '' : '820px';
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', boot, { once:true });
} else {
  boot();
}
