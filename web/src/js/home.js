// ──────────────────────────────────────────────────────────────
// Auth & PBI wiring: dev (emulator/Codespaces) uses the Node shim
//   - principal:  /api/.auth/me
//   - pbi token:  /api/pbi-token
// Cloud/staging uses real SWA auth
//   - principal:  /.auth/me
// ──────────────────────────────────────────────────────────────

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

// Treat localhost + *.app.github.dev as emulator/dev
const host = location.hostname || "";
const isDevLike =
  host === "localhost" ||
  host === "127.0.0.1" ||
  host.endsWith(".app.github.dev");

// ---------------- Theme ----------------
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

// ---------------- Auth ----------------
async function getPrincipalDev() {
  // Emulator/Codespaces → Node shim
  try {
    const r = await fetch('/api/.auth/me', { cache: 'no-store' });
    if (!r.ok) return null;
    const j = await r.json();
    return j?.clientPrincipal || j?.principal || null;
  } catch { return null; }
}

async function getPrincipalCloud() {
  // Deployed SWA → real auth
  try {
    const r = await fetch('/.auth/me', { cache: 'no-store' });
    if (!r.ok) return null;
    const j = await r.json();
    return j?.clientPrincipal || null;
  } catch { return null; }
}

async function getPrincipal() {
  return isDevLike ? await getPrincipalDev() : await getPrincipalCloud();
}

// ---------------- UI toggle ----------------
function showSignedOut(){
  els.welcome?.classList.remove('hide');
  els.dashboard?.classList.add('hide');
  els.pbiSection?.classList.add('hide');

  if (isDevLike) {
    // In dev we sign-in via shim; hide auth buttons to avoid confusion
    els.signInBtn?.setAttribute('hidden','true');
    els.signInPrimary?.setAttribute('hidden','true');
    els.signOutBtn?.setAttribute('hidden','true');
  } else {
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
    els.signInBtn?.setAttribute('hidden','true');
    els.signInPrimary?.setAttribute('hidden','true');
    els.signOutBtn?.setAttribute('hidden','true');
  } else {
    els.signInBtn?.setAttribute('hidden','true');
    els.signInPrimary?.setAttribute('hidden','true');
    els.signOutBtn?.removeAttribute('hidden');
  }
}

// ---------------- Power BI ----------------
async function tryEmbedPBI() {
  const section = document.getElementById('pbiSection');
  const host = document.getElementById('reportContainer');
  const hint = document.getElementById('pbiHint');

  if (!section || !host) return;

  // Don’t show the card until we know we can embed
  section.classList.add('hide');

  try {
    // 1) Get embed details from our API
    const r = await fetch('/api/pbi-token', { cache: 'no-store' });
    if (!r.ok) throw new Error(`pbi-token ${r.status}`);
    const { embedUrl, accessToken, reportId } = await r.json();
    if (!embedUrl || !accessToken || !reportId)
      throw new Error('pbi-token: missing fields');

    // 2) Ensure Power BI SDK is available
    const pbi = window.powerbi;
    const models = window['powerbi-client']?.models || pbi?.models;
    if (!pbi || !models) throw new Error('powerbi client not loaded');

    // 3) Embed
    const config = {
      type: 'report',
      tokenType: models.TokenType.Embed,
      accessToken,
      embedUrl,
      id: reportId,
      settings: {
        panes: { filters: { visible: false }, pageNavigation: { visible: false } },
        navContentPaneEnabled: false
      }
    };

    // Ensure visible height
    if (!host.style.minHeight) host.style.minHeight = '560px';

    pbi.reset(host);
    pbi.embed(host, config);

    // 4) Reveal the card and hide the hint
    section.classList.remove('hide');
    hint?.classList.add('hide');
  } catch (e) {
    // Show the card with a helpful hint if we can’t embed
    section.classList.remove('hide');
    host.setAttribute('aria-busy', 'false');
    hint?.classList.remove('hide');
    console.warn('Power BI embed skipped:', e);
  }
}

// ---------------- boot ----------------
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
