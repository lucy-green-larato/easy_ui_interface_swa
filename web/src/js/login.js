// If already signed in, skip to main menu
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
    const btn = document.getElementById('themeToggle');
    btn?.addEventListener('click', () => {
      const cur = document.documentElement.getAttribute('data-theme') || '';
      const next = cur === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem('theme', next);
      btn.setAttribute('aria-pressed', String(next === 'dark'));
    });
  }catch{}
})();

(async () => {
  const p = await getPrincipal();
  if (p) window.location.replace('/');
})();
