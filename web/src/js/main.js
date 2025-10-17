// web/src/js/main.js 14-10-2025 v3 ----

// --- global resize nudge throttle (prevents event storms) ---
let __resizeTimer;
function nudgeResize() {
  clearTimeout(__resizeTimer);
  __resizeTimer = setTimeout(() => window.dispatchEvent(new Event('resize')), 120);
}

function buildReturnTarget() {
  return encodeURIComponent("/");
}

function friendlyName(raw) {
  if (!raw) return "User";
  if (raw.includes("@")) {
    const base = raw
      .split("@")[0]
      .split(/[._-]/)
      .filter(Boolean)
      .map((s) => s[0].toUpperCase() + s.slice(1))
      .slice(0, 2)
      .join(" ");
    return base || raw;
  }
  return raw;
}

async function setQualApiKey() {
  // Don’t refetch if we already have it this tab
  if (window.__QUAL_API_KEY || sessionStorage.getItem('QUAL_API_KEY')) {
    window.__QUAL_API_KEY = window.__QUAL_API_KEY || sessionStorage.getItem('QUAL_API_KEY');
    return;
  }

  // This endpoint must be implemented server-side to return the shared secret
  // ONLY for authenticated users (and ideally role-checked).
  const res = await fetch('/api/qual-key', { credentials: 'include', cache: 'no-store' });
  if (!res.ok) throw new Error('Could not obtain QUAL API key');

  const { key } = await res.json();
  if (!key) throw new Error('QUAL API key missing in response');

  // Make available to all pages on the same origin
  window.__QUAL_API_KEY = key;
  sessionStorage.setItem('QUAL_API_KEY', key);
}

// ===== Prevent navigation from disabled tiles =====
document.addEventListener("click", (e) => {
  if (e.target.closest(".tool.disabled, [aria-disabled='true']")) e.preventDefault();
});

// ===== Force hard navigation on enabled tiles; preserve right/middle-click & modifiers =====
function wireToolLinks() {
  // Attach a single hard-nav handler to primary clicks only.
  // Preserve new tab/window (middle/right click and modifiers).
  const links = document.querySelectorAll('a.tool[href]:not(.disabled)');
  links.forEach((a) => {
    if (a._hardNavWired) return;
    a._hardNavWired = true;
    a.addEventListener("click", (ev) => {
      if (ev.defaultPrevented) return;
      // Only left-button, no modifiers
      if (ev.button !== 0 || ev.metaKey || ev.ctrlKey || ev.shiftKey || ev.altKey) return;
      ev.preventDefault();
      window.location.href = a.href;
    });
  });
}
// Expose for callers that expect a window property even in module contexts
// (index may load this as a module; this ensures availability)
window.wireToolLinks = wireToolLinks;

// ===== Theme toggle (persisted; respects system when unset) =====
// --- Deterministic theme boot: default LIGHT; no OS mirroring
// NOTE: Keep prepaint minimal: set attribute only; no storage writes; no resize.
(function themeToggle() {
  const root = document.documentElement;
  const STORAGE_KEY = 'theme'; // 'light' | 'dark'
  const meta = document.querySelector('meta[name="color-scheme"]');

  // Determine current effective theme (saved -> attr -> default light)
  const saved = localStorage.getItem(STORAGE_KEY);
  const initial = saved === 'dark' ? 'dark' : 'light';

  // Set attribute only to avoid triggering layout work during prepaint
  if (root.getAttribute('data-theme') !== initial) {
    root.setAttribute('data-theme', initial);
  }
  if (meta) meta.setAttribute('content', 'light dark');
})();

// ===== Power BI embed helpers =====
let pbiReport = null;
async function getJsonFromApi(path) {
  const url = `${path}${path.includes("?") ? "&" : "?"}ts=${Date.now()}`; // cache-buster
  const resp = await fetch(url, { credentials: "include", cache: "no-store" });

  const ct = resp.headers.get("content-type") || "";
  const text = await resp.text(); // always read text so we can diagnose

  if (!resp.ok) {
    throw new Error(`${path} HTTP ${resp.status} | ${ct} | ${text.slice(0, 400)}`);
  }
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`Non-JSON from ${path} (${ct}). First 200 chars: ${text.slice(0, 200)}`);
  }
}

function scheduleTokenRefresh(report) {
  setTimeout(async () => {
    try {
      const r = await fetch("/api/pbi-token", { credentials: "include", cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const data = await getJsonFromApi("/api/pbi-token");
      const token =
        data.token ??
        (data.embedToken && data.embedToken.token) ??
        data.accessToken ??
        null;
      if (!token) throw new Error("No token in refresh response");
      await report.setAccessToken(token);
      scheduleTokenRefresh(report);
    } catch (e) {
      console.error("Token refresh failed:", e);
    }
  }, 55 * 60 * 1000);
}

function showPbiDisabled(msg) {
  const el = document.getElementById("pbi-status");
  if (el) el.textContent = msg || "Power BI is disabled.";
  // Keep the section visible so users can see the status message.
  // document.getElementById("pbiSection")?.classList.add("hide");
}

function waitForPowerBi(ms = 10000) {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    (function check() {
      if (window.powerbi) return resolve(window.powerbi);
      if (Date.now() - start > ms) return reject(new Error("Power BI client not loaded"));
      requestAnimationFrame(check);
    })();
  });
}

async function renderReport() {
  try {
    const res = await fetch("/api/pbi-token", { credentials: "include", cache: "no-store" });
    if (!res.ok) throw new Error(await res.text());
    const data = await getJsonFromApi("/api/pbi-token");

    // Compute fields first; proceed if we have the essentials even if `disabled` is present.
    const embedUrl = data?.embedUrl ?? null;
    const token =
      data?.token ??
      data?.embedToken?.token ??
      data?.accessToken ??
      null;
    const reportId = data?.reportId ?? (embedUrl ? new URL(embedUrl).searchParams.get("reportId") : null);

    // If nothing usable was returned, show the disabled message.
    if (!embedUrl || !token || !reportId) {
      showPbiDisabled("Power BI not configured for this environment.");
      return;
    }

    if (!embedUrl || !token || !reportId) {
      showPbiDisabled("Missing embed parameters (embedUrl/token/reportId).");
      return;
    }

    const pbiGlobal = await waitForPowerBi(10000);
    const models = pbiGlobal && pbiGlobal.models ? pbiGlobal.models : (window.powerbi && window.powerbi.models);
    if (!models) {
      showPbiDisabled("Power BI models not available.");
      return;
    }

    const container = document.getElementById("reportContainer");
    if (!container) return;

    try {
      const existing = window.powerbi.get(container);
      if (existing) window.powerbi.reset(container);
    } catch { /* no-op */ }

    pbiReport = window.powerbi.embed(container, {
      type: "report",
      id: reportId,
      embedUrl,
      accessToken: token,
      tokenType: models.TokenType.Embed,
      permissions: models.Permissions.Read,
      viewMode: models.ViewMode.View,
      settings: {
        layoutType: models.LayoutType.FitToWidth,
        panes: {
          filters: { visible: false, expanded: false },
          pageNavigation: { visible: true }
        },
        navContentPaneEnabled: false,
        background: models.BackgroundType.Transparent
      }
    });

    pbiReport.on("loaded", async () => {
      try {
        await pbiReport.updateSettings({
          settings: {
            layoutType: models.LayoutType.FitToWidth,
            panes: { filters: { visible: false, expanded: false }, pageNavigation: { visible: true } },
            navContentPaneEnabled: false,
            background: models.BackgroundType.Transparent
          }
        });
      } catch { }
      const status = document.getElementById("pbi-status");
      if (status) status.textContent = "";
      console.log("Power BI report loaded");
    });

    pbiReport.on("error", (evt) => {
      console.error("PowerBI embed error:", evt?.detail || evt);
    });

    scheduleTokenRefresh(pbiReport);
  } catch (err) {
    const status = document.getElementById("pbi-status");
    if (status) status.textContent = "Power BI token error.";
    console.error("Embed failed:", err);
  }
}
// Ensure the embed function is available in window even when loaded as module
window.renderReport = renderReport;

// Aspect ratio fallback
(function ensureAspectRatio() {
  const host = document.getElementById("reportContainer");
  if (!host) return;
  if (host.style.minHeight || (window.CSS && CSS.supports && CSS.supports("aspect-ratio: 16 / 9"))) return;

  const MIN = 520; // must align with CSS token min-height
  const resize = () => {
    const h = Math.round(host.clientWidth * 9 / 16);
    host.style.minHeight = Math.max(h, MIN) + "px";
  };
  resize();
  window.addEventListener("resize", resize);
  try {
    new ResizeObserver(resize).observe(host.parentElement || document.body);
  } catch { /* older engines */ }
})();

// Fullscreen / Expand
// NOTE: Legacy handler retained for compatibility; delegates to unified handler below if present.
(function setupPBIExpand() {
  const btn = document.getElementById("btnExpandPBI");
  const section = document.getElementById("pbiSection");
  if (!btn || !section) return;

  // If unified handler (below) already wired, do nothing here.
  if (btn._pbiUnifiedFs) return;

  const hasNativeFs = !!section.requestFullscreen;

  const enter = async () => {
    section.classList.add("pbi-fullscreen");
    document.body.classList.add("fs-lock");
    try { if (hasNativeFs) await section.requestFullscreen(); } catch { }
    btn.textContent = "Exit";
    btn.setAttribute("aria-pressed", "true");
    btn.setAttribute("aria-label", "Exit fullscreen");
    nudgeResize();
  };

  const exit = async () => {
    section.classList.remove("pbi-fullscreen");
    document.body.classList.remove("fs-lock");
    try { if (document.fullscreenElement && document.exitFullscreen) await document.exitFullscreen(); } catch { }
    btn.textContent = "Expand";
    btn.setAttribute("aria-pressed", "false");
    btn.setAttribute("aria-label", "Enter fullscreen");
    nudgeResize();
  };

  const handler = () => {
    const cssOn = section.classList.contains("pbi-fullscreen");
    const realFsOn = !!document.fullscreenElement;
    (cssOn || realFsOn) ? exit() : enter();
  };

  if (!btn._pbiLegacyFs) {
    btn.addEventListener("click", handler);
    btn._pbiLegacyFs = true;
  }

  document.addEventListener("fullscreenchange", () => {
    if (!document.fullscreenElement && section.classList.contains("pbi-fullscreen")) {
      section.classList.remove("pbi-fullscreen");
      document.body.classList.remove("fs-lock");
      btn.textContent = "Expand";
      btn.setAttribute("aria-pressed", "false");
      btn.setAttribute("aria-label", "Enter fullscreen");
      nudgeResize();
    }
  });
})();

// ===== Auth & bootstrap =====
(async function init() {
  // --- one-time boot guard (prevents double bootstrap) ---
  if (window.__SWA_MAIN_BOOTED__) return;
  window.__SWA_MAIN_BOOTED__ = true;

  // ---------------- THEME (local, no external helpers) -------------------
  const root = document.documentElement;
  const STORAGE_KEY = 'theme'; // 'light' | 'dark'
  let META = document.querySelector('meta[name="color-scheme"]');
  if (!META) {
    META = document.createElement('meta');
    META.setAttribute('name', 'color-scheme');
    document.head.appendChild(META);
  }
  META.setAttribute('content', 'light dark');
  const saved = localStorage.getItem(STORAGE_KEY);
  const attr = root.getAttribute('data-theme');
  const initial = (saved === 'dark' || saved === 'light')
    ? saved
    : (attr === 'dark' || attr === 'light' ? attr : 'light');

  function setTheme(next) {
    const t = next === 'dark' ? 'dark' : 'light';
    if (root.getAttribute('data-theme') !== t) {
      root.setAttribute('data-theme', t);
    }
    // only write if value changed
    if (localStorage.getItem(STORAGE_KEY) !== t) {
      localStorage.setItem(STORAGE_KEY, t);
    }
    // keep UA widgets happy
    META.setAttribute('content', 'light dark');
    syncToggleButton();
    nudgeResize();
  }
  function getTheme() {
    return root.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
  }

  function syncToggleButton() {
    const btn = document.getElementById('themeToggle');
    if (!btn) return;
    const dark = getTheme() === 'dark';
    btn.setAttribute('aria-pressed', dark ? 'true' : 'false');
    btn.textContent = dark ? '☀️' : '🌙';
    btn.setAttribute('aria-label', dark ? 'Switch to light mode' : 'Switch to dark mode');
  }

  setTheme(initial);

  document.addEventListener('click', (e) => {
    const target = e.target;
    if (!(target instanceof Element)) return;
    if (target.id === 'themeToggle') {
      setTheme(getTheme() === 'dark' ? 'light' : 'dark');
      // No need to stopPropagation; harmless for chips
    }
  }, { capture: false });

  // Sync across tabs (no writeback loop; setTheme already checks for change)
  window.addEventListener('storage', (e) => {
    if (e.key === STORAGE_KEY) {
      const next = e.newValue === 'dark' ? 'dark' : 'light';
      if (next !== getTheme()) setTheme(next);
    }
  });

  window.__appSetTheme = setTheme;
  syncToggleButton();

  // -------------------- DOM refs & utilities -----------------------------
  const welcome = document.getElementById('welcome');
  const dash = document.getElementById('dashboard');
  const pbiSection = document.getElementById('pbiSection');
  const nameEl = document.getElementById('userName');
  const authButtons = document.getElementById('authButtons');
  const brandLogo = document.getElementById('brandLogo');

  function setLikelyAuthed(flag) {
    document.documentElement.setAttribute("data-likely-authed", flag ? "1" : "0");
  }

  // Logo fallback
  brandLogo?.addEventListener('error', () => {
    if (!brandLogo.dataset.fallback) {
      brandLogo.dataset.fallback = '1';
      try {
        // Resolve against the current base URI so it works under /web/
        brandLogo.src = new URL('assets/larato-logo-bullet.svg', document.baseURI).toString();
      } catch {
        brandLogo.src = '/web/assets/larato-logo-bullet.svg';
      }
    }
  });

  // Tool links (optional helper)
  if (typeof window.wireToolLinks === 'function') {
    try { window.wireToolLinks(); } catch { }
  }

  async function getClientPrincipal() {
    try {
      const res = await fetch('/.auth/me', { credentials: 'include', cache: 'no-store' });
      if (!res.ok) return null;
      const data = await res.json().catch(() => null);
      return data?.clientPrincipal ?? null;
    } catch {
      return null;
    }
  }

  // Inject QUAL API key for the qualification app
  {
    const m = document.querySelector('meta[name="qual-api-key"]');
    if (m && m.content) {
      window.__QUAL_API_KEY = m.content.trim();
      console.debug('[menu] QUAL API key injected for downstream app');
    }
  }

  // ====== AUTH FUNCTIONS ARE LEFT UNCHANGED BY REQUEST ======
  function buildLoginUrl() {
    return '/.auth/login/aad?post_login_redirect_uri=' + encodeURIComponent('/');
  }

  function showSignIn() {
    welcome?.classList.remove('hide');
    dash?.classList.add('hide');
    pbiSection?.classList.add('hide');

    if (authButtons) {
      const btn = document.getElementById('themeToggle');
      // Keep the toggle chip visible
      btn ? authButtons.replaceChildren(btn) : authButtons.replaceChildren();
      const inBtn = document.createElement('a');
      inBtn.className = 'chip';
      inBtn.id = 'btnSignIn';
      inBtn.href = buildLoginUrl();
      inBtn.textContent = 'Sign in';
      authButtons.appendChild(inBtn);
    }

    const hero = document.getElementById('btnSignInPrimary');
    if (hero) hero.setAttribute('href', buildLoginUrl());
  }

  function resolveFirstName(principal) {
    // Helper to Title-case a single word: "lucy" -> "Lucy"
    const tc = (w) => w ? (w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()) : w;

    // If caller passed a plain string (e.g. "Lucy Green" or "lucy.green@larato.co.uk")
    if (typeof principal === 'string') {
      const s = principal.trim();
      if (!s) return 'there';

      // Email string? Use local-part before @, split on dot/underscore/hyphen
      if (s.includes('@')) {
        const local = s.split('@')[0];
        const first = (local.split(/[.\-_]/)[0] || '').trim();
        return first ? tc(first) : 'there';
      }

      // Regular name string: take the first whitespace-separated token
      return tc(s.split(/\s+/)[0]);
    }

    // Null/undefined guard
    if (!principal) return 'there';

    // EasyAuth / AAD principal object
    const claims = principal.userClaims || principal.claims || [];
    const map = new Map(claims.map(c => [c.typ || c.type, c.val || c.value]));

    // Prefer given_name
    const givenName =
      map.get('given_name') ||
      map.get('http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname');

    // Common display names
    const displayName =
      map.get('name') ||
      principal.name ||
      principal.userDetails ||
      '';

    const firstFromDisplay = (displayName.trim().split(/\s+/)[0] || '');

    // Fallback: email local-part
    const emailLike = principal.userId || principal.userDetails || '';
    const firstFromEmail = ((emailLike.split('@')[0] || '').split(/[.\-_]/)[0] || '').trim();

    return tc(givenName || firstFromDisplay || firstFromEmail || 'there');
  }

  function showSignedIn(userDetails) {
    welcome?.classList.add('hide');
    dash?.classList.remove('hide');
    pbiSection?.classList.remove('hide');

    if (nameEl) nameEl.textContent = resolveFirstName(userDetails);

    if (authButtons) {
      const btn = document.getElementById('themeToggle');
      btn ? authButtons.replaceChildren(btn) : authButtons.replaceChildren();

      const signed = document.createElement('span');
      signed.className = 'chip';
      signed.textContent = 'Signed in';
      authButtons.appendChild(signed);

      const userChip = document.createElement('span');
      userChip.className = 'chip';
      userChip.textContent = friendlyName(userDetails);
      authButtons.appendChild(userChip);

      const outBtn = document.createElement('a');
      outBtn.className = 'chip';
      outBtn.id = 'btnSignOut';
      outBtn.href = '/.auth/logout';
      outBtn.textContent = 'Sign out';
      authButtons.appendChild(outBtn);
    }
  }

  // ----------------------- Auth + PBI boot -------------------------------
  try {
    const princ = await getClientPrincipal();
    if (princ) {
      setLikelyAuthed(true);
      showSignedIn(princ.userDetails);
      try { window.renderReport?.(); } catch { /* non-blocking */ }
    } else {
      setLikelyAuthed(false);
      showSignIn();
    }
  } catch {
    setLikelyAuthed(false);                     // <<< added
    // If auth probe fails, show sign-in so the user can try again
    showSignIn();
  } finally {
    // Always reveal UI
    document.body.classList.remove('preauth');
  }
})();

// --- Power BI "Expand" / fullscreen toggle (runs after DOM is parsed) ---
// Unified handler; marks the button so legacy handler above won’t double-wire.
(() => {
  const btn = document.getElementById('btnExpandPBI');
  const section = document.getElementById('pbiSection');
  if (!btn || !section) return;

  btn._pbiUnifiedFs = true;

  // Prefer the actual report host for a better fullscreen experience
  const host = document.getElementById('reportContainer');

  function enter() {
    section.classList.add('pbi-fullscreen');
    document.body.classList.add('fs-lock');

    // Request browser fullscreen on the host if available (ignore failures)
    (host?.requestFullscreen?.() || Promise.resolve()).catch(() => { });

    btn.textContent = 'Exit';
    btn.setAttribute('aria-pressed', 'true');
    btn.setAttribute('aria-label', 'Exit fullscreen');

    // Nudge layout (Power BI listens for resize)
    nudgeResize();
  }

  function exit() {
    // If browser fullscreen is on, exit it (ignore failures)
    if (document.fullscreenElement) {
      document.exitFullscreen().catch(() => { });
    }
    section.classList.remove('pbi-fullscreen');
    document.body.classList.remove('fs-lock');

    btn.textContent = 'Expand';
    btn.setAttribute('aria-pressed', 'false');
    btn.setAttribute('aria-label', 'Enter fullscreen');

    nudgeResize();
  }

  // Click toggles between the two states; works whether or not fullscreen API succeeds
  if (!btn._pbiClickWired) {
    btn.addEventListener('click', () => {
      const cssOn = section.classList.contains('pbi-fullscreen');
      const fsOn = !!document.fullscreenElement;
      (cssOn || fsOn) ? exit() : enter();
    });
    btn._pbiClickWired = true;
  }

  // If the user exits fullscreen with ESC, keep CSS/UI in sync
  document.addEventListener('fullscreenchange', () => {
    if (!document.fullscreenElement && section.classList.contains('pbi-fullscreen')) {
      exit();
    }
  });
})();
