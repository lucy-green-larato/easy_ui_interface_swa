// ===== Auth helpers =====
function buildReturnTarget() {
  const target = window.location.pathname + window.location.search + window.location.hash;
  return encodeURIComponent(target || "/");
}
function login() { location.href = "/.auth/login/aad?post_login_redirect_uri=" + buildReturnTarget(); }
function logout() { location.href = "/.auth/logout"; }

// Prevent navigation from disabled tiles
document.addEventListener("click", (e) => {
  if (e.target.closest(".tool.disabled")) e.preventDefault();
});

// Wire up sign-in buttons
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("btnSignIn")?.addEventListener("click", login);
  document.getElementById("btnSignInPrimary")?.addEventListener("click", login);
});

// ===== Theme toggle (persisted, overrides OS) =====
(() => {
  const KEY = "theme"; // 'light' | 'dark' | 'auto'
  const btn = document.getElementById("themeToggle") || document.getElementById("btnThemeToggle");

  const prefersDark = () =>
    window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;

  function applyTheme(mode) {
    const root = document.documentElement;
    if (mode === "light" || mode === "dark") {
      root.setAttribute("data-theme", mode);
    } else {
      root.removeAttribute("data-theme"); // 'auto' => follow OS
    }
    updateToggleUI(mode);
  }

  function updateToggleUI(mode) {
    if (!btn) return;
    const isDark = mode === "dark" || (mode === "auto" && prefersDark());
    btn.setAttribute("aria-pressed", isDark ? "true" : "false");
    btn.setAttribute("aria-label", isDark ? "Switch to light mode" : "Switch to dark mode");
    btn.title = btn.getAttribute("aria-label");
    // show the opposite action as the icon
    btn.textContent = isDark ? "☀️" : "🌙";
  }

  const getTheme = () => localStorage.getItem(KEY) || "auto";
  const setTheme = (mode) => { localStorage.setItem(KEY, mode); applyTheme(mode); };

  // init
  applyTheme(getTheme());

  // react to OS changes only when user is on 'auto'
  const mql = window.matchMedia("(prefers-color-scheme: dark)");
  mql.addEventListener?.("change", () => { if (getTheme() === "auto") applyTheme("auto"); });

  // click: toggle light <-> dark (first click from 'auto' chooses the opposite of OS)
  btn?.addEventListener("click", () => {
    const cur = getTheme();
    if (cur === "dark") setTheme("light");
    else if (cur === "light") setTheme("dark");
    else setTheme(prefersDark() ? "light" : "dark");
  });
})();

// ===== Power BI embed =====
let pbiReport = null;

function scheduleTokenRefresh(report) {
  setTimeout(async () => {
    try {
      const r = await fetch("/api/pbi-token", { credentials: "include" });
      if (!r.ok) throw new Error(await r.text());
      const { token } = await r.json();
      await report.setAccessToken(token);
      scheduleTokenRefresh(report);
    } catch (e) {
      console.error("Token refresh failed:", e);
    }
  }, 55 * 60 * 1000);
}

async function renderReport() {
  try {
    const res = await fetch("/api/pbi-token", { credentials: "include" });
    if (!res.ok) throw new Error(await res.text());
    const { embedUrl, reportId, token } = await res.json();

    const pbiGlobal = window["powerbi-client"] || window.powerbi;
    if (!pbiGlobal || !window.powerbi) throw new Error("Power BI client not loaded.");
    const models = pbiGlobal.models || window.powerbi.models;

    const container = document.getElementById("reportContainer"); // <-- matches your HTML
    if (!container) return;

    // reset if re-rendering
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
      // inside renderReport() -> window.powerbi.embed(...settings...)
      settings: {
        layoutType: models.LayoutType.FitToWidth,
        panes: {
          filters: { visible: false, expanded: false },
          pageNavigation: { visible: true }         // was true – hide bottom tab bar
        },
        navContentPaneEnabled: false,
        background: models.BackgroundType.Transparent
      }

    });

    pbiReport.on("loaded", async () => {
      // Re-assert settings in case tenant defaults override
      try {
        await pbiReport.updateSettings({
          settings: {
            layoutType: models.LayoutType.FitToWidth,
            panes: { filters: { visible: false, expanded: false }, pageNavigation: { visible: true } },
            navContentPaneEnabled: false,
            background: models.BackgroundType.Transparent
          }
        });
      } catch { /* no-op */ }
      console.log("Report loaded");
    });

    pbiReport.on("error", (evt) => console.error("PowerBI embed error:", evt?.detail || evt));
    scheduleTokenRefresh(pbiReport);
  } catch (err) {
    console.error("Embed failed:", err);
    throw err;
  }
}

// Fallback if a browser ignores CSS aspect-ratio (rare, but safe)
(function ensureAspectRatio() {
  const host = document.getElementById('reportContainer');
  if (!host) return;
  if (host.style.height || (window.CSS && CSS.supports && CSS.supports('aspect-ratio: 16 / 9'))) return;
  const resize = () => { host.style.height = Math.round(host.clientWidth * 9 / 16) + 'px'; };
  resize();
  window.addEventListener('resize', resize);
  new ResizeObserver(resize).observe(host.parentElement || document.body);
})();

// ===== Init: toggle signed-in/out UI and render report =====
(async function init() {
  const welcome = document.getElementById("welcome");
  const dash = document.getElementById("dashboard");
  const pbiSection = document.getElementById("pbiSection");
  const nameEl = document.getElementById("userName");
  const authButtons = document.getElementById("authButtons");

  function showSignIn() {
    welcome?.classList.remove("hide");
    dash?.classList.add("hide");
    pbiSection?.classList.add("hide");
    if (authButtons) {
      authButtons.replaceChildren();
      const inBtn = document.createElement("button");
      inBtn.className = "btn";
      inBtn.textContent = "Sign in";
      inBtn.addEventListener("click", login);
      authButtons.appendChild(inBtn);
    }
  }

  function showSignedIn(userDetails) {
    welcome?.classList.add("hide");
    dash?.classList.remove("hide");
    pbiSection?.classList.remove("hide");
    if (nameEl) nameEl.textContent = userDetails ? `, ${userDetails}` : "";
    if (authButtons) {
      authButtons.replaceChildren();
      const outBtn = document.createElement("button");
      outBtn.className = "btn";
      outBtn.textContent = "Sign out";
      outBtn.addEventListener("click", logout);
      authButtons.appendChild(outBtn);
    }
  }

  try {
    const res = await fetch("/.auth/me", { credentials: "include" });
    if (!res.ok) throw new Error("auth check failed");
    const { clientPrincipal: princ } = await res.json();

    if (princ) {
      showSignedIn(princ.userDetails);
      await renderReport(); // embed after host is visible
    } else {
      showSignIn();
    }
  } catch (e) {
    console.warn("Auth check failed; showing signed-out view.", e);
    showSignIn();
  }

  // ===== Light/Dark theme toggle (persists, respects system when unset) =====
  (function themeToggle() {
    const root = document.documentElement;
    const mq = window.matchMedia('(prefers-color-scheme: dark)');

    function currentIsDark() {
      const explicit = root.getAttribute('data-theme');
      if (explicit === 'dark') return true;
      if (explicit === 'light') return false;
      return mq.matches;
    }

    function updateButton() {
      const btn = document.getElementById('themeToggle');
      if (!btn) return;
      const dark = currentIsDark();
      btn.setAttribute('aria-pressed', dark ? 'true' : 'false');
      btn.textContent = dark ? '☀️' : '🌙';
      btn.setAttribute('aria-label', dark ? 'Switch to light mode' : 'Switch to dark mode');
      document.querySelector('meta[name="color-scheme"]')?.setAttribute('content', 'light dark');
    }

    function setTheme(value) { // 'light' | 'dark' | '' (system)
      if (value === 'light' || value === 'dark') {
        root.setAttribute('data-theme', value);
        localStorage.setItem('theme', value);
      } else {
        root.removeAttribute('data-theme');
        localStorage.removeItem('theme');
      }
      updateButton();
    }
    // Always wire the Expand button (both paths)
    const expandBtn = document.getElementById("btnExpandPBI");
    expandBtn?.addEventListener("click", () => {
      const root = document.getElementById("pbiSection");
      const on = root.classList.toggle("pbi-fullscreen");
      expandBtn.setAttribute("aria-pressed", on ? "true" : "false");
      expandBtn.textContent = on ? "Close" : "Expand";
      setTimeout(() => window.dispatchEvent(new Event("resize")), 50);
    });
  })();

  document.addEventListener('DOMContentLoaded', () => {
    const saved = localStorage.getItem('theme'); // 'light' | 'dark' | null
    setTheme(saved || ''); // '' => follow system
    document.getElementById('themeToggle')?.addEventListener('click', () => {
      const now = root.getAttribute('data-theme');
      setTheme(now === 'dark' ? 'light' : 'dark');
    });
  });

  mq.addEventListener?.('change', () => {
    if (!localStorage.getItem('theme')) updateButton();
  });
})();
