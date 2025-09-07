// ===== Auth helpers =====
function buildReturnTarget() {
  const target = window.location.pathname + window.location.search + window.location.hash;
  return encodeURIComponent(target || "/");
}

// Prevent navigation from disabled tiles
document.addEventListener("click", (e) => {
  if (e.target.closest(".tool.disabled")) e.preventDefault();
});

// (removed the old DOMContentLoaded wireup here; we'll wire after login() exists)

// ===== Theme toggle (persisted; respects system when unset) =====
(function themeToggle() {
  const root = document.documentElement;
  const storageKey = "theme"; // 'light' | 'dark'
  const mq = window.matchMedia("(prefers-color-scheme: dark)");
  const meta = document.querySelector('meta[name="color-scheme"]');

  function explicitTheme() {
    // returns 'light' | 'dark' | null (system)
    return root.getAttribute("data-theme") || localStorage.getItem(storageKey) || null;
  }
  function isDarkNow() {
    const exp = explicitTheme();
    return exp ? exp === "dark" : mq.matches;
  }
  function updateButton() {
    const btn = document.getElementById("themeToggle");
    if (!btn) return;
    const dark = isDarkNow();
    btn.setAttribute("aria-pressed", dark ? "true" : "false");
    btn.textContent = dark ? "☀️" : "🌙";
    btn.setAttribute("aria-label", dark ? "Switch to light mode" : "Switch to dark mode");
  }
  function apply(theme /* 'light' | 'dark' | null */) {
    if (theme === "light" || theme === "dark") {
      root.setAttribute("data-theme", theme);
      localStorage.setItem(storageKey, theme);
    } else {
      root.removeAttribute("data-theme");
      localStorage.removeItem(storageKey);
    }
    if (meta) meta.setAttribute("content", "light dark");
    updateButton();
    // Nudge PBI to recalc layout if scrollbars/theme change
    setTimeout(() => window.dispatchEvent(new Event("resize")), 60);
  }

  // init from storage or system
  const saved = localStorage.getItem(storageKey);
  if (saved === "light" || saved === "dark") {
    root.setAttribute("data-theme", saved);
  } else {
    root.removeAttribute("data-theme"); // system
  }
  if (meta) meta.setAttribute("content", "light dark");
  updateButton();

  // click toggle (two-state: light <-> dark)
  document.addEventListener("click", (e) => {
    const btn = e.target.closest("#themeToggle");
    if (!btn) return;
    const exp = explicitTheme();
    apply(exp === "dark" ? "light" : "dark");
  });

  // reflect system changes only when using system (no explicit theme)
  const onSystemChange = () => {
    if (!localStorage.getItem(storageKey) && !root.getAttribute("data-theme")) {
      updateButton();
    }
  };
  if (mq.addEventListener) mq.addEventListener("change", onSystemChange);
  else if (mq.addListener) mq.addListener(onSystemChange);
})();

// ===== Power BI embed (tolerant to disabled/local) =====
let pbiReport = null;

function scheduleTokenRefresh(report) {
  setTimeout(async () => {
    try {
      const r = await fetch("/api/pbi-token", { credentials: "include" });
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      const token = data.token ?? data.embedToken?.token;
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
  const pbiSection = document.getElementById("pbiSection");
  pbiSection?.classList.add("hide");
}

async function renderReport() {
  try {
    const res = await fetch("/api/pbi-token", { credentials: "include" });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();

    if (!data || data.disabled) {
      showPbiDisabled("Power BI not configured for this environment.");
      return;
    }

    const embedUrl = data.embedUrl;
    const token = data.token ?? data.embedToken?.token;
    // Prefer explicit reportId if API returns it; otherwise derive from embedUrl (?reportId=...)
    const reportId = data.reportId ?? (embedUrl ? new URL(embedUrl).searchParams.get("reportId") : null);

    if (!embedUrl || !token || !reportId) {
      showPbiDisabled("Missing embed parameters (embedUrl/token/reportId).");
      return;
    }

    const pbiGlobal = window["powerbi-client"] || window.powerbi;
    if (!pbiGlobal || !window.powerbi) throw new Error("Power BI client not loaded.");
    const models = pbiGlobal.models || window.powerbi.models;

    const container = document.getElementById("reportContainer");
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
      settings: {
        layoutType: models.LayoutType.FitToWidth,
        panes: {
          filters: { visible: false, expanded: false },
          pageNavigation: { visible: true } // show bottom tab bar
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
            panes: {
              filters: { visible: false, expanded: false },
              pageNavigation: { visible: true }
            },
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
    // Don't throw; keep the rest of the page alive
  }
}

// Fallback if a browser ignores CSS aspect-ratio (rare, but safe)
(function ensureAspectRatio() {
  const host = document.getElementById("reportContainer");
  if (!host) return;
  if (host.style.height || (window.CSS && CSS.supports && CSS.supports("aspect-ratio: 16 / 9"))) return;
  const resize = () => { host.style.height = Math.round(host.clientWidth * 9 / 16) + "px"; };
  resize();
  window.addEventListener("resize", resize);
  new ResizeObserver(resize).observe(host.parentElement || document.body);
})();

// Fullscreen / Expand for PBI section
(function setupPBIExpand() {
  const btn = document.getElementById("btnExpandPBI");
  const section = document.getElementById("pbiSection");
  if (!btn || !section) return;

  const hasNativeFs = !!section.requestFullscreen;

  const enter = async () => {
    section.classList.add("pbi-fullscreen");
    document.body.classList.add("fs-lock");
    try {
      if (hasNativeFs) await section.requestFullscreen();
    } catch (e) {
      console.warn("Fullscreen request blocked; using CSS fallback.", e);
    }
    btn.textContent = "Close";
    btn.setAttribute("aria-pressed", "true");
    btn.setAttribute("aria-label", "Exit fullscreen");
    setTimeout(() => window.dispatchEvent(new Event("resize")), 60);
  };

  const exit = async () => {
    section.classList.remove("pbi-fullscreen");
    document.body.classList.remove("fs-lock");
    try {
      if (document.fullscreenElement && document.exitFullscreen) {
        await document.exitFullscreen();
      }
    } catch (e) {
      console.warn("Exit fullscreen error (safe to ignore):", e);
    }
    btn.textContent = "Expand";
    btn.setAttribute("aria-pressed", "false");
    btn.setAttribute("aria-label", "Enter fullscreen");
    setTimeout(() => window.dispatchEvent(new Event("resize")), 60);
  };

  btn.addEventListener("click", () => {
    const cssOn = section.classList.contains("pbi-fullscreen");
    const realFsOn = !!document.fullscreenElement;
    (cssOn || realFsOn) ? exit() : enter();
  });

  document.addEventListener("fullscreenchange", () => {
    if (!document.fullscreenElement && section.classList.contains("pbi-fullscreen")) {
      section.classList.remove("pbi-fullscreen");
      document.body.classList.remove("fs-lock");
      btn.textContent = "Expand";
      btn.setAttribute("aria-pressed", "false");
      btn.setAttribute("aria-label", "Enter fullscreen");
      setTimeout(() => window.dispatchEvent(new Event("resize")), 60);
    }
  });

  window.addEventListener("orientationchange", () => {
    if (section.classList.contains("pbi-fullscreen")) {
      setTimeout(() => window.dispatchEvent(new Event("resize")), 250);
    }
  });
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

  // --- Auth (works in dev + Azure) ---
  const IS_LOCAL =
    location.hostname === "127.0.0.1" ||
    location.hostname === "localhost" ||
    location.hostname.endsWith(".github.dev") ||
    location.port === "4280";

  async function getClientPrincipal() {
    const res = await fetch("/api/.auth/me", { credentials: "include" });
    if (!res.ok) return null;
    try {
      const { clientPrincipal } = await res.json();
      return clientPrincipal || null;
    } catch {
      return null;
    }
  }

  function login() {
    if (IS_LOCAL) { location.reload(); return; } // local dev uses stub principal
    location.href = "/.auth/login/aad?post_login_redirect_uri=" + buildReturnTarget();
  }

  function logout() {
    if (IS_LOCAL) { location.reload(); return; }
    location.href = "/.auth/logout";
  }

  // Also wire header/hero buttons *after* login() exists
  document.getElementById("btnSignIn")?.addEventListener("click", login);
  document.getElementById("btnSignInPrimary")?.addEventListener("click", login);

  try {
    const princ = await getClientPrincipal();
    if (princ) {
      showSignedIn(princ.userDetails);
      await (typeof renderReport === "function" ? renderReport() : Promise.resolve());
    } else {
      showSignIn();
    }
  } catch (e) {
    console.warn("Auth check failed; showing signed-out view.", e);
    showSignIn();
  }
})();
