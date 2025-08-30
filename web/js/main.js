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
      settings: {
        // Choose ONE layout:
        // 1) Fit to width (bigger visual, may need taller container)
        layoutType: models.LayoutType.FitToWidth,
        // 2) Or Fit to page (always whole page, smaller visual)
        // layoutType: models.LayoutType.FitToPage,

        panes: {
          filters: { visible: false, expanded: false },  // hide Filters pane
          pageNavigation: { visible: true }              // keep tabs (Direct / Channel)
        },
        navContentPaneEnabled: false                      // hide the left nav content pane
      }
    });

    pbiReport.on("loaded", async () => {
      // Re-assert settings in case tenant defaults override
      try {
        await pbiReport.updateSettings({
          settings: {
            layoutType: models.LayoutType.FitToWidth,
            panes: { filters: { visible: false, expanded: false }, pageNavigation: { visible: true } },
            navContentPaneEnabled: false
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

  if (!(window.CSS && CSS.supports && CSS.supports('aspect-ratio: 16 / 9'))) {
    const resize = () => { host.style.height = Math.round(host.clientWidth * 9 / 16) + 'px'; };
    resize();
    window.addEventListener('resize', resize);
    new ResizeObserver(resize).observe(host.parentElement || document.body);
  }
})();

// ===== Init: toggle signed-in/out UI and render report =====
(async function init() {
  try {
    const res = await fetch("/.auth/me", { credentials: "include" });
    if (!res.ok) throw new Error("auth check failed");
    const data = await res.json();
    const princ = data?.clientPrincipal;

    const welcome = document.getElementById("welcome");
    const dash = document.getElementById("dashboard");
    const pbiSection = document.getElementById("pbiSection");
    const nameEl = document.getElementById("userName");
    const authButtons = document.getElementById("authButtons");
    authButtons.replaceChildren();

    if (princ) {
      welcome?.classList.add("hide");
      dash?.classList.remove("hide");
      pbiSection?.classList.remove("hide"); // show the PBI section when signed in
      if (nameEl) nameEl.textContent = princ.userDetails ? `, ${princ.userDetails}` : "";

      const outBtn = document.createElement("button");
      outBtn.className = "btn";
      outBtn.textContent = "Sign out";
      outBtn.addEventListener("click", logout);
      authButtons.appendChild(outBtn);

      await renderReport(); // embed after the host is visible
    } else {
      welcome?.classList.remove("hide");
      dash?.classList.add("hide");
      pbiSection?.classList.add("hide"); // hide PBI section when signed out

      const inBtn = document.createElement("button");
      inBtn.className = "btn";
      inBtn.textContent = "Sign in";
      inBtn.addEventListener("click", login);
      authButtons.appendChild(inBtn);
    }
  } catch (e) {
    console.warn("Auth check failed; showing signed-out view.", e);
    const welcome = document.getElementById("welcome");
    const dash = document.getElementById("dashboard");
    const pbiSection = document.getElementById("pbiSection");
    const authButtons = document.getElementById("authButtons");
    welcome?.classList.remove("hide");
    dash?.classList.add("hide");
    pbiSection?.classList.add("hide");

    if (authButtons && !authButtons.children.length) {
      const inBtn = document.createElement("button");
      inBtn.className = "btn";
      inBtn.textContent = "Sign in";
      inBtn.addEventListener("click", login);
      authButtons.appendChild(inBtn);
    }
  }
})();
