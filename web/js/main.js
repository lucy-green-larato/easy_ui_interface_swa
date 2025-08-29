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
    } catch (err) {
      console.error("Token refresh failed:", err);
    }
  }, 55 * 60 * 1000);
}

async function renderReport() {
  const res = await fetch("/api/pbi-token", { credentials: "include" });
  if (!res.ok) throw new Error(await res.text());
  const { embedUrl, reportId, token } = await res.json();

  const pbiGlobal = window["powerbi-client"] || window.powerbi;
  if (!pbiGlobal || !window.powerbi) throw new Error("Power BI client not loaded.");
  const models = pbiGlobal.models || window.powerbi.models;

  const container = document.getElementById("reportContainer");
  if (!container) return;

  // reset if re-rendering
  try {
    const existing = window.powerbi.get(container);
    if (existing) window.powerbi.reset(container);
  } catch {}

  pbiReport = window.powerbi.embed(container, {
    type: "report",
    id: reportId,
    embedUrl,
    accessToken: token,
    tokenType: models.TokenType.Embed,
    permissions: models.Permissions.All,
    settings: {
      panes: {
        filters: { visible: false, expanded: false }     // HIDE Filters pane
      },
      pageNavigationPosition: models.PageNavigationPosition.Bottom, // keep Direct/Channel tabs
      navContentPaneEnabled: false,                      // no left nav
      layoutType: models.LayoutType.Custom               // we'll force fit-to-width below
    }
  });

  pbiReport.on("loaded", async () => {
    // Force **Fit to width** so the page fills the box (no tiny canvas)
    try {
      await pbiReport.updateSettings({
        layoutType: models.LayoutType.Custom,
        customLayout: { displayOption: models.DisplayOption.FitToWidth }
      });
    } catch (e) {
      console.warn("Could not set FitToWidth:", e);
    }
    console.log("Report loaded");
  });

  pbiReport.on("error", (evt) => console.error("PowerBI embed error:", evt?.detail || evt));
  scheduleTokenRefresh(pbiReport);

  // Nudge layout after window resizes
  let t;
  window.addEventListener("resize", () => {
    clearTimeout(t);
    t = setTimeout(() => { pbiReport?.refresh().catch(()=>{}); }, 120);
  });
}

// ===== Init: toggle signed-in/out UI and render report =====
(async function init() {
  try {
    const res = await fetch("/.auth/me", { credentials: "include" });
    if (!res.ok) throw new Error("auth check failed");
    const data = await res.json();
    const princ = data?.clientPrincipal;

    const welcome = document.getElementById("welcome");
    const dash = document.getElementById("dashboard");
    const nameEl = document.getElementById("userName");
    const authButtons = document.getElementById("authButtons");
    authButtons.replaceChildren();

    if (princ) {
      welcome?.classList.add("hide");
      dash?.classList.remove("hide");
      if (nameEl) nameEl.textContent = princ.userDetails ? `, ${princ.userDetails}` : "";

      const outBtn = document.createElement("button");
      outBtn.className = "btn";
      outBtn.textContent = "Sign out";
      outBtn.addEventListener("click", logout);
      authButtons.appendChild(outBtn);

      await renderReport();
    } else {
      welcome?.classList.remove("hide");
      dash?.classList.add("hide");

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
    const authButtons = document.getElementById("authButtons");
    welcome?.classList.remove("hide");
    dash?.classList.add("hide");
    if (authButtons && !authButtons.children.length) {
      const inBtn = document.createElement("button");
      inBtn.className = "btn";
      inBtn.textContent = "Sign in";
      inBtn.addEventListener("click", login);
      authButtons.appendChild(inBtn);
    }
  }
})();
