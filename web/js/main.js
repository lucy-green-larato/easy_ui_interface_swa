// ===== Auth helpers =====
function buildReturnTarget() {
  const target = window.location.pathname + window.location.search + window.location.hash;
  return encodeURIComponent(target || "/");
}
function login() { location.href = "/.auth/login/aad?post_login_redirect_uri=" + buildReturnTarget(); }
function logout() { location.href = "/.auth/logout"; }

// Prevent navigation from disabled tiles (kept from your page)
document.addEventListener("click", (e) => {
  const t = e.target.closest(".tool.disabled");
  if (t) e.preventDefault();
});

// Wire up sign-in buttons (no inline handlers)
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("btnSignIn")?.addEventListener("click", login);
  document.getElementById("btnSignInPrimary")?.addEventListener("click", login);
});

// ===== Power BI embed =====
let pbiReport = null;

async function renderReport() {
  try {
    const res = await fetch("/api/pbi-token", { credentials: "include" });
    if (!res.ok) throw new Error(await res.text());
    const { embedUrl, reportId, token } = await res.json();

    const models = window["powerbi-client"].models;
    const container = document.getElementById("pbi");
    if (!container) return;

    // reset if re-rendering
    window.powerbi.reset(container);

    pbiReport = window.powerbi.embed(container, {
      type: "report",
      id: reportId,
      embedUrl,
      accessToken: token,
      tokenType: models.TokenType.Embed,
      permissions: models.Permissions.All,
      settings: { panes: { filters: { visible: true } } }
    });

    // after powerbi.embed(...)
    scheduleTokenRefresh(report);

    function scheduleTokenRefresh(report) {
      // refresh ~55 mins after issuing the token, or sooner if you prefer
      setTimeout(async () => {
        const r = await fetch("/api/pbi-token");
        const { token } = await r.json();
        await report.setAccessToken(token);
        scheduleTokenRefresh(report);
      }, 55 * 60 * 1000);
    }
  }

// ===== Init: toggle signed-in/out UI and render report on dashboard =====
(async function init() {
    try {
      const res = await fetch("/.auth/me", { credentials: "include" });
      if (!res.ok) throw new Error("auth check failed");
      const data = await res.json();
      const princ = data && data.clientPrincipal;

      const welcome = document.getElementById("welcome");
      const dash = document.getElementById("dashboard");
      const nameEl = document.getElementById("userName");
      const authButtons = document.getElementById("authButtons");

      // clear header auth area
      authButtons.replaceChildren();

      if (princ) {
        welcome.classList.add("hide");
        dash.classList.remove("hide");
        nameEl.textContent = princ.userDetails ? `, ${princ.userDetails}` : "";

        const outBtn = document.createElement("button");
        outBtn.className = "btn";
        outBtn.textContent = "Sign out";
        outBtn.addEventListener("click", logout);
        authButtons.appendChild(outBtn);

        // render report on the menu page
        await renderReport();
      } else {
        welcome.classList.remove("hide");
        dash.classList.add("hide");

        const inBtn = document.createElement("button");
        inBtn.className = "btn";
        inBtn.textContent = "Sign in";
        inBtn.addEventListener("click", login);
        authButtons.appendChild(inBtn);
      }
    } catch {
      // default to signed-out view if auth check fails
    }
  })();
