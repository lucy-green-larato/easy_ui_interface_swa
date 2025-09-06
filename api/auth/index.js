module.exports = async function (context, req) {
  try {
    // Always-signed-in principal for local dev
    const principal = {
      identityProvider: "dev",
      userId: "local-dev-user",
      userDetails: "lucy.green@larato.co.uk",
      userRoles: ["anonymous", "authenticated"]
    };

    const raw = (req.params && req.params.path) || "";
    const path = String(raw).toLowerCase();

    if (path === "" || path === "me" || path === "refresh") {
      context.res = {
        status: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ clientPrincipal: principal })
      };
      return;
    }

    if (path.startsWith("login") || path.startsWith("logout")) {
      // Pretend success and redirect home in local dev
      context.res = { status: 302, headers: { "Location": "/" }, body: "" };
      return;
    }

    context.res = {
      status: 404,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Not found" })
    };
  } catch (e) {
    context.log.error("auth exception", e);
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "auth exception", detail: String(e && e.message || e) })
    };
  }
};
