// Robust /.auth shim for local + tools.
const ONE_DAY = 60 * 60 * 24;

function principalFromCookie(req) {
  const cookie = (req.headers && req.headers.cookie) || "";
  const authed = /(?:^|;\s*)dev_auth=1(?:;|$)/.test(cookie);
  if (!authed) return null;
  return {
    identityProvider: "dev",
    userId: "local-dev-user",
    userDetails: "lucy.green@larato.co.uk",
    userRoles: ["anonymous", "authenticated"]
  };
}

module.exports = async function (context, req) {
  try {
    const path = String((context.bindingData && context.bindingData.path) || "").toLowerCase();

    if (path === "me" || path === "") {
      const clientPrincipal = principalFromCookie(req);
      context.res = {
        status: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ clientPrincipal })
      };
      return;
    }

    if (path.startsWith("login")) {
      const dest = (req.query && (req.query.post_login_redirect_url || req.query.post_login_redirect_uri)) || "/";
      const cookie = ["dev_auth=1","Path=/",`Max-Age=${ONE_DAY}`,"HttpOnly","Secure","SameSite=None"].join("; ");
      context.res = { status: 302, headers: { "Set-Cookie": cookie, "Location": dest }, body: "" };
      return;
    }

    if (path.startsWith("logout")) {
      const dest = (req.query && req.query.post_logout_redirect_uri) || "/";
      const cookie = ["dev_auth=; Path=/","Max-Age=0","HttpOnly","Secure","SameSite=None"].join("; ");
      context.res = { status: 302, headers: { "Set-Cookie": cookie, "Location": dest }, body: "" };
      return;
    }

    if (path.startsWith("refresh")) {
      const clientPrincipal = principalFromCookie(req);
      context.res = { status: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ clientPrincipal }) };
      return;
    }

    context.res = { status: 404, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: "Not found" }) };
  } catch (e) {
    context.log.error("auth error", e);
    context.res = { status: 500, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: "auth exception", detail: String(e && e.message || e) }) };
  }
};
