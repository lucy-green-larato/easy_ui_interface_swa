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
  const raw = (req.params && req.params.path) || "";
  const path = String(raw).toLowerCase();

  if (path === "me") {
    const clientPrincipal = principalFromCookie(req);
    context.res = { status: 200, headers: { "Content-Type": "application/json" }, body: { clientPrincipal } };
    return;
  }

  if (path.startsWith("login")) {
    const dest = (req.query && (req.query.post_login_redirect_url || req.query.post_login_redirect_uri)) || "/";
    const cookie = ["dev_auth=1","Path=/",`Max-Age=${ONE_DAY}`,"HttpOnly","Secure","SameSite=None"].join("; ");
    context.res = { status: 302, headers: { "Set-Cookie": cookie, "Location": dest } };
    return;
  }

  if (path.startsWith("logout")) {
    const dest = (req.query && req.query.post_logout_redirect_uri) || "/";
    const cookie = ["dev_auth=; Path=/","Max-Age=0","HttpOnly","Secure","SameSite=None"].join("; ");
    context.res = { status: 302, headers: { "Set-Cookie": cookie, "Location": dest } };
    return;
  }

  if (path.startsWith("refresh")) {
    const clientPrincipal = principalFromCookie(req);
    context.res = { status: 200, headers: { "Content-Type": "application/json" }, body: { clientPrincipal } };
    return;
  }

  context.res = { status: 404, headers: { "Content-Type": "application/json" }, body: { error: "Not found" } };
};
