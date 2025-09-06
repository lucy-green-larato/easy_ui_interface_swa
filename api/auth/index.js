// Local dev .auth bridge for SWA emulator → Functions
// Returns { clientPrincipal } and NEVER 500s.

function readPrincipal(req) {
  try {
    const b64 = req.headers?.["x-ms-client-principal"];
    if (!b64) return null;
    const json = Buffer.from(b64, "base64").toString("utf8");
    return JSON.parse(json);
  } catch {
    return null;
  }
}

function devStub() {
  const email = process.env.AUTH_DEV_EMAIL || "dev@example.com";
  return {
    identityProvider: "dev",
    userId: "dev-user",
    userDetails: email,
    userRoles: ["anonymous", "authenticated"]
  };
}

module.exports = async function (context, req) {
  try {
    // SWA CLI injects x-ms-client-principal for authenticated users
    let principal = readPrincipal(req);

    // Optional forced stub when header isn’t present (e.g., direct curl)
    if (!principal && process.env.AUTH_DEV_ALWAYS === "1") {
      principal = devStub();
    }

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ clientPrincipal: principal })
    };
  } catch (e) {
    context.log.warn("auth bridge error", e);
    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ clientPrincipal: null, note: "auth error in local bridge" })
    };
  }
};
