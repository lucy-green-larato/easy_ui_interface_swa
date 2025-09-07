// Local dev .auth bridge for SWA emulator → Functions
// ALWAYS returns 200 with { clientPrincipal }, never 500.

function readPrincipal(req) {
  try {
    // Functions lowercases header names
    const b64 = req.headers && req.headers["x-ms-client-principal"];
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

function isTruthy(v) {
  return /^(1|true|yes)$/i.test(String(v || ""));
}

module.exports = async function (_context, req) {
  try {
    let principal = readPrincipal(req);

    // Optional forced stub when header isn’t present (e.g., direct curl)
    if (!principal && isTruthy(process.env.AUTH_DEV_ALWAYS)) {
      principal = devStub();
    }

    // Return via $return binding
    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: { clientPrincipal: principal }
    };
  } catch (_e) {
    // Still never 500: return benign payload
    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: { clientPrincipal: null, note: "auth error in local bridge" }
    };
  }
};
