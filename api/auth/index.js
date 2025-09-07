// Local dev .auth bridge for SWA emulator → Functions
// Returns { clientPrincipal } and NEVER 500s.

function readPrincipal(req) {
  try {
    const b64 = req.headers?.["x-ms-client-principal"] || req.headers?.["X-MS-CLIENT-PRINCIPAL"];
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

module.exports = async function (_context, req) {
  let principal = readPrincipal(req);
  if (!principal && process.env.AUTH_DEV_ALWAYS === "1") {
    principal = devStub();
  }
  return {
    status: 200,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ clientPrincipal: principal })
  };
};
