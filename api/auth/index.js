// api/auth/index.js
// Local dev .auth bridge for SWA emulator → Functions
// Always returns 200 with { clientPrincipal, source, correlationId } and NEVER 500s.

function readPrincipalFromHeader(req) {
  try {
    const b64 =
      req.headers?.["x-ms-client-principal"] ||
      req.headers?.["X-MS-CLIENT-PRINCIPAL"];
    if (!b64) return null;
    const json = Buffer.from(b64, "base64").toString("utf8");
    return JSON.parse(json);
  } catch {
    return null;
  }
}

function devStubPrincipal() {
  const email = process.env.AUTH_DEV_EMAIL || "dev@example.com";
  return {
    identityProvider: "dev",
    userId: "dev-user",
    userDetails: email,
    userRoles: ["anonymous", "authenticated"],
  };
}

// Very small uuid-ish fallback (no deps)
function genId() {
  const s = () =>
    Math.floor((1 + Math.random()) * 0x10000)
      .toString(16)
      .slice(1);
  return `${s()}${s()}-${s()}-${s()}-${s()}-${s()}${s()}${s()}`;
}

function getCorrelationId(req) {
  return (
    req.headers?.["x-correlation-id"] ||
    req.headers?.["X-CORRELATION-ID"] ||
    genId()
  );
}

// DEV bypass: only active when DEV_BYPASS_FLAG_NAME === "true" AND a role header is present
function tryDevBypass(req) {
  const bypass =
    String(process.env.DEV_BYPASS_FLAG_NAME || "")
      .trim()
      .toLowerCase() === "true";
  const devRole =
    req.headers?.["x-dev-role"] || req.headers?.["X-DEV-ROLE"] || "";
  if (!bypass || !devRole) return null;

  const email = process.env.AUTH_DEV_EMAIL || "dev@example.com";
  return {
    identityProvider: "dev-bypass",
    userId: "dev-bypass-user",
    userDetails: email,
    userRoles: ["authenticated", String(devRole)],
  };
}

module.exports = async function (context, req) {
  const correlationId = getCorrelationId(req);

  let clientPrincipal = null;
  let source = "none";

  // 1) Real EasyAuth header (SWA/Entra or forwarded)
  const headerPrincipal = readPrincipalFromHeader(req);
  if (headerPrincipal) {
    clientPrincipal = headerPrincipal;
    source = "x-ms-client-principal";
  }

  // 2) Dev bypass (explicit, safe): requires flag + X-DEV-ROLE
  if (!clientPrincipal) {
    const bypassPrincipal = tryDevBypass(req);
    if (bypassPrincipal) {
      clientPrincipal = bypassPrincipal;
      source = "dev-bypass";
    }
  }

  // 3) Legacy dev stub (your original behaviour): AUTH_DEV_ALWAYS=1
  if (!clientPrincipal && process.env.AUTH_DEV_ALWAYS === "1") {
    clientPrincipal = devStubPrincipal();
    source = "dev-stub";
  }

  // 4) Response (always 200)
  context.res = {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "x-correlation-id": correlationId,
      // CORS: relax for local tools; SWA/Functions can tighten in prod
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers":
        "authorization, content-type, x-ms-client-principal, x-correlation-id, x-dev-role",
    },
    body: JSON.stringify({
      clientPrincipal,
      source,
      correlationId,
    }),
  };
};
