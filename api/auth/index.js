// /api/auth/index.js Sales Accelerator 22-10-2025 v2 (drop-in, backward compatible)
// Local dev .auth bridge for SWA emulator → Functions
// Always returns 200 with { clientPrincipal, source, correlationId } and NEVER 500s.
// Enhancements for Campaign (without breaking anything):
// - Merge roles from claims into userRoles (some setups place roles only in claims).
// - Support either DEV_BYPASS_FLAG_NAME or ALLOW_DEV_BYPASS for dev bypass.
// - Case-insensitive header handling and explicit OPTIONS handling.
// - Preserve EXACT response shape and semantics used by the SWA suite.

function readHeader(req, name) {
  // Case-insensitive header getter
  if (!req?.headers) return undefined;
  const h = req.headers;
  return h[name] ?? h[name.toLowerCase()] ?? h[name.toUpperCase()];
}

function readPrincipalFromHeader(req) {
  try {
    const b64 =
      readHeader(req, "x-ms-client-principal") ||
      readHeader(req, "X-MS-CLIENT-PRINCIPAL");
    if (!b64) return null;
    const json = Buffer.from(String(b64), "base64").toString("utf8");
    const principal = JSON.parse(json);
    return normalizePrincipalRoles(principal);
  } catch {
    return null;
  }
}

function normalizePrincipalRoles(principal) {
  // Ensure userRoles includes any roles found in claims (type/…/role or "roles"/"role").
  try {
    const roles = new Set(Array.isArray(principal?.userRoles) ? principal.userRoles : []);
    const claims = Array.isArray(principal?.claims) ? principal.claims : [];
    for (const c of claims) {
      const typ = (c?.typ || c?.type || "").toLowerCase();
      const val = c?.val ?? c?.value;
      if (!val) continue;
      if (typ.includes("/role") || typ === "roles" || typ === "role") {
        roles.add(String(val));
      }
    }
    principal.userRoles = Array.from(roles);
  } catch {
    // keep original on any parsing issue
  }
  return principal;
}

function devStubPrincipal() {
  const email = process.env.AUTH_DEV_EMAIL || "dev@example.com";
  return normalizePrincipalRoles({
    identityProvider: "dev",
    userId: "dev-user",
    userDetails: email,
    userRoles: ["anonymous", "authenticated"],
  });
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
    readHeader(req, "x-correlation-id") ||
    readHeader(req, "X-CORRELATION-ID") ||
    genId()
  );
}

// DEV bypass: active when either DEV_BYPASS_FLAG_NAME==="true" OR ALLOW_DEV_BYPASS==="true"
// AND a role header (x-dev-role) is present.
// This keeps your current behavior and adds compatibility with ALLOW_DEV_BYPASS used elsewhere.
function isDevBypassEnabled() {
  const a = String(process.env.DEV_BYPASS_FLAG_NAME || "").trim().toLowerCase() === "true";
  const b = String(process.env.ALLOW_DEV_BYPASS || "").trim().toLowerCase() === "true";
  return a || b;
}

function tryDevBypass(req) {
  const bypass = isDevBypassEnabled();
  const devRole = readHeader(req, "x-dev-role") || readHeader(req, "X-DEV-ROLE") || "";
  if (!bypass || !devRole) return null;

  const email = process.env.AUTH_DEV_EMAIL || "dev@example.com";
  return normalizePrincipalRoles({
    identityProvider: "dev-bypass",
    userId: "dev-bypass-user",
    userDetails: email,
    userRoles: ["authenticated", String(devRole)],
  });
}

module.exports = async function (context, req) {
  const correlationId = getCorrelationId(req);

  // CORS preflight handled explicitly (still always 200)
  if (String(req?.method || "").toUpperCase() === "OPTIONS") {
    context.res = {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "x-correlation-id": correlationId,
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers":
          "authorization, content-type, x-ms-client-principal, x-correlation-id, x-dev-role",
      },
      body: JSON.stringify({ clientPrincipal: null, source: "preflight", correlationId }),
    };
    return;
  }

  let clientPrincipal = null;
  let source = "none";

  // 1) Real EasyAuth header (SWA/Entra or forwarded)
  const headerPrincipal = readPrincipalFromHeader(req);
  if (headerPrincipal) {
    clientPrincipal = headerPrincipal;
    source = "x-ms-client-principal";
  }

  // 2) Dev bypass (explicit, safe): requires flag + x-dev-role
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
