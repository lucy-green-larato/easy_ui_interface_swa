// /api/lib/auth.js 26-09-2025 v1
// Reusable auth guard for Azure Functions (Node 20) behind SWA EasyAuth.
//
// Supported call styles (backwards-compatible):
//  A) STRICT (throws on failure):  requireAuth(context, req, allowedRoles) -> principal
//  B) NON-THROWING (gate result):  requireAuth(req, allowedRoles) -> { ok, principal?, code?, error?, message? }
// Also exported: authorize(req, allowedRoles)  // same as Style B
//
// Dev bypass: set process.env.DEV_BYPASS_FLAG_NAME = "true"
// Optionally send x-dev-role: "campaign,campaign-admin" to simulate roles.
//
// Exports:
// - requireAuth
// - authorize
// - devBypass
// - ensureCorrelationId
// - mapStatusToCode
// - authResponse                 <-- NEW
// - respondIfNotAuthorized       <-- NEW

"use strict";

const crypto = require("crypto");

// ---------- Principal decoding & role normalization ----------

function decodePrincipal(req) {
  const hdr =
    req.headers?.["x-ms-client-principal"] ||
    req.headers?.["X-MS-CLIENT-PRINCIPAL"];
  if (!hdr) return null;

  try {
    const json = Buffer.from(hdr, "base64").toString("utf8");
    const principal = JSON.parse(json);

    // Collect roles from userRoles and claims
    const roleSet = new Set();

    if (Array.isArray(principal.userRoles)) {
      principal.userRoles.forEach((r) => r && roleSet.add(String(r).toLowerCase()));
    }

    if (Array.isArray(principal.claims)) {
      principal.claims.forEach((c) => {
        const typ = String(c.typ || c.type || "").toLowerCase();
        if (typ.endsWith("/role") || typ === "roles" || typ.endsWith(":role")) {
          const val = c.val ?? c.value;
          if (Array.isArray(val)) {
            val.forEach((v) => v && roleSet.add(String(v).toLowerCase()));
          } else if (typeof val === "string") {
            val
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
              .forEach((v) => roleSet.add(v.toLowerCase()));
          }
        }
      });
    }

    return {
      ...principal,
      userId:
        principal.userId ||
        principal.userDetails ||
        principal.nameid ||
        principal.sub ||
        "unknown",
      userRoles: Array.from(roleSet),
      name:
        principal.name ||
        principal.userPrincipalName ||
        principal.userDetails ||
        "unknown",
    };
  } catch {
    return null;
  }
}

function hasAnyRole(userRoles = [], allowedRoles = []) {
  if (!allowedRoles || allowedRoles.length === 0) return true;
  const set = new Set((userRoles || []).map((r) => String(r).toLowerCase()));
  return allowedRoles.some((r) => set.has(String(r).toLowerCase()));
}

// ---------- Envelopes & utilities ----------

function envelope(status, code, message, details = {}) {
  return { status, body: { error: code, message, details } };
}

function mapStatusToCode(status) {
  switch (status) {
    case 400: return "bad_request";
    case 401: return "unauthenticated";
    case 403: return "forbidden";
    case 413: return "payload_too_large";
    case 415: return "unsupported_media_type";
    case 429: return "rate_limited";
    default: return "internal";
  }
}

// Correlation ID: echo if present & sane, else generate UUID
function ensureCorrelationId(req) {
  const cid =
    req.headers?.["x-correlation-id"] ||
    req.headers?.["X-Correlation-Id"];
  if (cid && typeof cid === "string" && cid.length <= 128) return cid;
  return crypto.randomUUID();
}

// ---------- Dev bypass ----------
// If process.env.DEV_BYPASS_FLAG_NAME === "true", synthesize a principal using x-dev-role header(s)
function devBypass(req, defaultRoles = []) {
  const enabled =
    String(process.env.DEV_BYPASS_FLAG_NAME || "").toLowerCase() === "true";
  if (!enabled) return null;

  const roleHeader = req.headers?.["x-dev-role"] || req.headers?.["X-DEV-ROLE"];
  const roles = roleHeader
    ? String(roleHeader)
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean)
    : (defaultRoles || []).map((r) => String(r).toLowerCase());

  return {
    userId: "dev-bypass",
    name: "Developer Bypass",
    userRoles: roles,
    claims: [{ typ: "roles", val: roles }],
  };
}

// ---------- Authorization (non-throwing) ----------

function authorize(req, allowedRoles = []) {
  // Dev bypass first
  const bypass = devBypass(req, allowedRoles);
  const principal = bypass || decodePrincipal(req);

  if (!principal) {
    return {
      ok: false,
      code: 401,
      error: "unauthenticated",
      message:
        "Missing or invalid identity. Ensure SWA auth is enabled and x-ms-client-principal is forwarded.",
    };
  }
  if (!hasAnyRole(principal.userRoles, allowedRoles)) {
    return {
      ok: false,
      code: 403,
      error: "forbidden",
      message: "You do not have permission to perform this action.",
      principal,
    };
  }
  return { ok: true, principal };
}

// ---------- requireAuth (dual-mode, backwards compatible) ----------
// - requireAuth(context, req, allowedRoles)  => STRICT (throws envelope on failure)
// - requireAuth(req, allowedRoles)           => NON-THROWING (returns gate result)

function requireAuth(a, b, c) {
  // Style B: (req, allowedRoles)
  if (a && a.method && !b?.method) {
    return authorize(a, b);
  }
  // Style A: (context, req, allowedRoles)
  const context = a;
  const req = b;
  const allowedRoles = c || [];
  const gate = authorize(req, allowedRoles);
  if (!gate.ok) {
    // Provide details to caller in strict mode
    const details = {
      required: allowedRoles,
      actual: gate.principal?.userRoles || [],
    };
    throw envelope(gate.code, gate.error, gate.message, details);
  }
  return gate.principal;
}

// ---------- NEW: HTTP helpers to avoid 500s on auth failures ----------

function authResponse(status, code, message, correlationId, extraHeaders = {}, details = {}) {
  return {
    status,
    headers: {
      "Content-Type": "application/json",
      "x-correlation-id": correlationId,
      ...extraHeaders,
    },
    body: { error: code, message, correlationId, details },
  };
}

/**
 * Non-throwing guard that **sets context.res** on failure and returns false.
 * Use at the top of your handler:
 *   if (!respondIfNotAuthorized(context, req, allowed, cid, CORS)) return;
 */
function respondIfNotAuthorized(context, req, allowedRoles = [], correlationId, extraHeaders = {}) {
  const gate = authorize(req, allowedRoles);
  if (gate.ok) return true;
  const res = authResponse(
    gate.code,
    gate.error,
    gate.message,
    correlationId,
    extraHeaders,
    { required: allowedRoles, actual: gate.principal?.userRoles || [] }
  );
  context.res = res;
  return false;
}

function requireRoleAdapter(req, correlationId, extraHeaders = {}) {
  // Read allowed roles from env (same list you use elsewhere)
  const allowed = JSON.parse(process.env.ALLOWED_ROLES_CHS || '["campaign","campaign-admin","sales-admin"]');

  const gate = authorize(req, allowed);
  if (gate.ok) return null;  // authorized

  // Return the shape your handler sets directly on context.res
  return {
    status: gate.code,                  // 401 or 403
    headers: {
      "Content-Type": "application/json",
      "x-correlation-id": correlationId,
      ...extraHeaders,
    },
    jsonBody: {                         // use jsonBody to match your other helpers
      error: gate.error,                // "unauthenticated" | "forbidden"
      message: gate.message,
      correlationId,
      details: { required: allowed, actual: gate.principal?.userRoles || [] }
    }
  };
}

module.exports = {
  // Main API
  requireAuth,
  authorize,
  devBypass,
  ensureCorrelationId,
  mapStatusToCode,

  // NEW exports
  authResponse,
  respondIfNotAuthorized,
  requireRole: requireRoleAdapter 
};
