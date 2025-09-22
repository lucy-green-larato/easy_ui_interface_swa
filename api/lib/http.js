// api/lib/http.js
'use strict';

/**
 * HTTP Utilities for Azure Functions + Express
 * --------------------------------------------
 * Exports:
 *  - cors                      : default CORS header set (object)
 *  - preflight(req, res)       : handle OPTIONS, returns true if responded
 *  - uuid()                    : RFC-like random id (uses crypto.randomUUID when available)
 *  - setCors(res)              : apply default CORS headers to a response
 *  - attachCommon(app, opts)   : apply CORS + correlationId + parsers to an Express app
 *  - correlationMiddleware()   : only correlation-id middleware
 *  - ok(res, body, code, cid, extraHeaders)
 *  - error(res, err, code, cid, extraHeaders)
 *  - sendCsv(res, csv, filename, cid)
 *  - sendStream(res, stream, { contentType, filename, size }, cid)
 *  - wrapAsync(handler)        : async route wrapper -> forwards errors
 *  - getPrincipal(req)         : decode SWA x-ms-client-principal
 *  - requireRole(...roles)     : role gate middleware
 *  - validateEvidenceTag(s)    : throws 400 on invalid tag
 *  - notFound(req, res)        : 404 helper
 *  - pick(obj, keys)           : small utility
 */

const { webcrypto } = require('crypto');
const crypto = globalThis.crypto ?? webcrypto;

// ---------------------------
// CORS + response primitives
// ---------------------------
const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': [
    'authorization',
    'content-type',
    'x-ms-client-principal',
    'x-correlation-id'
  ].join(', '),
};

function preflight(req, res) {
  if (req.method !== 'OPTIONS') return false;
  setCors(res);
  // If the request specified a specific origin/method/headers, echo them if needed
  if (req.headers.origin) res.setHeader('Access-Control-Allow-Origin', req.headers.origin);
  res.status(204).end();
  return true;
}

function setCors(res, extra = {}) {
  Object.entries({ ...cors, ...extra }).forEach(([k, v]) => res.setHeader(k, v));
}

// ---------------------------
// IDs, errors, wrappers
// ---------------------------
function uuid() {
  return typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, c =>
        (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16));
}

function normalizeError(e) {
  if (!e) return { message: 'Internal error', code: 500 };
  if (typeof e === 'string') return { message: e, code: 500 };
  const code = Number(e.statusCode || e.status || e.code || 500);
  const message = e.message || 'Internal error';
  const details = e.details;
  return { message, code: isNaN(code) ? 500 : code, details };
}

function ok(res, body = {}, code = 200, cid, extraHeaders = {}) {
  const headers = {
    ...cors,
    'Content-Type': 'application/json; charset=utf-8',
    ...extraHeaders
  };
  if (cid) headers['X-Correlation-Id'] = cid;
  res.status(code).set(headers).end(JSON.stringify(body));
}

function error(res, e, code, cid, extraHeaders = {}) {
  const n = normalizeError(e);
  const status = code ? Number(code) : n.code;
  const headers = {
    ...cors,
    'Content-Type': 'application/json; charset=utf-8',
    ...extraHeaders
  };
  if (cid) headers['X-Correlation-Id'] = cid;
  const payload = { ok: false, code: status, message: n.message };
  if (n.details !== undefined) payload.details = n.details;
  res.status(status).set(headers).end(JSON.stringify(payload));
}

/**
 * Send CSV payload (string or Buffer) with a suggested filename.
 */
function sendCsv(res, csv, filename = 'export.csv', cid) {
  const buf = Buffer.isBuffer(csv) ? csv : Buffer.from(String(csv ?? ''), 'utf8');
  const headers = {
    ...cors,
    'Content-Type': 'text/csv; charset=utf-8',
    'Content-Disposition': `attachment; filename="${filename}"`,
    'Content-Length': String(buf.length),
  };
  if (cid) headers['X-Correlation-Id'] = cid;
  res.status(200).set(headers).end(buf);
}

/**
 * Stream sender for large payloads.
 * opts: { contentType?: string, filename?: string, size?: number }
 */
function sendStream(res, stream, opts = {}, cid) {
  const headers = {
    ...cors,
    'Content-Type': opts.contentType || 'application/octet-stream',
  };
  if (opts.filename) headers['Content-Disposition'] = `attachment; filename="${opts.filename}"`;
  if (opts.size != null) headers['Content-Length'] = String(opts.size);
  if (cid) headers['X-Correlation-Id'] = cid;
  res.status(200).set(headers);
  stream.pipe(res);
}

/**
 * Async route wrapper: wrapAsync((req,res)=>{...})
 * Forwards thrown/rejected errors to Express .use(err) handler or returns JSON error.
 */
function wrapAsync(fn) {
  return function wrapped(req, res, next) {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

// ---------------------------
// Common middlewares
// ---------------------------
/**
 * Adds:
 *  - CORS headers for all responses
 *  - OPTIONS preflight short-circuit
 *  - Correlation ID on request + response header
 *  - JSON/urlencoded parsers with safe defaults (if express provided)
 */
function attachCommon(app, { jsonLimit = '2mb', urlencLimit = '64kb' } = {}) {
  if (!app || typeof app.use !== 'function') {
    throw new Error('attachCommon requires an Express app instance');
  }

  // Preflight first
  app.use((req, res, next) => { if (preflight(req, res)) return; next(); });

  // Always set CORS
  app.use((req, res, next) => { setCors(res); next(); });

  // Correlation IDs
  app.use(correlationMiddleware());

  // Body parsers (only if express.json/urlencoded exist)
  if (typeof require('express').json === 'function') {
    const express = require('express');
    app.use(express.json({ limit: jsonLimit }));
    app.use(express.urlencoded({ extended: true, limit: urlencLimit }));
  }
}

/**
 * Only correlation-id injection (useful if you already set up parsers elsewhere).
 */
function correlationMiddleware() {
  return (req, res, next) => {
    const existing = typeof req.headers['x-correlation-id'] === 'string'
      ? req.headers['x-correlation-id'].trim()
      : null;
    const cid = existing || uuid();
    req.correlationId = cid;
    res.setHeader('X-Correlation-Id', cid);
    next();
  };
}

// ---------------------------
// SWA auth helpers
// ---------------------------
function getPrincipal(req) {
  try {
    const raw = req.headers['x-ms-client-principal'];
    if (!raw) return null;
    const json = Buffer.from(raw, 'base64').toString('utf8');
    return JSON.parse(json);
  } catch {
    return null;
  }
}

/**
 * Example: app.post('/admin', requireRole('admin', 'superuser'), handler)
 */
function requireRole(...roles) {
  const required = new Set(roles.filter(Boolean));
  return (req, res, next) => {
    if (required.size === 0) return next();
    const p = getPrincipal(req);
    const userRoles = Array.isArray(p?.userRoles) ? p.userRoles : [];
    const allowed = userRoles.some(r => required.has(r));
    if (!allowed) return error(res, 'Forbidden', 403, req.correlationId);
    next();
  };
}

// ---------------------------
// Input helpers & misc
// ---------------------------
function validateEvidenceTag(tag, { required = false } = {}) {
  const s = (tag ?? '').toString().trim();
  if (!s && !required) return null;
  if (!/^[A-Za-z0-9 _-]{1,50}$/.test(s)) {
    const e = new Error('Invalid evidenceTag'); e.statusCode = 400; throw e;
  }
  return s;
}

function notFound(req, res) {
  return error(res, `Not found: ${req.method} ${req.path}`, 404, req.correlationId);
}

function pick(obj, keys) {
  const out = {};
  for (const k of keys) if (Object.prototype.hasOwnProperty.call(obj, k)) out[k] = obj[k];
  return out;
}

// ---------------------------
// Exports
// ---------------------------
module.exports = {
  cors,
  preflight,
  setCors,
  uuid,
  ok,
  error,
  sendCsv,
  sendStream,
  wrapAsync,
  attachCommon,
  correlationMiddleware,
  getPrincipal,
  requireRole,
  validateEvidenceTag,
  notFound,
  pick,
};
