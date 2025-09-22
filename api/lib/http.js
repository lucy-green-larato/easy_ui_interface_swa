/** api/lib/http.js no Express, just Azure Functions 22/09/2025 */
'use strict';

// CORS for SWA + local dev
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, content-type, x-ms-client-principal, x-correlation-id'
};
exports.CORS = CORS;

function uuid() {
  // Avoid importing crypto just for this
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = (Math.random() * 16) | 0, v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
exports.uuid = uuid;

function getCorrelationId(req) {
  const hdr = req?.headers?.['x-correlation-id'];
  return (typeof hdr === 'string' && hdr.trim()) || uuid();
}
exports.getCorrelationId = getCorrelationId;

function ok(code, body, cid) {
  return {
    status: code,
    headers: { ...CORS, 'Content-Type': 'application/json; charset=utf-8', 'X-Correlation-Id': cid },
    body
  };
}
exports.ok = ok;

function err(code, message, cid) {
  const msg = typeof message === 'string' ? message : (message?.message || 'Internal error');
  return {
    status: code,
    headers: { ...CORS, 'Content-Type': 'application/json; charset=utf-8', 'X-Correlation-Id': cid },
    body: { ok: false, code, message: msg }
  };
}
exports.err = err;

function preflight(req) {
  return (req?.method || '').toUpperCase() === 'OPTIONS';
}
exports.preflight = preflight;
