/** api/lib/http.js September 22 2025 v1  */
'use strict';

const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, content-type, x-ms-client-principal, x-correlation-id'
};

const { webcrypto } = require('crypto');
const _crypto = globalThis.crypto ?? webcrypto;

function preflight(req, res) {
  if (req.method !== 'OPTIONS') return false;
  res.set({
    ...cors,
    'Access-Control-Max-Age': '86400'
  }).status(204).end();
  return true;
}
module.exports = { cors, ok, error, uuid, preflight };

function getPrincipal(req) {
  try {
    const b64 = req.headers['x-ms-client-principal'];
    if (!b64) return null;
    return JSON.parse(Buffer.from(b64, 'base64').toString('utf8'));
  } catch { return null; }
}
module.exports = { cors, ok, error, uuid, preflight, getPrincipal };

function newCorrelationId() {
  return [..._crypto.getRandomValues(new Uint8Array(16))]
    .map(b => b.toString(16).padStart(2,'0')).join('');
}

// Fall back if crypto.randomUUID not available in this worker
const uuid = () => (_crypto?.randomUUID?.() ?? newCorrelationId());

function error(res, code, message, details, cid) {
  const payload = { code, message, correlationId: cid || uuid() };
  if (details) payload.details = details;
  res.status(code).set({ ...cors, 'Content-Type': 'application/json' }).end(JSON.stringify(payload));
  return payload.correlationId;
}

function ok(res, code, body, cid) {
  const headers = { ...cors, 'Content-Type': 'application/json' };
  if (cid) headers['X-Correlation-Id'] = cid;
  res.status(code).set(headers).end(JSON.stringify(body ?? {}));
}

module.exports = { cors, ok, error, uuid };
