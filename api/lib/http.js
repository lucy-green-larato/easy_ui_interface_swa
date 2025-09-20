'use strict';

const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, content-type, x-ms-client-principal'
};

function newCorrelationId() {
  return [...crypto.getRandomValues(new Uint8Array(16))]
    .map(b => b.toString(16).padStart(2,'0')).join('');
}

// Fall back if crypto.randomUUID not available in this worker
const uuid = () => (globalThis.crypto?.randomUUID?.() ?? newCorrelationId());

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
