'use strict';

const { createHandler } = require('azure-function-express');
const express = require('express');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');

const chStrategic = require('../generate/kinds/ch-strategic');
const { error } = require('../lib/http');
const { getPrincipal } = require('../lib/auth'); // SWA principal helper

const app = express();
app.disable('x-powered-by');

// ---------- Global middleware ----------

// Correlation ID on every request (and response)
app.use((req, res, next) => {
  const incoming = req.headers['x-correlation-id'];
  const cid = (typeof incoming === 'string' && incoming.trim()) ? incoming.trim() : uuidv4();
  req.correlationId = cid;
  res.setHeader('X-Correlation-Id', cid);
  next();
});

// Parse bodies with sensible limits
app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true, limit: '16kb' }));

// CORS for SWA front end + Functions API
app.use((req, res, next) => {
  res.set({
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'authorization, content-type, x-ms-client-principal, x-correlation-id'
  });
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

// ---------- Config ----------

// OPTIONAL: strict allow-lists for future /pbi-export usage
// Provide as JSON in env CH_STRATEGIC_PBI_ALLOW, e.g.:
// {"workspaces":["..."],"reports":["..."],"visuals":["..."]}
const allowPbi = (() => {
  try {
    return JSON.parse(process.env.CH_STRATEGIC_PBI_ALLOW || '{}');
  } catch {
    return {};
  }
})();

// ---------- Polished in-memory rate limiter (global) ----------

const RPM = parseInt(process.env.CH_STRATEGIC_RPM || '60', 10);
const WINDOW_MS = 60_000;
const buckets = new Map();

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of buckets) if (now - v.ts > WINDOW_MS * 2) buckets.delete(k);
}, WINDOW_MS).unref();

function rateLimit(req, res, next) {
  const now = Date.now();
  // Use principal if present, otherwise IP
  const p = getPrincipal(req);
  const key = p?.userId ? `p:${p.userId}` : `ip:${(req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown').split(',')[0].trim()}`;
  const b = buckets.get(key) || { ts: now, count: 0 };
  if (now - b.ts > WINDOW_MS) { b.ts = now; b.count = 0; }
  b.count += 1;
  buckets.set(key, b);
  if (b.count > RPM) {
    return res.status(429).json({
      code: 429,
      message: 'Too many requests, please slow down.',
      correlationId: req.correlationId
    });
  }
  next();
}

app.use(rateLimit);

// ---------- Auth gate for ch-strategic routes ----------

app.use('/ch-strategic', (req, res, next) => {
  const principal = getPrincipal(req);
  if (!principal) {
    return error(res, 401, 'Unauthenticated', undefined, req.correlationId);
  }
  req.principal = principal;
  next();
});

// Role guard helper (uses req.principal set above)
function requireRole(role) {
  return (req, res, next) => {
    const roles = req.principal?.userRoles || [];
    if (!roles.includes(role)) {
      return error(res, 403, `Missing required role: ${role}`, undefined, req.correlationId);
    }
    next();
  };
}

// ---------- Feedback RPM limiter (per minute) ----------

const feedbackCounts = new Map();
const FEEDBACK_RPM = parseInt(process.env.CH_STRATEGIC_FEEDBACK_RPM || '5', 10);

function actorKey(req) {
  const p = req.principal;
  if (p?.userId) return `p:${p.userId}`;
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  return `ip:${ip || 'unknown'}`;
}

setInterval(() => { feedbackCounts.clear(); }, 60_000).unref();

function limitFeedback(req, res, next) {
  const k = actorKey(req);
  const n = (feedbackCounts.get(k) || 0) + 1;
  feedbackCounts.set(k, n);
  if (n > FEEDBACK_RPM) {
    return res.status(429).json({
      code: 429,
      message: 'Too many feedback submissions. Please wait a minute.',
      correlationId: req.correlationId
    });
  }
  next();
}

// Apply feedback limiter specifically to feedback endpoint path
app.use('/ch-strategic/feedback', limitFeedback);

// ---------- Mount ch-strategic feature module ----------
// IMPORTANT: pass the multer **module**, not a preconfigured instance.
chStrategic.mount(app, { multer, allowPbi });

// ---------- Optional PBI export (role-gated) ----------
app.post('/ch-strategic/pbi-export', requireRole('pbi-exporter'), async (req, res) => {
  try {
    // TODO: your Power BI export logic (respect allowPbi)
    res.json({ ok: true, cid: req.correlationId });
  } catch (err) {
    return error(res, err.status || 500, err.message || 'Internal error', undefined, req.correlationId);
  }
});

// ---------- Health ----------

app.get('/_health', (req, res) => {
  res.status(200).json({ ok: true });
});

// One-response-only safety net
app.use((err, req, res, next) => {
  if (res.headersSent) return;
  res.status(err?.status || 500).json({
    code: err?.status || 500,
    message: err?.message || 'Internal error',
    correlationId: req.correlationId
  });
});

module.exports = createHandler(app);
