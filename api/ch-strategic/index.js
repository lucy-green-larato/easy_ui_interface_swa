'use strict';

const { createHandler } = require('azure-function-express');
const express = require('express');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');

const chStrategic = require('../generate/kinds/ch-strategic');
const { error } = require('../lib/http'); // keep this if ../lib/http exists

const app = express();
app.disable('x-powered-by');

// ---------- Correlation ID on every request ----------
app.use((req, res, next) => {
  const incoming = req.headers['x-correlation-id'];
  const cid = (typeof incoming === 'string' && incoming.trim()) ? incoming.trim() : uuidv4();
  req.correlationId = cid;
  res.setHeader('X-Correlation-Id', cid);
  next();
});

// ---------- Body parsing ----------
app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true, limit: '16kb' }));

// ---------- CORS ----------
app.use((req, res, next) => {
  res.set({
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'authorization, content-type, x-ms-client-principal, x-correlation-id'
  });
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

// ---------- Principal extraction (inline) ----------
function getPrincipal(req) {
  try {
    const b64 = req.headers['x-ms-client-principal'];
    if (!b64) return null;
    const json = Buffer.from(b64, 'base64').toString('utf8');
    return JSON.parse(json);
  } catch {
    return null;
  }
}

// ---------- Global rate limiter ----------
const RPM = parseInt(process.env.CH_STRATEGIC_RPM || '60', 10);
const WINDOW_MS = 60_000;
const buckets = new Map();

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of buckets) if (now - v.ts > WINDOW_MS * 2) buckets.delete(k);
}).unref();

function rateLimit(req, res, next) {
  const now = Date.now();
  const p = getPrincipal(req);
  const key = p?.userId
    ? `p:${p.userId}`
    : `ip:${(req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown').split(',')[0].trim()}`;
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

// ---------- Defensive small-run POST (multipart) ----------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }, // 20MB
});

// Accepts: multipart/form-data with fields: file (csv), evidence (string)
const smallRunPaths = [
  '/',                          // some hosts strip the route prefix
  '/ch-strategic',              // direct route
  '/ch-strategic/',             // trailing slash
  '/api/ch-strategic',          // SWA/Functions keeps /api prefix
  '/api/ch-strategic/',         // trailing slash with /api
  /^\/ch-strategic(\/.*)?$/,    // any subpath under ch-strategic
  /^\/api\/ch-strategic(\/.*)?$/ // any subpath under /api/ch-strategic
];

app.post(smallRunPaths, upload.single('file'), async (req, res) => {
  try {
    const ctype = req.headers['content-type'] || req.headers['Content-Type'] || '';
    if (!ctype.includes('multipart/form-data')) {
      return res.status(400).json({
        code: 400,
        message: 'Expected multipart/form-data',
        correlationId: req.correlationId,
      });
    }

    // Inputs
    const evidenceRaw = (req.body && (req.body.evidence ?? req.body.evidenceTag)) ?? '';
    const evidence = (typeof evidenceRaw === 'string') ? evidenceRaw.trim() : '';
    if (!evidence) {
      return res.status(400).json({
        code: 400,
        message: 'Missing evidence',
        correlationId: req.correlationId,
      });
    }
    if (!req.file || !req.file.buffer) {
      return res.status(400).json({
        code: 400,
        message: 'Missing file',
        correlationId: req.correlationId,
      });
    }

    // Don’t compute rows at all (avoid any `.length` paths)
    // Just prove end-to-end flow and return a stub.
    return res.status(200).json({
      ok: true,
      mode: 'small',
      evidence,
      // echo basic file metadata safely
      file: {
        fieldname: req.file.fieldname || 'file',
        originalname: req.file.originalname || 'upload.csv',
        mimetype: req.file.mimetype || 'text/csv'
      },
      correlationId: req.correlationId,
    });
  } catch (err) {
    return res.status(500).json({
      code: 500,
      message: String(err && err.message || err),
      correlationId: req.correlationId,
    });
  }
});

const evidence = (req.body?.evidence || '').trim();
if (!evidence) {
  return res.status(400).json({
    code: 400,
    message: 'Missing evidence',
    correlationId: req.correlationId,
  });
}
if (!req.file) {
  return res.status(400).json({
    code: 400,
    message: 'Missing file',
    correlationId: req.correlationId,
  });
}

let csvText = '';
try { csvText = req.file.buffer?.toString('utf8') ?? ''; } catch { }
const lines = csvText ? csvText.split(/\r?\n/) : [];
const rowCount = Math.max(0, lines.length - 1);

// TEMP stub — replace with real chStrategic.smallRun(...) later
return res.status(200).json({
  ok: true,
  mode: 'small',
  evidence,
  rows: rowCount,
  correlationId: req.correlationId,
});
  } catch (err) {
  return res.status(500).json({
    code: 500,
    message: String(err?.message || err),
    correlationId: req.correlationId,
  });
}
});

// ---------- Health ----------
app.get(['/_health', '/ch-strategic/_health', '/api/ch-strategic/_health'], (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

// ---------- Final JSON 404 so we never hang ----------
app.all('*', (req, res) => {
  res.status(404).json({
    code: 404,
    message: `No route for ${req.method} ${req.originalUrl || req.url}`,
    correlationId: req.correlationId || 'unknown',
  });
});

// Health (optional)
app.get(['/_health', '/ch-strategic/_health'], (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

// ---------- Final JSON 404 so we never hang ----------
app.all('*', (req, res) => {
  res.status(404).json({
    code: 404,
    message: `No route for ${req.method} ${req.originalUrl || req.url}`,
    correlationId: req.correlationId || 'unknown',
  });
});

app.post('/', upload.single('file'), async (req, res) => {
  try {
    // Correlation already set by middleware
    // Basic header sanity
    const ctype = req.headers['content-type'] || req.headers['Content-Type'] || '';
    if (!ctype.includes('multipart/form-data')) {
      return res.status(400).json({
        code: 400,
        message: 'Expected multipart/form-data',
        correlationId: req.correlationId,
      });
    }

    // Validate inputs
    const evidence = (req.body?.evidence || '').trim();
    if (!evidence) {
      return res.status(400).json({
        code: 400,
        message: 'Missing evidence',
        correlationId: req.correlationId,
      });
    }
    if (!req.file) {
      return res.status(400).json({
        code: 400,
        message: 'Missing file',
        correlationId: req.correlationId,
      });
    }

    // Light CSV sanity (don’t crash if text decode fails)
    // Robust CSV row counting (never throws)
    let rowCount = 0;
    try {
      const buf = req.file && req.file.buffer;
      if (buf && typeof buf.toString === 'function') {
        const csvText = buf.toString('utf8') || '';
        if (csvText) {
          const lines = csvText.split(/\r?\n/);
          // ignore empty lines and header-only
          const nonEmpty = Array.isArray(lines) ? lines.filter(l => l && l.trim().length > 0) : [];
          rowCount = Math.max(0, nonEmpty.length - 1);
        }
      }
    } catch { rowCount = 0; }

    // TODO: replace with your real small-run implementation, e.g.:
    // const result = await chStrategic.smallRun({ csv: req.file.buffer, evidence, cid: req.correlationId });
    // return res.status(200).json({ ...result, correlationId: req.correlationId });

    // Temporary success stub so the UI can proceed
    return res.status(200).json({
      ok: true,
      mode: 'small',
      evidence,
      rows: rowCount,
      correlationId: req.correlationId,
    });
  } catch (err) {
    return res.status(500).json({
      code: 500,
      message: String(err?.message || err),
      correlationId: req.correlationId,
    });
  }
});

// ---------- Health (keeps UI check happy) ----------
app.get('/_health', (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

// ---------- Final JSON error fence (no blank 500s) ----------
app.use((err, req, res, _next) => {
  // Ensure we never emit an empty 500
  try { console.error('[api/ch-strategic] unhandled', err); } catch { }
  res.status(500).json({
    code: 500,
    message: String(err?.message || err),
    correlationId: req?.correlationId || 'unknown',
  });
});

// ---------- Auth gate for /ch-strategic ----------
app.use('/ch-strategic', (req, res, next) => {
  const principal = getPrincipal(req);
  if (!principal) {
    return error
      ? error(res, 401, 'Unauthenticated', undefined, req.correlationId)
      : res.status(401).json({ code: 401, message: 'Unauthenticated', correlationId: req.correlationId });
  }
  req.principal = principal;
  next();
});

// ---------- Role guard ----------
function requireRole(role) {
  return (req, res, next) => {
    const roles = req.principal?.userRoles || [];
    if (!roles.includes(role)) {
      return error
        ? error(res, 403, `Missing required role: ${role}`, undefined, req.correlationId)
        : res.status(403).json({ code: 403, message: `Missing required role: ${role}`, correlationId: req.correlationId });
    }
    next();
  };
}

// ---------- Feedback RPM limiter ----------
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
app.use('/ch-strategic/feedback', limitFeedback);

// ---------- Optional allow-list for PBI export ----------
const allowPbi = (() => {
  try { return JSON.parse(process.env.CH_STRATEGIC_PBI_ALLOW || '{}'); } catch { return {}; }
})();

// ---------- Mount feature module ----------
chStrategic.mount(app, { multer, allowPbi });

// ---------- Optional PBI export route ----------
app.post('/ch-strategic/pbi-export', requireRole('pbi-exporter'), async (req, res) => {
  try {
    // TODO: implement Power BI export using allowPbi
    res.json({ ok: true, cid: req.correlationId });
  } catch (err) {
    const status = err?.status || 500;
    const msg = err?.message || 'Internal error';
    return error
      ? error(res, status, msg, undefined, req.correlationId)
      : res.status(status).json({ code: status, message: msg, correlationId: req.correlationId });
  }
});

// ---------- Health ----------
app.get('/_health', (req, res) => res.status(200).json({ ok: true }));

// ---------- Safety net ----------
app.use((err, req, res, next) => {
  if (res.headersSent) return;
  const status = err?.status || 500;
  res.status(status).json({
    code: status,
    message: err?.message || 'Internal error',
    correlationId: req.correlationId
  });
});

module.exports = createHandler(app);
