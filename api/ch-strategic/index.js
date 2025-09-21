// api/ch-strategic/index.js (isolation)
'use strict';

const { createHandler } = require('azure-function-express');
const express = require('express');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.disable('x-powered-by');

// Correlation ID
app.use((req, res, next) => {
  const incoming = req.headers['x-correlation-id'];
  const cid = (typeof incoming === 'string' && incoming.trim()) ? incoming.trim() : uuidv4();
  req.correlationId = cid;
  res.setHeader('X-Correlation-Id', cid);
  next();
});

// Parsers + CORS
app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true, limit: '16kb' }));
app.use((req, res, next) => {
  res.set({
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'authorization, content-type, x-ms-client-principal, x-correlation-id'
  });
  if (req.method === 'OPTIONS') return res.status(204).end();
  next();
});

// Minimal RPM limiter
const WINDOW_MS = 60_000;
const RPM = 120;
const buckets = new Map();
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of buckets) if (now - v.ts > WINDOW_MS * 2) buckets.delete(k);
}).unref();
app.set('trust proxy', 1);
app.use((req, res, next) => {
  const now = Date.now();
  const key = `ip:${(req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown').split(',')[0].trim()}`;
  const b = buckets.get(key) || { ts: now, count: 0 };
  if (now - b.ts > WINDOW_MS) { b.ts = now; b.count = 0; }
  b.count += 1;
  buckets.set(key, b);
  if (b.count > RPM) {
    return res.status(429).json({ code: 429, message: 'Too many requests', correlationId: req.correlationId });
  }
  next();
});

// ======== SMALL-RUN ONLY ========
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 },
});

const smallRunPaths = [
  '/', '/ch-strategic', '/ch-strategic/', '/api/ch-strategic', '/api/ch-strategic/',
  /^\/ch-strategic(\/.*)?$/, /^\/api\/ch-strategic(\/.*)?$/
];

app.post(smallRunPaths, upload.single('file'), async (req, res) => {
  try {
    const ct = req.headers['content-type'] || req.headers['Content-Type'] || '';
    if (!ct.includes('multipart/form-data')) {
      return res.status(400).json({ code: 400, message: 'Expected multipart/form-data', correlationId: req.correlationId });
    }
    const evidenceRaw = (req.body && (req.body.evidence ?? req.body.evidenceTag)) ?? '';
    const evidence = (typeof evidenceRaw === 'string') ? evidenceRaw.trim() : '';
    if (!evidence) return res.status(400).json({ code: 400, message: 'Missing evidence', correlationId: req.correlationId });
    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ code: 400, message: 'Missing file', correlationId: req.correlationId });
    }

    // No `.length` anywhere — just echo back metadata
    return res.status(200).json({
      ok: true,
      mode: 'small',
      evidence,
      file: {
        fieldname: req.file.fieldname || 'file',
        originalname: req.file.originalname || 'upload.csv',
        size: typeof req.file.size === 'number' ? req.file.size : (req.file.buffer?.byteLength || undefined),
        mimetype: req.file.mimetype || 'text/csv',
      },
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

// Health
app.get(['/_health', '/ch-strategic/_health', '/api/ch-strategic/_health'], (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

// JSON 404 (never hang)
app.all('*', (req, res) => {
  res.status(404).json({
    code: 404,
    message: `No route for ${req.method} ${req.originalUrl || req.url}`,
    correlationId: req.correlationId || 'unknown',
  });
});

// Last-resort error fence
app.use((err, req, res, _next) => {
  if (res.headersSent) return;
  res.status(500).json({
    code: 500,
    message: String(err?.message || err),
    correlationId: req?.correlationId || 'unknown',
  });
});

module.exports = createHandler(app);
