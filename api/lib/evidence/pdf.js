// api/lib/evidence/pdf.js 24-10-2025 v2
// Extracts text from uploaded PDFs (assumes user OCR’d when needed).
// Reads runtime config from Function App environment variables with sensible defaults.
//
// Config (all optional):
//   PDF_MAX_TEXT_PER_FILE          (int, default 14000)     // max chars kept per file
//   PDF_MAX_FILES                  (int, default 2)         // max PDFs to parse per run
//   PDF_PARSE_TIMEOUT_MS           (int, default 15000)     // per-file parse timeout
//   PDF_CHILD_CONCURRENCY          (int, default = PDF_MAX_FILES, >=1)
//   PDF_MIN_TEXT_FOR_NOT_SCANNED   (int, default 1000)      // below this → scannedLikely=true
//   PDF_STRIP_NULLS                (bool "1"/"true", default true)

const pdfParse = require("pdf-parse");

/* -------------------- env + defaults -------------------- */
function envInt(name, def) {
  const v = process.env[name];
  if (v == null || v === "") return def;
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : def;
}
function envBool(name, def) {
  const v = (process.env[name] || "").toString().trim().toLowerCase();
  if (!v) return def;
  return v === "1" || v === "true" || v === "yes";
}

const CONFIG = {
  MAX_TEXT_PER_FILE: envInt("PDF_MAX_TEXT_PER_FILE", 14000),
  MAX_FILES: envInt("PDF_MAX_FILES", 2),
  PARSE_TIMEOUT_MS: envInt("PDF_PARSE_TIMEOUT_MS", 15000),
  CONCURRENCY: Math.max(1, envInt("PDF_CHILD_CONCURRENCY", envInt("PDF_MAX_FILES", 2))),
  MIN_TEXT_FOR_NOT_SCANNED: envInt("PDF_MIN_TEXT_FOR_NOT_SCANNED", 1000),
  STRIP_NULLS: envBool("PDF_STRIP_NULLS", true),
};

/* -------------------- helpers -------------------- */
function normaliseText(s) {
  const t = typeof s === "string" ? s : String(s || "");
  return CONFIG.STRIP_NULLS ? t.replace(/\u0000/g, "") : t;
}

function looksPdf(file = {}) {
  const ct = (file.contentType || file.mimetype || "").toLowerCase();
  if (ct.includes("pdf")) return true;
  const name = (file.filename || file.name || "").toLowerCase();
  return name.endsWith(".pdf");
}

// Simple concurrency limiter
async function allWithConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let i = 0;
  const runners = new Array(Math.min(limit, items.length)).fill(null).map(async () => {
    while (i < items.length) {
      const idx = i++;
      try {
        results[idx] = await worker(items[idx], idx);
      } catch (e) {
        results[idx] = { error: String(e?.message || e) };
      }
    }
  });
  await Promise.all(runners);
  return results;
}

function withTimeout(promise, ms, onTimeout) {
  let t;
  const timeout = new Promise((_, rej) => {
    t = setTimeout(() => rej(new Error(onTimeout || "pdf_parse_timeout")), ms);
  });
  return Promise.race([promise.finally(() => clearTimeout(t)), timeout]);
}

/* -------------------- core -------------------- */
async function parseOne(file) {
  try {
    const data = await withTimeout(pdfParse(file.buffer), CONFIG.PARSE_TIMEOUT_MS, "pdf_parse_timeout");
    const raw = normaliseText(data.text || "").trim();
    const scannedLikely = raw.length < CONFIG.MIN_TEXT_FOR_NOT_SCANNED;
    return {
      filename: file.filename || file.name || "unnamed.pdf",
      pages: data.numpages || data.numpages === 0 ? data.numpages : null,
      scannedLikely,
      text: raw.slice(0, CONFIG.MAX_TEXT_PER_FILE)
    };
  } catch (e) {
    return {
      filename: file.filename || file.name || "unnamed.pdf",
      error: String(e?.message || e)
    };
  }
}

async function buildPdfPack(files = []) {
  const pdfs = files.filter(looksPdf).slice(0, CONFIG.MAX_FILES);
  if (!pdfs.length) return [];
  const results = await allWithConcurrency(pdfs, CONFIG.CONCURRENCY, parseOne);
  // Keep order, drop empties (shouldn’t have)
  return results.filter(Boolean);
}

module.exports = { buildPdfPack };
