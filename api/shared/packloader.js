// /api/shared/packloader.js — 05-12-2025 Azure-Safe v7
// Loads packs.json / packs.js without throwing. Prefers CJS (Azure default).
// Guarantees: returns {} on failure, never throws, never blocks pipeline.

const fs = require("fs");
const path = require("path");
const { pathToFileURL } = require("url");

const ROOT = __dirname;
let __memoPacks = null;

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------
function isPlainObject(v) {
  return v && typeof v === "object" && !Array.isArray(v);
}

function logWarn(msg, extra) {
  try {
    console.warn(`[packloader] ${msg}`, extra ? { extra } : "");
  } catch {}
}

/**
 * Normalise a module shape into a plain packs object:
 *   - { packs: {...} }
 *   - { default: { packs: {...} } }
 *   - { default: {...} }
 *   - {...}
 */
function coercePacksShape(v) {
  if (!isPlainObject(v)) return {};

  if (isPlainObject(v.packs)) return v.packs;
  if (isPlainObject(v.default)) {
    const d = v.default;
    if (isPlainObject(d.packs)) return d.packs;
    if (isPlainObject(d)) return d;
  }

  return v;
}

/**
 * JSON-with-comments / trailing-comma tolerant parser
 */
function parseJsonLenient(text) {
  try {
    const noComments = String(text)
      .replace(/\/\*[\s\S]*?\*\//g, "")
      .replace(/(^|[^:])\/\/.*$/gm, "$1");

    const noTrailing = noComments.replace(/,\s*([}\]])/g, "$1");

    return JSON.parse(noTrailing);
  } catch (err) {
    logWarn("JSON parse failed; using {} fallback", err?.message);
    return {};
  }
}

// ---------------------------------------------------------------------------
// Candidate resolution
// ---------------------------------------------------------------------------
function resolveCandidates() {
  const candidates = [];

  const envPath = process.env.PACKS_PATH && String(process.env.PACKS_PATH).trim();
  const envBase =
    (process.env.PACKS_BASENAME && String(process.env.PACKS_BASENAME).trim()) ||
    "packs";

  if (envPath) {
    const p = path.isAbsolute(envPath) ? envPath : path.join(ROOT, envPath);

    if (fs.existsSync(p) && fs.statSync(p).isDirectory()) {
      candidates.push(path.join(p, `${envBase}.json`));
      candidates.push(path.join(p, `${envBase}.js`));
    } else {
      candidates.push(p);
    }
  }

  // Preferred
  candidates.push(path.join(ROOT, `${envBase}.json`));
  candidates.push(path.join(ROOT, `${envBase}.js`));

  // Legacy fallbacks
  candidates.push(path.join(ROOT, "packs.json"));
  candidates.push(path.join(ROOT, "packs.js"));

  return [...new Set(candidates)];
}

// ---------------------------------------------------------------------------
// Loader: tries JSON → JS (CJS) → JS (ESM), in that order
// ---------------------------------------------------------------------------
async function loadPacks() {
  if (__memoPacks) return __memoPacks;

  const files = resolveCandidates();

  for (const file of files) {
    if (!fs.existsSync(file)) continue;

    const ext = path.extname(file).toLowerCase();

    // JSON first
    if (ext === ".json") {
      try {
        const raw = fs.readFileSync(file, "utf8");
        const json = parseJsonLenient(raw);
        __memoPacks = coercePacksShape(json);
        return __memoPacks;
      } catch (e) {
        logWarn("Failed to parse packs.json", e?.message);
        continue;
      }
    }

    // JS (prefer CJS)
    if (ext === ".js") {
      try {
        const mod = require(file);
        __memoPacks = coercePacksShape(mod);
        return __memoPacks;
      } catch (cjsErr) {
        // CJS failed — maybe file is ESM
        try {
          const esm = await import(pathToFileURL(file).href);
          __memoPacks = coercePacksShape(esm);
          return __memoPacks;
        } catch (esmErr) {
          logWarn("Failed to load packs.js as CJS or ESM", {
            cjs: cjsErr?.message,
            esm: esmErr?.message
          });
          continue;
        }
      }
    }
  }

  // Nothing worked
  __memoPacks = {};
  return __memoPacks;
}

// ---------------------------------------------------------------------------
// Public getter
// ---------------------------------------------------------------------------
async function getPacks() {
  const pk = await loadPacks();
  return isPlainObject(pk) ? pk : {};
}

module.exports = {
  loadPacks,
  getPacks
};
