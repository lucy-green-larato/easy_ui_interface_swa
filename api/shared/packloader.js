// /api/shared/packloader.js 17-11-2025 v6
// Returns: packs (plain object) — never throws on load/parse failures.

const fs = require("fs");
const path = require("path");
const { pathToFileURL } = require("url");

const ROOT = __dirname;
let __memoPacks; // simple in-process memoization of the packs object

// ---- tiny utils ----
function isPlainObject(v) {
  return v != null && typeof v === "object" && !Array.isArray(v);
}

/**
 * Normalise any module/JSON shape into a plain "packs" object.
 * Accepts:
 *   - { packs: {...} }
 *   - default export object
 *   - bare packs object
 * Anything else → {}.
 */
function coercePacksShape(v) {
  // v might be:
  //   - the raw JSON object
  //   - a CJS module export
  //   - an ESM module namespace object ({ default: ... })
  if (!isPlainObject(v)) return {};

  // Prefer an explicit .packs property if present
  if (isPlainObject(v.packs)) return v.packs;

  // If this looks like an ESM namespace with a default export,
  // and that default is a plain object, use it.
  if (isPlainObject(v.default)) {
    const dv = v.default;
    if (isPlainObject(dv.packs)) return dv.packs;
    if (isPlainObject(dv)) return dv;
  }

  // Otherwise treat v itself as the packs object
  return isPlainObject(v) ? v : {};
}

function logWarn(msg, extra) {
  try {
    console.warn(`[packloader] ${msg}`, extra ? { extra } : "");
  } catch {
    // ignore logging failures
  }
}

// Allow JSONC (strip // and /* */ comments) and trailing commas
function parseJsonLenient(s) {
  try {
    const noComments = String(s)
      .replace(/\/\*[\s\S]*?\*\//g, "")
      .replace(/(^|[^:])\/\/.*$/gm, "$1");
    const noTrailingCommas = noComments.replace(/,\s*([}\]])/g, "$1");
    return JSON.parse(noTrailingCommas);
  } catch (e) {
    logWarn("JSON parse failed; returning {}", String(e?.message || e));
    return {};
  }
}

function resolveCandidates() {
  // Priority:
  // 1) PACKS_PATH (file or directory)
  // 2) PACKS_BASENAME (e.g. packs-prod.{json,js,mjs}) under ROOT
  // 3) Default packs.{json,js,mjs} under ROOT (legacy)
  const envPath = process.env.PACKS_PATH && String(process.env.PACKS_PATH).trim();
  const envBase =
    (process.env.PACKS_BASENAME && String(process.env.PACKS_BASENAME).trim()) ||
    "packs";

  const candidates = [];

  if (envPath) {
    const p = path.isAbsolute(envPath) ? envPath : path.join(ROOT, envPath);
    if (fs.existsSync(p) && fs.statSync(p).isDirectory()) {
      candidates.push(path.join(p, `${envBase}.json`));
      candidates.push(path.join(p, `${envBase}.js`));
      candidates.push(path.join(p, `${envBase}.mjs`));
    } else {
      // Treat as explicit file path
      candidates.push(p);
    }
  }

  // Preferred names under ROOT
  candidates.push(path.join(ROOT, `${envBase}.json`));
  candidates.push(path.join(ROOT, `${envBase}.js`));
  candidates.push(path.join(ROOT, `${envBase}.mjs`));

  // Legacy defaults
  candidates.push(path.join(ROOT, "packs.json"));
  candidates.push(path.join(ROOT, "packs.js"));
  candidates.push(path.join(ROOT, "packs.mjs"));

  // Deduplicate while keeping order
  return [...new Set(candidates)];
}

// ---- core loader ----
async function loadPacks() {
  if (__memoPacks) return __memoPacks;

  const paths = resolveCandidates();

  for (const p of paths) {
    try {
      if (!fs.existsSync(p)) continue;

      const ext = path.extname(p).toLowerCase();

      // 1) JSON (preferred)
      if (ext === ".json") {
        const raw = fs.readFileSync(p, "utf8");
        const obj = parseJsonLenient(raw);
        const packs = coercePacksShape(obj);
        __memoPacks = isPlainObject(packs) ? packs : {};
        return __memoPacks;
      }

      // 2) JS: attempt CJS require first, then ESM import fallback
      if (ext === ".js") {
        try {
          // eslint-disable-next-line import/no-dynamic-require, global-require
          const mod = require(p);
          const packs = coercePacksShape(mod);
          __memoPacks = isPlainObject(packs) ? packs : {};
          return __memoPacks;
        } catch (eCjs) {
          try {
            const esm = await import(pathToFileURL(p).href);
            const packs = coercePacksShape(esm);
            __memoPacks = isPlainObject(packs) ? packs : {};
            return __memoPacks;
          } catch (eEsm) {
            logWarn("Failed to load packs.js as CJS or ESM", {
              cjs: String(eCjs?.message || eCjs),
              esm: String(eEsm?.message || eEsm)
            });
            continue;
          }
        }
      }

      // 3) MJS: ESM only
      if (ext === ".mjs") {
        try {
          const esm = await import(pathToFileURL(p).href);
          const packs = coercePacksShape(esm);
          __memoPacks = isPlainObject(packs) ? packs : {};
          return __memoPacks;
        } catch (e) {
          logWarn("Failed to import packs.mjs", String(e?.message || e));
          continue;
        }
      }
    } catch (err) {
      logWarn("Unhandled packs load error (continuing to next candidate)", {
        file: p,
        err: String(err?.message || err)
      });
      // keep trying next candidate
    }
  }

  // Nothing usable → empty packs object
  __memoPacks = {};
  return __memoPacks;
}

/**
 * Convenience helper: return packs as a plain object.
 * Always resolves; never throws.
 */
async function getPacks() {
  const packs = await loadPacks();
  return isPlainObject(packs) ? packs : {};
}

module.exports = {
  loadPacks,
  getPacks
};
