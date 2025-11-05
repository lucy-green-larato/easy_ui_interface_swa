// /api/shared/packloader.js 2025-11-05 v3
// Returns: { packs } — always a plain object; never throws on load/parse failures.

const fs = require("fs");
const path = require("path");
const { pathToFileURL } = require("url");

const ROOT = __dirname;
let __memo; // simple in-process memoization

// ---- tiny utils ----
function isPlainObject(v) {
  return v != null && typeof v === "object" && !Array.isArray(v);
}
function coercePacksShape(v) {
  // Accept: {packs: {...}} | default export object | bare object | anything else → {}
  const candidate = (isPlainObject(v) && (v.packs ?? v.default ?? v)) || {};
  return isPlainObject(candidate) ? candidate : {};
}
function logWarn(msg, extra) {
  try { console.warn(`[packloader] ${msg}`, extra ? { extra } : ""); } catch {}
}

// Allow JSONC (strip // and /* */ comments) and trailing commas
function parseJsonLenient(s) {
  try {
    const noComments = String(s)
      .replace(/\/\*[\s\S]*?\*\//g, "")
      .replace(/(^|[^:])\/\/.*$/gm, "$1");
    const noTrailingCommas = noComments
      .replace(/,\s*([}\]])/g, "$1");
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
  // 3) Default packs.{json,js,mjs} under ROOT
  const envPath = process.env.PACKS_PATH && String(process.env.PACKS_PATH).trim();
  const envBase = (process.env.PACKS_BASENAME && String(process.env.PACKS_BASENAME).trim()) || "packs";

  const candidates = [];

  if (envPath) {
    const p = path.isAbsolute(envPath) ? envPath : path.join(ROOT, envPath);
    if (fs.existsSync(p) && fs.statSync(p).isDirectory()) {
      candidates.push(path.join(p, `${envBase}.json`));
      candidates.push(path.join(p, `${envBase}.js`));
      candidates.push(path.join(p, `${envBase}.mjs`));
    } else {
      candidates.push(p); // treat as explicit file path
    }
  }

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
  if (__memo) return __memo;

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
        __memo = { packs };
        return __memo;
      }

      // 2) JS/MJS: attempt CJS require first for .js; always allow ESM fallback
      if (ext === ".js") {
        try {
          // eslint-disable-next-line import/no-dynamic-require, global-require
          const mod = require(p);
          const packs = coercePacksShape(mod);
          __memo = { packs };
          return __memo;
        } catch (eCjs) {
          // fallthrough to ESM import
          try {
            const esm = await import(pathToFileURL(p).href);
            const packs = coercePacksShape(esm);
            __memo = { packs };
            return __memo;
          } catch (eEsm) {
            logWarn("Failed to load packs.js as CJS or ESM", { cjs: String(eCjs?.message || eCjs), esm: String(eEsm?.message || eEsm) });
            continue;
          }
        }
      }

      if (ext === ".mjs") {
        try {
          const esm = await import(pathToFileURL(p).href);
          const packs = coercePacksShape(esm);
          __memo = { packs };
          return __memo;
        } catch (e) {
          logWarn("Failed to import packs.mjs", String(e?.message || e));
          continue;
        }
      }
    } catch (err) {
      logWarn("Unhandled packs load error (continuing to next candidate)", { file: p, err: String(err?.message || err) });
      // keep trying next candidate
    }
  }

  // 3) Nothing usable → empty
  __memo = { packs: {} };
  return __memo;
}

module.exports = { loadPacks };
