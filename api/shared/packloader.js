// /api/shared/packloader.js 22-10-2025 v2
// Loads packs from packs.json (preferred) or packs.js (CJS or ESM).
// Returns: { packs }  — a plain object that will pass through.

const fs = require("fs");
const path = require("path");
const { pathToFileURL } = require("url");

const ROOT = __dirname;

async function loadPacks() {
  const jsonPath = path.join(ROOT, "packs.json");
  const jsPath = path.join(ROOT, "packs.js");

  // 1) JSON source (simple and fast if present)
  if (fs.existsSync(jsonPath)) {
    const raw = fs.readFileSync(jsonPath, "utf8");
    const obj = JSON.parse(raw);
    // If the JSON file itself contains { packs: {...} }, unwrap it; else use as-is
    return { packs: obj?.packs ?? obj ?? {} };
  }

  // 2) JS module: try CJS require first
  if (fs.existsSync(jsPath)) {
    try {
      // CommonJS module?
      // eslint-disable-next-line import/no-dynamic-require, global-require
      const mod = require(jsPath);
      const packs = mod?.packs ?? mod?.default ?? mod ?? {};
      return { packs };
    } catch (err) {
      // 3) Fallback to ESM dynamic import (supports `export const packs = {...}` or `export default {...}`)
      const href = pathToFileURL(jsPath).href;
      const esm = await import(href);
      const packs = esm?.packs ?? esm?.default ?? esm ?? {};
      return { packs };
    }
  }

  // 4) Nothing present — return empty
  return { packs: {} };
}

module.exports = { loadPacks };
