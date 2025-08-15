// api/shared/packLoader.js
const fs = require("fs");
const path = require("path");

// ONE source of truth: prefer /home/site/wwwroot/packs.json (what packs-debug showed)
// Fall back to /api/packs.json for local dev.
const CANDIDATES = [
  path.join(process.cwd(), "packs.json"),          // /home/site/wwwroot/packs.json in Azure SWA
  path.join(__dirname, "..", "packs.json"),        // /api/packs.json (local / func start)
];

function loadPacks() {
  const tried = [];
  for (const p of CANDIDATES) {
    try {
      const text = fs.readFileSync(p, "utf8");
      const packs = JSON.parse(text);
      return { packs, packPath: p };
    } catch (e) {
      tried.push(`${p}: ${e.message}`);
    }
  }
  const msg = `packs.json not found. Tried:\n- ${tried.join("\n- ")}`;
  throw new Error(msg);
}

module.exports = { loadPacks };
