// api/packs-debug/index.js
const fs = require("fs");
const path = require("path");

// Try common locations; adjust if your packs.json sits elsewhere.
const CANDIDATES = [
  process.env.PACKS_PATH,
  path.join(__dirname, "..", "packs.json"),
  path.join(__dirname, "..", "prompts", "packs.json"),
  path.join(process.cwd(), "packs.json")
].filter(Boolean);

module.exports = async function (context, req) {
  for (const p of CANDIDATES) {
    try {
      const text = fs.readFileSync(p, "utf8");
      const packs = JSON.parse(text);
      const core = packs["uk_b2b_sales_core"] || {};
      const names = Object.keys(core.templates || {});
      context.res = {
        headers: { "content-type": "application/json" },
        body: { readingFrom: p, templates: names }
      };
      return;
    } catch (e) { /* try next path */ }
  }
  context.res = {
    status: 500,
    body: { error: "packs.json not found", tried: CANDIDATES }
  };
};
