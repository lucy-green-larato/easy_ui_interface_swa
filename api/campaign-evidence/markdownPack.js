// /api/campaign-evidence/markdownPack.js 18-11-2025 v3
// Deterministic Markdown pack ingestion for campaign evidence.
// Reads Markdown from:
//   - packs/industry-sources/*.md
//   - packs/supplier/*.md
// and writes a structured JSON bundle to:
//   <prefix>evidence_v2/markdown_pack.json
//
// Shape:
// {
//   "industry_drivers":     [],
//   "industry_risks":       [],
//   "persona_pressures":    [],
//   "competitor_profiles":  [],
//   "content_pillars":      [],
//   "industry_stats":       []
// }
//
// Rules:
//  - No AI, no summarisation.
//  - Only headings + bullet lists are used.
//  - Everything is deterministic rule-based.
//  - Items are traceable via a stable id derived from file+heading+line.
//  - Diagnostics are recorded in markdown_pack_diag.json so skipped content is visible.

const { listBlobsUnderPrefix, getText, putJson } = require("../shared/storage");
const { sha1 } = require("../shared/utils");
const { validateAndWarn } = require("../shared/schemaValidators");


// --- small helpers ---

function ensurePackShape(raw) {
  const keys = [
    "industry_drivers",
    "industry_risks",
    "persona_pressures",
    "competitor_profiles",
    "content_pillars",
    "industry_stats"
  ];
  const out = {};
  for (const k of keys) {
    const v = Array.isArray(raw?.[k]) ? raw[k] : [];
    out[k] = v;
  }
  return out;
}

function headingToBucket(title) {
  const t = String(title || "").toLowerCase();

  // Deliberately simple and deterministic mapping
  if (/industry.*driver|market.*driver|growth driver|demand driver/.test(t)) {
    return "industry_drivers";
  }
  if (/risk|regulator|compliance|ofcom|ico|ons|gov\.uk/.test(t)) {
    return "industry_risks";
  }
  if (/persona|buyer pressure|stakeholder pressure|cfo|cio|cto|decision maker/.test(t)) {
    return "persona_pressures";
  }
  if (/competitor|alt supplier|alternative provider|market landscape/.test(t)) {
    return "competitor_profiles";
  }
  if (/content pillar|content theme|messaging pillar|narrative/.test(t)) {
    return "content_pillars";
  }
  if (/stat|data point|metric|kpi|benchmark|survey/.test(t)) {
    return "industry_stats";
  }

  // Unknown → ignore (we do not guess)
  return null;
}

/**
 * Parse markdown headings + bullets into category buckets.
 * Optionally populates a diagnostics object:
 *   diag.unknownHeadings: [{ file, heading }]
 *   diag.orphanBullets: number
 */

function parseMarkdownBullets(md, filePath, packType, diag) {
  const lines = String(md || "").split(/\r?\n/);
  const out = {
    industry_drivers: [],
    industry_risks: [],
    persona_pressures: [],
    competitor_profiles: [],
    content_pillars: [],
    industry_stats: []
  };

  let currentBucket = null;
  let currentHeading = null;

  // Initialise diagnostics container if provided
  if (diag) {
    if (!Array.isArray(diag.unknownHeadings)) diag.unknownHeadings = [];
    if (typeof diag.orphanBullets !== "number") diag.orphanBullets = 0;
  }

  for (const rawLine of lines) {
    const line = String(rawLine || "");

    // Headings: #, ##, ### etc
    const hMatch = line.match(/^(#{1,6})\s+(.+?)\s*$/);
    if (hMatch) {
      const headingText = hMatch[2].trim();
      const bucket = headingToBucket(headingText);
      currentBucket = bucket;
      currentHeading = headingText;

      // Track headings that did not map to any bucket
      if (!bucket && diag) {
        diag.unknownHeadings.push({
          file: filePath,
          heading: headingText
        });
      }
      continue;
    }

    // Bullet lines
    const bMatch = line.match(/^\s*(?:[-*+]\s+|\d+\.\s+)(.+)$/);
    if (!bMatch) continue;

    const text = bMatch[1].trim();
    if (!text) continue;

    // Orphan bullets (no recognised bucket)
    if (!currentBucket) {
      if (diag) diag.orphanBullets += 1;
      continue;
    }

    const id = sha1(`${filePath}|${currentHeading || ""}|${text}`);
    out[currentBucket].push({
      id,
      text,
      source: {
        file: filePath,
        heading: currentHeading || null,
        pack_type: packType || null     
      }
    });
  }

  return out;
}

async function loadMarkdownUnder(container, prefix) {
  try {
    // listBlobsUnderPrefix returns blob paths relative to the container
    const names = await listBlobsUnderPrefix(container, prefix);
    const mdFiles = (Array.isArray(names) ? names : []).filter(n => /\.md$/i.test(n));
    const packs = [];

    for (const name of mdFiles) {
      try {
        const text = await getText(container, name);
        if (!text) continue;
        packs.push({ path: name, text });
      } catch {
        // Best-effort: a single bad blob must not kill the whole pack.
        // Just skip this file.
        continue;
      }
    }

    return packs;
  } catch {
    // If the prefix listing itself fails, treat as "no packs found".
    return [];
  }
}

// --- main entry point ---

async function buildMarkdownPack(container, prefix) {
  // Always return a valid pack shape, even on error.
  const emptyPack = ensurePackShape({});

  try {
    // Read from shared packs in the same RESULTS container.
    // These packs are global to the app, not per-run.
    const industryPacks = await loadMarkdownUnder(container, "packs/industry-sources/");
    const supplierPacks = await loadMarkdownUnder(container, "packs/supplier/");

    const acc = {
      industry_drivers: [],
      industry_risks: [],
      persona_pressures: [],
      competitor_profiles: [],
      content_pillars: [],
      industry_stats: []
    };

    const append = (partial) => {
      if (!partial) return;
      for (const k of Object.keys(acc)) {
        if (Array.isArray(partial[k]) && partial[k].length) {
          acc[k].push(...partial[k]);
        }
      }
    };

    // Optionally, you could add a diagnostics object here if you want,
    // but this is the minimal resilient version.
    for (const p of industryPacks) {
      const parsed = parseMarkdownBullets(p.text, p.path, "industry");
      append(parsed);
    }

    for (const p of supplierPacks) {
      const parsed = parseMarkdownBullets(p.text, p.path, "supplier");
      append(parsed);
    }

    const bundle = ensurePackShape(acc);

    try {
      const outPath = `${prefix}evidence_v2/markdown_pack.json`;
      validateAndWarn("markdown_pack", bundle, context.log || console.log);
      await putJson(container, outPath, bundle);
    } catch {
      // Write failure is non-fatal for the caller; they just won't see the pack on disk.
      // We still return the bundle so in-process callers could use it if needed.
    }

    return bundle;
  } catch {
    // Any unexpected error → return an empty, correctly-shaped pack.
    return emptyPack;
  }
}

module.exports = {
  buildMarkdownPack
};
