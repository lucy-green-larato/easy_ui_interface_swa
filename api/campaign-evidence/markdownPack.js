// /api/campaign-evidence/markdownPack.js 09-12-2025 v5
// Deterministic Markdown pack ingestion for campaign evidence.
//
// Reads Markdown from the shared INPUT container:
//   - <INPUT_CONTAINER>/packs/industry-sources/*.md
//   - <INPUT_CONTAINER>/packs/supplier/*.md
//
// Writes structured JSON bundle to the RESULTS container:
//   <prefix>evidence_v2/markdown_pack.json
// Rules:
//  - No AI, no summarisation.
//  - Only headings + bullet lists are used.
//  - Everything is deterministic rule-based.
//  - Items are traceable via a stable id derived from file+heading+line.

const {
  listBlobsUnderPrefix,
  getText,
  putJson,
  getInputContainerClient
} = require("../shared/storage");
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
    "industry_stats",

    // NEW: Supplier buckets (Tier-2a)
    "supplier_strengths",
    "supplier_capabilities",
    "supplier_differentiators",
    "supplier_value_proposition"
  ];
  const out = {};
  for (const k of keys) {
    const v = Array.isArray(raw?.[k]) ? raw[k] : [];
    out[k] = v;
  }
  return out;
}

// IMPORTANT: All markdown heading styles used in packs must be mapped here.
// Unmatched headings are ignored by design.
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
  // Supplier strengths
  if (/strength|core competency|core strength|advantage|what we do best/i.test(t)) {
    return "supplier_strengths";
  }

  // Supplier capabilities
  if (/capabilit|delivery model|service capability|technical capability|platform|infrastructure/i.test(t)) {
    return "supplier_capabilities";
  }

  // Supplier differentiators
  if (/differentiator|why us|why choose|unique value|our edge|our advantage/i.test(t)) {
    return "supplier_differentiators";
  }

  // Supplier value proposition
  if (/value proposition|our value|customer value|benefit|promise/i.test(t)) {
    return "supplier_value_proposition";
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
  const out = ensurePackShape({});

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

/**
 * Load all .md files under a prefix from the given container.
 */
async function loadMarkdownUnder(containerClient, prefix) {
  try {
    // listBlobsUnderPrefix returns blob paths relative to the container
    const names = await listBlobsUnderPrefix(containerClient, prefix);
    const mdFiles = (Array.isArray(names) ? names : []).filter(n => /\.md$/i.test(n));
    const packs = [];

    for (const name of mdFiles) {
      try {
        const text = await getText(containerClient, name);
        if (!text) continue;
        packs.push({ path: name, text });
      } catch {
        // Best-effort: a single bad blob must not kill the whole pack.
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

/**
 * Build a combined markdown pack.
 *
 * Reads from the INPUT container (packs/*) and writes the combined
 * json bundle to the RESULTS container using the provided `resultsContainer`.
 *
 * @param {import("@azure/storage-blob").ContainerClient} resultsContainer
 *   The results container client (usually from getContainerClient/ getResultsContainerClient).
 * @param {string} prefix
 *   Run-specific prefix under the RESULTS container, e.g. "runs/<runId>/".
 */
async function buildMarkdownPack(resultsContainer, prefix) {
  // Always return a valid pack shape, even on error.
  const emptyPack = ensurePackShape({});

  try {
    // Read from shared packs in the INPUT container.
    const inputContainer = getInputContainerClient();

    const industryPacks = await loadMarkdownUnder(inputContainer, "packs/industry-sources/");
    const supplierPacks = await loadMarkdownUnder(inputContainer, "packs/supplier/");
    const runScopedSupplier = await loadMarkdownUnder(
      resultsContainer,
      `${prefix}packs/`
    );

    // Start from a fully shaped, empty pack (includes supplier buckets)
    const acc = ensurePackShape({});

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
    // Merge run-scoped supplier markdown
    for (const p of runScopedSupplier) {
      const parsed = parseMarkdownBullets(p.text, p.path, "supplier_run");
      append(parsed);
    }
    for (const p of supplierPacks) {
      const parsed = parseMarkdownBullets(p.text, p.path, "supplier");
      append(parsed);
    }

    const bundle = ensurePackShape(acc);

    try {
      const outPath = `${prefix}evidence_v2/markdown_pack.json`;
      validateAndWarn("markdown_pack", bundle, console.log);
      await putJson(resultsContainer, outPath, bundle);
    } catch {
      // Write failure is non-fatal for the caller; they just won't see
      // the pack on disk. We still return the bundle for in-process use.
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
