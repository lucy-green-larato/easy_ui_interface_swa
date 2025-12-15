// /api/campaign-markdown-pack/index.js
// v1 — Deterministic Markdown Pack Builder (Model A)
// 15-12-2025

"use strict";

const crypto = require("crypto");
const { BlobServiceClient } = require("@azure/storage-blob");
const { enqueueTo } = require("../lib/campaign-queue");

const STORAGE = process.env.AzureWebJobsStorage;
const INPUT_CONTAINER = "input";
const RESULTS_CONTAINER = "results";

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function nowISO() {
  return new Date().toISOString();
}

function stripMarkdown(s) {
  return String(s)
    .replace(/^\s*[-*•]\s+/, "")
    .replace(/^\s*\d+\.\s+/, "")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/[*_`]/g, "")
    .trim();
}

function headingToBucket(h, scope) {
  const k = String(h || "").toLowerCase();

  if (scope === "industry") {
    if (k.includes("driver")) return "industry_drivers";
    if (k.includes("risk")) return "industry_risks";
    if (k.includes("persona")) return "persona_pressures";
    if (k.includes("competitor")) return "competitor_profiles";
    if (k.includes("pillar")) return "content_pillars";
    if (k.includes("stat")) return "industry_stats";
    return "industry_other";
  }

  if (scope === "supplier") {
    if (
      k.includes("strength") ||
      k.includes("capabil") ||
      k.includes("different")
    ) {
      return "supplier_markdown";
    }
    return "supplier_other";
  }

  return "industry_other";
}

async function streamToString(readable) {
  const chunks = [];
  for await (const chunk of readable) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

async function readBlobText(container, name) {
  const blob = container.getBlockBlobClient(name);
  if (!(await blob.exists())) return null;
  const dl = await blob.download();
  return streamToString(dl.readableStreamBody);
}

// -----------------------------------------------------------------------------
// Markdown parser (deterministic)
// -----------------------------------------------------------------------------

function parseMarkdown({ text, source_file, scope }) {
  const lines = String(text || "").split(/\r?\n/);

  let currentHeading = "General";
  let currentBucket = headingToBucket(currentHeading, scope);

  const out = [];
  const seen = new Set();

  lines.forEach((raw, i) => {
    const line = raw.trim();
    if (!line) return;

    // Headings (## / ### only)
    if (/^#{2,3}\s+/.test(line)) {
      currentHeading = line.replace(/^#{2,3}\s+/, "").trim();
      currentBucket = headingToBucket(currentHeading, scope);
      return;
    }

    // Bullets
    if (/^([-*•]|\d+\.)\s+/.test(line)) {
      const textClean = stripMarkdown(line);
      if (!textClean) return;

      const hash = sha1(`${source_file}|${currentHeading}|${textClean}`);
      if (seen.has(hash)) return;
      seen.add(hash);

      out.push({
        id: `md_${hash.slice(0, 8)}`,
        text: textClean,
        bucket: currentBucket,
        source_type: "markdown",
        source_file,
        source_heading: currentHeading,
        line_no: i + 1,
        sha1: hash,
        created_at: nowISO()
      });
    }
  });

  return out;
}

// -----------------------------------------------------------------------------
// Azure Function handler
// -----------------------------------------------------------------------------

module.exports = async function (context, msg) {
  const {
    runId,
    prefix,
    industry_slug,
    supplier_slug,
    competitor_slugs = [],
    page = "campaign"
  } = msg || {};

  if (!runId || !prefix) {
    context.log.error("[markdown_pack] missing runId or prefix");
    return;
  }

  const blobSvc = BlobServiceClient.fromConnectionString(STORAGE);
  const input = blobSvc.getContainerClient(INPUT_CONTAINER);
  const results = blobSvc.getContainerClient(RESULTS_CONTAINER);

  const base = prefix.endsWith("/") ? prefix : `${prefix}/`;

  const pack = {
    schema: "markdown-pack-v1",
    generated_at: nowISO(),
    source: {
      industry_slug: industry_slug || null,
      supplier_slug: supplier_slug || null,
      competitor_slugs
    },
    industry_drivers: [],
    industry_risks: [],
    persona_pressures: [],
    competitor_profiles: [],
    content_pillars: [],
    industry_stats: [],
    supplier_markdown: [],
    industry_other: [],
    supplier_other: []
  };

  // ---------------------------------------------------------------------------
  // Industry markdown
  // ---------------------------------------------------------------------------
  if (industry_slug) {
    const path = `packs/industry/${industry_slug}.md`;
    const txt = await readBlobText(input, path);
    if (txt) {
      const items = parseMarkdown({
        text: txt,
        source_file: `input/${path}`,
        scope: "industry"
      });
      items.forEach(i => pack[i.bucket]?.push(i));
    }
  }

  // ---------------------------------------------------------------------------
  // Supplier markdown
  // ---------------------------------------------------------------------------
  if (supplier_slug) {
    const path = `packs/supplier/${supplier_slug}.md`;
    const txt = await readBlobText(input, path);
    if (txt) {
      const items = parseMarkdown({
        text: txt,
        source_file: `input/${path}`,
        scope: "supplier"
      });
      items.forEach(i => pack[i.bucket]?.push(i));
    }
  }

  // ---------------------------------------------------------------------------
  // Competitor markdown
  // ---------------------------------------------------------------------------
  for (const slug of competitor_slugs) {
    const path = `packs/competitor/${slug}.md`;
    const txt = await readBlobText(input, path);
    if (!txt) continue;

    const items = parseMarkdown({
      text: txt,
      source_file: `input/${path}`,
      scope: "industry"
    });
    items.forEach(i => pack[i.bucket]?.push(i));
  }

  // ---------------------------------------------------------------------------
  // Write markdown_pack.json
  // ---------------------------------------------------------------------------
  const outPath = `${base}evidence_v2/markdown_pack.json`;
  const outBlob = results.getBlockBlobClient(outPath);

  await outBlob.upload(
    JSON.stringify(pack, null, 2),
    Buffer.byteLength(JSON.stringify(pack))
  );

  context.log("[markdown_pack] written", {
    runId,
    path: outPath,
    counts: Object.fromEntries(
      Object.entries(pack)
        .filter(([, v]) => Array.isArray(v))
        .map(([k, v]) => [k, v.length])
    )
  });

  // ---------------------------------------------------------------------------
  // Hand off to router → aftermarkdown
  // ---------------------------------------------------------------------------
  await enqueueTo(
    process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs",
    {
      op: "aftermarkdown",
      runId,
      page,
      prefix: base
    }
  );

  context.log("[markdown_pack] aftermarkdown enqueued", { runId, prefix: base });
};
