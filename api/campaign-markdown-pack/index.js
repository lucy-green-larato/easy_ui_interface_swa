// /api/campaign-markdown-pack/index.js 15-12-25 v1
// v1 — Deterministic Markdown Pack Builder (Option A)

const crypto = require("crypto");
const { BlobServiceClient } = require("@azure/storage-blob");

const STORAGE = process.env.AzureWebJobsStorage;
const INPUT_CONTAINER = "input";
const RESULTS_CONTAINER = "results";

// ---------------- helpers ----------------

async function markStageComplete(container, prefix, stage, runId, context) {
  const statusPath = `${prefix}status.json`;
  const status = (await container.getBlockBlobClient(statusPath).exists())
    ? JSON.parse(await (await container.getBlockBlobClient(statusPath).download()).readableStreamBody.read())
    : { runId, markers: {}, history: [] };

  status.markers = status.markers || {};
  status.markers[stage] = true;
  status.state = stage;
  status.history = status.history || [];
  status.history.push({
    stage,
    completed_at: new Date().toISOString()
  });

  await container
    .getBlockBlobClient(statusPath)
    .upload(
      JSON.stringify(status, null, 2),
      Buffer.byteLength(JSON.stringify(status))
    );

  context.log(`[markdown_pack] stage complete`, { stage, prefix });
}

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
  const k = h.toLowerCase();

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
    if (k.includes("strength") || k.includes("capabil") || k.includes("different"))
      return "supplier_markdown";
    return "supplier_other";
  }

  return "industry_other";
}

async function readBlobText(container, name) {
  const bc = container.getBlockBlobClient(name);
  if (!(await bc.exists())) return null;
  const dl = await bc.download();
  return await streamToString(dl.readableStreamBody);
}

async function streamToString(readable) {
  const chunks = [];
  for await (const ch of readable) chunks.push(ch);
  return Buffer.concat(chunks).toString("utf8");
}

// ---------------- parser ----------------

function parseMarkdown({ text, source_file, scope }) {
  const lines = text.split(/\r?\n/);

  let currentHeading = "General";
  let currentBucket = headingToBucket(currentHeading, scope);

  const out = [];
  const seen = new Set();

  lines.forEach((raw, i) => {
    const line = raw.trim();
    if (!line) return;

    // Heading
    if (/^#{2,3}\s+/.test(line)) {
      currentHeading = line.replace(/^#{2,3}\s+/, "").trim();
      currentBucket = headingToBucket(currentHeading, scope);
      return;
    }

    // Bullet
    if (/^([-*•]|\d+\.)\s+/.test(line)) {
      const textClean = stripMarkdown(line);
      if (!textClean) return;

      const hash = sha1(`${source_file}|${currentHeading}|${textClean}`);
      if (seen.has(hash)) return;
      seen.add(hash);

      out.push({
        id: "md_" + hash.slice(0, 8),
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

// ---------------- handler ----------------

module.exports = async function (context, msg) {
  const { runId, prefix, industry_slug, supplier_slug, competitor_slugs = [] } = msg;

  const blobSvc = BlobServiceClient.fromConnectionString(STORAGE);
  const input = blobSvc.getContainerClient(INPUT_CONTAINER);
  const results = blobSvc.getContainerClient(RESULTS_CONTAINER);

  const base = prefix.endsWith("/") ? prefix : prefix + "/";

  const pack = {
    schema: "markdown-pack-v1",
    generated_at: nowISO(),
    source: {
      industry_slug,
      supplier_slug,
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

  // ---- industry ----
  if (industry_slug) {
    const p = `packs/industry/${industry_slug}.md`;
    const txt = await readBlobText(input, p);
    if (txt) {
      const items = parseMarkdown({ text: txt, source_file: `input/${p}`, scope: "industry" });
      items.forEach(i => pack[i.bucket]?.push(i));
    }
  }

  // ---- supplier ----
  if (supplier_slug) {
    const p = `packs/supplier/${supplier_slug}.md`;
    const txt = await readBlobText(input, p);
    if (txt) {
      const items = parseMarkdown({ text: txt, source_file: `input/${p}`, scope: "supplier" });
      items.forEach(i => pack[i.bucket]?.push(i));
    }
  }

  // ---- competitors ----
  for (const slug of competitor_slugs) {
    const p = `packs/competitor/${slug}.md`;
    const txt = await readBlobText(input, p);
    if (!txt) continue;
    const items = parseMarkdown({ text: txt, source_file: `input/${p}`, scope: "industry" });
    items.forEach(i => pack[i.bucket]?.push(i));
  }

  // ---- write ----
  const outBlob = results.getBlockBlobClient(
    `${base}evidence_v2/markdown_pack.json`
  );

  await markStageComplete(
    results,
    base,
    "markdownPackCompleted",
    runId,
    context
  );

  await outBlob.upload(
    JSON.stringify(pack, null, 2),
    Buffer.byteLength(JSON.stringify(pack))
  );

  context.log("[markdown_pack] written", {
    runId,
    counts: Object.fromEntries(
      Object.entries(pack).filter(([k, v]) => Array.isArray(v)).map(([k, v]) => [k, v.length])
    )
  });
};

const { enqueueTo } = require("../lib/campaign-queue");

await enqueueTo(
  process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs",
  {
    op: "aftermarkdown",
    runId,
    page: "campaign",
    prefix
  }
);

