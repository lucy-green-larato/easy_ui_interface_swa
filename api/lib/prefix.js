// /api/_lib/prefix.js puts results files into the correct blob folders. 04-11-2025 v1
function sanitizeSegment(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_\-]/g, "-")
    .replace(/-+/g, "-");
}

export function computePrefix({ userId, page, runId, date = new Date() }) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");

  const segUser = sanitizeSegment(userId || "anonymous");
  const segPage = sanitizeSegment(page || "campaign");
  const segRun  = sanitizeSegment(runId);

  // with date bucketing (recommended)
  return `runs/${segPage}/${segUser}/${y}/${m}/${d}/${segRun}/`;
  // or without date:
  // return `runs/${segPage}/${segUser}/${segRun}/`;
}
