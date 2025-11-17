// /api/lib/featureFlags.js 14-11-2025 v2
// Safe feature flag helpers with robust handling of non-object status/flags.

function getFlags(status) {
  const defaultFlags = {
    use_new_evidence: false,
    use_new_insights: false,
    use_new_strategy: false,
    use_writer_assembler: false
  };

  // If status is not a plain object, just return defaults
  if (!status || typeof status !== "object") {
    return { ...defaultFlags };
  }

  // Only merge in flags if they are a plain object
  const rawFlags =
    status.flags && typeof status.flags === "object"
      ? status.flags
      : {};

  return {
    ...defaultFlags,
    ...rawFlags
  };
}

function setFlags(status, flagsUpdate) {
  // Normalise status to a plain object so spread is always safe
  const base =
    status && typeof status === "object"
      ? status
      : {};

  // Get the current flags (already merged with defaults)
  const currentFlags = getFlags(base);

  // Only accept an object for updates; anything else becomes {}
  const update =
    flagsUpdate && typeof flagsUpdate === "object"
      ? flagsUpdate
      : {};

  return {
    ...base,
    flags: {
      ...currentFlags,
      ...update
    }
  };
}

module.exports = {
  getFlags,
  setFlags
};
