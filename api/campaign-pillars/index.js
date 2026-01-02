// /api/campaign-pillars/index.js
// 02-01-2026 — Campaign Pillars Doctrine (Option A) — content-pillars-v3
// Produces:
//   {prefix}content_pillars.json
// Schema:
//   content-pillars-v2
//
// Guarantees:
//   - core_pillars are markdown-only (stable truth)
//   - proof_enrichment is additive (evidence linkage)
//   - strict typed provenance (source_refs) validated against markdown_pack registry
//   - strict typed claim refs validated against evidence.json
//   - no LLM; no fact invention
//   - idempotent and router-safe

"use strict";

const crypto = require("crypto");

const { enqueueTo } = require("../lib/campaign-queue");
const { nowIso } = require("../shared/utils");
const { updateStatus } = require("../shared/status");
const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");

const ROUTER_QUEUE_NAME = process.env.Q_CAMPAIGN_ROUTER || "campaign-router-jobs";
const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

// -----------------------------------------------------------------------------
// Utility helpers
// -----------------------------------------------------------------------------

function parseQueueItem(queueItem) {
  if (!queueItem) return {};
  if (typeof queueItem === "string") {
    try { return JSON.parse(queueItem); } catch { return {}; }
  }
  return (queueItem && typeof queueItem === "object") ? queueItem : {};
}

function normPrefix(prefix) {
  let p = String(prefix || "").trim();
  if (!p) return "";
  if (p.startsWith(`${RESULTS_CONTAINER}/`)) p = p.slice(`${RESULTS_CONTAINER}/`.length);
  p = p.replace(/^\/+/, "");
  if (!p.endsWith("/")) p += "/";
  return p;
}

function stableString(v) {
  return String(v ?? "").trim();
}

function safeArray(v) {
  return Array.isArray(v) ? v : [];
}

function uniqNonEmpty(arr) {
  const out = [];
  const seen = new Set();
  for (const v of safeArray(arr)) {
    const s = stableString(v);
    if (!s) continue;
    if (seen.has(s)) continue;
    seen.add(s);
    out.push(s);
  }
  return out;
}

function stableStringify(obj) {
  if (obj === null || typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return "[" + obj.map(stableStringify).join(",") + "]";
  const keys = Object.keys(obj).sort();
  return "{" + keys.map(k => JSON.stringify(k) + ":" + stableStringify(obj[k])).join(",") + "}";
}

function sha1OfJson(obj) {
  return crypto.createHash("sha1").update(stableStringify(obj ?? null)).digest("hex");
}

function sha1OfString(s) {
  return crypto.createHash("sha1").update(String(s ?? "")).digest("hex");
}

function tokens(s) {
  return String(s || "")
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean)
    .filter(t => t.length >= 3);
}

function setFromTokens(text) {
  const set = new Set();
  for (const t of tokens(text)) set.add(t);
  return set;
}

function overlapScore(a, b) {
  if (!a || !b || !a.size || !b.size) return 0;
  let m = 0;
  for (const t of a) if (b.has(t)) m++;
  return m;
}

// -----------------------------------------------------------------------------
// Markdown pack registry (mechanically enforceable provenance)
// -----------------------------------------------------------------------------

// We build a strict registry of "pillar_id" values from markdown_pack.json.
// This lets us enforce: every source_ref.pillar_id exists.
//
// If markdown_pack objects already have an id/pillar_id/key, we use that.
// Otherwise we deterministically derive one from the object content *and type path*.
function buildMarkdownPillarRegistry(markdownPack) {
  const registry = {}; // pillar_id -> { type, supplier_slug, industry_slug, path, title, sha1 }
  const errors = [];

  const supplier_slug = stableString(markdownPack?.supplier_slug || markdownPack?.supplier || null) || null;
  const industry_slug = stableString(markdownPack?.industry_slug || markdownPack?.industry || null) || null;

  function derivePillarId(type, path, obj) {
    const base = stableString(obj?.id || obj?.pillar_id || obj?.key || obj?.ref || "");
    if (base) return base;

    // Deterministic derived id: MPL-<sha1(type|path|content)>
    const contentSig = stableStringify({
      title: stableString(obj?.title || obj?.label || obj?.name || ""),
      text: stableString(obj?.text || obj?.summary || obj?.description || obj?.bullet || ""),
      tags: safeArray(obj?.tags).map(stableString).filter(Boolean)
    });

    const h = sha1OfString(`${type}|${path}|${contentSig}`).slice(0, 12);
    return `MPL-${h}`;
  }

  function normCandidate(obj) {
    if (typeof obj === "string") {
      const t = stableString(obj);
      if (!t) return null;
      return { title: t.slice(0, 140), text: t, tags: [] };
    }
    if (!obj || typeof obj !== "object") return null;

    const title =
      stableString(obj.title) ||
      stableString(obj.label) ||
      stableString(obj.name) ||
      stableString(obj.heading) ||
      "";

    const text =
      stableString(obj.text) ||
      stableString(obj.summary) ||
      stableString(obj.description) ||
      stableString(obj.bullet) ||
      stableString(obj.body) ||
      "";

    const combined = [title, text].filter(Boolean).join(": ").trim();
    if (!combined) return null;

    return {
      title: title ? title.slice(0, 140) : combined.slice(0, 140),
      text: combined,
      tags: uniqNonEmpty(obj.tags).slice(0, 8),
      raw: obj
    };
  }

  function addAll(type, path, arr) {
    const a = safeArray(arr);
    for (let i = 0; i < a.length; i++) {
      const raw = a[i];
      const n = normCandidate(raw);
      if (!n) continue;

      const pillar_id = derivePillarId(type, `${path}[${i}]`, raw);
      if (registry[pillar_id]) {
        errors.push(`duplicate_pillar_id:${pillar_id}`);
        continue;
      }

      const sha1 = sha1OfString(stableStringify(n));

      registry[pillar_id] = {
        pillar_id,
        type,
        supplier_slug,
        industry_slug,
        path: `${path}[${i}]`,
        title: n.title,
        sha1,
        // Note: we do not persist full raw markdown here; content_pillars will carry only refs.
      };
    }
  }

  // IMPORTANT: These are the known keys in your current markdown_pack artefacts.
  addAll("markdown_pillar", "supplier_capabilities", markdownPack?.supplier_capabilities);
  addAll("markdown_pillar", "supplier_strengths", markdownPack?.supplier_strengths);
  addAll("markdown_pillar", "supplier_differentiators", markdownPack?.supplier_differentiators);

  addAll("industry_pillar", "industry_drivers", markdownPack?.industry_drivers);
  addAll("industry_pillar", "industry_risks", markdownPack?.industry_risks);
  addAll("industry_pillar", "persona_pressures", markdownPack?.persona_pressures);

  // Optional seeds (these may already be synthesised earlier)
  addAll("content_seed", "content_pillars", markdownPack?.content_pillars);

  const registrySha1 = sha1OfJson(registry);

  return { registry, registrySha1, errors };
}

// -----------------------------------------------------------------------------
// Evidence index + deterministic matching
// -----------------------------------------------------------------------------

function buildClaimIndex(evidenceClaims) {
  const idx = [];
  for (const c of safeArray(evidenceClaims)) {
    const claim_id = stableString(c?.claim_id || c?.id);
    if (!claim_id) continue;

    const title = stableString(c?.title);
    const summary = stableString(c?.summary);
    const quote = stableString(c?.quote);

    const tier =
      (typeof c?.tier === "number")
        ? c.tier
        : (typeof c?.tier === "string" && c.tier.trim())
          ? Number(c.tier)
          : null;

    const tier_group = stableString(c?.tier_group || c?.tag || "other") || "other";

    idx.push({
      claim_id,
      tier,
      tier_group,
      tok: new Set([...tokens(title), ...tokens(summary), ...tokens(quote)])
    });
  }

  idx.sort((a, b) => a.claim_id.localeCompare(b.claim_id));
  return idx;
}

function bestClaimRefsForText(claimIndex, pillarText, n = 6) {
  const t = setFromTokens(pillarText);
  if (!t.size) return [];

  const scored = [];
  for (const c of claimIndex) {
    const s = overlapScore(t, c.tok);
    if (s > 0) scored.push({ claim_id: c.claim_id, tier_group: c.tier_group, s, tier: c.tier ?? 99 });
  }

  scored.sort((a, b) => (b.s - a.s) || (a.tier - b.tier) || a.claim_id.localeCompare(b.claim_id));
  return scored.slice(0, Math.max(0, n | 0)).map(x => ({ claim_id: x.claim_id, tier_group: x.tier_group }));
}

// -----------------------------------------------------------------------------
// Candidate reading from markdown registry
// -----------------------------------------------------------------------------

function takeRegistryByType(registry, type, cap) {
  const out = Object.values(registry)
    .filter(x => x && x.type === type)
    .sort((a, b) => `${a.path}:${a.pillar_id}`.localeCompare(`${b.path}:${b.pillar_id}`));
  return out.slice(0, Math.max(0, cap | 0));
}

// -----------------------------------------------------------------------------
// content-pillars-v2 builder
// -----------------------------------------------------------------------------

function buildContentPillarsV2({
  prefix,
  runId,
  markdownPackSha1,
  evidenceSha1,
  markdownRegistry,
  markdownRegistrySha1,
  evidenceClaims,
  supplierSlugs,
  industrySlugs,
  framingMode = true
}) {
  const claimIndex = buildClaimIndex(evidenceClaims);
  const nextId = (() => {
    const seed = sha1OfString(String(prefix || "")).slice(0, 8);
    let i = 0;
    return () => `PIL-${seed}-${String(++i).padStart(3, "0")}`;
  })();

  const assertion_policy = {
    assertable_requires_claims: true,
    framing_requires_disclaimer: true,
    numeric_statements_require_claims: true
  };

  const guardrails = {
    framing_modal_language: ["may", "can help", "often", "typically", "in many cases"],
    framing_prohibited_patterns: ["\\d", "%", "£", "customer", "achieved", "reduced by", "increased by"]
  };

  // Choose seeds first (best pre-synthesised)
  const seeds = takeRegistryByType(markdownRegistry, "content_seed", 10);
  const supplier = takeRegistryByType(markdownRegistry, "markdown_pillar", 24);
  const industry = takeRegistryByType(markdownRegistry, "industry_pillar", 24);

  const core_pillars = [];
  const proof_enrichment = [];

  const usedTitleKey = new Set();
  function dedupeTitle(title) {
    const k = stableString(title).toLowerCase();
    if (!k) return true;
    if (usedTitleKey.has(k)) return true;
    usedTitleKey.add(k);
    return false;
  }

  function mkSourceRef(regEntry) {
    return {
      type: regEntry.type === "industry_pillar" ? "industry_pillar" : "markdown_pillar",
      supplier_slug: regEntry.supplier_slug || null,
      industry_slug: regEntry.industry_slug || null,
      pillar_id: regEntry.pillar_id
    };
  }

  function mkCorePillar({ title, mode, source_refs, claims }) {
    const id = nextId();
    return {
      id,
      title: stableString(title).slice(0, 140) || "Untitled pillar",
      mode: mode === "assertable" ? "assertable" : "framing",
      source_refs: safeArray(source_refs),
      claims: {
        value_prop: stableString(claims?.value_prop).slice(0, 800),
        why_it_matters: stableString(claims?.why_it_matters).slice(0, 800),
        constraints: safeArray(claims?.constraints).map(stableString).filter(Boolean).slice(0, 8)
      }
    };
  }

  function attachProof(pillar_id, claim_refs) {
    const refs = safeArray(claim_refs)
      .map(x => ({
        claim_id: stableString(x?.claim_id),
        tier_group: stableString(x?.tier_group || "other") || "other"
      }))
      .filter(x => x.claim_id);

    if (!refs.length) return null;
    return { pillar_id, claim_refs: refs };
  }

  function enforceAssertableHasClaims(corePillar, proof) {
    if (corePillar.mode !== "assertable") return { corePillar, proof };

    const has = proof && Array.isArray(proof.claim_refs) && proof.claim_refs.length > 0;
    if (has) return { corePillar, proof };

    // Strict default: fail upstream rather than silently downgrading.
    const err = new Error(`assertable_pillar_missing_claim_refs:${corePillar.id}`);
    err.code = "assertable_pillar_missing_claim_refs";
    err.pillar_id = corePillar.id;
    throw err;
  }

  // A) Seed-based pillars (prefer)
  for (const s of seeds) {
    if (core_pillars.length >= 10) break;
    if (!s) continue;

    const title = s.title || "Content pillar";
    if (dedupeTitle(title)) continue;

    const pillarText = `${s.title} | ${s.path}`;
    const claim_refs = bestClaimRefsForText(claimIndex, pillarText, 8);

    // Mode decision:
    // - If claims exist => assertable
    // - Else => framing (allowed)
    const mode = claim_refs.length ? "assertable" : "framing";

    const core = mkCorePillar({
      title,
      mode,
      source_refs: [mkSourceRef(s)],
      claims: {
        value_prop: s.title,
        why_it_matters: "",
        constraints: mode === "framing" ? ["FRAMING_ONLY_NO_PROOF_YET"] : []
      }
    });

    const proof = attachProof(core.id, claim_refs);
    enforceAssertableHasClaims(core, proof);

    core_pillars.push(core);
    if (proof) proof_enrichment.push(proof);
  }

  // B) Supplier + industry blend pillars
  for (const sp of supplier) {
    if (core_pillars.length >= 10) break;
    if (!sp) continue;

    const title = sp.title;
    if (dedupeTitle(title)) continue;

    // Find best industry match by token overlap (deterministic)
    const spTok = setFromTokens(`${sp.title} ${sp.path}`);
    let best = null;
    let bestScore = 0;
    for (const ind of industry) {
      const iTok = setFromTokens(`${ind.title} ${ind.path}`);
      const score = overlapScore(spTok, iTok);
      if (score > bestScore) { bestScore = score; best = ind; }
    }

    const combinedTitle = best ? `${sp.title} (${best.title})` : sp.title;
    if (dedupeTitle(combinedTitle)) continue;

    const pillarText = `${sp.title} | ${sp.path} | ${best ? `${best.title} | ${best.path}` : ""}`;
    const claim_refs = bestClaimRefsForText(claimIndex, pillarText, 8);

    const mode = claim_refs.length ? "assertable" : "framing";

    const core = mkCorePillar({
      title: combinedTitle,
      mode,
      source_refs: uniqNonEmpty([sp.pillar_id, best?.pillar_id].filter(Boolean)).map(pid => mkSourceRef(markdownRegistry[pid])),
      claims: {
        value_prop: sp.title,
        why_it_matters: best ? best.title : "",
        constraints: mode === "framing" ? ["FRAMING_ONLY_NO_PROOF_YET"] : []
      }
    });

    const proof = attachProof(core.id, claim_refs);
    enforceAssertableHasClaims(core, proof);

    core_pillars.push(core);
    if (proof) proof_enrichment.push(proof);
  }

  // Ensure deterministic order
  core_pillars.sort((a, b) => a.id.localeCompare(b.id));
  proof_enrichment.sort((a, b) => a.pillar_id.localeCompare(b.pillar_id));

  const stats = {
    total: core_pillars.length,
    assertable: core_pillars.filter(p => p.mode === "assertable").length,
    framing: core_pillars.filter(p => p.mode === "framing").length,
    proof_links: proof_enrichment.reduce((n, x) => n + (Array.isArray(x.claim_refs) ? x.claim_refs.length : 0), 0)
  };

  return {
    content: {
      schema: "content-pillars-v2",
      meta: {
        run_id: runId,
        generated_at: nowIso(),
        version: "02-01-2026 v3",
        generator: "campaign-pillars/index.js v3",
        provenance_validated: true,
        markdown_registry_sha1: markdownRegistrySha1
      },
      inputs: {
        markdown_pack_path: "evidence_v2/markdown_pack.json",
        markdown_pack_sha1: markdownPackSha1,
        evidence_path: "evidence.json",
        evidence_sha1: evidenceSha1,
        markdown_files: safeArray(markdownPackSha1 ? [{ path: "evidence_v2/markdown_pack.json", sha1: markdownPackSha1 }] : []),
        supplier_slugs: safeArray(supplierSlugs),
        industry_slugs: safeArray(industrySlugs)
      },
      assertion_policy,
      guardrails,
      core_pillars,
      proof_enrichment,
      stats
    },
    stats
  };
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

module.exports = async function (context, queueItem) {
  const log = context.log;
  const msg = parseQueueItem(queueItem);

  const prefix = normPrefix(msg.prefix || "");
  if (!prefix) {
    log("[pillars] missing prefix; aborting", { op: msg.op || null });
    return;
  }

  let runId =
    (stableString(msg.runId) || stableString(msg.run_id) || "") ||
    (prefix.split("/").filter(Boolean).pop() || "unknown");

  const page = msg.page || "campaign";

  log("[pillars] start", { runId, prefix, op: msg.op || null });

  const container = await getResultsContainerClient();
  const statusPath = `${prefix}status.json`;

  // Load status
  let status = (await getJson(container, statusPath)) || {};
  if (!status || typeof status !== "object") status = {};
  status.markers = (status.markers && typeof status.markers === "object") ? status.markers : {};

  // If already completed, do nothing
  if (status.markers.pillarsSynthCompleted === true) {
    log("[pillars] already completed; skipping", { runId, prefix });
    return;
  }

  // Load markdown pack
  const markdownPackPath = `${prefix}evidence_v2/markdown_pack.json`;
  let markdownPack = null;
  try { markdownPack = await getJson(container, markdownPackPath); } catch { markdownPack = null; }

  if (!markdownPack || typeof markdownPack !== "object") {
    await updateStatus(
      container,
      prefix,
      {
        state: "Failed",
        error: { code: "pillars_missing_markdown_pack", message: "evidence_v2/markdown_pack.json missing" },
        failedAt: nowIso()
      },
      { phase: "PillarsSynth", note: "failed: markdown_pack missing" }
    );
    throw new Error("PillarsSynth: evidence_v2/markdown_pack.json missing");
  }

  // Load evidence
  const evidencePath = `${prefix}evidence.json`;
  let evidenceBundle = null;
  try { evidenceBundle = await getJson(container, evidencePath); } catch { evidenceBundle = null; }

  if (!evidenceBundle || typeof evidenceBundle !== "object") {
    await updateStatus(
      container,
      prefix,
      {
        state: "Failed",
        error: { code: "pillars_missing_evidence", message: "evidence.json missing" },
        failedAt: nowIso()
      },
      { phase: "PillarsSynth", note: "failed: evidence missing" }
    );
    throw new Error("PillarsSynth: evidence.json missing");
  }

  const evidenceClaims = safeArray(evidenceBundle?.claims);

  // input.json for slugs + meta
  let input = {};
  try {
    const persisted = await getJson(container, `${prefix}input.json`);
    if (persisted && typeof persisted === "object") input = persisted;
  } catch { /* ignore */ }

  const supplier_slugs = uniqNonEmpty([
    input?.supplier_slug,
    input?.supplier_company_slug,
    markdownPack?.supplier_slug
  ].filter(Boolean));

  const industry_slugs = uniqNonEmpty([
    input?.selected_industry_slug,
    input?.campaign_industry_slug,
    markdownPack?.industry_slug
  ].filter(Boolean));

  const markdownPackSha1 = sha1OfJson(markdownPack);
  const evidenceSha1 = sha1OfJson(evidenceBundle);

  // Idempotency check: if existing v2 matches input hashes, skip rewrite
  let existing = null;
  try { existing = await getJson(container, `${prefix}content_pillars.json`); } catch { existing = null; }

  if (existing && typeof existing === "object") {
    const schemaOk = stableString(existing?.schema) === "content-pillars-v2";
    const inSha = existing?.inputs || {};
    if (
      schemaOk &&
      stableString(inSha.markdown_pack_sha1) === markdownPackSha1 &&
      stableString(inSha.evidence_sha1) === evidenceSha1
    ) {
      const contentPillarsSha1 = sha1OfJson(existing);

      status.markers.pillarsSynthCompleted = true;
      status.markers.contentPillarsSha1 = contentPillarsSha1;
      status.markers.markdownPackSha1 = markdownPackSha1;
      status.markers.evidenceSha1 = evidenceSha1;
      status.markers.contentPillarsLocked = true;
      status.updatedAt = nowIso();

      if (!Array.isArray(status.history)) status.history = [];
      status.history.push({
        at: nowIso(),
        phase: "pillars_idempotent_skip",
        note: "content_pillars.json already matches input hashes (v2)"
      });

      await putJson(container, statusPath, status);

      log("[pillars] content_pillars.json already matches; completed", { runId, prefix, sha1: contentPillarsSha1 });
      return;
    }
  }

  // Phase start marker
  await updateStatus(
    container,
    prefix,
    {
      state: "PillarsSynth",
      updatedAt: nowIso(),
      markers: {
        ...(status.markers || {}),
        markdownPackSha1,
        evidenceSha1
      }
    },
    { phase: "PillarsSynth", note: "start" }
  );

  // Build markdown registry (mechanical provenance check)
  const { registry, registrySha1, errors: registryErrors } = buildMarkdownPillarRegistry(markdownPack);
  if (registryErrors.length) {
    // Not fatal unless duplicates are severe; record warning for audit.
    log("[pillars] markdown registry warnings", registryErrors.slice(0, 6));
  }

  // Build content-pillars-v2
  let built;
  try {
    built = buildContentPillarsV2({
      prefix,
      runId,
      markdownPackSha1,
      evidenceSha1,
      markdownRegistry: registry,
      markdownRegistrySha1: registrySha1,
      evidenceClaims,
      supplierSlugs: supplier_slugs,
      industrySlugs: industry_slugs,
      framingMode: true
    });
  } catch (e) {
    // Hard fail: assertable pillars must have proof refs
    const code = e && e.code ? e.code : "pillars_build_failed";
    await updateStatus(
      container,
      prefix,
      {
        state: "Failed",
        error: { code, message: String(e?.message || e) },
        failedAt: nowIso()
      },
      { phase: "PillarsSynth", note: `failed: ${code}` }
    );
    throw e;
  }

  const contentPack = built.content;
  const contentPillarsSha1 = sha1OfJson(contentPack);

  // Persist content_pillars.json
  await putJson(container, `${prefix}content_pillars.json`, contentPack);

  // Update status markers
  const cur = (await getJson(container, statusPath)) || {};
  cur.markers = (cur.markers && typeof cur.markers === "object") ? cur.markers : {};
  if (!Array.isArray(cur.history)) cur.history = [];

  cur.markers.pillarsSynthCompleted = true;
  cur.markers.contentPillarsSha1 = contentPillarsSha1;
  cur.markers.markdownPackSha1 = markdownPackSha1;
  cur.markers.evidenceSha1 = evidenceSha1;
  cur.markers.contentPillarsLocked = true;

  cur.markers.contentPillarsTotal = built.stats.total;
  cur.markers.contentPillarsAssertableCount = built.stats.assertable;
  cur.markers.contentPillarsFramingCount = built.stats.framing;
  cur.markers.contentPillarsProofLinks = built.stats.proof_links;

  cur.state = "pillars_completed";
  cur.updatedAt = nowIso();
  cur.history.push({
    at: nowIso(),
    phase: "pillars_completed",
    note: `core_pillars=${built.stats.total}; assertable=${built.stats.assertable}; framing=${built.stats.framing}; proof_links=${built.stats.proof_links}`
  });

  await putJson(container, statusPath, cur);

  // Router handoff (idempotent)
  if (!cur.markers.afterpillarsSent) {
    await enqueueTo(ROUTER_QUEUE_NAME, { op: "afterpillars", runId, page, prefix });
    cur.markers.afterpillarsSent = true;
    cur.history.push({ at: nowIso(), phase: "router_enqueued", note: "afterpillars" });
    await putJson(container, statusPath, cur);
  }

  log("[pillars] completed", {
    runId,
    prefix,
    core_pillars: built.stats.total,
    assertable: built.stats.assertable,
    framing: built.stats.framing,
    sha1: contentPillarsSha1
  });
};
