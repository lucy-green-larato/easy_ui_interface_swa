// /api/campaign-linkedin/index.js
// LinkedIn Activation (Option B) — downstream-only, bounded to validated artefacts
// 15-12-2025 v1.0
//
// Trigger: run_linkedin (enqueued only after afterwrite)
// Reads:  strategy_v2/campaign_strategy.json, campaign.json
// Writes: linkedin.json
//
// Node 20 / Azure Functions v4 / CommonJS

"use strict";

const { getResultsContainerClient, getJson, putJson } = require("../shared/storage");

const RESULTS_CONTAINER =
  process.env.CAMPAIGN_RESULTS_CONTAINER ||
  process.env.RESULTS_CONTAINER ||
  "results";

// Azure OpenAI (preferred)
const AZURE_OPENAI_ENDPOINT = process.env.AZURE_OPENAI_ENDPOINT || process.env.AZURE_OPENAI_BASE_URL || "";
const AZURE_OPENAI_KEY = process.env.AZURE_OPENAI_API_KEY || "";
const AZURE_OPENAI_DEPLOYMENT = process.env.AZURE_OPENAI_DEPLOYMENT || "";
const AZURE_OPENAI_API_VERSION = process.env.AZURE_OPENAI_API_VERSION || "2024-08-01-preview";

// OpenAI (fallback)
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";

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

function nowISO() {
  return new Date().toISOString();
}

function pushHistory(status, phase, note) {
  if (!status.history || !Array.isArray(status.history)) status.history = [];
  status.history.push({
    at: nowISO(),
    phase: String(phase || "status"),
    note: note ? String(note) : ""
  });
}

function safeArray(v) {
  return Array.isArray(v) ? v : [];
}

function takeStrings(arr, n) {
  return safeArray(arr).map(x => (x == null ? "" : String(x))).map(s => s.trim()).filter(Boolean).slice(0, n);
}

function compactText(s, max = 500) {
  const t = (s == null ? "" : String(s)).trim();
  return t.length <= max ? t : (t.slice(0, max - 1) + "…");
}

function buildActivationInputs(campaign, strategy) {
  const sv2 = (strategy && typeof strategy === "object") ? strategy : {};
  const story = sv2.story_spine || {};
  const vp = sv2.value_proposition || {};
  const buyer = sv2.buyer_strategy || {};
  const gtm = sv2.gtm_strategy || {};
  const proof = sv2.proof_points || [];

  const out = {
    story_spine: {
      environment: takeStrings(story.environment, 6),
      case_for_action: takeStrings(story.case_for_action, 6),
      how_we_win: takeStrings(story.how_we_win, 6),
      success: takeStrings(story.success, 6),
      next_steps: takeStrings(story.next_steps, 6)
    },
    value_proposition: {
      moore_chain: (vp && typeof vp === "object" && vp.moore_chain) ? vp.moore_chain : null
    },
    buyer_strategy: {
      problems: takeStrings(buyer.problems, 10),
      urgency: takeStrings(buyer.urgency, 8),
      barriers: takeStrings(buyer.barriers, 8)
    },
    gtm_strategy: {
      route_implications: takeStrings(gtm.route_implications, 8)
    },
    proof_points: takeStrings(proof, 8),
    campaign_fields: {
      // campaign.json may contain additional messaging architecture; keep it bounded
      executive_summary_title: compactText(campaign?.executive_summary?.title || "", 140),
      messaging_pillars: takeStrings(campaign?.messaging_matrix?.pillars, 8),
      key_audiences: takeStrings(campaign?.messaging_matrix?.audiences, 8),
      support_points: takeStrings(campaign?.messaging_matrix?.support_points, 10)
    }
  };

  return out;
}

function emptyOutput() {
  return {
    schema: "linkedin-activation-v1",
    generated_at: nowISO(),
    derived_from: { strategy: "strategy_v2", campaign: "campaign.json" },
    posts: []
  };
}

function validateLinkedInJson(obj) {
  if (!obj || typeof obj !== "object") return { ok: false, reason: "not_object" };
  if (obj.schema !== "linkedin-activation-v1") return { ok: false, reason: "bad_schema" };
  if (!Array.isArray(obj.posts)) return { ok: false, reason: "posts_not_array" };

  // enforce a sane max
  if (obj.posts.length > 12) return { ok: false, reason: "too_many_posts" };

  for (const p of obj.posts) {
    if (!p || typeof p !== "object") return { ok: false, reason: "post_not_object" };
    const req = ["id", "angle", "hook", "body", "cta", "tone", "evidence_refs"];
    for (const k of req) if (!(k in p)) return { ok: false, reason: `missing_${k}` };

    if (!Array.isArray(p.evidence_refs)) return { ok: false, reason: "evidence_refs_not_array" };

    // hard caps for safety
    if (String(p.hook || "").length > 240) return { ok: false, reason: "hook_too_long" };
    if (String(p.body || "").length > 2500) return { ok: false, reason: "body_too_long" };
    if (String(p.cta || "").length > 240) return { ok: false, reason: "cta_too_long" };
  }

  return { ok: true };
}

async function callAzureOpenAI({ system, user }) {
  if (!AZURE_OPENAI_ENDPOINT || !AZURE_OPENAI_KEY || !AZURE_OPENAI_DEPLOYMENT) {
    throw new Error("Azure OpenAI not configured (endpoint/key/deployment missing)");
  }

  const url =
    `${AZURE_OPENAI_ENDPOINT.replace(/\/+$/, "")}` +
    `/openai/deployments/${encodeURIComponent(AZURE_OPENAI_DEPLOYMENT)}` +
    `/chat/completions?api-version=${encodeURIComponent(AZURE_OPENAI_API_VERSION)}`;

  const payload = {
    temperature: 0.3,
    top_p: 0.9,
    max_tokens: 1400,
    messages: [
      { role: "system", content: system },
      { role: "user", content: user }
    ]
  };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "api-key": AZURE_OPENAI_KEY
    },
    body: JSON.stringify(payload)
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Azure OpenAI error ${res.status}: ${text.slice(0, 300)}`);
  }

  const json = JSON.parse(text);
  const content = json?.choices?.[0]?.message?.content || "";
  return String(content);
}

async function callOpenAI({ system, user }) {
  if (!OPENAI_API_KEY) throw new Error("OpenAI not configured (OPENAI_API_KEY missing)");

  const url = "https://api.openai.com/v1/chat/completions";
  const payload = {
    model: OPENAI_MODEL,
    temperature: 0.3,
    max_tokens: 1400,
    messages: [
      { role: "system", content: system },
      { role: "user", content: user }
    ]
  };

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": `Bearer ${OPENAI_API_KEY}`
    },
    body: JSON.stringify(payload)
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`OpenAI error ${res.status}: ${text.slice(0, 300)}`);

  const json = JSON.parse(text);
  const content = json?.choices?.[0]?.message?.content || "";
  return String(content);
}

function buildPrompts({ runId, activationInputs }) {
  const system = [
    "You generate LinkedIn activation content ONLY from provided inputs.",
    "DO NOT invent facts, competitors, metrics, buyer problems, or claims.",
    "DO NOT introduce new strategic ideas. You only express existing strategy.",
    "Return STRICT JSON only. No markdown. No commentary.",
    "Schema must be linkedin-activation-v1 with fields: schema, generated_at, derived_from, posts[].",
    "Each post must contain: id, angle, hook, body, cta, evidence_refs (array), tone.",
    "evidence_refs must be empty array unless claim_ids are explicitly provided in inputs (none are, unless shown).",
    "Tone must be one of: insightful, authoritative, pragmatic.",
    "Hook <= 240 chars. CTA <= 240 chars. Body <= 2500 chars.",
    "If inputs are too thin, return posts: [] (empty), still valid schema.",
  ].join("\n");

  const user = JSON.stringify(
    {
      runId,
      doctrine: {
        linkedInRole: "activation_surface_only",
        downstreamOnly: true,
        forbidden: [
          "new buyer problems",
          "new competitors",
          "new market stats",
          "overriding strategy"
        ]
      },
      inputs: activationInputs,
      required_output: {
        schema: "linkedin-activation-v1",
        generated_at: "ISO-8601",
        derived_from: { strategy: "strategy_v2", campaign: "campaign.json" },
        posts_count_target: 5,
        post_angles_allowed: ["buyer_problem", "industry_shift", "competitive_reframe", "execution_hint", "proof_point"],
        evidence_refs_rule: "empty_array_unless_claim_ids_provided"
      }
    },
    null,
    2
  );

  return { system, user };
}

module.exports = async function (context, queueItem) {
  const log = context.log;

  const msg = parseQueueItem(queueItem);

  const op = String(msg.op || "").trim();
  const runId = msg.runId || msg.run_id || "";
  const page = msg.page || "campaign";
  const prefix = normPrefix(msg.prefix || "");

  log("[linkedin] received", { op, runId, prefix });

  if (op !== "run_linkedin") {
    log("[linkedin] ignoring op", op);
    return;
  }
  if (!runId || !prefix) {
    log("[linkedin] missing runId or prefix");
    return;
  }

  const container = await getResultsContainerClient();
  const statusPath = `${prefix}status.json`;

  // Load status for history (fail-safe)
  let status = (await getJson(container, statusPath)) || { runId, markers: {}, history: [] };
  if (!status || typeof status !== "object") status = { runId, markers: {}, history: [] };
  if (!status.markers || typeof status.markers !== "object") status.markers = {};
  if (!Array.isArray(status.history)) status.history = [];

  pushHistory(status, "linkedin_working");
  await putJson(container, statusPath, status);

  // --- HARD GATES ---
  const strategyPath = `${prefix}strategy_v2/campaign_strategy.json`;
  const campaignPath = `${prefix}campaign.json`;
  const linkedinPath = `${prefix}linkedin.json`;

  const strategy = await getJson(container, strategyPath);
  const campaign = await getJson(container, campaignPath);

  // Gate on existence
  if (!strategy || !campaign) {
    const missing = [
      !strategy ? "strategy_v2/campaign_strategy.json" : null,
      !campaign ? "campaign.json" : null
    ].filter(Boolean);

    log("[linkedin] gate failed: missing inputs", missing);

    const out = emptyOutput();
    out.note = `Skipped: missing required inputs: ${missing.join(", ")}`;

    await putJson(container, linkedinPath, out);

    status.markers.linkedinWritten = true;
    pushHistory(status, "linkedin_written", out.note);
    await putJson(container, statusPath, status);
    return;
  }

  // Gate on completed/assembled if present (do not fail, just record)
  const st = String(status.state || "").toLowerCase();
  const okState = (st === "assembled" || st === "completed" || st === "writer_ready" || st === "completed");
  if (!okState) {
    log("[linkedin] warning: status.state not terminal-ish", status.state);
    pushHistory(status, "linkedin_gate_warn", `state=${status.state || "(none)"}`);
    await putJson(container, statusPath, status);
  }

  // Build bounded inputs
  const activationInputs = buildActivationInputs(campaign, strategy);

  const { system, user } = buildPrompts({ runId, activationInputs });

  // Call model (Azure preferred, OpenAI fallback)
  let raw = "";
  try {
    if (AZURE_OPENAI_ENDPOINT && AZURE_OPENAI_KEY && AZURE_OPENAI_DEPLOYMENT) {
      raw = await callAzureOpenAI({ system, user });
    } else if (OPENAI_API_KEY) {
      raw = await callOpenAI({ system, user });
    } else {
      throw new Error("No LLM configured (Azure OpenAI or OpenAI)");
    }
  } catch (err) {
    log("[linkedin] model call failed", String(err?.message || err));

    const out = emptyOutput();
    out.note = "Skipped: model call failed";

    await putJson(container, linkedinPath, out);

    status.markers.linkedinWritten = true;
    pushHistory(status, "linkedin_written", out.note);
    await putJson(container, statusPath, status);
    return;
  }

  // Parse + validate output (fail closed)
  let obj = null;
  try {
    obj = JSON.parse(raw);
  } catch {
    obj = null;
  }

  if (!obj) {
    log("[linkedin] invalid JSON from model (not parsable)");

    const out = emptyOutput();
    out.note = "Model returned non-JSON output; wrote empty activation pack";

    await putJson(container, linkedinPath, out);

    status.markers.linkedinWritten = true;
    pushHistory(status, "linkedin_written", out.note);
    await putJson(container, statusPath, status);
    return;
  }

  // Force required top-level fields (defensive)
  obj.schema = "linkedin-activation-v1";
  obj.generated_at = nowISO();
  obj.derived_from = { strategy: "strategy_v2", campaign: "campaign.json" };
  if (!Array.isArray(obj.posts)) obj.posts = [];

  // Validate structure & caps
  const v = validateLinkedInJson(obj);
  if (!v.ok) {
    log("[linkedin] invalid JSON schema from model", v.reason);

    const out = emptyOutput();
    out.note = `Model output failed validation: ${v.reason}`;

    await putJson(container, linkedinPath, out);

    status.markers.linkedinWritten = true;
    pushHistory(status, "linkedin_written", out.note);
    await putJson(container, statusPath, status);
    return;
  }

  // Write
  await putJson(container, linkedinPath, obj);

  status.markers.linkedinWritten = true;
  pushHistory(status, "linkedin_written", `posts=${obj.posts.length}`);
  await putJson(container, statusPath, status);

  log("[linkedin] written", { runId, path: linkedinPath, posts: obj.posts.length });
};
