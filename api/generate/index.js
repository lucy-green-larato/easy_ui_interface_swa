const https = require("https");

/**
 * Azure Functions (Node) — /api/generate
 * - Supports: GET (health), POST (generate), OPTIONS (CORS)
 * - Calls Azure OpenAI via REST (no SDK, no extra deps)
 * - Tools: lead_qualification, intro_builder, email_gen, follow_up, competition, checklist
 */

module.exports = async function (context, req) {
  const CORS = {
    "Access-Control-Allow-Origin": process.env.ALLOWED_ORIGIN || "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization"
  };

  // CORS preflight
  if (req.method === "OPTIONS") {
    context.res = { status: 200, headers: CORS };
    return;
  }

  // Health check
  if (req.method === "GET") {
    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json", ...CORS },
      body: JSON.stringify({ ok: true, message: "GENERATE_OK" })
    };
    return;
  }

  try {
    const p = (req.body && typeof req.body === "object") ? req.body : {};
    const tool = String(p.tool || "").trim();

    // Validate tool
    const ALLOWED = new Set(["lead_qualification","intro_builder","email_gen","follow_up","competition","checklist"]);
    if (!ALLOWED.has(tool)) throw new Error("Unknown tool");

    // --- Build prompt (harden inputs a little) ---
    const clean = (v, max = 2000) =>
      String(v || "").replace(/\r/g, "").slice(0, max);

    // Normalise/trim fields used in prompts
    const N = {
      role:        clean(p.role),
      prospect:    clean(p.prospect),
      company:     clean(p.company),
      industry:    clean(p.industry),
      competitors: clean(p.competitors, 500),
      behaviour:   clean(p.behaviour),
      drivers:     clean(p.drivers),
      leaders:     clean(p.leaders),
      value:       clean(p.value, 1200),
      cta:         clean(p.cta, 200),
      first_touch: clean(p.first_touch, 1000),
      objections:  clean(p.objections, 800),
      criteria:    clean(p.criteria, 600)
    };

    const system =
      "You are a B2B technology sales specialist following Larato best practice. " +
      "Use only the evidence provided in the inputs. If data is missing, list it clearly. " +
      "Write concise, role-adapted outputs suitable for first-touch engagement.";

    const promptMap = {
      email_gen: (x) => `Write a first-touch email (75–140 words), personalised and evidence-based.

Recipient: ${x.prospect} (${x.role}) at ${x.company} (${x.industry})
Buyer behaviour: ${x.behaviour}
Purchase drivers: ${x.drivers}
Leaders & contacts: ${x.leaders}
Competitors: ${x.competitors}
Value points:
${x.value}
CTA: ${x.cta || "Suggest a 20-minute intro call next week."}

Output:
- 3 subject line options
- Email body
- One-sentence CTA
- P.S. with proof metric if present
List any material missing info at the end.`,

      intro_builder: (x) => `Create a first call introduction openers + talking points.

Role: ${x.role}
Prospect: ${x.prospect} at ${x.company} (${x.industry})
Behaviour: ${x.behaviour}
Drivers: ${x.drivers}
Leaders & contacts: ${x.leaders}
Competitors: ${x.competitors}
Value points:
${x.value}
Next step: ${x.cta || "Suggest a 20-minute intro call next week."}

Output:
- Call opener (one sentence)
- Role-aligned talking points (3 bullets)
- Differentiation (vs competitor if provided)
- Next step
Missing info: list if material.`,

      lead_qualification: (x) => `Early-stage lead qualification.

Role: ${x.role}   Company: ${x.company}   Industry: ${x.industry}
Behaviour: ${x.behaviour}
Drivers: ${x.drivers}
Leaders & contacts: ${x.leaders}
Competitors: ${x.competitors}

Output:
- Qualification summary (3–6 bullets)
- Viability score (High/Medium/Low) with reasons
- Critical gaps to close
- Immediate next steps (≤5)
- Risks & mitigations (≤5)
(No assumptions—flag missing info.)`,

      follow_up: (x) => `Follow-up plan for next 14 days.

Prospect/company: ${x.role} @ ${x.company}
First touch summary: ${x.first_touch || "N/A"}
Behaviour: ${x.behaviour}
Likely objections: ${x.objections || "N/A"}

Output:
- Cadence (day 2, day 5, day 10) with purpose
- Follow-up email (≤120 words)
- Voicemail script (≤45s)
- Three objection talk tracks
- One success metric and what to adjust`,

      competition: (x) => `Competitive positioning.

Competitor: ${x.competitors}
Behaviour: ${x.behaviour}
Decision criteria: ${x.criteria || "N/A"}
Value points:
${x.value}

Output:
- Where we win / where they win (3 and 3)
- Role-specific messaging (strategic, commercial, technical)
- Two proof points
- Two ethical landmine questions
- Risks and how to steer`,

      checklist: (x) => `First-step engagement checklist.

Role: ${x.role}
Company/Industry: ${x.company} / ${x.industry}
Behaviour: ${x.behaviour}

Output:
- Before (5–7 items)
- During (5–7 items)
- After (3–5 items)
- Missing info to capture (checklist)`
    };

    const user = promptMap[tool](N);

    // --- Azure OpenAI REST call ---
    const endpoint   = process.env.AZURE_OPENAI_ENDPOINT;      // e.g. https://<resource>.openai.azure.com
    const deployment = process.env.AZURE_OPENAI_DEPLOYMENT;    // deployment name (case-sensitive)
    const apiKey     = process.env.AZURE_OPENAI_API_KEY;
    const apiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-06-01";

    if (!endpoint || !deployment || !apiKey) {
      throw new Error("Configuration error: missing AOAI env vars");
    }

    const url = new URL(
      `${endpoint.replace(/\/+$/, "")}/openai/deployments/${deployment}/chat/completions?api-version=${encodeURIComponent(apiVersion)}`
    );

    const body = JSON.stringify({
      messages: [
        { role: "system", content: system },
        { role: "user",   content: user }
      ],
      temperature: 0.4
    });

    const { status, text } = await httpsPost(
      url,
      { "Content-Type": "application/json", "api-key": apiKey },
      body,
      25000 // timeout ms
    );

    if (status < 200 || status >= 300) {
      // surface AOAI error payload directly for fast diagnosis
      throw new Error(text || `AOAI HTTP ${status}`);
    }

    let j = {};
    try { j = JSON.parse(text); } catch (_) {}
    const content = j?.choices?.[0]?.message?.content?.trim() || "";

    // Extract up to 5 short “insights used” bullets from pasted blocks
    const insights = [];
    for (const block of [N.behaviour, N.drivers, N.leaders]) {
      (block || "")
        .split(/\n|;|\.|•|-/)
        .map(s => s.trim())
        .filter(Boolean)
        .slice(0, 3)
        .forEach(x => insights.push(x));
    }

    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json", ...CORS },
      body: JSON.stringify({ content, insightsUsed: insights.slice(0, 5) })
    };
  } catch (e) {
    context.log("AOAI error:", e?.message || e);
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json", ...CORS },
      body: JSON.stringify({ error: `${e.name}: ${e.message}` })
    };
  }
};

/** Minimal HTTPS POST helper (no deps) */
function httpsPost(url, headers, data, timeoutMs = 25000) {
  return new Promise((resolve, reject) => {
    const options = {
      method: "POST",
      hostname: url.hostname,
      path: url.pathname + url.search,
      port: 443,
      headers
    };
    const req = https.request(options, (res) => {
      const chunks = [];
      res.on("data", (d) => chunks.push(d));
      res.on("end", () => resolve({ status: res.statusCode, text: Buffer.concat(chunks).toString("utf8") }));
    });
    req.on("error", reject);
    req.setTimeout(timeoutMs, () => { req.destroy(new Error("Request timeout")); });
    if (data) req.write(data);
    req.end();
  });
}
