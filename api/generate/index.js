const https = require("https");

module.exports = async function (context, req) {
  const CORS = {
    "Access-Control-Allow-Origin": process.env.ALLOWED_ORIGIN || "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization"
  };

  if (req.method === "OPTIONS") { context.res = { status: 200, headers: CORS }; return; }
  if (req.method === "GET") {
    context.res = { status: 200, headers: { "Content-Type":"application/json", ...CORS },
      body: JSON.stringify({ ok:true, message:"GENERATE_OK" }) };
    return;
  }

  try {
    const p = (req.body && typeof req.body === "object") ? req.body : {};
    const tool = (p.tool || "").trim();
    const allowed = new Set(["lead_qualification","intro_builder","email_gen","follow_up","competition","checklist"]);
    if (!allowed.has(tool)) throw new Error("Unknown tool");

    const system = "You are a B2B technology sales specialist following Larato best practice. Use only the evidence provided. If data is missing, list it clearly. Be concise and role-adapted.";

    const promptMap = {
      email_gen: (p)=>`Write a first-touch email (75–140 words), personalised and evidence-based.

Recipient: ${p.prospect||""} (${p.role||""}) at ${p.company||""} (${p.industry||""})
Buyer behaviour: ${p.behaviour||""}
Purchase drivers: ${p.drivers||""}
Leaders & contacts: ${p.leaders||""}
Competitors: ${p.competitors||""}
Value points:
${p.value||""}
CTA: ${p.cta||"Suggest a 20-minute intro call next week."}

Output:
- 3 subject line options
- Email body
- One-sentence CTA
- P.S. with proof metric if present
List any material missing info at the end.`,
      intro_builder: (p)=>`Create a first call introduction openers + talking points.

Role: ${p.role||""}
Prospect: ${p.prospect||""} at ${p.company||""} (${p.industry||""})
Behaviour: ${p.behaviour||""}
Drivers: ${p.drivers||""}
Leaders & contacts: ${p.leaders||""}
Competitors: ${p.competitors||""}
Value points:
${p.value||""}
Next step: ${p.cta||"Suggest a 20-minute intro call next week."}

Output:
- Call opener (one sentence)
- Role-aligned talking points (3 bullets)
- Differentiation (vs competitor if provided)
- Next step
Missing info: list if material.`
      // add other tools when ready
    };

    const user = (promptMap[tool] || (()=>"Unknown tool"))(p);
    if (user === "Unknown tool") throw new Error("Unknown tool");

    const endpoint   = process.env.AZURE_OPENAI_ENDPOINT;      // e.g. https://<resource>.openai.azure.com
    const deployment = process.env.AZURE_OPENAI_DEPLOYMENT;    // deployment name (case-sensitive)
    const apiKey     = process.env.AZURE_OPENAI_API_KEY;
    const apiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-06-01";
    if (!endpoint || !deployment || !apiKey) throw new Error("Configuration error: missing AOAI env vars");

    const url = new URL(`${endpoint.replace(/\/+$/,"")}/openai/deployments/${deployment}/chat/completions?api-version=${encodeURIComponent(apiVersion)}`);
    const body = JSON.stringify({
      messages: [
        { role: "system", content: system },
        { role: "user",   content: user }
      ],
      temperature: 0.4
    });

    const { status, text } = await httpsPost(url, {
      "Content-Type": "application/json",
      "api-key": apiKey
    }, body, 25000);

    if (status < 200 || status >= 300) {
      throw new Error(text || `AOAI HTTP ${status}`);
    }

    let j = {};
    try { j = JSON.parse(text); } catch {}
    const content = j?.choices?.[0]?.message?.content?.trim() || "";

    // quick insights from pasted blocks
    const insights = [];
    for (const block of [p.behaviour||"", p.drivers||"", p.leaders||""]) {
      (block || "").split(/\n|;|\.|•|-/).map(s=>s.trim()).filter(Boolean).slice(0,3).forEach(x => insights.push(x));
    }

    context.res = {
      status: 200,
      headers: { "Content-Type":"application/json", ...CORS },
      body: JSON.stringify({ content, insightsUsed: insights.slice(0,5) })
    };
  } catch (e) {
    context.log("AOAI error:", e?.message || e);
    context.res = {
      status: 500,
      headers: { "Content-Type":"application/json", ...CORS },
      body: JSON.stringify({ error: `${e.name}: ${e.message}` })
    };
  }
};

/** Minimal HTTPS POST helper (no deps) */
function httpsPost(url, headers, data, timeoutMs=25000) {
  return new Promise((resolve, reject) => {
    const options = {
      method: "POST",
      hostname: url.hostname,
      path: url.pathname + url.search,
      port: 443,
      headers
    };
    const req = https.request(options, (res) => {
      let chunks = [];
      res.on("data", (d) => chunks.push(d));
      res.on("end", () => resolve({ status: res.statusCode, text: Buffer.concat(chunks).toString("utf8") }));
    });
    req.on("error", reject);
    req.setTimeout(timeoutMs, () => { req.destroy(new Error("Request timeout")); });
    if (data) req.write(data);
    req.end();
  });
}
