const fetchFn = (typeof fetch !== "undefined")
  ? fetch
  : (...args) => import("node-fetch").then(m => m.default(...args));

module.exports = async function (context, req) {
  const base = process.env.PY_API_BASE || "http://127.0.0.1:7071";
  const url = `${base}/api/orchestrators/CampaignOrchestration`;
  context.log(`[campaign-start] POST ${url}`);

  try {
    const resp = await fetchFn(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req.body || {})
    });

    const text = await resp.text();
    let runId = "";

    // Try to extract a runId from any payload shape
    try {
      const js = text ? JSON.parse(text) : {};
      runId = js.runId || js.id || "";
      if (!runId && js.statusQueryGetUri) {
        const m = String(js.statusQueryGetUri).match(/instances\/([^\/\?]+)/);
        if (m) runId = m[1];
      }
    } catch (_) {
      // non-JSON upstream; keep text for error detail
    }

    if (!resp.ok && !runId) {
      return {
        status: resp.status,
        headers: { "Content-Type": "application/json" },
        body: { error: "upstream start failed", detail: text || null }
      };
    }

    if (!runId) {
      return {
        status: 502,
        headers: { "Content-Type": "application/json" },
        body: { error: "no runId in starter response", detail: text || null }
      };
    }

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: { runId }
    };
  } catch (e) {
    return {
      status: 500,
      headers: { "Content-Type": "application/json" },
      body: { error: "start error", detail: e.message }
    };
  }
};
