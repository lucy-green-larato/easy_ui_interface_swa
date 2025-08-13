module.exports = async function (context, req) {
  const CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization"
  };

  if (req.method === "OPTIONS") {
    context.res = { status: 200, headers: CORS };
    return;
  }

  if (req.method === "GET") {
    context.res = {
      status: 200,
      headers: { "Content-Type": "application/json", ...CORS },
      body: JSON.stringify({ ok: true, message: "GENERATE_OK" })
    };
    return;
  }

  const payload = (req.body && typeof req.body === "object") ? req.body : {};
  context.res = {
    status: 200,
    headers: { "Content-Type": "application/json", ...CORS },
    body: JSON.stringify({ ok: true, received_tool: payload.tool, echo: payload })
  };
};
