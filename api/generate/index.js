// index.js — wrapper around generate-call-script.js

let realHandler;
try {
  realHandler = require("./generate-call-script.js");
} catch (e) {
  module.exports = async function (context, req) {
    context.log.error("[require(generate-call-script.js) failed]", e?.stack || e);
    context.res = {
      status: 500,
      headers: { "Content-Type": "application/json" },
      body: {
        error: "Require failed",
        detail: String(e?.message || e),
        stack: String(e?.stack || e),
      },
    };
  };
  return;
}

module.exports = realHandler;
