module.exports = async function (context, req) {
  context.res = { headers: { "content-type": "application/json" }, body: { ok: true, where: "managed-swa-functions" } };
};
