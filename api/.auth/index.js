// Local dev stub for /.auth/me so it always returns JSON.
module.exports = async function (context, req) {
  context.res = {
    status: 200,
    headers: { "Content-Type": "application/json" },
    body: { clientPrincipal: null }
  };
};
