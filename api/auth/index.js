// ALWAYS 200 with { clientPrincipal } — never 500.
function readPrincipal(req) {
  try {
    const b64 = req.headers && req.headers["x-ms-client-principal"];
    if (!b64) return null;
    const json = Buffer.from(b64, "base64").toString("utf8");
    return JSON.parse(json);
  } catch { return null; }
}
function isTruthy(v){ return /^(1|true|yes)$/i.test(String(v||"")); }
function devStub(){
  const email = process.env.AUTH_DEV_EMAIL || "dev@example.com";
  return { identityProvider:"dev", userId:"dev-user", userDetails:email, userRoles:["anonymous","authenticated"] };
}
module.exports = async function (_ctx, req) {
  try {
    let principal = readPrincipal(req);
    if (!principal && isTruthy(process.env.AUTH_DEV_ALWAYS)) principal = devStub();
    return { status:200, headers:{ "Content-Type":"application/json" }, body:{ clientPrincipal: principal } };
  } catch {
    return { status:200, headers:{ "Content-Type":"application/json" }, body:{ clientPrincipal:null, note:"auth bridge error" } };
  }
};
