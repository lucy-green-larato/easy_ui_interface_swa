module.exports = async function (context, req) {
  context.res = {
    status: 200,
    headers: { "Content-Type": "text/plain", "Access-Control-Allow-Origin": "*" },
    body: "pong"
  };
};
