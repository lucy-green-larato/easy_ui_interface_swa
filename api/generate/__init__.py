import json
import azure.functions as func

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization"
}

def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse(status_code=200, headers=CORS)
    if req.method == "GET":
        return func.HttpResponse(json.dumps({"ok": True, "message": "GENERATE_OK"}), status_code=200,
                                 mimetype="application/json", headers=CORS)
    try:
        payload = req.get_json()
    except ValueError:
        payload = {}
    return func.HttpResponse(
        json.dumps({"ok": True, "received_tool": payload.get("tool"), "echo": payload}),
        status_code=200, mimetype="application/json", headers=CORS
    )
