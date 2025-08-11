import os, json, base64
import azure.functions as func
from azure.functions import FunctionApp, HttpRequest, HttpResponse
from openai import AzureOpenAI

app = FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _current_user(req: HttpRequest) -> Optional[dict]:
    hdr = req.headers.get("x-ms-client-principal")
    if not hdr: return None
    try:
        return json.loads(base64.b64decode(hdr).decode("utf-8"))
    except Exception:
        return None

def _load_system_prompt(tool_id: str) -> str:
    root = os.path.dirname(os.path.dirname(__file__))
    with open(os.path.join(root, "prompts", f"{tool_id}_system.md"), "r", encoding="utf-8") as f:
        return f.read()

@app.route(route="run-tool", methods=["POST"])
def run_tool(req: HttpRequest) -> HttpResponse:
    try: body = req.get_json()
    except ValueError: body = {}
    tool_id = body.get("toolId","sales_qualify")
    notes = (body.get("inputs",{}).get("notes") or "").strip()
    bias = body.get("inputs",{}).get("bias","balanced")
    system_prompt = _load_system_prompt(tool_id) + f"\n\nUser priority bias: {bias}"
    client = AzureOpenAI(azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
                         api_key=os.environ.get("AZURE_OPENAI_API_KEY"),
                         api_version=os.environ.get("AZURE_OPENAI_API_VERSION","2024-06-01"))
    dep = os.environ["AZURE_OPENAI_DEPLOYMENT"]
    completion = client.chat.completions.create(model=dep, messages=[
        {"role":"system","content":system_prompt},
        {"role":"user","content":notes or "No notes provided."}], temperature=0.2)
    text = completion.choices[0].message.content
    usage = getattr(completion,"usage",None)
    usage_out = {"model": dep, "total_tokens": usage.total_tokens} if usage else None
    return HttpResponse(json.dumps({"result": text, "usage": usage_out}, ensure_ascii=False), mimetype="application/json")

@app.route(route="ping", methods=["GET"])
def ping(req: HttpRequest) -> HttpResponse:
    return HttpResponse("ok", status_code=200)

@app.route(route="diag", methods=["GET"])
def diag(req: HttpRequest) -> HttpResponse:
    import os, json
    keys = ["AZURE_OPENAI_ENDPOINT","AZURE_OPENAI_API_VERSION","AZURE_OPENAI_DEPLOYMENT"]
    data = {k: os.environ.get(k, "") for k in keys}
    data["AZURE_OPENAI_API_KEY_SET"] = bool(os.environ.get("AZURE_OPENAI_API_KEY"))
    return HttpResponse(json.dumps(data), mimetype="application/json")

