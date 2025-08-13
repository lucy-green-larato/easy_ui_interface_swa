import os, sys, json
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    info = {}

    # Python + OpenAI package presence
    info["python_version"] = sys.version
    try:
        import importlib.util
        info["openai_found"] = importlib.util.find_spec("openai") is not None
        if info["openai_found"]:
            import openai
            info["openai_version"] = getattr(openai, "__version__", "unknown")
    except Exception as e:
        info["openai_error"] = f"{type(e).__name__}: {e}"

    # Environment variables set?
    for k in ["AZURE_OPENAI_ENDPOINT","AZURE_OPENAI_DEPLOYMENT","AZURE_OPENAI_API_KEY","AZURE_OPENAI_API_VERSION"]:
        info[k] = bool(os.environ.get(k))

    # Try building an Azure OpenAI client
    try:
        from openai import AzureOpenAI
        client = AzureOpenAI(
            azure_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT",""),
            api_key=os.environ.get("AZURE_OPENAI_API_KEY",""),
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION","2024-06-01"),
        )
        info["client_ok"] = True
        info["deployment_name"] = os.environ.get("AZURE_OPENAI_DEPLOYMENT","")
    except Exception as e:
        info["client_ok"] = False
        info["client_error"] = f"{type(e).__name__}: {e}"

    return func.HttpResponse(
        json.dumps(info), status_code=200, mimetype="application/json",
        headers={"Access-Control-Allow-Origin":"*"}
    )
