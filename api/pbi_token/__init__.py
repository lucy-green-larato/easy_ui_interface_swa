import azure.functions as func
import os, json, requests
import msal

TENANT_ID     = os.environ["TENANT_ID"]
CLIENT_ID     = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
WORKSPACE_ID  = os.environ["WORKSPACE_ID"]
REPORT_ID     = os.environ["REPORT_ID"]
DATASET_ID    = os.environ["DATASET_ID"]

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE     = ["https://analysis.windows.net/powerbi/api/.default"]
PBI_API   = "https://api.powerbi.com/v1.0/myorg"

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _aad_token():
    cca = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
    )
    result = cca.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(result.get("error_description", "Failed to get AAD token"))
    return result["access_token"]

@app.route(route="pbi-token", methods=["GET"])
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        bearer = _aad_token()
        headers = {"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"}

        # 1) get report to read embedUrl (handy if it ever changes)
        r = requests.get(f"{PBI_API}/groups/{WORKSPACE_ID}/reports/{REPORT_ID}",
                         headers=headers, timeout=20)
        r.raise_for_status()
        embed_url = r.json()["embedUrl"]

        # 2) generate embed token for report + dataset
        body = {
            "accessLevel": "View",
            "allowSaveAs": False,
            "reports":  [{"id": REPORT_ID}],
            "datasets": [{"id": DATASET_ID}],
            "targetWorkspaces": [{"id": WORKSPACE_ID}]
        }
        t = requests.post(f"{PBI_API}/GenerateToken", headers=headers, json=body, timeout=20)
        t.raise_for_status()
        token = t.json()["token"]

        return func.HttpResponse(
            json.dumps({"embedUrl": embed_url, "reportId": REPORT_ID, "token": token}),
            mimetype="application/json"
        )
    except Exception as e:
        # surface the message while you're testing; you can tighten later
        return func.HttpResponse(str(e), status_code=500)
