# /api-python/function_app.py
import azure.functions as func
import azure.durable_functions as df

# Single DF (v2) app instance. Default HTTP auth = FUNCTION (keys required),
# but we will override to ANONYMOUS for the local starter route only.
app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# ---- HTTP STARTER for the Durable orchestrator (v2 model) ----
# Exposes: POST /api/orchestrators/CampaignOrchestration
# This starts the orchestrator named exactly "CampaignOrchestration"
@app.function_name(name="CampaignOrchestration_HttpStart")
@app.route(
    route="orchestrators/CampaignOrchestration",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS  # allow curl during local smoke tests
)
@app.durable_client_input(client_name="client")
def CampaignOrchestration_HttpStart(
    req: func.HttpRequest,
    client: df.DurableOrchestrationClient
):
    instance_id = client.start_new("CampaignOrchestration", None, None)
    return client.create_check_status_response(req, instance_id)

# ---- Helper to import other modules so their decorators register on *this* app ----
def _safe_import(module: str):
    try:
        __import__(module)
        print(f"[function_app] loaded: {module}")
    except Exception as e:
        print(f"[function_app] skipped {module}: {e}")

# HTTP endpoints (Node wrapper parity / convenience routes)
_safe_import("campaign.start.__init__")
_safe_import("campaign.status.__init__")
_safe_import("campaign.fetch.__init__")
_safe_import("campaign.regenerate.__init__")
_safe_import("campaign.download.__init__")
_safe_import("runs.index")

# Durable (orchestrator + activities)
# Ensure the orchestrator function is named "CampaignOrchestration"
_safe_import("orchestrators.campaign_orchestrator")

