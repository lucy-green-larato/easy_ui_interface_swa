# /api-python/function_app.py
import logging
import azure.functions as func
import azure.durable_functions as df

# One DFApp for the v2 model. Default HTTP auth is FUNCTION, but we override per-route.
app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# -------------------------------------------------------------------
# HTTP STARTER for Durable (diagnostic-friendly)
# POST /api/orchestrators/CampaignOrchestration
# -------------------------------------------------------------------
@app.function_name(name="CampaignOrchestration_HttpStart")
@app.route(
    route="orchestrators/CampaignOrchestration",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS  # allow curl locally with no keys
)
@app.durable_client_input(client_name="client")
def CampaignOrchestration_HttpStart(
    req: func.HttpRequest,
    client: df.DurableOrchestrationClient
):
    """
    Starts the orchestrator named EXACTLY 'CampaignOrchestration'.
    If anything fails, return HTTP 500 with the exception message so we can see the real cause.
    """
    try:
        instance_id = client.start_new("CampaignOrchestration", None, None)
        return client.create_check_status_response(req, instance_id)
    except Exception as e:
        # Log full details and return the message in-body for quick diagnosis
        logging.exception("Failed to start orchestrator")
        return func.HttpResponse(
            f"Failed to start 'CampaignOrchestration': {e}",
            status_code=500,
            mimetype="text/plain"
        )

# -------------------------------------------------------------------
# Helper to import modules so their decorators register on THIS app
# -------------------------------------------------------------------
def _safe_import(module: str):
    try:
        __import__(module)
        logging.info("[function_app] loaded: %s", module)
    except Exception as exc:
        logging.warning("[function_app] skipped %s: %s", module, exc)

# HTTP endpoints (Node-compatible helpers etc.)
_safe_import("campaign.start.__init__")
_safe_import("campaign.status.__init__")
_safe_import("campaign.fetch.__init__")
_safe_import("campaign.regenerate.__init__")
_safe_import("campaign.download.__init__")
_safe_import("runs.index")

# Durable: orchestrator + activities
# IMPORTANT: The orchestrator function inside this module MUST be named exactly "CampaignOrchestration"
# and decorated on THIS 'app' with the v2 decorators shown below (example).
_safe_import("orchestrators.campaign_orchestrator")
