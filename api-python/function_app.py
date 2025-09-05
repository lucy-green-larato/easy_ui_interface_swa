# /api-python/function_app.py
import logging, json
import azure.functions as func
import azure.durable_functions as df

# One DFApp for v2 Durable. Default auth is FUNCTION; we override per-route below.
app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# -------------------------------
# HTTP starter (local-friendly)
# POST /api/orchestrators/CampaignOrchestration
# Returns: 202 + {"runId": "<instanceId>"}
# -------------------------------
@app.function_name(name="CampaignOrchestration_HttpStart")
@app.route(
    route="orchestrators/CampaignOrchestration",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS
)
@app.durable_client_input(client_name="client")
async def CampaignOrchestration_HttpStart(
    req: func.HttpRequest,
    client: df.DurableOrchestrationClient
) -> func.HttpResponse:
    try:
        # FIX: await start_new — it returns a coroutine in this worker/runtime
        instance_id = await client.start_new("CampaignOrchestration", None, None)
        return func.HttpResponse(
            json.dumps({"runId": instance_id}),
            status_code=202,
            mimetype="application/json"
        )
    except Exception as e:
        logging.exception("Failed to start orchestrator")
        return func.HttpResponse(
            f"Failed to start 'CampaignOrchestration': {e}",
            status_code=500,
            mimetype="text/plain"
        )

# --------------------------------
# Helper: import modules so their
# decorators register on *this* app
# --------------------------------
def _safe_import(module: str):
    try:
        __import__(module)
        logging.info("[function_app] loaded: %s", module)
    except Exception as exc:
        logging.warning("[function_app] skipped %s: %s", module, exc)

# HTTP endpoints
_safe_import("campaign.start.__init__")
_safe_import("campaign.status.__init__")
_safe_import("campaign.fetch.__init__")
_safe_import("campaign.regenerate.__init__")
_safe_import("campaign.download.__init__")
_safe_import("runs.index")

# Durable (orchestrator + activities)
_safe_import("orchestrators.campaign_orchestrator")
