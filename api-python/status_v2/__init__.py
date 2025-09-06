from function_app import app
import azure.functions as func
import inspect

# Wrap the existing classic module to preserve behavior
from CampaignStatus import __init__ as legacy_status

@app.function_name("CampaignStatusV2")
@app.route(route="campaign/status",
           methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
@app.durable_client_input(client_name="starter")
async def campaign_status_v2(req: func.HttpRequest, starter: str):
    res = legacy_status.main(req, starter)  # classic signature declares 'starter'
    if inspect.isawaitable(res):
        return await res
    return res
