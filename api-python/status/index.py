# /api-python/status/index.py
from function_app import app
import azure.functions as func
import azure.durable_functions as df
from CampaignStatus import main as status_main

@app.route(route="campaign/status", methods=["GET"])
@app.durable_client_input(client_name="starter")
async def status(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    # Delegate to the classic CampaignStatus.main (async)
    return await status_main(req, starter)
