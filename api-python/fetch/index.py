# /api-python/fetch/index.py
from function_app import app
import azure.functions as func
from CampaignFetch import main as fetch_main

@app.route(route="campaign/fetch", methods=["GET"])
async def fetch(req: func.HttpRequest) -> func.HttpResponse:
    # Delegate to the classic CampaignFetch.main (async)
    return await fetch_main(req)
