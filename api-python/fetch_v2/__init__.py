# api-python/fetch_v2/__init__.py
from function_app import app
import azure.functions as func
import inspect, json, traceback
from CampaignFetch import __init__ as legacy_fetch

@app.function_name("CampaignFetchV2")
@app.route(route="campaign/fetch", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
async def campaign_fetch_v2(req: func.HttpRequest):
    try:
        res = legacy_fetch.main(req)
        return await res if inspect.isawaitable(res) else res
    except Exception as e:
        return func.HttpResponse(json.dumps({"error":"fetch_failed","message":str(e)}),
                                 status_code=500, mimetype="application/json")


