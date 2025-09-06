from function_app import app
import azure.functions as func
import inspect

# Wrap the existing classic module to preserve behavior
from CampaignFetch import __init__ as legacy_fetch

@app.function_name("CampaignFetchV2")
@app.route(route="campaign/fetch",
           methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
async def campaign_fetch_v2(req: func.HttpRequest):
    res = legacy_fetch.main(req)  # classic fetch usually doesn't take 'starter'
    if inspect.isawaitable(res):
        return await res
    return res

