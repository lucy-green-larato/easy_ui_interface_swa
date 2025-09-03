# /api/function_app.py
# Single shared entry-point for Python v2 Functions + Durable.

import azure.functions as func
import azure.durable_functions as df

# Shared apps used by all modules
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
dfapp = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# Import modules so their decorators run at import time and attach to the shared apps
# (Guarded imports so missing endpoints don't crash local dev.)
for mod in (
    "campaign.start.__init__",
    "campaign.status.__init__",
    "campaign.fetch.__init__",
    "campaign.regenerate.__init__",
    "campaign.download.__init__",
    "runs.index",
    "orchestrators.campaign_orchestrator",
):
    try:
        __import__(mod)
    except Exception as e:
        # Optional: print to logs; doesn't break startup if some endpoints aren't present yet.
        print(f"[function_app] optional import skipped: {mod} ({e})")
