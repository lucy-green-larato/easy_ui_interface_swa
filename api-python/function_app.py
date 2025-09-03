# /api-python/function_app.py
# One shared entrypoint for ALL Python v2 HTTP functions and Durable functions.

import azure.functions as func
import azure.durable_functions as df

# Shared apps that the host will load
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
dfapp = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# Import your modules so their decorators register on the shared apps.
# If a module is temporarily missing, we don't crash startup.
def _safe_import(name: str):
    try:
        __import__(name)
        print(f"[function_app] loaded: {name}")
    except Exception as e:
        print(f"[function_app] skipped {name}: {e}")

# HTTP endpoints
_safe_import("campaign.start.__init__")
_safe_import("campaign.status.__init__")
_safe_import("campaign.fetch.__init__")
_safe_import("campaign.regenerate.__init__")
_safe_import("campaign.download.__init__")
_safe_import("runs.index")

# Durable orchestrator + activities
_safe_import("orchestrators.campaign_orchestrator")
