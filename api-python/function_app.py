# /api-python/function_app.py
import azure.functions as func
import azure.durable_functions as df

# ✅ Single app instance (DFApp can host HTTP routes + Durable)
app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

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

# Durable (orchestrator + activities)
_safe_import("orchestrators.campaign_orchestrator")
