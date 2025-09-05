# /api-python/function_app.py
import azure.functions as func
import azure.durable_functions as df

# Single DFApp for HTTP + Durable
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _safe_import(name: str):
    try:
        __import__(name)
        print(f"[function_app] loaded: {name}")
    except Exception as e:
        print(f"[function_app] skipped {name}: {e}")

# ---- HTTP routes (DFApp-decorated, top-level) ----
_safe_import("status.__init__")       # /api-python/status/__init__.py
_safe_import("fetch.__init__")        # /api-python/fetch/__init__.py
_safe_import("download.__init__")     # optional
_safe_import("regenerate.__init__")   # optional
_safe_import("runs.index")            # /api-python/runs/index.py

# ---- Durable orchestrator + activities ----
_safe_import("orchestrators.campaign_orchestrator")

# NOTE: do NOT import start.__init__ while using the classic HttpStart in CampaignOrchestration_HttpStart/
