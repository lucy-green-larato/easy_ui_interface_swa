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

# ---- HTTP routes (match your actual folders) ----
_safe_import("status.__init__")   # /api-python/status/__init__.py -> /api/campaign/status
_safe_import("fetch.__init__")    # /api-python/fetch/__init__.py  -> /api/campaign/fetch
_safe_import("runs.index")        # /api-python/runs/index.py      -> /api/runs

# ---- Orchestrator + activities ----
_safe_import("orchestrators.campaign_orchestrator")
