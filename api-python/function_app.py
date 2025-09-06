# /api-python/function_app.py
import azure.functions as func
import azure.durable_functions as df

# One DFApp for Durable + any DFApp HTTP you keep
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _safe_import(name: str):
    try:
        __import__(name)
        print(f"[function_app] loaded: {name}")
    except Exception as e:
        print(f"[function_app] skipped {name}: {e}")

# ---- DFApp modules ONLY ----
# Do NOT import status/fetch here; they are classic functions now.
_safe_import("orchestrators.campaign_orchestrator")

# If these files use DFApp decorators in your repo, keep them; otherwise remove these lines.
_safe_import("runs.index")
_safe_import("download.__init__")
_safe_import("regenerate.__init__")
