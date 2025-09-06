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

# DFApp modules ONLY — do NOT import classic functions (CampaignStatus/CampaignFetch)
_safe_import("orchestrators.campaign_orchestrator")
_safe_import("runs.index")   # keep if you want /api/runs; comment out if not needed

# leave these out for now to avoid import-time errors:
# _safe_import("download.__init__")
# _safe_import("regenerate.__init__")
