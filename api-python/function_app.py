import azure.functions as func
import azure.durable_functions as df

# One DFApp for Durable + any DFApp HTTP you keep
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _safe_import(name: str):
    __import__(name)

# DFApp modules ONLY — do NOT import classic functions (CampaignStatus/CampaignFetch)
_safe_import("orchestrators.campaign_orchestrator")
_safe_import("runs.index")
_safe_import("start.__init__")

# leave these out for now to avoid import-time errors:
# _safe_import("download.__init__")
# _safe_import("regenerate.__init__")
