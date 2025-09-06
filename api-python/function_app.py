# function_app.py
import os, sys, importlib, pathlib
import azure.functions as func
import azure.durable_functions as df

# Ensure the function app root is importable regardless of cwd
APP_ROOT = pathlib.Path(__file__).parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def _safe_import(name: str):
    importlib.import_module(name)

# Register decorated functions
_safe_import("orchestrators.campaign_orchestrator")
_safe_import("runs.index")
_safe_import("start")  # ← import the package (executes start/__init__.py)
# leave other optional modules commented unless they exist
# _safe_import("download")     
# _safe_import("regenerate")
