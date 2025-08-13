import os, json, logging
import azure.functions as func
from openai import AzureOpenAI

SYSTEM_SHARED = (
    "You are a B2B technology sales specialist following Larato best practice. "
    "Use only the evidence provided in the inputs. If data is missing, list it clearly. "
    "Write concise, role-adapted outputs suitable for first-touch engagement."
)

ALLOWED_TOOLS = {
    "lead_qualification","intro_builder","email_gen","follow_up","competition","checklist"
}

# --------- CORS ----------
def _cors_headers():
    return {
        "Access-Control-Allow-Origin": os.getenv("ALLOWED_ORIGIN", "*"),
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }

# --------- Lazy client with validation (avoids module import crashes) ----------
_client = None
def get_client():
    """
    Build the Azure OpenAI client only when first needed and validate required settings.
    If anything is missing/misnamed, we raise a controlled error that is returned to the caller.
    """
    global _client
    if _client is not None:
        return _client

    endpoint    = os.getenv("AZURE_OPENAI_ENDPOINT")     # e.g. https://<resource>.openai.azure.com
    deployment  = os.getenv("AZURE_OPENAI_DEPLOYMENT")   # model deployment name (exact)
    api_key     = os.getenv("AZURE_OPENAI_API_KEY")
    api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01")

    missing = [name for name, val in [
        ("AZURE_OPENAI_ENDPOINT", endpoint),
        ("AZURE_OPENAI_DEPLOYMENT", deployment),
        ("AZURE_OPENAI_API_KEY", api_key),
    ] if not val]
    if missing:
        raise RuntimeError(f"Missing required app settings: {', '.join(missing)}")

    # Save the deployment name on the client so we can read it in main()
    client = AzureOpenAI(azure_endpoint=endpoint, api_key=api_key, api_version=api_version)
    client._larato_deployment = deployment  # store for later use
    _client = client
    return _client

# --------- Prompt builders ----------
def build_prompt(tool: str, p: dict) -> str:
    if tool == "email_gen":
        return f"""Write a first-touch email (75–140 words), personalised and evidence-based.

Recipient: {p.get('prospect','')} ({p.get('role','')}) at {p.get('company','')} ({p.get('industry','')})
Buyer behaviour: {p.get('behaviour','')}
Purchase drivers: {p.get('drivers','')}
Leaders & contacts: {p.get('leaders','')}
Competitors: {p.get('competitors','')}
Value points:
{p.get('value','')}
CTA: {p.get('cta','Suggested 20-minute intro call next week.')}

Output:
- 3 subject line options
- Email body
- One-sentence CTA
- P.S. with proof metric if present
List any material missing info at the end."""
    if tool == "intro_builder":
        return f"""Create a first call introduction openers + talking points.

Role: {p.get('role','')}
Prospect: {p.get('prospect','')} at {p.get('company','')} ({p.get('industry','')})
Behaviour: {p.get('behaviour','')}
Drivers: {p.get('drivers','')}
Leaders & contacts: {p.get('leaders','')}
Competitors: {p.get('competitors','')}
Value points:
{p.get('value','')}
Next step: {p.get('cta','Suggest a 20-minute intro call next week.')}

Output:
- Call opener (one sentence)
- Role-aligned talking points (3 bullets)
- Differentiation (vs competitor if provided)
- Next step
Missing info: list if material."""
    if tool == "lead_qualification":
        return f"""Early-stage lead qualification.

Role: {p.get('role','')}  Company: {p.get('company','')}  Industry: {p.get('industry','')}
Behaviour: {p.get('behaviour','')}
Drivers: {p.get('drivers','')}
Leaders & contacts: {p.get('leaders','')}
Competitors: {p.get('competitors','')}
