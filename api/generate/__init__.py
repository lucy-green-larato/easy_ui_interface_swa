import os, json, logging
import azure.functions as func
from openai import AzureOpenAI

# -------- Env (all MUST be set in SWA > Configuration) --------
AOAI_ENDPOINT    = os.environ["AZURE_OPENAI_ENDPOINT"]      # e.g. https://<resource>.openai.azure.com
AOAI_DEPLOYMENT  = os.environ["AZURE_OPENAI_DEPLOYMENT"]    # deployment name, e.g. gpt-4o-mini OR your "sales-tools"
AOAI_API_KEY     = os.environ["AZURE_OPENAI_API_KEY"]
AOAI_API_VERSION = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-06-01")
ALLOWED_ORIGIN   = os.environ.get("ALLOWED_ORIGIN", "*")

# -------- Azure OpenAI client (API version REQUIRED for Azure) --------
client = AzureOpenAI(
    azure_endpoint=AOAI_ENDPOINT,
    api_key=AOAI_API_KEY,
    api_version=AOAI_API_VERSION
)

SYSTEM_SHARED = (
    "You are a B2B technology sales specialist following Larato best practice. "
    "Use only the evidence provided in the inputs. If data is missing, list it clearly. "
    "Write concise, role-adapted outputs suitable for first-touch engagement."
)

ALLOWED_TOOLS = {"lead_qualification","intro_builder","email_gen","follow_up","competition","checklist"}

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

Output:
- Qualification summary (3–6 bullets)
- Viability score (High/Medium/Low) with reasons
- Critical gaps to close
- Immediate next steps (≤5)
- Risks & mitigations (≤5)
(No assumptions—flag missing info.)"""
    if tool == "follow_up":
        return f"""Follow-up plan for next 14 days.

Prospect/company: {p.get('role','')} @ {p.get('company','')}
First touch summary: {p.get('first_touch','N/A')}
Behaviour: {p.get('behaviour','')}
Likely objections: {p.get('objections','N/A')}

Output:
- Cadence (day 2, day 5, day 10) with purpose
- Follow-up email (≤120 words)
- Voicemail script (≤45s)
- Three objection talk tracks
- One success metric and what to adjust"""
    if tool == "competition":
        return f"""Competitive positioning.

Competitor: {p.get('competitors','')}
Behaviour: {p.get('behaviour','')}
Decision criteria: {p.get('criteria','N/A')}
Value points:
{p.get('value','')}

Output:
- Where we win / where they win (3 and 3)
- Role-specific messaging (strategic, commercial, technical)
- Two proof points
- Two ethical landmine questions
- Risks and how to steer"""
    if tool == "checklist":
        return f"""First-step engagement checklist.

Role: {p.get('role','')}
Company/Industry: {p.get('company','')} / {p.get('industry','')}
Behaviour: {p.get('behaviour','')}

Output:
- Before (5–7 items)
- During (5–7 items)
- After (3–5 items)
- Missing info to capture (checklist)"""
    return "Unknown tool."

def extract_insights_used(p: dict) -> list:
    raw = " / ".join(filter(None, [p.get('behaviour',''), p.get('drivers',''), p.get('leaders','')]))
    out = []
    for seg in raw.replace("•"," ").replace(";"," ").splitlines():
        for part in seg.replace("."," ").split("-"):
            s = part.strip()
            if s: out.append(s)
    return out[:5]

def _cors():
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }

def main(req: func.HttpRequest) -> func.HttpResponse:
    # CORS preflight
    if req.method == "OPTIONS":
        return func.HttpResponse(status_code=200, headers=_cors())

    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400, headers=_cors())

    tool = (payload.get("tool") or "").strip()
    if tool not in ALLOWED_TOOLS:
        return func.HttpResponse("Unknown tool", status_code=400, headers=_cors())

    prompt = build_prompt(tool, payload)
    if prompt == "Unknown tool.":
        return func.HttpResponse("Unknown tool", status_code=400, headers=_cors())

    try:
        resp = client.chat.completions.create(
            model=AOAI_DEPLOYMENT,  # deployment name
            messages=[
                {"role": "system", "content": SYSTEM_SHARED},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4
        )
        content = (resp.choices[0].message.content if resp and resp.choices else "").strip()
        body = {"content": content, "insightsUsed": extract_insights_used(payload)}
        return func.HttpResponse(json.dumps(body), status_code=200, mimetype="application/json", headers=_cors())
    except Exception as e:
        logging.exception("Azure OpenAI call failed")
        err = {"error": f"{type(e).__name__}: {str(e)}"}
        return func.HttpResponse(json.dumps(err), status_code=500, mimetype="application/json", headers=_cors())
