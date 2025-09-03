# /api/campaign/download/__init__.py
# GET .docx export built from campaign.json + evidence_log.json
# Usage:
#   GET /api/campaign/download?runId=<id>
#
# Requires dependency in /api/requirements.txt:
#   python-docx>=0.8.11

import os
import io
import json
from urllib.parse import urlsplit

import azure.functions as func
import azure.durable_functions as df
from azure.storage.blob import BlobServiceClient
from docx import Document
from docx.shared import Pt, Inches


from function_app import app


def _blob_service() -> BlobServiceClient:
    # Build a BlobServiceClient from UPLOADS_SAS_URL:
    # https://<account>.blob.core.windows.net/?sv=...&ss=b&...
    sas_url = os.environ["UPLOADS_SAS_URL"].strip()
    parts = urlsplit(sas_url)
    account_base = f"{parts.scheme}://{parts.netloc}"
    sas_token = parts.query.lstrip("?")
    return BlobServiceClient(account_url=account_base, credential=sas_token)


def _results_container_name() -> str:
    return os.environ["CAMPAIGN_RESULTS_CONTAINER"]


def _find_blob_path(cc, run_id: str, filename: str) -> str:
    prefix = "results/campaign/"
    for b in cc.list_blobs(name_starts_with=prefix):
        if b.name.endswith(f"/{run_id}/{filename}"):
            return b.name
    return ""


def _add_heading(doc: Document, text: str, level: int = 1):
    h = doc.add_heading(text, level=level)
    return h


def _add_para(doc: Document, text: str):
    doc.add_paragraph(text if text else "")


def _add_table_from_evidence(doc: Document, evidence: list):
    if not evidence:
        return
    headers = ["ID", "Publisher", "Title", "Date", "URL"]
    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Light List"
    hdr_cells = table.rows[0].cells
    for i, h in enumerate(headers):
        hdr_cells[i].text = h
    for item in evidence:
        row = table.add_row().cells
        row[0].text = str(item.get("id", ""))
        row[1].text = str(item.get("publisher", ""))
        row[2].text = str(item.get("title", ""))
        row[3].text = str(item.get("date", ""))
        row[4].text = str(item.get("url", ""))


@app.route(route="campaign/download", methods=["GET"])
def download(req: func.HttpRequest) -> func.HttpResponse:
    run_id = req.params.get("runId")
    if not run_id:
        return func.HttpResponse("Missing runId", status_code=400)

    try:
        bsc = _blob_service()
        container = _results_container_name()
        cc = bsc.get_container_client(container)

        # Load campaign.json
        path_campaign = _find_blob_path(cc, run_id, "campaign.json")
        if not path_campaign:
            return func.HttpResponse("campaign.json not found", status_code=404)
        campaign = json.loads(cc.get_blob_client(path_campaign).download_blob().readall())

        # Load evidence (optional)
        evidence = []
        path_evidence = _find_blob_path(cc, run_id, "evidence_log.json")
        if path_evidence:
            evidence = json.loads(cc.get_blob_client(path_evidence).download_blob().readall())

        # Build .docx
        doc = Document()
        doc.core_properties.title = f"Campaign {run_id}"

        _add_heading(doc, "Evidence-first Campaign Pack", level=0)
        _add_para(doc, f"Run ID: {run_id}")

        exec_sum = campaign.get("executive_summary") or campaign.get("overview") or ""
        if exec_sum:
            _add_heading(doc, "Executive summary", level=1)
            _add_para(doc, exec_sum)

        lp = campaign.get("landing_page") or {}
        if lp:
            _add_heading(doc, "Landing page", level=1)
            for key in ["hero", "why_it_matters", "what_you_get", "how_it_works", "outcomes", "customer_proof"]:
                val = lp.get(key)
                if val:
                    _add_heading(doc, key.replace("_", " ").capitalize(), level=2)
                    _add_para(doc, val)
            ctas = lp.get("ctas") or []
            if ctas:
                _add_heading(doc, "Calls to action", level=2)
                for c in ctas:
                    _add_para(doc, f"• {c}")

        emails = campaign.get("emails") or []
        if emails:
            _add_heading(doc, "Email sequence", level=1)
            for i, em in enumerate(emails, 1):
                _add_heading(doc, f"Email {i}: {em.get('subject','')}", level=2)
                body = em.get("body") or ""
                _add_para(doc, body)

        se = campaign.get("sales_enablement") or {}
        if se:
            _add_heading(doc, "Sales enablement", level=1)
            dqs = se.get("discovery_questions") or []
            if dqs:
                _add_heading(doc, "Discovery questions", level=2)
                for q in dqs:
                    _add_para(doc, f"• {q}")
            objs = se.get("objection_cards") or []
            if objs:
                _add_heading(doc, "Objection handling", level=2)
                for oc in objs:
                    _add_para(doc, f"- {oc.get('blocker','')}")
                    _add_para(doc, f"  Reframe: {oc.get('reframe','')}")
                    claim_ids = oc.get("claim_ids") or []
                    if claim_ids:
                        _add_para(doc, f"  Claims: {', '.join(claim_ids)}")

        if evidence:
            _add_heading(doc, "Evidence log", level=1)
            _add_table_from_evidence(doc, evidence)

        input_proof = campaign.get("input_proof") or {}
        if input_proof:
            _add_heading(doc, "Input proof", level=1)
            for k, v in input_proof.items():
                _add_para(doc, f"{k}: {v}")

        buf = io.BytesIO()
        doc.save(buf)
        buf.seek(0)
        headers = {
            "Content-Disposition": f'attachment; filename="campaign-{run_id}.docx"'
        }
        return func.HttpResponse(
            body=buf.getvalue(),
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers=headers,
            status_code=200,
        )
    except KeyError as ke:
        return func.HttpResponse(f"Missing environment variable: {ke}", status_code=500)
    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)
