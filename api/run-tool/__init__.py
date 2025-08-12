import logging, json, os
import azure.functions as func
from openai import AzureOpenAI

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('run-tool function triggered.')

    try:
        try:
            body = req.get_json()
        except ValueError:
            body = {}

        # Accept either {input: "..."} or {inputs: {notes: "...", bias: "..."}}
        user_input = (body.get("input") or
                      (body.get("inputs", {}) or {}).get("notes") or
                      "").strip()
        bias = (body.get("inputs", {}) or {}).get("bias", "balanced")

        if not user_input:
            return func.HttpResponse("Missing input text", status_code=400)

        client = AzureOpenAI(
            api_key=os.environ["AZURE_OPENAI_API_KEY"],
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-06-01"),
            azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"]
        )

        deployment = os.environ["AZURE_OPENAI_DEPLOYMENT"]
        system_prompt = (
            "You are a helpful assistant for sales opportunity qualification.\n"
            f"User priority bias: {bias}"
        )

        resp = client.chat.completions.create(
            model=deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_input}
            ],
            temperature=0.7,
            max_tokens=500
        )

        output_text = resp.choices[0].message.content
        return func.HttpResponse(
            json.dumps({"result": output_text}, ensure_ascii=False),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception("run-tool error")
        return func.HttpResponse(f"Error running tool: {e}", status_code=500)
