import logging
import os
import azure.functions as func
from openai import AzureOpenAI

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('run-tool function triggered.')
    
    try:
        body = req.get_json()
        user_input = body.get("input", "")

        client = AzureOpenAI(
            api_key=os.environ["AZURE_OPENAI_KEY"],
            api_version="2024-06-01",
            azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"]
        )

        response = client.chat.completions.create(
            model=os.environ["AZURE_OPENAI_DEPLOYMENT"],
            messages=[
                {"role": "system", "content": "You are a helpful assistant for sales opportunity qualification."},
                {"role": "user", "content": user_input}
            ],
            temperature=0.7,
            max_tokens=500
        )

        output_text = response.choices[0].message.content
        return func.HttpResponse(output_text, status_code=200)

    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Error running tool: {e}", status_code=500)
