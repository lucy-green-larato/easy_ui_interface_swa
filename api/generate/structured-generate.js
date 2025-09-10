// api/generate/structured-generate.js
// Single responsibility: load schema, call OpenAI with strict structured output, return parsed JSON.

const fs = require("fs");
const path = require("path");
const OpenAI = require("openai");

const MODEL = "gpt-4.1-mini";
const SCHEMA_FILE = path.join(
  process.env.AzureWebJobsScriptRoot || path.join(__dirname, ".."),
  "schemas",
  "campaign.schema.json"
);

function loadSchema() {
  const raw = fs.readFileSync(SCHEMA_FILE, "utf8");
  return JSON.parse(raw);
}

async function generateCampaignJson(prompt) {
  if (!prompt || typeof prompt !== "string" || !prompt.trim()) {
    throw new Error("generateCampaignJson: 'prompt' (non-empty string) is required.");
  }
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error("Missing OPENAI_API_KEY app setting.");

  const schema = loadSchema();
  const client = new OpenAI({ apiKey });

  const resp = await client.responses.create({
    model: MODEL,
    input: prompt,
    temperature: 0.2,
    response_format: {
      type: "json_schema",
      json_schema: { name: "campaign_schema", schema, strict: true }
    }
  });

  const text = resp.output_text;
  if (!text || !text.trim()) {
    throw new Error("Model returned empty output_text.");
  }

  try {
    return JSON.parse(text);
  } catch (e) {
    const snippet = text.slice(0, 600);
    const err = new Error("Model returned invalid JSON.");
    err.snippet = snippet;
    throw err;
  }
}

module.exports = { generateCampaignJson };
