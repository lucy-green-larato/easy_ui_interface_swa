import React, { useState } from "react";
//-- import { z } from "zod";

// --- Schema to assure required variables are present ---
const VarsSchema = z.object({
  company: z.string().min(1, "Company is required"),
  industry: z.string().min(1, "Industry is required"),
  website: z
    .string()
    .min(1, "Website is required")
    .url("Website must be a valid URL (https://…)")
    .or(z.string().length(0)), // allow empty if you want: change to .min(1) to make mandatory
  sources: z.string().optional(), // comma or newline separated sources
});

// --- Supported packs/templates in your SWA ---
const PACKS: Record<string, { label: string; templates: { key: string; label: string }[] }> = {
  uk_b2b_sales_core: {
    label: "UK B2B Sales Core",
    templates: [
      { key: "opportunity_qualification", label: "Opportunity Qualification" },
      // You can list other generic templates here, e.g. email_gen, intro_builder, etc.
    ],
  },
  // Example: legacy Larato pack kept for compatibility
  larato_core: {
    label: "Larato Core (legacy)",
    templates: [
      { key: "email_gen", label: "Email Generator" },
      { key: "intro_builder", label: "Intro Builder" },
      { key: "lead_qualification", label: "Lead Qualification" },
    ],
  },
};

// --- Request payload builder ---
function buildPayload(pack: string, template: string, v: z.infer<typeof VarsSchema>) {
  const vars = {
    company: v.company,
    industry: v.industry,
    website: v.website,
    // Normalise sources into an array for the backend, but also include raw text
    sources_raw: v.sources ?? "",
    sources: (v.sources ?? "")
      .split(/\n|,/)
      .map((s) => s.trim())
      .filter(Boolean),
  };

  return {
    pack, // e.g. "uk_b2b_sales_core"
    template, // e.g. "opportunity_qualification"
    variables: vars,
  } as const;
}

export default function PromptVariableAssurer() {
  const [pack, setPack] = useState<keyof typeof PACKS>("uk_b2b_sales_core");
  const [template, setTemplate] = useState(PACKS["uk_b2b_sales_core"].templates[0].key);
  const [company, setCompany] = useState("");
  const [industry, setIndustry] = useState("");
  const [website, setWebsite] = useState("");
  const [sources, setSources] = useState("");

  const [errors, setErrors] = useState<string[]>([]);
  const [preview, setPreview] = useState<string>("");
  const [result, setResult] = useState<string>("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const currentTemplates = PACKS[pack].templates;

  function validate() {
    const parsed = VarsSchema.safeParse({ company, industry, website, sources });
    if (!parsed.success) {
      setErrors(parsed.error.errors.map((e) => e.message));
      return null;
    }
    setErrors([]);
    return parsed.data;
  }

  function handlePreview() {
    const parsed = validate();
    if (!parsed) return;
    const payload = buildPayload(pack, template, parsed);
    setPreview(JSON.stringify(payload, null, 2));
  }

  async function handleSend() {
    const parsed = validate();
    if (!parsed) return;
    const payload = buildPayload(pack, template, parsed);
    setIsSubmitting(true);
    setResult("");
    try {
      // Adjust the endpoint to your SWA API route
      const res = await fetch("/api/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const text = await res.text();
      setResult(text);
    } catch (err: any) {
      setResult(`Request failed: ${err?.message ?? String(err)}`);
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <div className="mx-auto max-w-3xl p-6">
      <h1 className="text-2xl font-semibold mb-4">Prompt Variable Assurer</h1>
      <p className="text-sm mb-6">This form guarantees the SWA front end passes the required variables (company, industry, website, sources) to your /api/generate endpoint in a consistent JSON payload.</p>

      <div className="grid grid-cols-1 gap-4">
        {/* Pack & Template Selectors */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <label className="block">
            <span className="text-sm font-medium">Pack</span>
            <select
              className="mt-1 w-full rounded-xl border p-2"
              value={pack}
              onChange={(e) => {
                const next = e.target.value as keyof typeof PACKS;
                setPack(next);
                setTemplate(PACKS[next].templates[0].key);
              }}
            >
              {Object.entries(PACKS).map(([k, v]) => (
                <option key={k} value={k}>
                  {v.label} ({k})
                </option>
              ))}
            </select>
          </label>

          <label className="block">
            <span className="text-sm font-medium">Template</span>
            <select
              className="mt-1 w-full rounded-xl border p-2"
              value={template}
              onChange={(e) => setTemplate(e.target.value)}
            >
              {currentTemplates.map((t) => (
                <option key={t.key} value={t.key}>
                  {t.label} ({t.key})
                </option>
              ))}
            </select>
          </label>
        </div>

        {/* Variables */}
        <label className="block">
          <span className="text-sm font-medium">Company *</span>
          <input
            className="mt-1 w-full rounded-xl border p-2"
            placeholder="Acme Networks Ltd"
            value={company}
            onChange={(e) => setCompany(e.target.value)}
          />
        </label>

        <label className="block">
          <span className="text-sm font-medium">Industry *</span>
          <input
            className="mt-1 w-full rounded-xl border p-2"
            placeholder="Managed Services / Cybersecurity"
            value={industry}
            onChange={(e) => setIndustry(e.target.value)}
          />
        </label>

        <label className="block">
          <span className="text-sm font-medium">Website *</span>
          <input
            className="mt-1 w-full rounded-xl border p-2"
            placeholder="https://www.example.co.uk"
            value={website}
            onChange={(e) => setWebsite(e.target.value)}
          />
        </label>

        <label className="block">
          <span className="text-sm font-medium">Sources (comma or newline separated)</span>
          <textarea
            className="mt-1 w-full rounded-xl border p-2 h-28"
            placeholder={"Annual report URL, LinkedIn company page, Press release URL"}
            value={sources}
            onChange={(e) => setSources(e.target.value)}
          />
        </label>

        {/* Errors */}
        {errors.length > 0 && (
          <div className="rounded-xl border border-red-300 bg-red-50 p-3 text-sm">
            <ul className="list-disc ml-5">
              {errors.map((e, i) => (
                <li key={i}>{e}</li>
              ))}
            </ul>
          </div>
        )}

        {/* Actions */}
        <div className="flex gap-3">
          <button
            onClick={handlePreview}
            className="rounded-2xl px-4 py-2 border shadow-sm"
            type="button"
          >
            Preview Payload
          </button>
          <button
            onClick={handleSend}
            disabled={isSubmitting}
            className="rounded-2xl px-4 py-2 border shadow-sm disabled:opacity-60"
            type="button"
          >
            {isSubmitting ? "Sending…" : "Send to /api/generate"}
          </button>
        </div>

        {/* Preview */}
        {preview && (
          <div>
            <h2 className="text-lg font-semibold mt-4 mb-2">Preview</h2>
            <pre className="rounded-xl border bg-gray-50 p-3 overflow-auto text-sm">
              {preview}
            </pre>
          </div>
        )}

        {/* Result */}
        {result && (
          <div>
            <h2 className="text-lg font-semibold mt-4 mb-2">API Response</h2>
            <pre className="rounded-xl border bg-gray-50 p-3 overflow-auto text-sm">
              {result}
            </pre>
          </div>
        )}

        {/* Implementation notes */}
        <div className="rounded-xl border bg-white p-4 text-sm">
          <h3 className="font-semibold mb-2">Implementation Notes</h3>
          <ol className="list-decimal ml-5 space-y-1">
            <li>Place this component under <code>/web/src/pages/</code> or mount in your router.</li>
            <li>Ensure your SWA API exposes a POST <code>/api/generate</code> endpoint that accepts <code>{`{ pack, template, variables }`}</code>.</li>
            <li>On the backend, validate the payload (repeat the same Zod schema) and route to the correct prompt template based on <code>pack</code> and <code>template</code>.</li>
            <li>Log payloads server-side for auditability (no PII beyond business context).</li>
            <li>Extend <code>PACKS</code> to expose more templates (email_gen, intro_builder, etc.).</li>
          </ol>
        </div>
      </div>
    </div>
  );
}
