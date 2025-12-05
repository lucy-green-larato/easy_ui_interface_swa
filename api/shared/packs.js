// /api/shared/packs.js (CommonJS safe version)
// 05-12-2025 — Fix for Azure Functions ESM/CJS conflict
// IMPORTANT:
//   - No `export`
//   - No TypeScript (`as const`, `export type`)
//   - No ESM `import/export`
//
// This file must remain PURE CommonJS because Azure Functions loads CJS first.
// packloader.js will now be able to load this file without throwing
// "Unexpected token 'export'" or ESM parse errors.

const packs = {
  uk_b2b_sales_core: {
    default: {
      system:
        "You are a top-performing B2B sales professional focused on the UK technology sector. Use only provided evidence. List missing info.",
      temperature: 0.4,
    },

    opportunity_qualification:
      "Opportunity qualification framework.\n\nCompany: {{company}}\nIndustry: {{industry}}\nWebsite: {{website}}\nSources: {{sources}}\n\nOutput:\n- Company profile (employees, revenue, segment, GTM approach, performance, decision-makers, events, estimated event ROI)\n- Pain points (growth, M&A, performance trends, alignment to our offering)\n- Budget and spend potential\n- Decision-making process\n- Competition and differentiation\n- Channel/adoption readiness\n- Prioritisation score (0–100 with rationale)\n- Missing info list",
  },

  larato_core: {
    default: {
      system:
        "You are a B2B technology sales specialist following Larato best practice. Use only provided evidence. List missing info.",
      temperature: 0.4,
    },

    email_gen:
      "Write a first-touch email (75–140 words), personalised and evidence-based.\n\nRecipient: {{prospect}} ({{role}}) at {{company}} ({{industry}})\nBuyer behaviour: {{behaviour}}\nPurchase drivers: {{drivers}}\nLeaders & contacts: {{leaders}}\nCompetitors: {{competitors}}\nValue points:\n{{value}}\nCTA: {{cta}}\n\nOutput:\n- 3 subject line options\n- Email body\n- One-sentence CTA\n- P.S. with proof metric if present\nList any material missing info at the end.",
  },
};

module.exports = { packs };
