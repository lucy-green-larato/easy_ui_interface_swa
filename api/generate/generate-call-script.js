// api-generate-call-script.js 11-10-2025 v1. For campaign app?
import OpenAI from 'openai'

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
})

export default async function (context, req) {
  if (req.method !== 'POST') {
    return { status: 405, body: 'Method not allowed' }
  }

  const {
    pitch,
    tone,
    cta,
    buyerType,
    sellerName,
    sellerCompany,
    prospectName,
    prospectRole,
    prospectCompany,
    value_proposition,
    other_points
  } = req.body || {}

  if (!pitch) {
    return { status: 400, body: 'Missing pitch input' }
  }

  const toneDescriptor = /warm/i.test(tone)
    ? 'warm and professional'
    : 'formal and corporate'

  // CTA fallback if none provided
  const chosenCTA = (cta && cta.trim()) ||
    'a short follow-up call to confirm scope and timelines'

  const prompt = `
Using the researched sales pitch and context below, generate a cold call script that:

- Is written in formal, British business English
- Has no slang and avoids all US-style phrasing (e.g. "blocker", "flex", "roll out")
- Flows as a natural spoken conversation
- Starts with a polite introduction of the salesperson to the prospect:
  "Hello <prospectName>, this is <sellerName> from <sellerCompany>."
-Create a soft conversational starter based on the salespersons input into other points to cover.
- References observations from similar businesses, but makes no assumptions about the prospect’s current situation
- Introduces the offer as a lightweight operational enhancement, not a rebuild
- Elegantly weaves in the provided Unique Selling Points (if any) and Other Points (if any), placing them where they naturally strengthen the conversation
- Includes one specific, relevant customer example with measurable results
- Handles common objections factually and without pressure
- Ends with a confident, specific call to action using this wording:
"${chosenCTA}"
- Always close with: "Thank you for your time."

Tone setting: ${toneDescriptor}

If tone is "warm and professional":
- Use natural phrasing and a more conversational rhythm
- Be highly conversational and friendly
- Soften transitions and remove overly formal connectors
- Prioritise clarity and rapport over technical density

If tone is "formal and corporate":
- Use precise, structured business language
- Maintain professional distance and clear articulation
- Be conversational and friendly
- Avoid casual phrasing or implied familiarity

After the script, add a short section:
**Sales tips for colleagues conducting similar calls**
Provide exactly 3 practical tips in a warm, instructional tone for less experienced colleagues.

Context for the call:
- Prospect: ${prospectName || '—'}, ${prospectRole || '—'} at ${prospectCompany || '—'}
- Seller: ${sellerName || '—'} from ${sellerCompany || '—'}
- Buyer type: ${buyerType || '—'}
- Unique Selling Points: ${value_proposition || 'none provided'}
- Other Points to cover: ${other_points || 'none provided'}

Researched sales pitch:
"""${pitch}"""
`

  const completion = await openai.chat.completions.create({
    model: 'gpt-4o',
    messages: [
      {
        role: 'system',
        content: 'You are a highly effective UK B2B salesperson writing a cold call script.'
      },
      { role: 'user', content: prompt }
    ],
    temperature: 0.6
  })

  const output = completion.choices[0]?.message?.content || ''

  const [scriptText, tipsBlock] = output.split('**Sales tips for colleagues conducting similar calls**')
  const tips = tipsBlock
    ?.split('\n')
    ?.filter(line => line.trim().match(/^[0-9]+\. /))
    ?.map(tip => tip.replace(/^[0-9]+\. /, '').trim())

  return {
    status: 200,
    body: {
      script: {
        text: (scriptText || '').trim(),
        tips: tips || []
      }
    }
  }
}
