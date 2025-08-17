import OpenAI from 'openai'

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
})

const pitchLibrary = {
  connectivity: {
    innovator: `You are not chasing bleeding-edge tech. You are scaling a business and need infrastructure that can keep up...`,
    pragmatist: `Most growing businesses hit the same tipping point: SIMs are bought ad hoc with no oversight...`,
    'risk-averse': `You want practical control. Not an overhaul, but tools that let your teams move faster with less hassle...`
  }
}

export default async function (context, req) {
  if (req.method !== 'POST') {
    return { status: 405, body: 'Method not allowed' }
  }

  const { product, buyerType, tone, cta } = req.body

  const salesPitch = pitchLibrary?.[product]?.[buyerType]

  if (!salesPitch) {
    return { status: 400, body: 'Pitch not found for this product and buyer type' }
  }

  const toneDescriptor = tone === 'warm' ? 'warm and professional' : 'formal and corporate'

  const prompt = `
Using the sales pitch below, generate a cold call script that:

- Is written in formal, British business English
- Has no headings, no slang, and avoids all US-style phrasing (e.g. blocker, flex, roll out)
- Flows as a natural spoken conversation
- Begins by referencing observations from similar businesses, but makes no assumptions about the prospect’s current situation
- Introduces the offer as a lightweight operational enhancement, not a rebuild
- Includes one specific, relevant customer example with measurable results
- Handles common objections factually and without pressure
- Ends with a confident, specific call to action using the following wording:
"${cta}"

Then write a short section titled:
**Sales tips for colleagues conducting similar calls**
Provide exactly 3 practical tips in a warm, instructional tone for less experienced colleagues.

Tone: ${toneDescriptor}
Role being called: ${buyerType}

Sales pitch:
"""${salesPitch}"""
`

  const completion = await openai.chat.completions.create({
    model: 'gpt-4o',
    messages: [
      { role: 'system', content: 'You are a highly effective UK B2B salesperson writing a cold call script.' },
      { role: 'user', content: prompt }
    ],
    temperature: 0.6
  })

  const output = completion.choices[0]?.message?.content || ''
  const [scriptText, tipsBlock] = output.split('**Sales tips for colleagues conducting similar calls**')
  const tips = tipsBlock
    ?.split('\n')
    ?.filter(line => line.trim().match(/^[0-9]+\\.\\s/))
    ?.map(tip => tip.replace(/^[0-9]+\\.\\s/, '').trim())

  return {
    status: 200,
    body: {
      script: {
        text: scriptText.trim(),
        tips: tips || []
      }
    }
  }
}
