// /web/src/js/engagement/motivation.js 10-10-2025 v1-----//
// Loads ./content/call-library/motivation/quotes.json (relative URL).

let QUOTES = null; // null = not loaded yet; [] = loaded but empty
let LOADING = null;

export async function initMotivation() {
  if (QUOTES !== null) return;
  if (LOADING) return LOADING;
  LOADING = (async () => {
    try {
      const res = await fetch("/content/call-library/motivation/quotes.json", { cache: "no-store" });
      if (res.ok) {
        const data = await res.json();
        QUOTES = Array.isArray(data) ? data.map(v => String(v).trim()).filter(Boolean) : [];
      } else {
        QUOTES = [];
      }
    } catch { QUOTES = []; }
  })();
  return LOADING;
}

export function getRandomQuote() {
  const fallback = "You’ve got this. This guide gives you concise, practical coaching for your next call — what to focus on, how to steer the conversation, and how to land a clear next step.";
  if (!Array.isArray(QUOTES) || QUOTES.length === 0) return fallback;
  const i = Math.floor(Math.random() * QUOTES.length);
  return QUOTES[i];
}