// Home page: campaign status polling (CSP-safe external module)

let activeController = null;
const rendered = new Set();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function pollOnce(runId, signal) {
  const res = await fetch(`/api/campaign/status?runId=${encodeURIComponent(runId)}`, { signal });
  if (!res.ok) throw new Error(`status ${res.status}`);
  const status = await res.json();

  if (window.CampaignPage) {
    window.CampaignPage.setRunId(runId);
    window.CampaignPage.updateStage(status.state);
  }
  if (status.state === 'Completed' && !rendered.has(runId) && window.CampaignUI) {
    rendered.add(runId);
    await window.CampaignUI.handleCompleted(runId);
  }
  return status.state;
}

async function loop(runId, controller) {
  let delay = 1200;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (controller.signal.aborted) return;
    try {
      const state = await pollOnce(runId, controller.signal);
      if (state === 'Completed' || state === 'Failed') return;
      await sleep(delay);
      if (delay < 2000) delay += 200;
    } catch (err) {
      if (controller.signal.aborted) return;
      console.warn('status poll error:', err);
      await sleep(2500);
    }
  }
}

export function startCampaignStatusPoll(runId) {
  if (activeController) activeController.abort();
  const controller = new AbortController();
  activeController = controller;
  if (window.CampaignPage) window.CampaignPage.setRunId(runId);
  loop(runId, controller);
  return () => controller.abort();
}

export async function startNewCampaignRun(body = {}) {
  const res = await fetch('/api/campaign/start', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  const json = await res.json();
  const runId = json.runId;
  if (!runId) throw new Error('No runId returned from /api/campaign/start');
  startCampaignStatusPoll(runId);
  return runId;
}

// Optional button wiring if present on the page
const newRunBtn = document.getElementById('newRunBtn');
if (newRunBtn) {
  newRunBtn.addEventListener('click', async () => {
    await startNewCampaignRun({ page: 'leadgen', rowCount: 0 });
  });
}
