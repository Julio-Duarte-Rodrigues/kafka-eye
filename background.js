// Kafka Eye — service worker
//
// Sole job: heal open tabs after the extension is reloaded or updated.
//
// When an extension reloads, every content script already running is severed
// from it ("Extension context invalidated"). The manifest only injects on
// navigation, so those tabs stay dead until the user manually refreshes. This
// re-injects into matching tabs so they recover on their own.

const MATCH_PATTERNS = ['*://*/ui/clusters/*'];

// Probe runs in the *current* isolated world. After an extension reload that
// world is fresh, so the flag is absent and we know injection is needed. If a
// live script is already there the flag is set and we leave it alone —
// re-running content.js in a world that already has it would throw on
// top-level `let` redeclaration.
function isKafkaEyeAlive() {
  return !!window.__kafkaEyeAlive;
}

async function reinjectIntoOpenTabs(reason) {
  let tabs;
  try {
    tabs = await chrome.tabs.query({ url: MATCH_PATTERNS });
  } catch (e) {
    console.warn('[Kafka Eye] Could not query tabs:', e.message);
    return;
  }

  for (const tab of tabs) {
    if (!tab.id) continue;
    try {
      const [probe] = await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        func: isKafkaEyeAlive
      });
      if (probe?.result) continue; // already healthy

      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        files: ['content.js']
      });
      console.log(`[Kafka Eye] Re-injected into tab ${tab.id} (${reason})`);
    } catch (e) {
      // Expected for tabs we can't touch (discarded, restricted, mid-navigation).
      console.debug(`[Kafka Eye] Skipped tab ${tab.id}:`, e.message);
    }
  }
}

chrome.runtime.onInstalled.addListener((details) => {
  reinjectIntoOpenTabs(details.reason);
});

chrome.runtime.onStartup.addListener(() => {
  reinjectIntoOpenTabs('startup');
});
