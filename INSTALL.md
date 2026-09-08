# 👁️ Kafka Eye — Install

## Load the extension

1. Clone or download this repository.
2. Open your browser's extensions page:
   - **Brave**: `brave://extensions`
   - **Chrome**: `chrome://extensions`
   - **Edge**: `edge://extensions`
3. Enable **Developer mode** (toggle, top-right).
4. Click **Load unpacked** and select the folder containing `manifest.json`.
5. Open your Kafka UI (Kafbat) page and refresh.

The 👁️ Kafka Eye sidebar appears on the right.

## Updating after a code change

Click the reload icon on the Kafka Eye card in the extensions page. That's it —
a background service worker re-injects Kafka Eye into any open Kafka UI tab, so
the sidebar comes back on its own without a page refresh.

If a tab can't be reached (it was discarded, or was mid-navigation), the
sidebar shows a clickable **"⚠ Extension reloaded — click to refresh"** banner
as a fallback.

## Usage

- Click a **topic** to expand it and load its consumer groups (accordion —
  one topic open at a time).
- Selecting a topic scopes the metric cards to that topic.
- Select a **consumer** to see its lag trend, sparkline and ETA to zero lag.
- **⚡** switches to 3-second polling (default is 15s).
- **⇅ / ⏳** sorts topics by message count or by consumer lag.
- **🙈 / 👁️** hides empty topics; **◉** shows only selected topics.
- Hover any number for an explanation of what it measures.

## Requirements

A reachable Kafka UI (Kafbat) instance. The extension activates on any URL
matching `*://*/ui/clusters/*` and calls that same origin's REST API — there is
no separate configuration and no data leaves your browser.

## Troubleshooting

**Sidebar not appearing** — confirm the URL contains `/ui/clusters/`, then hard
refresh (⌘⇧R / Ctrl+⇧R). Check the console (F12) for `[Kafka Eye]` lines.

**Consumers slow or timing out** — the per-topic consumer-groups endpoint can be
slow on large clusters. Kafka Eye serialises these requests and backs off
automatically; the row offers a **Retry** button.

**"Extension context invalidated"** — harmless. It's logged by the *old* content
script when the extension reloads out from under it. The service worker then
re-injects a fresh one automatically. Only if that fails do you need to click
the banner to refresh.

See `DEBUG.md` for more.
