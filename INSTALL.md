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

Click the reload icon on the Kafka Eye card in the extensions page, **then**
refresh the Kafka UI tab. Reloading only one of the two is the usual reason a
change appears not to take effect.

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

**"Extension context invalidated"** — expected after reloading the extension
while a page is still open. Refresh the page; a banner prompts you.

See `DEBUG.md` for more.
