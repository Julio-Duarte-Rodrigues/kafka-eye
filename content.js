// Kafka Eye - Uses Kafka UI API
// Version 1.5.6 - Only label near-zero, near-complete drains as stable

const SELECTED_TOPICS_KEY_PREFIX = 'selectedTopics_';
const SELECTED_CONSUMERS_KEY_PREFIX = 'selectedConsumers_';
const FAST_MODE_KEY = 'kafbatFastMode';
const SHOW_NON_EMPTY_ONLY_KEY = 'kafkaEyeShowNonEmptyOnly';
const SHOW_SELECTED_ONLY_KEY = 'kafkaEyeShowSelectedOnly';
const SORT_MODE_KEY = 'kafkaEyeSortMode';
const HIDE_NO_CONSUMERS_KEY = 'kafkaEyeHideNoConsumers';

let sidebarShown = false;
let currentClusterId = null;
let clusterName = null;
let baseApiUrl = null;
let pollingInterval = null;
let fastModeEnabled = false;
let showNonEmptyOnly = false;
let showSelectedOnly = false;
let selectedTopics = {};      // { topicName: true }
let selectedConsumers = {};   // { consumerName: true }
let expandedTopics = {};      // { topicName: true } — accordion expand state
let topicConsumers = {};      // { topicName: [ consumer, ... ] }
let loadingConsumers = {};    // { topicName: true } — in-flight requests
let sidebarMinimized = false;
let sortMode = 'messages';    // 'messages' | 'lag'
let hideNoConsumers = false;  // hide topics/consumers with no active lag

// Background consumer scan. Consumer groups are only known for topics that have
// been fetched, and those are fetched on demand (one at a time, by design — see
// the connection-starvation note on fetchConsumersForTopic). Both lag sorting
// and the "hide topics with no consumers" filter are meaningless until that
// data exists, so either mode slowly backfills it: ONE topic per poll, through
// the same mutex and backoff as every other consumer request. Never fan out
// here — that reintroduces the fetch storm.
const LAG_SCAN_MAX_TOPICS = 60; // don't crawl an unbounded topic list
let lagScanCursor = 0;

// History tracking for trends / rates / ETA
let topicHistory = {};        // { topicName: [{ t, count }] }
let consumerLagHistory = {};  // { consumerName: [{ t, lag }] }
const HISTORY_MAX = 30;
const TREND_WINDOW = 20;  // samples used for BOTH the rate label and the sparkline
const IDLE_POLLS_THRESHOLD = 3;
const STABLE_LAG_MAX = 50;
const STABLE_ETA_MAX_SECONDS = 10;

// Consumer fetch resilience
const CONSUMER_FETCH_TIMEOUT = 10000;
// The background scan must not hold the single-request mutex for the full
// user-facing timeout — a slow topic would otherwise block refreshes of the
// topic the user actually has open. Speculative scans give up sooner and
// simply retry later.
const CONSUMER_SCAN_TIMEOUT = 4000;
const CONSUMER_BACKOFF_BASE = 30000;   // 30s after first failure
const CONSUMER_BACKOFF_MAX = 300000;   // cap at 5m
let consumerFetchFailures = {};        // { topicName: { count, nextRetryAt, message } }
let consumerFetchInFlight = false;     // global mutex — at most 1 consumer request at a time

// Extension lifecycle
let contextInvalidated = false;
let urlWatchInterval = null;

const OPEN_EYE = '👁️';
const CLOSED_EYE = '🙈';

// Performance
let cachedTopics = null;
let cachedTopicsTimestamp = 0;
const TOPICS_CACHE_TTL = 5000;
const LOG_DEDUPE_WINDOW_MS = 60000;
let lastLogAtByKey = {};      // { key: unixMs } - warn once per window, debug otherwise

let lastRenderedMetrics = null;
let lastRenderedTopicsJson = null;
let initStarted = false;

// ── Init ──────────────────────────────────────────────────────────────────────

// Global safety net: catch any context-invalidation error that escapes a guard
// (e.g. thrown asynchronously from a chrome.* callback) and recover quietly.
window.addEventListener('error', (evt) => {
  if (isContextError(evt.error || evt.message)) {
    handleInvalidatedContext();
    evt.preventDefault();
  }
});
window.addEventListener('unhandledrejection', (evt) => {
  if (isContextError(evt.reason)) {
    handleInvalidatedContext();
    evt.preventDefault();
  }
});

document.addEventListener('DOMContentLoaded', initKafkaEye);
window.addEventListener('load', initKafkaEye);
// When the service worker re-injects after an extension reload, both events
// above have long since fired — so start immediately if the document is ready.
if (document.readyState === 'interactive' || document.readyState === 'complete') {
  initKafkaEye();
}

function initKafkaEye() {
  if (initStarted) return;
  initStarted = true;

  // Liveness flag probed by the service worker before re-injecting after an
  // extension reload — see background.js.
  try { window.__kafkaEyeAlive = true; } catch (e) { /* ignore */ }

  // A sidebar from a previous (now-invalidated) context may still be in the DOM
  const orphan = document.querySelector('#kafbatml-sidebar-metrics');
  if (orphan) orphan.remove();

  setTimeout(() => {
    if (!isExtensionContextValid()) { handleInvalidatedContext(); return; }
    detectCluster();
    createSidebar();
    loadSettings();
    startPolling();

    let lastUrl = window.location.href;
    urlWatchInterval = setInterval(() => {
      if (contextInvalidated) return;
      if (!isExtensionContextValid()) { handleInvalidatedContext(); return; }
      if (window.location.href !== lastUrl) {
        lastUrl = window.location.href;
        const changed = detectCluster();
        if (changed) {
          console.log('[Kafka Eye] Cluster changed, restarting...');
          cachedTopics = null;
          selectedTopics = {};
          selectedConsumers = {};
          expandedTopics = {};
          topicConsumers = {};
          loadingConsumers = {};
          topicHistory = {};
          consumerLagHistory = {};
          consumerFetchFailures = {};
          lagScanCursor = 0;
          lastRenderedTopicsJson = null;
          updateSelectionCounter();
          if (pollingInterval) clearInterval(pollingInterval);
          startPolling();
        }
      }
    }, 500);
  }, 500);
}

function detectCluster() {
  try {
    const url = window.location.href;
    const match = url.match(/\/ui\/clusters\/([^\/]+)/);
    if (match) {
      const newCluster = match[1];
      if (newCluster !== clusterName) {
        clusterName = newCluster;
        const urlObj = new URL(url);
        baseApiUrl = `${urlObj.protocol}//${urlObj.host}`;
        currentClusterId = btoa(clusterName).substring(0, 8);
        loadSelectedFilters();
        const label = document.getElementById('clusterLabel');
        if (label) label.textContent = clusterName;
        return true;
      }
    }
    const urlObj = new URL(url);
    baseApiUrl = `${urlObj.protocol}//${urlObj.host}`;
    return false;
  } catch (e) {
    console.error('[Kafka Eye] Cluster detection failed:', e);
    return false;
  }
}

// ── Sidebar HTML ──────────────────────────────────────────────────────────────

function createSidebar() {
  const sidebar = document.createElement('div');
  sidebar.id = 'kafbatml-sidebar-metrics';
  sidebar.innerHTML = `
    <style>
      #kafbatml-sidebar-metrics {
        position: fixed; right: 0; top: 0;
        width: 380px; height: 100vh;
        background: #1a1a1a; border-left: 1px solid #333;
        z-index: 10000; overflow-y: auto;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        color: #e0e0e0;
      }
      .sidebar-header {
        position: sticky; top: 0;
        background: #242424; padding: 12px;
        border-bottom: 1px solid #333; z-index: 10001;
      }
      .sidebar-title { font-size: 16px; font-weight: 600; margin-bottom: 8px; }
      .cluster-name { font-size: 11px; color: #888; margin-bottom: 8px; }
      .controls-row { display: flex; gap: 8px; margin-bottom: 8px; }
      .icon-btn {
        flex: 0 0 auto; width: 32px; height: 32px; padding: 6px;
        background: #333; border: 1px solid #444; color: #e0e0e0;
        border-radius: 4px; cursor: pointer; font-size: 16px;
        transition: all 0.2s; display: flex; align-items: center; justify-content: center;
      }
      .icon-btn:hover { background: #444; }
      .icon-btn.active { background: #0ea5e9; border-color: #0284c7; }
      .search-row { position: relative; margin-top: 8px; }
      .search-box {
        width: 100%; padding: 8px; padding-right: 72px;
        background: #2a2a2a; border: 1px solid #444;
        border-radius: 4px; color: #e0e0e0; font-size: 13px;
        box-sizing: border-box;
      }
      .search-box::placeholder { color: #888; }
      .search-counter {
        position: absolute; right: 8px; top: 50%;
        transform: translateY(-50%);
        font-size: 12px; color: #888; user-select: none;
        display: flex; align-items: center; gap: 4px;
      }
      .search-counter button {
        background: none; border: none; color: #888;
        cursor: pointer; font-size: 14px; padding: 0 2px;
      }
      .content-section { padding: 12px; border-bottom: 1px solid #333; }
      .section-title {
        font-size: 12px; font-weight: 600; text-transform: uppercase;
        color: #888; margin-bottom: 8px;
      }
      .metrics-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
      .metric-card {
        background: #2a2a2a; border: 1px solid #333;
        border-radius: 6px; padding: 10px; text-align: center;
      }
      .metric-value { font-size: 18px; font-weight: 600; color: #0ea5e9; word-break: break-word; }
      .metric-label { font-size: 11px; color: #888; margin-top: 4px; }
      .list-container { overflow-y: auto; }

      /* ── Topic rows ── */
      .topic-row {
        background: #2a2a2a; border-left: 2px solid transparent;
        margin-bottom: 2px; border-radius: 3px;
        cursor: pointer; user-select: none;
        transition: background 0.15s;
      }
      .topic-row:hover { background: #333; }
      .topic-row.expanded { border-left-color: #0ea5e9; }
      .topic-row.selected { background: #0ea5e9; border-left-color: #0284c7; color: #fff; }
      .topic-row.selected:hover { background: #0284c7; }
      .topic-header {
        display: flex; align-items: center; padding: 8px;
        gap: 6px;
      }
      .topic-chevron { font-size: 10px; color: #888; min-width: 12px; transition: transform 0.2s; }
      .topic-row.expanded .topic-chevron { transform: rotate(90deg); }
      .topic-row.selected .topic-chevron { color: rgba(255,255,255,0.7); }
      .topic-info { flex: 1; min-width: 0; }
      .topic-name { font-weight: 500; font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
      .topic-meta { font-size: 11px; opacity: 0.7; margin-top: 2px; }
      .topic-rate { color: #ef4444; font-weight: 500; }
      .topic-row.selected .topic-rate { color: #fecaca; }
      .topic-idle {
        color: #a1a1aa; background: #3f3f46;
        font-size: 9px; padding: 1px 5px; border-radius: 3px;
        text-transform: uppercase; letter-spacing: 0.3px;
      }
      .topic-lag { font-weight: 600; }
      .topic-row.selected .topic-lag { color: #fff !important; }
      .topic-lag-pending {
        color: #a1a1aa; font-style: italic; opacity: 0.8;
      }
      .topic-lag-retry {
        color: #fbbf24; font-style: italic; opacity: 0.9;
      }
      .topic-row.selected .topic-lag-retry { color: #fef3c7; }
      .topic-row.selected .topic-lag-pending { color: #e0f2fe; }

      /* ── Consumer sub-rows ── */
      .consumers-panel {
        border-top: 1px solid #333; background: #1e1e1e;
        padding: 4px 4px 4px 20px;
      }
      .consumer-row {
        display: flex; align-items: center; gap: 6px;
        padding: 6px 8px; border-radius: 3px;
        cursor: pointer; user-select: none;
        transition: background 0.15s;
        border-left: 2px solid transparent;
      }
      .consumer-row:hover { background: #2a2a2a; }
      .consumer-row.selected {
        background: #164e63; border-left-color: #0ea5e9; color: #7dd3fc;
      }
      .consumer-dot { font-size: 8px; color: #555; }
      .consumer-row.selected .consumer-dot { color: #0ea5e9; }
      .consumer-info { flex: 1; min-width: 0; }
      .consumer-name { font-size: 12px; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
      .consumer-lag { font-size: 11px; opacity: 0.7; margin-top: 1px; }
      .consumer-trend { font-size: 11px; color: #f59e0b; margin-top: 2px; }
      .health-badge {
        font-size: 9px; padding: 1px 5px; border-radius: 3px;
        font-weight: 600; letter-spacing: 0.3px; vertical-align: middle;
        margin-left: 4px; white-space: nowrap;
      }
      .consumer-members { opacity: 0.75; }
      .consumer-spark {
        font-size: 11px; line-height: 1; color: #0ea5e9;
        letter-spacing: -1px; margin-top: 3px;
        white-space: nowrap; overflow: hidden;
      }
      .consumers-loading { padding: 6px 8px; font-size: 11px; color: #666; }
      .consumers-empty { padding: 6px 8px; font-size: 11px; color: #555; font-style: italic; }
      .consumers-error {
        padding: 6px 8px; font-size: 11px; color: #fbbf24;
        display: flex; align-items: center; gap: 6px; flex-wrap: wrap;
      }
      .consumer-retry {
        background: #3f3f46; border: 1px solid #52525b; color: #e0e0e0;
        font-size: 10px; padding: 2px 8px; border-radius: 3px; cursor: pointer;
      }
      .consumer-retry:hover { background: #52525b; }
      .stale-banner {
        margin-top: 8px; padding: 6px 8px;
        background: #422006; color: #fbbf24;
        border: 1px solid #78350f; border-radius: 4px;
        font-size: 11px; text-align: center; cursor: pointer;
      }
      .stale-banner:hover { background: #572d08; }

      .no-data { color: #666; text-align: center; padding: 12px; font-size: 12px; }
      .error { color: #ff6b6b; text-align: center; padding: 12px; font-size: 12px; }
    </style>

    <div class="sidebar-header">
      <div class="sidebar-title">👁️ Kafka Eye</div>
      <div class="cluster-name" id="clusterLabel">Detecting...</div>
      <div class="controls-row">
        <button class="icon-btn" id="expandBtn" title="Collapse sidebar">⤢</button>
        <button class="icon-btn" id="fastModeBtn" title="Toggle fast mode">⚡</button>
        <button class="icon-btn" id="nonEmptyEyeBtn" title="Show non-empty only">🙈</button>
        <button class="icon-btn" id="selectedOnlyBtn" title="Show selected only">◉</button>
        <button class="icon-btn" id="hasConsumersBtn" title="Show all topics and consumers (click to hide zero-lag items)">👥</button>
        <button class="icon-btn" id="sortBtn" title="Sort by messages (click to sort by lag)">⇅</button>
        <button class="icon-btn" id="closeBtn" title="Close">✕</button>
      </div>
      <div class="search-row">
        <input type="text" class="search-box" id="globalSearch" placeholder="Search topics & consumers...">
        <div class="search-counter">
          <span id="selectionCounter">0|0</span>
          <button id="clearSelectionsBtn" title="Clear selections">✕</button>
        </div>
      </div>
    </div>

    <div id="sidebarContent">
      <div class="content-section">
        <div class="section-title">Metrics</div>
        <div class="metrics-grid">
          <div class="metric-card" title="Total messages across the selected topics (all topics when nothing is selected).">
            <div class="metric-value" id="totalMsgValue">—</div>
            <div class="metric-label">Total Messages</div>
          </div>
          <div class="metric-card" title="Change in total messages since the previous poll, plus the equivalent per-second rate.&#10;Red = growing, green = shrinking. Only coloured past ±1,000.">
            <div class="metric-value" id="growthRateValue">—</div>
            <div class="metric-label">Growth Rate</div>
          </div>
          <div class="metric-card" id="lagCard" title="Consumer lag across the current selection — messages produced but not yet consumed.">
            <div class="metric-value" id="lagValue">—</div>
            <div class="metric-label">Total Lag</div>
          </div>
        </div>
      </div>

      <div class="content-section">
        <div class="section-title">Topics</div>
        <div class="list-container" id="topicsList">
          <div class="no-data">Loading topics...</div>
        </div>
      </div>
    </div>
  `;

  document.body.appendChild(sidebar);
  document.getElementById('clusterLabel').textContent = clusterName || 'Detecting...';
  document.getElementById('closeBtn').addEventListener('click', closeSidebar);
  document.getElementById('fastModeBtn').addEventListener('click', toggleFastMode);
  document.getElementById('expandBtn').addEventListener('click', toggleMinimize);
  document.getElementById('nonEmptyEyeBtn').addEventListener('click', toggleNonEmpty);
  document.getElementById('selectedOnlyBtn').addEventListener('click', toggleSelectedOnly);
  document.getElementById('hasConsumersBtn').addEventListener('click', toggleHideNoConsumers);
  document.getElementById('sortBtn').addEventListener('click', toggleSortMode);
  document.getElementById('globalSearch').addEventListener('input', debounce(onSearch, 300));
  document.getElementById('clearSelectionsBtn').addEventListener('click', clearSelections);

  safeStorageGet([FAST_MODE_KEY, SHOW_NON_EMPTY_ONLY_KEY, SHOW_SELECTED_ONLY_KEY, SORT_MODE_KEY, HIDE_NO_CONSUMERS_KEY], (result) => {
    if (result[FAST_MODE_KEY]) {
      fastModeEnabled = true;
      document.getElementById('fastModeBtn').classList.add('active');
    }
    showNonEmptyOnly = !!result[SHOW_NON_EMPTY_ONLY_KEY];
    showSelectedOnly = !!result[SHOW_SELECTED_ONLY_KEY];
    sortMode = result[SORT_MODE_KEY] === 'lag' ? 'lag' : 'messages';
    hideNoConsumers = !!result[HIDE_NO_CONSUMERS_KEY];
    updateNonEmptyToggleUi();
    updateSortToggleUi();
    updateHasConsumersToggleUi();
    if (showSelectedOnly) document.getElementById('selectedOnlyBtn').classList.add('active');
  });
}

// ── Controls ──────────────────────────────────────────────────────────────────

function toggleFastMode() {
  fastModeEnabled = !fastModeEnabled;
  const btn = document.getElementById('fastModeBtn');
  btn.classList.toggle('active', fastModeEnabled);
  safeStorageSet({ [FAST_MODE_KEY]: fastModeEnabled });
  if (pollingInterval) clearInterval(pollingInterval);
  startPolling();
}

function toggleNonEmpty() {
  showNonEmptyOnly = !showNonEmptyOnly;
  safeStorageSet({ [SHOW_NON_EMPTY_ONLY_KEY]: showNonEmptyOnly });
  updateNonEmptyToggleUi();
  pollMetrics();
}

function toggleSelectedOnly() {
  showSelectedOnly = !showSelectedOnly;
  document.getElementById('selectedOnlyBtn').classList.toggle('active', showSelectedOnly);
  safeStorageSet({ [SHOW_SELECTED_ONLY_KEY]: showSelectedOnly });
  pollMetrics();
}

function toggleSortMode() {
  sortMode = sortMode === 'lag' ? 'messages' : 'lag';
  lagScanCursor = 0;
  safeStorageSet({ [SORT_MODE_KEY]: sortMode });
  updateSortToggleUi();
  lastRenderedTopicsJson = null; // sort order changed — force re-render
  renderTopicsFromCache();
  pollMetrics();
}

function updateSortToggleUi() {
  const btn = document.getElementById('sortBtn');
  if (!btn) return;
  const lagMode = sortMode === 'lag';
  btn.textContent = lagMode ? '⏳' : '⇅';
  btn.classList.toggle('active', lagMode);
  btn.title = lagMode
    ? 'Sorting by consumer lag (click to sort by messages)'
    : 'Sorting by messages (click to sort by lag)';
}

// Hides topics that have been confirmed to have zero consumer groups. Topics
// whose consumers haven't been fetched yet are deliberately left visible
// (unknown is not "no consumers") and marked so the user knows they're pending.
function toggleHideNoConsumers() {
  hideNoConsumers = !hideNoConsumers;
  lagScanCursor = 0;
  safeStorageSet({ [HIDE_NO_CONSUMERS_KEY]: hideNoConsumers });
  updateHasConsumersToggleUi();
  lastRenderedTopicsJson = null;
  renderTopicsFromCache();
  pollMetrics();
}

function updateHasConsumersToggleUi() {
  const btn = document.getElementById('hasConsumersBtn');
  if (!btn) return;
  btn.classList.toggle('active', hideNoConsumers);
  btn.title = hideNoConsumers
    ? 'Hiding topics and consumers with zero lag (click to show all)'
    : 'Show all topics and consumers (click to hide zero-lag items)';
}

function updateNonEmptyToggleUi() {
  const btn = document.getElementById('nonEmptyEyeBtn');
  if (!btn) return;
  btn.textContent = showNonEmptyOnly ? OPEN_EYE : CLOSED_EYE;
  btn.classList.toggle('active', showNonEmptyOnly);
}

function toggleMinimize() {
  const sidebar = document.getElementById('kafbatml-sidebar-metrics');
  const content = document.getElementById('sidebarContent');
  const header = document.querySelector('.sidebar-header');
  const btn = document.getElementById('expandBtn');
  sidebarMinimized = !sidebarMinimized;
  if (sidebarMinimized) {
    sidebar.style.width = '50px';
    header.style.padding = '8px 6px';
    content.style.display = 'none';
    header.querySelectorAll('.sidebar-title,.cluster-name,.controls-row .icon-btn:not(#expandBtn),.search-row').forEach(el => el.style.display = 'none');
    const row = header.querySelector('.controls-row');
    if (row) { row.style.justifyContent = 'center'; }
    btn.style.width = '100%';
  } else {
    sidebar.style.width = '380px';
    header.style.padding = '12px';
    content.style.display = 'block';
    header.querySelectorAll('.sidebar-title,.cluster-name,.controls-row .icon-btn:not(#expandBtn),.search-row').forEach(el => el.style.display = '');
    const row = header.querySelector('.controls-row');
    if (row) { row.style.justifyContent = ''; }
    btn.style.width = '32px';
  }
}

function closeSidebar() {
  const sidebar = document.getElementById('kafbatml-sidebar-metrics');
  if (sidebar) sidebar.remove();
  sidebarShown = false;
  if (pollingInterval) clearInterval(pollingInterval);
}

function clearSelections() {
  selectedTopics = {};
  selectedConsumers = {};
  expandedTopics = {};
  saveSelectedFilters();
  updateSelectionCounter();
  debouncedPollMetrics();
}

function onSearch() {
  renderTopicsFromCache();
}

function updateSelectionCounter() {
  const t = Object.values(selectedTopics).filter(Boolean).length;
  const c = Object.values(selectedConsumers).filter(Boolean).length;
  const el = document.getElementById('selectionCounter');
  if (el) el.textContent = `${t}|${c}`;
}

// ── Polling ───────────────────────────────────────────────────────────────────

function startPolling() {
  const interval = fastModeEnabled ? 5000 : 15000;
  pollMetrics();
  pollingInterval = setInterval(pollMetrics, interval);
}

let pollMetricsTimeout = null;
function debouncedPollMetrics() {
  clearTimeout(pollMetricsTimeout);
  pollMetricsTimeout = setTimeout(pollMetrics, 300);
}

async function pollMetrics() {
  try {
    if (contextInvalidated) return;
    if (!isExtensionContextValid()) { handleInvalidatedContext(); return; }
    if (!clusterName || !baseApiUrl) return;
    const topics = await fetchTopics();
    if (topics) renderMetrics(topics);
    await refreshExpandedConsumers();
    if (topics && (sortMode === 'lag' || hideNoConsumers)) {
      await scanConsumersForVisibleTopics(visibleTopicsFor(topics));
    }
  } catch (e) {
    console.error('[Kafka Eye] Poll error:', e);
  }
}

// ── Data fetching ─────────────────────────────────────────────────────────────

async function fetchTopics() {
  try {
    const now = Date.now();
    if (cachedTopics && (now - cachedTopicsTimestamp) < TOPICS_CACHE_TTL) {
      return cachedTopics;
    }

    let allTopics = [];
    const seen = new Set();
    let page = 0;
    const perPage = 100;

    while (true) {
      const url = `${baseApiUrl}/api/clusters/${clusterName}/topics?page=${page}&perPage=${perPage}`;
      console.log('[Kafka Eye] Fetching topics page', page);
      const response = await fetch(url, { method: 'GET', headers: { 'Accept': 'application/json' }, mode: 'cors' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();

      const topics = Array.isArray(data) ? data : (data?.topics || data?.data || data?.content || []);
      if (topics.length === 0) break;

      topics.forEach(t => {
        const name = t.name || t.topicName || '';
        if (!name || seen.has(name)) return;
        seen.add(name);
        let messageCount = 0;
        if (t.partitions && Array.isArray(t.partitions)) {
          messageCount = t.partitions.reduce((s, p) => s + (p.offsetMax || 0), 0);
        }
        allTopics.push({ name, messageCount: Number(messageCount), partitions: Number(t.partitionCount || t.partitions?.length || 0) });
      });

      if (topics.length < perPage) break;
      page++;
    }

    cachedTopics = allTopics;
    cachedTopicsTimestamp = Date.now();
    console.log('[Kafka Eye] Topics loaded:', allTopics.length);
    return allTopics;
  } catch (e) {
    if (isContextError(e) || !isExtensionContextValid()) { handleInvalidatedContext(); return cachedTopics; }
    if (isNetworkFetchError(e)) {
      warnThrottled('topics-fetch-network', '[Kafka Eye] Topics API unreachable (network/CORS). Retrying with backoff.');
    } else {
      console.error('[Kafka Eye] Topics fetch failed:', e);
    }
    // Keep showing last good data rather than blanking the whole sidebar
    if (cachedTopics && cachedTopics.length > 0) {
      warnThrottled('topics-stale-cache', '[Kafka Eye] Serving stale topics cache after failure');
      return cachedTopics;
    }
    showError('topicsList', 'Failed to load topics: ' + e.message);
    return null;
  }
}

async function fetchConsumersForTopic(topicName, timeoutMs = CONSUMER_FETCH_TIMEOUT) {
  // Serialize consumer requests — concurrent hung requests starve the
  // browser's 6-connection-per-host pool and break the topics fetch.
  // Returns: array = success, null = failed, undefined = skipped (busy)
  if (consumerFetchInFlight) return undefined;
  consumerFetchInFlight = true;
  try {
    const encoded = encodeURIComponent(topicName);
    const url = `${baseApiUrl}/api/clusters/${clusterName}/topics/${encoded}/consumer-groups`;
    const response = await fetch(url, {
      method: 'GET',
      headers: { 'Accept': 'application/json' },
      mode: 'cors',
      signal: AbortSignal.timeout(timeoutMs)
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const raw = Array.isArray(data) ? data : [];

    delete consumerFetchFailures[topicName]; // success clears backoff

    return raw.map(c => ({
      name: c.groupId || c.name || '',
      lag: c.consumerLag ?? c.lag ?? 0,
      state: c.state || 'UNKNOWN',
      members: Number(c.members ?? c.memberCount ?? (Array.isArray(c.members) ? c.members.length : 0)) || 0
    })).filter(c => c.name);
  } catch (e) {
    if (isContextError(e) || !isExtensionContextValid()) { handleInvalidatedContext(); return undefined; }
    const prev = consumerFetchFailures[topicName];
    const count = (prev?.count || 0) + 1;
    const timedOut = e.name === 'TimeoutError';
    const networkDown = isNetworkFetchError(e);
    const delay = Math.min(CONSUMER_BACKOFF_BASE * Math.pow(2, count - 1), CONSUMER_BACKOFF_MAX);
    consumerFetchFailures[topicName] = {
      count,
      nextRetryAt: Date.now() + delay,
      timedOut,
      timeoutMs,
      message: timedOut ? 'timed out' : (networkDown ? 'network unavailable' : e.message)
    };
    if (networkDown) {
      warnThrottled(
        `consumers-fetch-network:${topicName}`,
        `[Kafka Eye] Consumers for ${topicName} unavailable (network/CORS) (${count}x, retry in ${Math.round(delay / 1000)}s).`
      );
    } else {
      // A slow consumer-groups endpoint is expected on some clusters and self-heals
      // via backoff, so only the first failure is a warning; the rest are debug
      // noise and would otherwise spam the console every backoff cycle.
      const log = count === 1 ? console.warn : console.debug;
      log(`[Kafka Eye] Consumers for ${topicName} failed (${count}x, retry in ${Math.round(delay / 1000)}s):`, e.message);
    }
    return null; // null = failed (distinct from [] = genuinely no consumers)
  } finally {
    consumerFetchInFlight = false;
  }
}

function isConsumerFetchBackedOff(topicName) {
  const f = consumerFetchFailures[topicName];
  return !!f && Date.now() < f.nextRetryAt;
}

// Total lag across a topic's consumer groups.
// Returns null when consumers have never been fetched for this topic — that is
// "unknown", which is deliberately NOT the same as 0 and must not sort as 0.
function topicKnownLag(topicName) {
  const consumers = topicConsumers[topicName];
  if (!Array.isArray(consumers)) return null;
  return consumers.reduce((sum, c) => sum + parseLag(c.lag), 0);
}

// Backfill consumer data for the visible topics, ONE per poll, so lag sorting
// and the no-consumer filter have something to work with. Honours the same
// mutex/backoff as on-demand fetches.
async function scanConsumersForVisibleTopics(visibleTopics) {
  if (sortMode !== 'lag' && !hideNoConsumers) return;
  if (consumerFetchInFlight) return;

  const candidates = visibleTopics
    .slice()
    .sort((a, b) => (b.messageCount || 0) - (a.messageCount || 0))
    .slice(0, LAG_SCAN_MAX_TOPICS)
    .map(t => t.name)
    .filter(name =>
      !Array.isArray(topicConsumers[name]) &&
      !loadingConsumers[name] &&
      !isConsumerFetchBackedOff(name)
    );
  if (candidates.length === 0) return;

  // Round-robin so a persistently slow topic can't block the rest
  if (lagScanCursor >= candidates.length) lagScanCursor = 0;
  const topicName = candidates[lagScanCursor];
  lagScanCursor++;

  loadingConsumers[topicName] = true;
  try {
    const consumers = await fetchConsumersForTopic(topicName, CONSUMER_SCAN_TIMEOUT);
    if (Array.isArray(consumers)) {
      topicConsumers[topicName] = consumers;
      recordConsumerLagHistory(consumers);
    }
    if (consumers !== undefined) {
      // Re-render on failure too, so the row can swap "scanning…" for a
      // "retrying" marker rather than implying the scan is still pending.
      lastRenderedTopicsJson = null;
      renderTopicsFromCache();
    }
  } finally {
    delete loadingConsumers[topicName];
  }
}

// ── History / trend helpers ───────────────────────────────────────────────────

function recordHistory(store, key, value) {
  if (!store[key]) store[key] = [];
  const arr = store[key];
  const last = arr[arr.length - 1];
  const now = Date.now();
  if (last && now - last.t < 500) return; // avoid duplicate samples
  arr.push({ t: now, v: value });
  if (arr.length > HISTORY_MAX) arr.shift();
}

function recordTopicHistory(topics) {
  topics.forEach(t => recordHistory(topicHistory, t.name, t.messageCount || 0));
}

function recordConsumerLagHistory(consumers) {
  consumers.forEach(c => recordHistory(consumerLagHistory, c.name, parseLag(c.lag)));
}

// Rate of change per second based on history window
// Rate of change per second, computed over the SAME window the sparkline draws
// so the label and the chart can never disagree. Uses least-squares slope,
// which is far less jumpy than a first-vs-last endpoint difference.
function ratePerSecond(store, key) {
  const arr = store[key];
  if (!arr || arr.length < 2) return null;
  const data = arr.slice(-TREND_WINDOW);
  if (data.length < 2) return null;

  const t0 = data[0].t;
  const xs = data.map(s => (s.t - t0) / 1000);
  const ys = data.map(s => s.v);
  const n = data.length;
  const meanX = xs.reduce((a, b) => a + b, 0) / n;
  const meanY = ys.reduce((a, b) => a + b, 0) / n;

  let num = 0, den = 0;
  for (let i = 0; i < n; i++) {
    num += (xs[i] - meanX) * (ys[i] - meanY);
    den += (xs[i] - meanX) ** 2;
  }
  if (den === 0) return null; // all samples at same timestamp
  return num / den;
}

// Is topic idle (no growth across recent polls)?
function isTopicIdle(topicName) {
  const arr = topicHistory[topicName];
  if (!arr || arr.length < IDLE_POLLS_THRESHOLD) return false;
  const recent = arr.slice(-IDLE_POLLS_THRESHOLD);
  return recent.every(s => s.v === recent[0].v);
}

// Lag trend for a consumer: rising / falling / stable / caught-up
//
// The rate is a least-squares slope over the last TREND_WINDOW samples, so it
// reflects the *trajectory during that window* — not just the instant now.
// If lag was draining from a backlog down to 0 partway through the window,
// the slope stays negative for the rest of the window's lifetime even though
// there's nothing left to drain. Reporting that as "falling" next to a
// current value of "0 lag" reads as a contradiction, so once lag has
// actually hit 0 we report "caught up" regardless of the historical slope —
// lag can't go negative, so there's no further fall to describe.
//
// A tiny negative slope is only called "stable" when the remaining backlog is
// negligible and will clear almost immediately. Otherwise it is still
// meaningfully falling, even if the rate is below the display threshold:
// a 17,998-message backlog draining at 0.02/s is not stable — its ETA is
// roughly ten days.
function lagTrend(consumerName, currentLag) {
  if (currentLag !== undefined && parseLag(currentLag) <= 0) {
    return { dir: 'caught-up', rate: 0, icon: '✓', color: '#22c55e' };
  }
  const rate = ratePerSecond(consumerLagHistory, consumerName);
  if (rate === null) return null;
  const lag = currentLag === undefined ? null : parseLag(currentLag);
  const eta = rate < -0.01 && lag !== null ? lag / Math.abs(rate) : null;
  if (
    Math.abs(rate) < 0.5 &&
    lag !== null &&
    lag < STABLE_LAG_MAX &&
    eta !== null &&
    eta < STABLE_ETA_MAX_SECONDS
  ) {
    return { dir: 'stable', rate, icon: '—', color: '#0ea5e9' };
  }
  if (rate > 0) return { dir: 'rising', rate, icon: '▲', color: '#ef4444' };
  return { dir: 'falling', rate, icon: '▼', color: '#22c55e' };
}

// ETA to zero lag (seconds) — only meaningful when lag is positive and draining
function etaToZero(consumerName, currentLag) {
  const lag = parseLag(currentLag);
  if (lag <= 0) return null;
  const rate = ratePerSecond(consumerLagHistory, consumerName);
  if (rate === null || rate >= -0.01) return null; // not draining
  return lag / Math.abs(rate);
}

function formatDuration(seconds) {
  if (seconds === null || !isFinite(seconds)) return '∞';
  if (seconds < 60) return Math.round(seconds) + 's';

  if (seconds < 3600) {
    const m = Math.floor(seconds / 60);
    const s = Math.round(seconds % 60);
    return s > 0 ? `${m}m ${s}s` : `${m}m`;
  }

  if (seconds < 86400) {
    const h = Math.floor(seconds / 3600);
    const m = Math.round((seconds % 3600) / 60);
    return m > 0 ? `${h}h ${m}m` : `${h}h`;
  }

  const d = Math.floor(seconds / 86400);
  const h = Math.round((seconds % 86400) / 3600);
  return h > 0 ? `${d}d ${h}h` : `${d}d`;
}

// Unicode block sparkline, max 20 glyphs
function sparkline(values) {
  if (!values || values.length < 2) return '';
  const blocks = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
  const data = values.slice(-TREND_WINDOW);
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min;
  if (range === 0) return blocks[0].repeat(data.length);
  return data.map(v => blocks[Math.min(blocks.length - 1, Math.floor(((v - min) / range) * (blocks.length - 1)))]).join('');
}

function healthBadge(state) {
  const s = (state || 'UNKNOWN').toUpperCase();
  const map = {
    STABLE:            { label: 'STABLE',   bg: '#14532d', fg: '#4ade80' },
    EMPTY:             { label: 'EMPTY',    bg: '#3f3f46', fg: '#a1a1aa' },
    DEAD:              { label: 'DEAD',     bg: '#450a0a', fg: '#f87171' },
    PREPARING_REBALANCE:{ label: 'REBAL',   bg: '#422006', fg: '#fbbf24' },
    COMPLETING_REBALANCE:{ label: 'REBAL',  bg: '#422006', fg: '#fbbf24' },
    UNKNOWN:           { label: '?',        bg: '#3f3f46', fg: '#a1a1aa' }
  };
  const cfg = map[s] || map.UNKNOWN;
  return `<span class="health-badge" style="background:${cfg.bg};color:${cfg.fg};">${cfg.label}</span>`;
}

// ── Rendering ─────────────────────────────────────────────────────────────────

function renderMetrics(topics) {
  recordTopicHistory(topics);
  const totalMessages = sumMessages(topics);
  const now = Date.now();
  const prevData = JSON.parse(localStorage.getItem('kafka_eye_metrics') || '{}');
  const prevTotal = typeof prevData.totalMessages === 'number' ? prevData.totalMessages : totalMessages;
  const prevTimestamp = prevData.timestamp || now;
  const elapsedSeconds = Math.max((now - prevTimestamp) / 1000, 0.001);
  const growth = totalMessages - prevTotal;
  const ratePerSecond = growth / elapsedSeconds;
  localStorage.setItem('kafka_eye_metrics', JSON.stringify({ totalMessages, timestamp: now }));

  const totalLag = sumSelectedConsumerLag();

  const metricsChanged = !lastRenderedMetrics ||
    lastRenderedMetrics.totalMessages !== totalMessages ||
    lastRenderedMetrics.growth !== growth ||
    lastRenderedMetrics.totalLag !== totalLag;

  if (metricsChanged) {
    document.getElementById('totalMsgValue').textContent = fmt(totalMessages);
    document.getElementById('growthRateValue').innerHTML = formatGrowthRate(growth, ratePerSecond);
    document.getElementById('lagValue').textContent = fmt(totalLag);
    lastRenderedMetrics = { totalMessages, growth, totalLag };
  }

  renderTopics(topics);
}

function formatGrowthRate(growth, ratePerSecond) {
  const GROWTH_COLOR_THRESHOLD = 1000;
  let arrow = '';
  let color = '#0ea5e9';
  if (growth > 0) {
    arrow = '▲ ';
    if (growth >= GROWTH_COLOR_THRESHOLD) color = '#ef4444';
  } else if (growth < 0) {
    arrow = '▼ ';
    if (Math.abs(growth) >= GROWTH_COLOR_THRESHOLD) color = '#22c55e';
  }

  const rateStr = fmtCompact(Math.abs(ratePerSecond));
  const span = color ? `style="color: ${color};"` : '';
  return `<span ${span}>${arrow}${fmt(growth)}</span><span style="font-size: 11px; opacity: 0.75; display:block; margin-top:2px;">${rateStr}/s</span>`;
}

// Badge for a topic whose consumer groups aren't known yet. Distinguishes
// "not reached yet" from "tried and failed", so a topic that keeps timing out
// doesn't sit on `scanning…` forever looking like it's still in progress.
function pendingConsumerBadge(topicName) {
  const f = consumerFetchFailures[topicName];
  if (f) {
    const secs = Math.max(0, Math.round((f.nextRetryAt - Date.now()) / 1000));
    const why = f.timedOut
      ? `The consumer-groups endpoint took longer than ${Math.round((f.timeoutMs || CONSUMER_SCAN_TIMEOUT) / 1000)}s to respond.`
      : `Request failed: ${f.message}.`;
    const tip = `${why}\nFailed ${f.count}x — retrying in ${secs}s.\nThis is usually a slow broker, not a problem with the topic.`;
    return `<span class="topic-lag-retry" title="${escapeAttr(tip)}">retrying ${secs}s</span>`;
  }
  const tip = 'Consumer groups not fetched yet.\nKafka UI only exposes consumer groups per topic, so Kafka Eye scans one topic per poll.';
  return `<span class="topic-lag-pending" title="${escapeAttr(tip)}">scanning…</span>`;
}

function escapeAttr(s) {
  return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
}

function renderTopicsFromCache() {
  if (cachedTopics) renderTopics(cachedTopics);
}

// Single source of truth for "which topics are on screen", shared by the
// renderer and the lag scan so the scan can never work on a different set
// than the user is actually looking at.
function visibleTopicsFor(topics) {
  const searchTerm = document.getElementById('globalSearch')?.value?.toLowerCase() || '';
  return topics.filter(t => {
    if (showSelectedOnly && !selectedTopics[t.name]) return false;
    if (showNonEmptyOnly && Number(t.messageCount || 0) <= 0) return false;
    // Only hide topics whose consumer data has been fetched and confirms no
    // active lag. Unfetched topics stay visible so the list doesn't silently
    // drop topics the scan simply hasn't reached yet.
    if (hideNoConsumers) {
      const consumers = topicConsumers[t.name];
      if (Array.isArray(consumers) && topicKnownLag(t.name) <= 0) return false;
    }
    if (searchTerm && !t.name.toLowerCase().includes(searchTerm)) return false;
    return true;
  });
}

function renderTopics(topics) {
  const container = document.getElementById('topicsList');
  if (!container) return;

  if (!topics || topics.length === 0) {
    container.innerHTML = '<div class="no-data">No topics found</div>';
    return;
  }

  let sorted = [...topics];
  if (sortMode === 'lag') {
    // Topics with known lag first (descending). Unknown lag sinks to the
    // bottom rather than masquerading as zero, since it's simply not fetched
    // yet — the background scan fills it in and the order settles.
    sorted.sort((a, b) => {
      const la = topicKnownLag(a.name);
      const lb = topicKnownLag(b.name);
      if (la === null && lb === null) return (b.messageCount || 0) - (a.messageCount || 0);
      if (la === null) return 1;
      if (lb === null) return -1;
      if (lb !== la) return lb - la;
      return (b.messageCount || 0) - (a.messageCount || 0);
    });
  } else {
    sorted.sort((a, b) => (b.messageCount || 0) - (a.messageCount || 0));
  }

  const filtered = visibleTopicsFor(sorted);

  if (filtered.length === 0) {
    container.innerHTML = '<div class="no-data">No topics match filters</div>';
    return;
  }

  // Change detection key includes expanded + selected + derived rate/idle state
  const stateKey = JSON.stringify({
    topics: filtered.map(t => ({
      n: t.name,
      m: t.messageCount,
      r: (ratePerSecond(topicHistory, t.name) ?? 0).toFixed(2),
      i: isTopicIdle(t.name),
      l: topicKnownLag(t.name),
      f: consumerFetchFailures[t.name]?.count || 0
    })),
    selectedTopics,
    expandedTopics,
    sortMode,
    hideNoConsumers
  });
  if (lastRenderedTopicsJson === stateKey) return;
  lastRenderedTopicsJson = stateKey;

  // Remember scroll position
  const scrollTop = container.scrollTop;

  // Build DOM
  container.innerHTML = '';
  filtered.forEach(topic => {
    const isSelected = !!selectedTopics[topic.name];
    const isExpanded = !!expandedTopics[topic.name];

    const row = document.createElement('div');
    row.className = `topic-row${isSelected ? ' selected' : ''}${isExpanded ? ' expanded' : ''}`;
    row.dataset.topic = topic.name;

    const tRate = ratePerSecond(topicHistory, topic.name);
    let rateHtml = '';
    if (tRate !== null && tRate > 0.01) {
      const rateTip = `Throughput: ~${fmtCompact(tRate)} messages/second arriving.\nLeast-squares slope over the last ${TREND_WINDOW} polls.`;
      rateHtml = ` <span class="topic-rate" title="${rateTip}">▲ ${fmtCompact(tRate)}/s</span>`;
    } else if (isTopicIdle(topic.name) && Number(topic.messageCount || 0) > 0) {
      rateHtml = ` <span class="topic-idle" title="No new messages over the last ${IDLE_POLLS_THRESHOLD} polls.">idle</span>`;
    }

    // In lag mode surface the number the sort is based on, otherwise the
    // ordering looks arbitrary. "scanning…" marks not-yet-fetched topics —
    // also shown under the no-consumer filter to explain why a topic that may
    // yet be filtered out is still on screen.
    let lagHtml = '';
    if (sortMode === 'lag') {
      const known = topicKnownLag(topic.name);
      if (known === null) {
        lagHtml = ' ' + pendingConsumerBadge(topic.name);
      } else {
        const lagColor = known > 0 ? '#ef4444' : '#22c55e';
        const groupCount = (topicConsumers[topic.name] || []).length;
        const lagTip = known > 0
          ? `Consumer lag: ${fmt(known)} messages produced but not yet consumed,\nsummed across ${groupCount} consumer group${groupCount === 1 ? '' : 's'}.\nThis is what the list is sorted by.`
          : `Fully caught up — no outstanding lag across ${groupCount} consumer group${groupCount === 1 ? '' : 's'}.`;
        lagHtml = ` <span class="topic-lag" style="color:${lagColor};" title="${lagTip}">${fmtCompactCount(known)} lag</span>`;
      }
    } else if (hideNoConsumers && !Array.isArray(topicConsumers[topic.name])) {
      lagHtml = ' ' + pendingConsumerBadge(topic.name);
    }

    const msgTip = `${fmt(topic.messageCount)} total messages in this topic\n(sum of offsetMax across all partitions).`;

    row.innerHTML = `
      <div class="topic-header">
        <span class="topic-chevron">▶</span>
        <div class="topic-info">
          <div class="topic-name" title="${topic.name}">${topic.name}</div>
          <div class="topic-meta"><span title="${msgTip}">${fmt(topic.messageCount)} messages</span>${rateHtml}${lagHtml}</div>
        </div>
      </div>
    `;

    // Consumers panel (shown when expanded)
    const panel = document.createElement('div');
    panel.className = 'consumers-panel';
    panel.style.display = isExpanded ? 'block' : 'none';
    panel.dataset.consumerPanel = topic.name;

    if (isExpanded) {
      populateConsumerPanel(panel, topic.name);
    }

    row.appendChild(panel);

    // Click: expand/collapse accordion (single topic at a time)
    row.querySelector('.topic-header').addEventListener('click', (e) => {
      e.stopPropagation();
      toggleTopicExpansion(topic.name);
    });

    container.appendChild(row);
  });

  container.scrollTop = scrollTop;
}

function toggleTopicExpansion(topicName) {
  const wasExpanded = !!expandedTopics[topicName];

  // Collapse all others (accordion: one open at a time)
  expandedTopics = {};
  selectedTopics = {};  // clicking a topic also selects it exclusively
  selectedConsumers = {};

  if (!wasExpanded) {
    expandedTopics[topicName] = true;
    selectedTopics[topicName] = true;
  }

  saveSelectedFilters();
  updateSelectionCounter();
  lastRenderedTopicsJson = null; // force re-render

  // Immediately re-render topics without waiting for poll
  if (cachedTopics) renderTopics(cachedTopics);

  // Fetch consumers for newly expanded topic
  if (expandedTopics[topicName]) {
    const panel = document.querySelector(`[data-consumer-panel="${topicName}"]`);
    if (panel) populateConsumerPanel(panel, topicName);
  }

  // Update metrics for selection change
  debouncedPollMetrics();
}

async function populateConsumerPanel(panel, topicName, forceRefresh = false) {
  const hasData = Array.isArray(topicConsumers[topicName]);

  // Respect backoff — don't hammer an endpoint that's timing out
  if (isConsumerFetchBackedOff(topicName)) {
    if (!hasData) renderConsumerError(panel, topicName);
    else renderConsumerPanel(panel, topicName);
    return;
  }

  if (!hasData) {
    panel.innerHTML = '<div class="consumers-loading">Loading consumers…</div>';
  }

  if ((forceRefresh || !hasData) && !loadingConsumers[topicName]) {
    loadingConsumers[topicName] = true;
    try {
      const consumers = await fetchConsumersForTopic(topicName);
      if (Array.isArray(consumers)) {
        topicConsumers[topicName] = consumers;
        recordConsumerLagHistory(consumers);
      } else if (consumers === undefined) {
        // Skipped because another consumer request was in flight — try next poll
        if (hasData) renderConsumerPanel(panel, topicName);
        return;
      }
    } finally {
      delete loadingConsumers[topicName];
    }
  }

  // Check if panel is still in DOM and topic still expanded
  if (!expandedTopics[topicName]) return;

  if (!Array.isArray(topicConsumers[topicName])) {
    renderConsumerError(panel, topicName);
    return;
  }

  renderConsumerPanel(panel, topicName);
}

function renderConsumerError(panel, topicName) {
  const f = consumerFetchFailures[topicName];
  const reason = f?.message || 'unavailable';
  const waitMs = f ? Math.max(0, f.nextRetryAt - Date.now()) : 0;
  const waitStr = waitMs > 0 ? ` · retrying in ${Math.ceil(waitMs / 1000)}s` : '';
  panel.innerHTML = `
    <div class="consumers-error">
      ⚠ Consumers ${reason}${waitStr}
      <button class="consumer-retry" data-retry="${topicName}">Retry</button>
    </div>
  `;
  panel.querySelector('.consumer-retry')?.addEventListener('click', (e) => {
    e.stopPropagation();
    delete consumerFetchFailures[topicName];
    populateConsumerPanel(panel, topicName, true);
  });
}

// Refresh consumer data for the currently expanded topic on each poll
async function refreshExpandedConsumers() {
  const expanded = Object.keys(expandedTopics).filter(k => expandedTopics[k]);
  for (const topicName of expanded) {
    const panel = document.querySelector(`[data-consumer-panel="${topicName}"]`);
    if (panel) await populateConsumerPanel(panel, topicName, true);
  }
}

function renderConsumerPanel(panel, topicName) {
  const consumers = topicConsumers[topicName];
  const searchTerm = document.getElementById('globalSearch')?.value?.toLowerCase() || '';

  if (!consumers || consumers.length === 0) {
    panel.innerHTML = '<div class="consumers-empty">No consumers for this topic</div>';
    return;
  }

  const filtered = consumers.filter(c => {
    if (hideNoConsumers && parseLag(c.lag) <= 0) return false;
    return !searchTerm || c.name.toLowerCase().includes(searchTerm);
  });

  if (filtered.length === 0) {
    panel.innerHTML = hideNoConsumers
      ? '<div class="consumers-empty">No consumers with positive lag</div>'
      : '<div class="consumers-empty">No consumers match search</div>';
    return;
  }

  panel.innerHTML = filtered.map(c => {
    const isSelected = !!selectedConsumers[c.name];
    const lagDisplay = formatLag(c.lag);
    const badge = healthBadge(c.state);
    const members = c.members > 0 ? `<span class="consumer-members">${c.members} member${c.members === 1 ? '' : 's'}</span>` : '';

    // Trend + ETA shown only for the selected consumer
    let detailHtml = '';
    if (isSelected) {
      const samples = consumerLagHistory[c.name] || [];
      const trend = lagTrend(c.name, c.lag);
      const eta = etaToZero(c.name, c.lag);
      const parts = [];
      if (trend) {
        if (trend.dir === 'caught-up') {
          parts.push(`<span style="color:${trend.color};" title="Lag is 0 right now. Any earlier decline in this window is history, not something still happening.">✓ caught up</span>`);
        } else if (trend.dir === 'stable') {
          parts.push(`<span style="color:#fbbf24;">⚓ stable</span>`);
        } else {
          parts.push(`<span style="color:${trend.color};">${trend.icon} ${trend.dir}</span>`);
          parts.push(`${fmtCompact(Math.abs(trend.rate))}/s`);
        }
      } else {
        parts.push(`<span style="opacity:.6;">◌ collecting trend… ${samples.length}/${TREND_WINDOW}</span>`);
      }
      if (eta !== null) parts.push(`ETA ${formatDuration(eta)}`);
      const windowVals = samples.slice(-TREND_WINDOW).map(s => s.v);
      const spark = sparkline(samples.map(s => s.v));
      let sparkTitle = '';
      if (windowVals.length >= 2) {
        const lo = Math.min(...windowVals), hi = Math.max(...windowVals);
        const spanSec = Math.round((samples[samples.length - 1].t - samples[samples.length - windowVals.length].t) / 1000);
        sparkTitle = `Last ${windowVals.length} samples over ~${formatDuration(spanSec)} · low ${fmtCompact(lo)} → high ${fmtCompact(hi)}`;
      }
      detailHtml = `<div class="consumer-trend">${parts.join(' · ')}</div>` +
                   (spark ? `<div class="consumer-spark" title="${sparkTitle}">${spark}</div>` : '');
    }

    return `
      <div class="consumer-row${isSelected ? ' selected' : ''}" data-consumer="${c.name}" data-topic="${topicName}">
        <span class="consumer-dot">●</span>
        <div class="consumer-info">
          <div class="consumer-name" title="${c.name}">${c.name} ${badge}</div>
          <div class="consumer-lag">${lagDisplay}${members ? ' · ' + members : ''}</div>
          ${detailHtml}
        </div>
      </div>
    `;
  }).join('');

  panel.querySelectorAll('.consumer-row').forEach(el => {
    el.addEventListener('click', (e) => {
      e.stopPropagation();
      const consumerName = el.dataset.consumer;
      // Toggle consumer selection (single consumer)
      const wasSelected = !!selectedConsumers[consumerName];
      selectedConsumers = {};
      if (!wasSelected) selectedConsumers[consumerName] = true;
      saveSelectedFilters();
      updateSelectionCounter();
      // Re-render panel in place (no full topic list re-render)
      renderConsumerPanel(panel, topicName);
      debouncedPollMetrics();
    });
  });
}

// ── Metrics helpers ───────────────────────────────────────────────────────────

function sumMessages(topics) {
  if (!topics) return 0;
  const hasSelection = Object.values(selectedTopics).some(Boolean);
  return topics.reduce((sum, t) => {
    if (hasSelection && !selectedTopics[t.name]) return sum;
    return sum + (t.messageCount || 0);
  }, 0);
}

function sumSelectedConsumerLag() {
  let total = 0;
  const hasConsumerSelection = Object.values(selectedConsumers).some(Boolean);
  if (!hasConsumerSelection) return 0;

  Object.keys(selectedConsumers).filter(k => selectedConsumers[k]).forEach(consumerName => {
    // Find consumer in topicConsumers
    Object.values(topicConsumers).forEach(consumers => {
      const c = consumers.find(c => c.name === consumerName);
      if (c) {
        const lagVal = typeof c.lag === 'string' ? parseLag(c.lag) : (c.lag || 0);
        total += lagVal;
      }
    });
  });
  return total;
}

function parseLag(lag) {
  if (typeof lag === 'number') return lag;
  if (typeof lag === 'string') {
    const m = lag.match(/\d+/);
    return m ? parseInt(m[0]) : 0;
  }
  return 0;
}

function formatLag(lag) {
  if (lag === null || lag === undefined) return '— lag';
  if (typeof lag === 'string') {
    // e.g. "Stable (24)" — treat as valid
    if (lag.toLowerCase().includes('stable')) return `⚓ ${lag}`;
    const n = parseLag(lag);
    return fmt(n) + ' lag';
  }
  if (lag === 0) return '0 lag';
  return fmt(lag) + ' lag';
}

// ── Persistence ───────────────────────────────────────────────────────────────

function loadSettings() {
  safeStorageGet([FAST_MODE_KEY, SHOW_NON_EMPTY_ONLY_KEY, SHOW_SELECTED_ONLY_KEY], (result) => {
    fastModeEnabled = !!result[FAST_MODE_KEY];
    showNonEmptyOnly = !!result[SHOW_NON_EMPTY_ONLY_KEY];
    showSelectedOnly = !!result[SHOW_SELECTED_ONLY_KEY];
  });
  updateSelectionCounter();
}

function loadSelectedFilters() {
  const topicsKey = SELECTED_TOPICS_KEY_PREFIX + currentClusterId;
  const consumersKey = SELECTED_CONSUMERS_KEY_PREFIX + currentClusterId;
  safeStorageGet([topicsKey, consumersKey], (result) => {
    selectedTopics = result[topicsKey] || {};
    selectedConsumers = result[consumersKey] || {};
    // Restore expanded state — accordion invariant: at most ONE topic expanded
    expandedTopics = {};
    const firstSelected = Object.keys(selectedTopics).find(k => selectedTopics[k]);
    if (firstSelected) expandedTopics[firstSelected] = true;
    updateSelectionCounter();
  });
}

function saveSelectedFilters() {
  const topicsKey = SELECTED_TOPICS_KEY_PREFIX + currentClusterId;
  const consumersKey = SELECTED_CONSUMERS_KEY_PREFIX + currentClusterId;
  safeStorageSet({ [topicsKey]: selectedTopics, [consumersKey]: selectedConsumers });
}

// ── Utilities ─────────────────────────────────────────────────────────────────

// True while this content script's extension context is still alive.
// After the extension is reloaded/updated, the old injected script keeps
// running but every chrome.* call throws "Extension context invalidated".
function isExtensionContextValid() {
  try {
    return !!(chrome && chrome.runtime && chrome.runtime.id);
  } catch (e) {
    return false;
  }
}

function isContextError(e) {
  const msg = (e && (e.message || String(e))) || '';
  return msg.includes('Extension context invalidated') ||
         msg.includes('Extension context was invalidated') ||
         msg.includes('message port closed') ||
         msg.includes('receiving end does not exist');
}

function isNetworkFetchError(e) {
  const msg = (e && (e.message || String(e))) || '';
  return e?.name === 'TypeError' && msg.includes('Failed to fetch');
}

function warnThrottled(key, message, ...args) {
  const now = Date.now();
  const lastAt = lastLogAtByKey[key] || 0;
  if (now - lastAt >= LOG_DEDUPE_WINDOW_MS) {
    lastLogAtByKey[key] = now;
    console.warn(message, ...args);
  } else {
    console.debug(message, ...args);
  }
}

// Called when we detect the context died — tear down cleanly and stop all work.
function handleInvalidatedContext() {
  if (contextInvalidated) return;
  contextInvalidated = true;
  // Clear the liveness flag so the service worker knows this world is dead and
  // re-injects a fresh script instead of assuming one is already running.
  try { window.__kafkaEyeAlive = false; } catch (e) { /* ignore */ }
  if (pollingInterval) { clearInterval(pollingInterval); pollingInterval = null; }
  if (urlWatchInterval) { clearInterval(urlWatchInterval); urlWatchInterval = null; }
  clearTimeout(pollMetricsTimeout);

  // The old content script cannot be revived after an extension reload.
  // Reloading the page is the only reliable way to install a fresh script,
  // and avoids leaving a stale error entry in the browser's extension errors.
  setTimeout(() => window.location.reload(), 0);
}

function safeStorageGet(keys, callback) {
  if (!isExtensionContextValid()) { handleInvalidatedContext(); return; }
  try {
    chrome.storage.local.get(keys, (result) => {
      // Everything here must be inside try: reading chrome.runtime.lastError
      // itself throws once the context has been invalidated.
      try {
        if (chrome.runtime.lastError) { handleInvalidatedContext(); return; }
        if (!isExtensionContextValid()) { handleInvalidatedContext(); return; }
        callback(result || {});
      } catch (e) {
        if (isContextError(e)) handleInvalidatedContext();
        else console.error('[Kafka Eye] storage callback error:', e);
      }
    });
  } catch (e) {
    handleInvalidatedContext();
  }
}

function safeStorageSet(items) {
  if (!isExtensionContextValid()) { handleInvalidatedContext(); return; }
  try {
    chrome.storage.local.set(items, () => {
      try {
        if (chrome.runtime.lastError) handleInvalidatedContext();
      } catch (e) {
        handleInvalidatedContext();
      }
    });
  } catch (e) {
    handleInvalidatedContext();
  }
}

function showError(containerId, message) {
  const container = document.getElementById(containerId);
  if (container) container.innerHTML = `<div class="error">${message}</div>`;
}

function debounce(func, delay) {
  let id;
  return (...args) => {
    clearTimeout(id);
    id = setTimeout(() => { try { func(...args); } catch (e) { console.error('[Kafka Eye] Debounce error:', e); } }, delay);
  };
}

function fmt(num) {
  if (num === undefined || num === null) return '—';
  if (num >= 1_000_000_000_000) return (num / 1_000_000_000_000).toFixed(1) + 'B (' + num.toLocaleString() + ')';
  if (num >= 1_000_000_000)     return (num / 1_000_000_000).toFixed(1) + 'KM (' + num.toLocaleString() + ')';
  if (num >= 1_000_000)         return (num / 1_000_000).toFixed(1) + 'M (' + num.toLocaleString() + ')';
  if (num >= 1_000)             return (num / 1_000).toFixed(1) + 'K (' + num.toLocaleString() + ')';
  return num.toLocaleString();
}

// Compact form (no full-number suffix), used for per-second rates
function fmtCompact(num) {
  if (num === undefined || num === null || isNaN(num)) return '—';
  if (num >= 1_000_000_000_000) return (num / 1_000_000_000_000).toFixed(1) + 'B';
  if (num >= 1_000_000_000)     return (num / 1_000_000_000).toFixed(1) + 'KM';
  if (num >= 1_000_000)         return (num / 1_000_000).toFixed(1) + 'M';
  if (num >= 1_000)             return (num / 1_000).toFixed(1) + 'K';
  return num.toFixed(1);
}

// Same scale as fmtCompact, but for whole-number COUNTS (messages, lag) rather
// than rates: below 1K a count is an integer, so "24 lag" not "24.0 lag".
function fmtCompactCount(num) {
  if (num === undefined || num === null || isNaN(num)) return '—';
  if (num < 1_000) return String(Math.round(num));
  return fmtCompact(num);
}
