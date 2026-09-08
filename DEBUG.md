# Debugging Kafka Eye

Open the browser console (F12) and filter for `[Kafka Eye]`.

## Expected startup output

```
[Kafka Eye] Detected cluster: my-cluster
[Kafka Eye] API URL: http://localhost:8085
```

If neither line appears, the content script never ran — check that the page URL
matches `*://*/ui/clusters/*` and that the extension is enabled.

## API endpoints used

All are relative to the origin of the Kafka UI page you have open:

- `GET /api/clusters/{cluster}/topics?page={n}&perPage=100`
- `GET /api/clusters/{cluster}/topics/{topic}/consumer-groups`

Note that consumer groups are fetched **per topic**. Kafka UI has no bulk
endpoint for this, which is why lag data appears progressively rather than all
at once.

Message counts are derived as the sum of `offsetMax` across a topic's
partitions — the API exposes no direct total.

## Common errors

| Symptom | Cause |
| --- | --- |
| `HTTP 404` | API path differs on your Kafka UI version |
| `HTTP 403` | Authentication or permissions |
| `Consumers for X failed: signal timed out` | Slow consumer-groups endpoint; Kafka Eye backs off and retries |
| `Topics fetch failed: TypeError: Failed to fetch` | Usually transient network/CORS; cached topics are reused |
| `Extension context invalidated` | Harmless — logged by the old content script when the extension reloads. The service worker re-injects automatically; click the banner if it doesn't |

## Behaviour worth knowing

- **One consumer request at a time.** Concurrent requests to a slow
  consumer-groups endpoint exhaust the browser's ~6-connection-per-host limit
  and starve the topics request. A global mutex prevents this.
- **Failure backoff** per topic: 30s → 60s → 2m → 4m, capped at 5m.
- **Rates use a least-squares slope** over the last 20 samples, so the trend
  label and the sparkline always agree and a single spike won't distort them.
