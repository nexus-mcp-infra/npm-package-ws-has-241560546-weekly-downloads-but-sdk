# ws-mcp

**WebSocket session management as MCP tools — with per-frame Shannon entropy tracking.**

`ws` has 241,560,546 weekly downloads. There are zero MCP servers that expose WebSocket primitives. This is that server.

---

## The problem

AI agents cannot interact with live WebSocket streams without custom glue code per integration. There is no standardized way for an LLM to open a persistent connection, send a typed frame, and receive structured events as tool call responses. Debugging message flows requires external tooling (Wireshark, browser devtools) that is incompatible with agentic workflows.

ws-mcp exposes five atomic operations over HTTP: open a session, send a frame, drain buffered frames, inspect telemetry, and close. Each frame — inbound or outbound — is scored with a Shannon entropy delta relative to the session's cumulative message distribution, so an agent can detect when a stream is diverging from its expected schema without any external monitoring.

---

## Why the entropy delta is not optional

The server maintains a live token-frequency `Counter` per `session_id` inside the same process that holds the socket open. For each frame, it computes:

```
delta = H(t) - H(t-1)
```

where `H(t)` is the Shannon entropy of the cumulative byte distribution after frame `t`. A delta near zero means the frame is statistically consistent with the session's established message pattern. A large positive delta means the frame introduced new information — schema divergence, anomalous payload, or a new message type.

This requires coupling MCP tool state with a live socket registry and a probabilistic frame classifier in the same process. It cannot be replicated by wrapping `ws.send()` in a stateless proxy.

---

## Base URL

```
https://npm-package-ws-has-241560546-w-production.up.railway.app
```

## Authentication

**None.** There is no API key, header, or auth mechanism of any kind on this asset today — every request is served regardless of headers sent.

## Pricing

- **`POST /ws-sessions/open`** requires an **x402 payment**: `$0.01` USDC on Base Sepolia (testnet), scheme `exact`. A request without payment gets `402 Payment Required` with the payment details in the `payment-required` response header.
- **All other operations** (`send-frame`, `drain-frames`, `telemetry`, `close`) act on a session that was already paid for at open time and are **not** separately charged.

---

## Endpoints

### `POST /ws-sessions/open`

Establishes a persistent WebSocket connection to a target URL and registers it in the stateful session registry. Returns a `session_id` for all subsequent operations. Requires an x402 payment (see Pricing above).

Use when an agent needs to initiate a long-lived connection before sending or receiving frames. Do NOT use to reconnect an already-open session — call `close` first, or use the `session_id` of an existing session.

**Request**

```json
{
  "target_url": "wss://echo.websocket.org",
  "connect_timeout_seconds": 10.0,
  "extra_headers": {
    "X-Custom-Header": "value"
  }
}
```

`target_url` must start with `ws://` or `wss://` (validated, not just a regex in the schema). `connect_timeout_seconds` defaults to `10.0`, range `0.5`–`60.0`. `extra_headers` is optional (HTTP headers sent on the upgrade request).

**Response** (`201`)

```json
{
  "session_id": "8662044208f54095b327ab4c29e1554c",
  "target_url": "wss://echo.websocket.org",
  "state": "OPEN",
  "opened_at_unix": 1785012145.03,
  "message": "Session '8662044208f54095b327ab4c29e1554c' is OPEN. Use session_id for all subsequent operations."
}
```

`session_id` is a 32-character hex string (`uuid.uuid4().hex`, no dashes) — use it as-is in the paths below.

Errors: `422` if `target_url` doesn't start with `ws://`/`wss://`; `504` if the handshake doesn't complete within `connect_timeout_seconds`; `502` if the host is unreachable or refuses the connection; `503` if the server is already at `NEXUS_WS_MAX_SESSIONS` concurrent sessions.

---

### `POST /ws-sessions/{session_id}/send-frame`

Sends a single text or binary frame over an open session and returns the frame's Shannon entropy delta relative to the session baseline.

Use for request-response patterns or publishing commands to a WebSocket server. Do NOT use to send a sequence of frames in bulk — call this endpoint once per frame. Fails if `session_id` does not exist or the connection is not in `OPEN` state.

**Request**

```json
{
  "payload": "{\"action\": \"subscribe\", \"channel\": \"trades\"}",
  "frame_type": "text"
}
```

`frame_type` is `"text"` (default) or `"binary"` — if binary, `payload` must be a hex-encoded string.

**Response** (`200`)

```json
{
  "session_id": "8662044208f54095b327ab4c29e1554c",
  "frame_type": "text",
  "payload_bytes": 44,
  "entropy_after": 4.237441,
  "entropy_delta": 0.245712,
  "schema_valid": true,
  "sent_at_unix": 1785012151.68
}
```

Errors: `404` if `session_id` is not in the registry (`session_id '<id>' not found in registry`); `409` if the session isn't in `OPEN` state; `422` if `frame_type` is `"binary"` and `payload` isn't valid hex; `502` if the underlying socket send fails.

---

### `POST /ws-sessions/{session_id}/drain-frames`

Returns up to `max_frames` buffered inbound frames received since the last drain (or session open), each annotated with its Shannon entropy delta and schema validation result.

Use to poll for incoming messages without holding a blocking connection. Do NOT use as a real-time streaming mechanism — it returns only frames already buffered (buffer capped at 512 frames per session; oldest frames are dropped once full).

**Request**

```json
{
  "max_frames": 10
}
```

`max_frames` defaults to `32`, range `1`–`256`.

**Response** (`200`)

```json
{
  "session_id": "8662044208f54095b327ab4c29e1554c",
  "frames_returned": 2,
  "frames": [
    {
      "payload": "{\"event\": \"trade\", \"price\": 42381.50}",
      "frame_type": "text",
      "entropy_after": 3.91,
      "entropy_delta": 0.12,
      "schema_valid": true,
      "received_at": 1785012155.203
    },
    {
      "payload": "{\"event\": \"liquidation\", \"size\": 180000, \"side\": \"long\"}",
      "frame_type": "text",
      "entropy_after": 5.38,
      "entropy_delta": 1.47,
      "schema_valid": false,
      "received_at": 1785012155.891
    }
  ],
  "buffer_remaining": 0
}
```

The `entropy_delta` of `1.47` on the second frame combined with `schema_valid: false` indicates the stream introduced content statistically inconsistent with the session's established byte distribution — a signal an agent can use to decide whether to escalate or re-negotiate the subscription.

Errors: `404` if `session_id` is not in the registry.

---

### `POST /ws-sessions/{session_id}/telemetry`

Returns real-time telemetry for a session: connection state, frame counts, cumulative and rolling Shannon entropy statistics, schema violation rate, and uptime. Takes no request body.

Use to diagnose whether a stream is diverging from its expected schema or to verify a session is still alive before sending. Do NOT use as the primary liveness check in a tight loop — it recomputes rolling statistics on every call.

**Response** (`200`)

```json
{
  "session_id": "8662044208f54095b327ab4c29e1554c",
  "target_url": "wss://echo.websocket.org",
  "state": "OPEN",
  "uptime_seconds": 108.77,
  "frames_sent": 1,
  "frames_received": 2,
  "current_entropy": 4.16695,
  "rolling_entropy_mean": 0.18,
  "rolling_entropy_std": 0.31,
  "rolling_entropy_max": 1.47,
  "cumulative_entropy_bits": 4.16695,
  "schema_violations": 1,
  "schema_violation_rate": 0.333333,
  "inbound_buffer_depth": 2
}
```

Errors: `404` if `session_id` is not in the registry.

---

### `POST /ws-sessions/{session_id}/close`

Sends a WebSocket close frame with the specified status code, waits for the server close handshake, and removes the session from the registry. Returns a final telemetry snapshot. `session_id` is permanently deallocated after this call and cannot be reused.

**Request** (both fields optional)

```json
{
  "status_code": 1000,
  "reason": "agent task complete"
}
```

`status_code` defaults to `1000`, range `1000`–`4999` (RFC 6455). `reason` defaults to `""`, max 123 bytes.

**Response** (`200`)

```json
{
  "session_id": "8662044208f54095b327ab4c29e1554c",
  "target_url": "wss://echo.websocket.org",
  "final_state": "CLOSED",
  "frames_sent": 1,
  "frames_received": 2,
  "terminal_entropy": 4.16695,
  "schema_violations": 1,
  "uptime_seconds": 310.2,
  "message": "Session '8662044208f54095b327ab4c29e1554c' closed with status 1000. Total frames exchanged: 3. session_id is permanently deallocated."
}
```

Errors: `404` if `session_id` is not in the registry.

---

## End-to-end example: open a session, send a frame, drain, close

```python
import httpx

BASE = "https://npm-package-ws-has-241560546-w-production.up.railway.app"

# 1. Open a session (requires x402 payment on this call — omitted here for brevity,
#    see https://docs.x402.org for how to attach a payment header)
r = httpx.post(f"{BASE}/ws-sessions/open", json={
    "target_url": "wss://echo.websocket.org",
    "connect_timeout_seconds": 10.0,
})
r.raise_for_status()
session_id = r.json()["session_id"]

# 2. Send a frame (not billed)
httpx.post(f"{BASE}/ws-sessions/{session_id}/send-frame", json={
    "payload": '{"action": "ping"}',
    "frame_type": "text",
}).raise_for_status()

# 3. Drain buffered inbound frames, alert on entropy spikes
ENTROPY_ALERT_THRESHOLD = 1.0

drain = httpx.post(f"{BASE}/ws-sessions/{session_id}/drain-frames", json={
    "max_frames": 20,
}).json()

for frame in drain["frames"]:
    if frame["entropy_delta"] > ENTROPY_ALERT_THRESHOLD or not frame["schema_valid"]:
        print(f"[ALERT] delta={frame['entropy_delta']:.2f} schema_valid={frame['schema_valid']}")
        print(f"        payload={frame['payload'][:120]}")

# 4. Inspect session health before deciding to continue
telemetry = httpx.post(f"{BASE}/ws-sessions/{session_id}/telemetry").json()
print(f"schema_violation_rate={telemetry['schema_violation_rate']:.3f} "
      f"current_entropy={telemetry['current_entropy']:.2f}")

# 5. Close cleanly
httpx.post(f"{BASE}/ws-sessions/{session_id}/close", json={
    "status_code": 1000,
    "reason": "task complete",
}).raise_for_status()
```

---

## curl example

```bash
# Open a session (requires x402 payment — this call alone will return 402
# without a valid payment header; see https://docs.x402.org)
curl -X POST https://npm-package-ws-has-241560546-w-production.up.railway.app/ws-sessions/open \
  -H "Content-Type: application/json" \
  -d '{"target_url": "wss://echo.websocket.org"}'

# Send a frame (replace SESSION_ID with the value returned above — not billed)
curl -X POST https://npm-package-ws-has-241560546-w-production.up.railway.app/ws-sessions/SESSION_ID/send-frame \
  -H "Content-Type: application/json" \
  -d '{"payload": "hello world", "frame_type": "text"}'

# Drain buffered inbound frames
curl -X POST https://npm-package-ws-has-241560546-w-production.up.railway.app/ws-sessions/SESSION_ID/drain-frames \
  -H "Content-Type: application/json" \
  -d '{"max_frames": 5}'
```

---

## MCP

Connect an MCP-compatible client to the streamable HTTP endpoint at:
```
https://npm-package-ws-has-241560546-w-production.up.railway.app/mcp
```

Exposes the same 5 operations as MCP tools (`open_websocket_session`, `send_typed_ws_frame`, `drain_ws_frame_buffer`, `inspect_ws_session_telemetry`, `close_websocket_session`), with the same parameters and payment requirement (`open_websocket_session` requires x402 payment; the other 4 do not).

---

## SDKs

- **`sdk_wrappers/sdk.js`** — real HTTP client (`axios`) against the base URL above. Published to npm as `@nexus-mcp-infra/npm-package-ws-has-241560546-weekly-downloads-but-sdk`. This is the one to use.
- **`mcp_wrapper/npm_package_ws_has_241560546_weekly_downloads_but_sdk.py`** — **do not use.** It defaults to a placeholder domain (`https://api.ws-mcp.nexus.ai/v1`) that isn't this service, calls routes (`/sessions/open`, `/frames/send`, etc.) that don't exist on this API, and requires an `api_key` that this service doesn't check. It was never fixed after the initial build; kept in the repo for history, not for use.

Direct HTTP calls to the endpoint above (via `httpx`, `requests`, or `curl` as shown) require no SDK.

---

## Error reference

| HTTP status | Condition |
|---|---|
| 402 | Payment required (only on `POST /ws-sessions/open`, x402) |
| 404 | `session_id` does not exist in the registry |
| 409 | Session is not in `OPEN` state (e.g., not yet connected or already closing) |
| 422 | `target_url` is not a valid `ws://`/`wss://` URL, or a binary `payload` is not valid hex |
| 502 | Network error connecting to `target_url`, or error sending a frame |
| 503 | Session registry at capacity |
| 504 | WebSocket handshake did not complete within `connect_timeout_seconds` |

---

## Stack

Python 3.13 — FastAPI — `websockets` (asyncio-native). The session registry and per-session entropy state live in-process; the entropy tracker maintains a token-frequency `Counter` per `session_id` that is updated on every frame without blocking the socket loop. Sessions do not survive a process restart/redeploy.
