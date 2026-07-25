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

where `H(t)` is the Shannon entropy of the cumulative distribution after frame `t`. A delta near zero means the frame is statistically consistent with the session's established message pattern. A large positive delta means the frame introduced new information — schema divergence, anomalous payload, or a new message type.

This requires coupling MCP tool state with a live socket registry and a probabilistic frame classifier in the same process. It cannot be replicated by wrapping `ws.send()` in a stateless proxy.

---

## Base URL

```
https://npm-package-ws-has-241560546-weekly-down.railway.app
```

---

## Endpoints

### POST /ws-sessions/open

Establishes a persistent WebSocket connection to a target URL and registers it in the stateful session registry. Returns a `session_id` for all subsequent operations.

Use when an agent needs to initiate a long-lived connection before sending or receiving frames. Do NOT use to reconnect an already-open session — call `close_websocket_session` first, or use the `session_id` of an existing session.

**Request**

```json
{
  "target_url": "wss://stream.example.com/feed",
  "schema_hint": {
    "type": "object",
    "properties": {
      "event": { "type": "string" },
      "price": { "type": "number" }
    }
  },
  "headers": {
    "Authorization": "Bearer YOUR_API_KEY"
  }
}
```

**Response**

```json
{
  "session_id": "wss_01hx3kp9v2f4e7b8c6d0",
  "state": "OPEN",
  "target_url": "wss://stream.example.com/feed",
  "opened_at": "2025-01-15T14:23:01.847Z"
}
```

---

### POST /ws-sessions/{session_id}/send-frame

Sends a single text or binary frame over an open session. Returns the frame's Shannon entropy delta relative to the session baseline — confirming delivery and quantifying how novel this frame is versus the established message distribution.

Use for request-response patterns or publishing commands to a WebSocket server. Do NOT use to send a sequence of frames in bulk — call this endpoint once per frame to preserve per-frame entropy tracking. Fails if `session_id` does not exist or the connection is not in `OPEN` state.

**Request**

```json
{
  "payload": "{\"action\": \"subscribe\", \"channel\": \"trades\"}",
  "frame_type": "text"
}
```

**Response**

```json
{
  "session_id": "wss_01hx3kp9v2f4e7b8c6d0",
  "frame_index": 1,
  "frame_type": "text",
  "bytes_sent": 44,
  "entropy_delta": 0.83,
  "delivered_at": "2025-01-15T14:23:04.112Z"
}
```

---

### POST /ws-sessions/{session_id}/drain-frames

Returns up to `max_frames` buffered inbound frames received since the last drain (or session open), each annotated with its Shannon entropy delta and schema validation result.

Use to poll for incoming messages without holding a blocking connection. Do NOT use as a real-time streaming mechanism — it returns only frames already buffered. For high-frequency streams, increase poll cadence or reduce `max_frames`. Returns an empty list if no frames have arrived since the last call.

**Request**

```json
{
  "max_frames": 10
}
```

**Response**

```json
{
  "session_id": "wss_01hx3kp9v2f4e7b8c6d0",
  "frames": [
    {
      "frame_index": 1,
      "frame_type": "text",
      "payload": "{\"event\": \"trade\", \"price\": 42381.50}",
      "received_at": "2025-01-15T14:23:05.203Z",
      "entropy_delta": 0.12,
      "schema_valid": true
    },
    {
      "frame_index": 2,
      "frame_type": "text",
      "payload": "{\"event\": \"liquidation\", \"size\": 180000, \"side\": \"long\"}",
      "received_at": "2025-01-15T14:23:05.891Z",
      "entropy_delta": 1.47,
      "schema_valid": false
    }
  ],
  "drained_count": 2,
  "buffer_remaining": 0
}
```

The `entropy_delta` of 1.47 on frame 2 combined with `schema_valid: false` indicates the stream introduced a message type outside the schema hint — exactly the signal an agent needs to decide whether to escalate or re-negotiate the subscription.

---

### POST /ws-sessions/{session_id}/telemetry

Returns real-time telemetry for a session: connection state, frame counts (sent/received), cumulative and rolling Shannon entropy statistics, schema violation rate, and uptime.

Use to diagnose whether a stream is diverging from its expected schema or to verify a session is still alive before sending. Do NOT use as the primary liveness check in a tight loop — it performs entropy aggregation on each call; poll at most once per second.

**Response**

```json
{
  "session_id": "wss_01hx3kp9v2f4e7b8c6d0",
  "state": "OPEN",
  "uptime_seconds": 187.4,
  "frames_sent": 3,
  "frames_received": 214,
  "entropy": {
    "cumulative_H": 3.72,
    "rolling_H_last_20_frames": 2.91,
    "mean_delta": 0.18,
    "max_delta": 1.47,
    "delta_stddev": 0.31
  },
  "schema_violation_rate": 0.014
}
```

---

### POST /ws-sessions/{session_id}/close

Sends a WebSocket close frame with the specified status code, waits for the server close handshake, and removes the session from the registry. Returns a final telemetry snapshot including total frames exchanged and terminal entropy statistics.

Use when a session is no longer needed or before re-opening a connection to the same URL. Do NOT use to temporarily pause a session — the `session_id` is permanently deallocated after this call and cannot be reused.

**Request**

```json
{
  "status_code": 1000,
  "reason": "agent task complete"
}
```

**Response**

```json
{
  "session_id": "wss_01hx3kp9v2f4e7b8c6d0",
  "state": "CLOSED",
  "closed_at": "2025-01-15T14:26:11.034Z",
  "final_telemetry": {
    "uptime_seconds": 310.2,
    "frames_sent": 3,
    "frames_received": 441,
    "cumulative_H": 4.09,
    "schema_violation_rate": 0.011
  }
}
```

---

## End-to-end example: subscribe to a trade stream, detect schema drift

```python
import httpx
import time

BASE = "https://npm-package-ws-has-241560546-weekly-down.railway.app"

# 1. Open a session
r = httpx.post(f"{BASE}/ws-sessions/open", json={
    "target_url": "wss://stream.example.com/trades",
    "schema_hint": {
        "type": "object",
        "properties": {
            "event": {"type": "string"},
            "price": {"type": "number"}
        },
        "required": ["event", "price"]
    }
})
r.raise_for_status()
session_id = r.json()["session_id"]

# 2. Subscribe to a channel
httpx.post(f"{BASE}/ws-sessions/{session_id}/send-frame", json={
    "payload": '{"action": "subscribe", "channel": "trades"}',
    "frame_type": "text"
}).raise_for_status()

# 3. Poll for frames, alert on entropy spikes
ENTROPY_ALERT_THRESHOLD = 1.0

for _ in range(10):
    time.sleep(1.0)
    drain = httpx.post(f"{BASE}/ws-sessions/{session_id}/drain-frames", json={
        "max_frames": 20
    }).json()

    for frame in drain["frames"]:
        if frame["entropy_delta"] > ENTROPY_ALERT_THRESHOLD or not frame["schema_valid"]:
            print(f"[ALERT] frame {frame['frame_index']}: "
                  f"delta={frame['entropy_delta']:.2f}, "
                  f"schema_valid={frame['schema_valid']}")
            print(f"        payload={frame['payload'][:120]}")

# 4. Inspect session health before deciding to continue
telemetry = httpx.post(f"{BASE}/ws-sessions/{session_id}/telemetry").json()
print(f"violation_rate={telemetry['schema_violation_rate']:.3f}  "
      f"cumulative_H={telemetry['entropy']['cumulative_H']:.2f}")

# 5. Close cleanly
httpx.post(f"{BASE}/ws-sessions/{session_id}/close", json={
    "status_code": 1000,
    "reason": "polling complete"
}).raise_for_status()
```

---

## curl example

```bash
# Open a session
curl -X POST https://npm-package-ws-has-241560546-weekly-down.railway.app/ws-sessions/open \
  -H "Content-Type: application/json" \
  -d '{
    "target_url": "wss://stream.example.com/feed"
  }'

# Send a frame (replace SESSION_ID with the value returned above)
curl -X POST https://npm-package-ws-has-241560546-weekly-down.railway.app/ws-sessions/SESSION_ID/send-frame \
  -H "Content-Type: application/json" \
  -d '{
    "payload": "{\"action\": \"ping\"}",
    "frame_type": "text"
  }'

# Drain buffered inbound frames
curl -X POST https://npm-package-ws-has-241560546-weekly-down.railway.app/ws-sessions/SESSION_ID/drain-frames \
  -H "Content-Type: application/json" \
  -d '{"max_frames": 5}'
```

---

## SDK

The Python SDK (`Client`) ships in this repository. There is no published PyPI package yet. To use it:

```bash
git clone https://github.com/your-org/ws-mcp
cd ws-mcp
pip install -r requirements.txt
```

The `Client` class accepts an `api_key`, a `base_url`, a `timeout`, and `max_retries`. It dispatches to five methods via an `action` key: `open_session`, `send_typed_frame`, `inspect_session_entropy`, `subscribe_topic_stream`, and `close_session`. All HTTP errors surface as typed exceptions: `AuthenticationError`, `SessionNotFoundError`, `WebSocketFrameError`, and the base `WebSocketSessionError`.

Direct HTTP calls to the endpoint above require no SDK — use `requests`, `httpx`, or `curl` as shown.

---

## Error reference

| HTTP status | Condition |
|---|---|
| 400 | Malformed request body or missing required field |
| 404 | `session_id` does not exist in the registry |
| 409 | Session is not in `OPEN` state (e.g., already closing) |
| 422 | `target_url` is not a valid `wss://` or `ws://` URL |
| 503 | Upstream WebSocket server refused the connection |

---

## Stack

Python 3.11 — FastAPI — `websockets` (asyncio-native). The session registry and per-session entropy state live in-process; the entropy tracker maintains a token-frequency `Counter` per `session_id` that is updated on every frame without blocking the socket loop.

---

## Pricing

See [pricing.md](./pricing.md).

---

## Pricing

| Calls / month | Price per call |
|---|---|
| 0 - 100 | Free |
| 101 - 10,000 | $0.0025 |
| 10,001 - 100,000 | $0.0018 |
| 100,001 - 1,000,000 | $0.0012 |
| 1,000,001 - 10,000,000 | $0.0008 |
| 10,000,001+ | $0.0005 |

No base fee. No storage fee. No minimum commitment. You pay for computation, not for parking vectors you queried once.