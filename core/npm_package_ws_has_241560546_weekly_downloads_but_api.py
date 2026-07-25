import asyncio
import hashlib
import json
import math
import time
import uuid
from collections import Counter, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import numpy as np
import websockets
from fastapi import FastAPI, HTTPException, Path
from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Domain enums and state containers
# ---------------------------------------------------------------------------

class WsConnectionState(str, Enum):
    CONNECTING = "CONNECTING"
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class WsFrameType(str, Enum):
    TEXT = "text"
    BINARY = "binary"


# ---------------------------------------------------------------------------
# Shannon entropy engine — fully self-contained, no NEXUS imports
# ---------------------------------------------------------------------------

def _token_frequency_from_frame(frame_payload: str | bytes) -> Counter:
    """Decompose a frame into byte-level tokens and return their frequency."""
    if isinstance(frame_payload, str):
        raw = frame_payload.encode("utf-8")
    else:
        raw = frame_payload
    return Counter(raw)


def _shannon_entropy_from_counter(counter: Counter) -> float:
    """H = -sum(p * log2(p)) over observed symbol probabilities."""
    total = sum(counter.values())
    if total == 0:
        return 0.0
    probs = np.array(list(counter.values()), dtype=np.float64) / total
    probs = probs[probs > 0]
    return float(-np.sum(probs * np.log2(probs)))


def _merged_entropy(base: Counter, new: Counter) -> float:
    """Entropy of the merged symbol distribution (baseline + new frame)."""
    merged = base + new
    return _shannon_entropy_from_counter(merged)


def _entropy_delta(base_counter: Counter, frame_counter: Counter) -> tuple[float, float]:
    """
    Returns (H_new, delta) where:
      H_base = entropy of current cumulative distribution
      H_new  = entropy after incorporating this frame
      delta  = H_new - H_base  (positive => novelty, negative => redundancy)
    """
    if not base_counter:
        h_base = 0.0
    else:
        h_base = _shannon_entropy_from_counter(base_counter)
    h_new = _merged_entropy(base_counter, frame_counter)
    return h_new, h_new - h_base


# ---------------------------------------------------------------------------
# Schema validator — JSON structural fingerprinting
# ---------------------------------------------------------------------------

def _json_schema_fingerprint(payload: str) -> Optional[frozenset]:
    """
    Returns a frozenset of top-level keys if payload is valid JSON object,
    None if JSON array or non-JSON.  Used for structural consistency checks.
    """
    try:
        obj = json.loads(payload)
        if isinstance(obj, dict):
            return frozenset(obj.keys())
        return None
    except (json.JSONDecodeError, ValueError):
        return None


def _schema_violation(baseline_fingerprint: Optional[frozenset],
                      current_fingerprint: Optional[frozenset]) -> bool:
    if baseline_fingerprint is None or current_fingerprint is None:
        return False
    return baseline_fingerprint != current_fingerprint


# ---------------------------------------------------------------------------
# Session data model
# ---------------------------------------------------------------------------

ROLLING_WINDOW_SIZE = 64   # frames kept for rolling entropy stats
FRAME_BUFFER_LIMIT   = 512  # max inbound frames buffered per session


@dataclass
class WsSessionRecord:
    session_id: str
    target_url: str
    state: WsConnectionState = WsConnectionState.CONNECTING
    websocket: Any = field(default=None, repr=False)
    listener_task: Any = field(default=None, repr=False)

    opened_at: float = field(default_factory=time.monotonic)
    frames_sent: int = 0
    frames_received: int = 0
    schema_violations: int = 0

    # Cumulative byte-frequency distribution across ALL frames (sent + received)
    cumulative_byte_counter: Counter = field(default_factory=Counter)
    current_entropy: float = 0.0

    # Rolling window of per-frame entropy deltas for variance calculation
    rolling_deltas: deque = field(default_factory=lambda: deque(maxlen=ROLLING_WINDOW_SIZE))

    # Baseline JSON schema fingerprint (first JSON frame establishes it)
    schema_fingerprint: Optional[frozenset] = None

    # Inbound frame buffer — each entry is (payload, entropy_delta, schema_ok, ts)
    inbound_buffer: deque = field(
        default_factory=lambda: deque(maxlen=FRAME_BUFFER_LIMIT)
    )

    def record_frame(self, payload: str | bytes, direction: str) -> tuple[float, float, bool]:
        """
        Update entropy state with a new frame.
        Returns (new_entropy, delta, schema_ok).
        direction: 'sent' | 'received'
        """
        frame_counter = _token_frequency_from_frame(payload)
        h_new, delta = _entropy_delta(self.cumulative_byte_counter, frame_counter)
        self.cumulative_byte_counter += frame_counter
        self.current_entropy = h_new
        self.rolling_deltas.append(delta)

        schema_ok = True
        if isinstance(payload, str):
            fp = _json_schema_fingerprint(payload)
            if fp is not None:
                if self.schema_fingerprint is None:
                    self.schema_fingerprint = fp
                elif _schema_violation(self.schema_fingerprint, fp):
                    schema_ok = False
                    self.schema_violations += 1

        if direction == "sent":
            self.frames_sent += 1
        else:
            self.frames_received += 1

        return h_new, delta, schema_ok


# ---------------------------------------------------------------------------
# In-process session registry
# ---------------------------------------------------------------------------

class WsSessionRegistry:
    def __init__(self) -> None:
        self._sessions: dict[str, WsSessionRecord] = {}
        self._lock = asyncio.Lock()

    async def create(self, target_url: str) -> WsSessionRecord:
        async with self._lock:
            sid = uuid.uuid4().hex
            record = WsSessionRecord(session_id=sid, target_url=target_url)
            self._sessions[sid] = record
            return record

    async def get(self, session_id: str) -> WsSessionRecord:
        async with self._lock:
            record = self._sessions.get(session_id)
        if record is None:
            raise HTTPException(
                status_code=404,
                detail=f"session_id '{session_id}' not found in registry"
            )
        return record

    async def remove(self, session_id: str) -> None:
        async with self._lock:
            self._sessions.pop(session_id, None)

    def count(self) -> int:
        return len(self._sessions)


# Global registry instance — singleton for the process lifetime
SESSION_REGISTRY = WsSessionRegistry()


# ---------------------------------------------------------------------------
# Background listener — pumps inbound frames into the session buffer
# ---------------------------------------------------------------------------

async def _inbound_frame_listener(record: WsSessionRecord) -> None:
    try:
        async for message in record.websocket:
            h_new, delta, schema_ok = record.record_frame(message, "received")
            record.inbound_buffer.append({
                "payload": message if isinstance(message, str) else message.hex(),
                "frame_type": "text" if isinstance(message, str) else "binary",
                "entropy_after": round(h_new, 6),
                "entropy_delta": round(delta, 6),
                "schema_valid": schema_ok,
                "received_at": time.time(),
            })
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception:
        pass
    finally:
        record.state = WsConnectionState.CLOSED


# ---------------------------------------------------------------------------
# Lifespan context — clean shutdown of open connections
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    for record in list(SESSION_REGISTRY._sessions.values()):
        if record.websocket and record.state == WsConnectionState.OPEN:
            try:
                await record.websocket.close(1001)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="WebSocket Session Manager API",
    version="1.0.0",
    description=(
        "Stateful WebSocket session registry with per-connection Shannon entropy "
        "delta tracking for schema divergence detection in agentic workflows."
    ),
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class OpenSessionRequest(BaseModel):
    target_url: str = Field(
        ...,
        description="WebSocket URL to connect to (ws:// or wss://)",
        min_length=6,
    )
    connect_timeout_seconds: float = Field(
        default=10.0,
        ge=0.5,
        le=60.0,
        description="Maximum seconds to wait for the handshake to complete.",
    )
    extra_headers: Optional[dict[str, str]] = Field(
        default=None,
        description="Optional HTTP headers for the upgrade request (e.g. auth tokens).",
    )

    @field_validator("target_url")
    @classmethod
    def validate_ws_scheme(cls, v: str) -> str:
        if not (v.startswith("ws://") or v.startswith("wss://")):
            raise ValueError("target_url must start with ws:// or wss://")
        return v


class OpenSessionResponse(BaseModel):
    session_id: str
    target_url: str
    state: WsConnectionState
    opened_at_unix: float
    message: str


class SendFrameRequest(BaseModel):
    payload: str = Field(
        ...,
        description="Frame payload. For binary frames, pass hex-encoded string.",
        min_length=0,
        max_length=131072,
    )
    frame_type: WsFrameType = Field(
        default=WsFrameType.TEXT,
        description="'text' sends payload as UTF-8 string; 'binary' decodes payload from hex.",
    )


class SendFrameResponse(BaseModel):
    session_id: str
    frame_type: WsFrameType
    payload_bytes: int
    entropy_after: float
    entropy_delta: float
    schema_valid: bool
    sent_at_unix: float


class DrainFramesRequest(BaseModel):
    max_frames: int = Field(
        default=32,
        ge=1,
        le=256,
        description="Maximum number of buffered frames to return in a single drain.",
    )


class DrainFramesResponse(BaseModel):
    session_id: str
    frames_returned: int
    frames: list[dict]
    buffer_remaining: int


class TelemetryResponse(BaseModel):
    session_id: str
    target_url: str
    state: WsConnectionState
    uptime_seconds: float
    frames_sent: int
    frames_received: int
    current_entropy: float
    rolling_entropy_mean: float
    rolling_entropy_std: float
    rolling_entropy_max: float
    cumulative_entropy_bits: float
    schema_violations: int
    schema_violation_rate: float
    inbound_buffer_depth: int


class CloseSessionRequest(BaseModel):
    status_code: int = Field(
        default=1000,
        ge=1000,
        le=4999,
        description="WebSocket close status code per RFC 6455.",
    )
    reason: str = Field(
        default="",
        max_length=123,
        description="Optional close reason string (max 123 bytes per RFC 6455).",
    )


class CloseSessionResponse(BaseModel):
    session_id: str
    target_url: str
    final_state: WsConnectionState
    frames_sent: int
    frames_received: int
    terminal_entropy: float
    schema_violations: int
    uptime_seconds: float
    message: str


# ---------------------------------------------------------------------------
# Endpoint 1 — open_websocket_session
# ---------------------------------------------------------------------------

@app.post(
    "/ws-sessions/open",
    response_model=OpenSessionResponse,
    status_code=201,
    summary="open_websocket_session",
)
async def open_websocket_session(body: OpenSessionRequest) -> OpenSessionResponse:
    record = await SESSION_REGISTRY.create(body.target_url)

    try:
        headers = list(body.extra_headers.items()) if body.extra_headers else []
        ws = await asyncio.wait_for(
            websockets.connect(
                body.target_url,
                additional_headers=headers,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ),
            timeout=body.connect_timeout_seconds,
        )
    except asyncio.TimeoutError:
        await SESSION_REGISTRY.remove(record.session_id)
        raise HTTPException(
            status_code=504,
            detail=(
                f"Handshake timed out after {body.connect_timeout_seconds}s "
                f"connecting to '{body.target_url}'"
            ),
        )
    except (websockets.exceptions.InvalidURI, ValueError) as exc:
        await SESSION_REGISTRY.remove(record.session_id)
        raise HTTPException(
            status_code=422,
            detail=f"Invalid WebSocket URI: {exc}",
        )
    except OSError as exc:
        await SESSION_REGISTRY.remove(record.session_id)
        raise HTTPException(
            status_code=502,
            detail=f"Network error connecting to '{body.target_url}': {exc}",
        )
    except Exception as exc:
        await SESSION_REGISTRY.remove(record.session_id)
        raise HTTPException(
            status_code=502,
            detail=f"Unexpected error during WebSocket handshake: {exc}",
        )

    record.websocket = ws
    record.state = WsConnectionState.OPEN
    record.opened_at = time.monotonic()
    record.listener_task = asyncio.create_task(
        _inbound_frame_listener(record),
        name=f"listener-{record.session_id}",
    )

    return OpenSessionResponse(
        session_id=record.session_id,
        target_url=record.target_url,
        state=record.state,
        opened_at_unix=time.time(),
        message=(
            f"Session '{record.session_id}' is OPEN. "
            "Use session_id for all subsequent operations."
        ),
    )


# ---------------------------------------------------------------------------
# Endpoint 2 — send_typed_ws_frame
# ---------------------------------------------------------------------------

@app.post(
    "/ws-sessions/{session_id}/send-frame",
    response_model=SendFrameResponse,
    status_code=200,
    summary="send_typed_ws_frame",
)
async def send_typed_ws_frame(
    body: SendFrameRequest,
    session_id: str = Path(..., min_length=32, max_length=32),
) -> SendFrameResponse:
    record = await SESSION_REGISTRY.get(session_id)

    if record.state != WsConnectionState.OPEN:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Session '{session_id}' is in state {record.state.value}, "
                "not OPEN. Cannot send frames."
            ),
        )

    if body.frame_type == WsFrameType.BINARY:
        try:
            wire_payload: str | bytes = bytes.fromhex(body.payload)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="frame_type is 'binary' but payload is not valid hex-encoded bytes.",
            )
        byte_count = len(wire_payload)
    else:
        wire_payload = body.payload
        byte_count = len(body.payload.encode("utf-8"))

    try:
        await record.websocket.send(wire_payload)
    except websockets.exceptions.ConnectionClosed as exc:
        record.state = WsConnectionState.CLOSED
        raise HTTPException(
            status_code=409,
            detail=f"Connection closed while sending frame: {exc}",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Unexpected error sending frame: {exc}",
        )

    h_new, delta, schema_ok = record.record_frame(wire_payload, "sent")

    return SendFrameResponse(
        session_id=session_id,
        frame_type=body.frame_type,
        payload_bytes=byte_count,
        entropy_after=round(h_new, 6),
        entropy_delta=round(delta, 6),
        schema_valid=schema_ok,
        sent_at_unix=time.time(),
    )


# ---------------------------------------------------------------------------
# Endpoint 3 — drain_ws_frame_buffer
# ---------------------------------------------------------------------------

@app.post(
    "/ws-sessions/{session_id}/drain-frames",
    response_model=DrainFramesResponse,
    status_code=200,
    summary="drain_ws_frame_buffer",
)
async def drain_ws_frame_buffer(
    body: DrainFramesRequest,
    session_id: str = Path(..., min_length=32, max_length=32),
) -> DrainFramesResponse:
    record = await SESSION_REGISTRY.get(session_id)

    drained: list[dict] = []
    for _ in range(min(body.max_frames, len(record.inbound_buffer))):
        if record.inbound_buffer:
            drained.append(record.inbound_buffer.popleft())

    return DrainFramesResponse(
        session_id=session_id,
        frames_returned=len(drained),
        frames=drained,
        buffer_remaining=len(record.inbound_buffer),
    )


# ---------------------------------------------------------------------------
# Endpoint 4 — inspect_ws_session_telemetry
# ---------------------------------------------------------------------------

@app.post(
    "/ws-sessions/{session_id}/telemetry",
    response_model=TelemetryResponse,
    status_code=200,
    summary="inspect_ws_session_telemetry",
)
async def inspect_ws_session_telemetry(
    session_id: str = Path(..., min_length=32, max_length=32),
) -> TelemetryResponse:
    record = await SESSION_REGISTRY.get(session_id)

    uptime = time.monotonic() - record.opened_at
    total_frames = record.frames_sent + record.frames_received

    if record.rolling_deltas:
        arr = np.array(list(record.rolling_deltas), dtype=np.float64)
        rolling_mean = float(np.mean(arr))
        rolling_std = float(np.std(arr))
        rolling_max = float(np.max(arr))
    else:
        rolling_mean = rolling_std = rolling_max = 0.0

    violation_rate = (
        record.schema_violations / total_frames if total_frames > 0 else 0.0
    )

    return TelemetryResponse(
        session_id=session_id,
        target_url=record.target_url,
        state=record.state,
        uptime_seconds=round(uptime, 3),
        frames_sent=record.frames_sent,
        frames_received=record.frames_received,
        current_entropy=round(record.current_entropy, 6),
        rolling_entropy_mean=round(rolling_mean, 6),
        rolling_entropy_std=round(rolling_std, 6),
        rolling_entropy_max=round(rolling_max, 6),
        cumulative_entropy_bits=round(record.current_entropy, 6),
        schema_violations=record.schema_violations,
        schema_violation_rate=round(violation_rate, 6),
        inbound_buffer_depth=len(record.inbound_buffer),
    )


# ---------------------------------------------------------------------------
# Endpoint 5 — close_websocket_session
# ---------------------------------------------------------------------------

@app.post(
    "/ws-sessions/{session_id}/close",
    response_model=CloseSessionResponse,
    status_code=200,
    summary="close_websocket_session",
)
async def close_websocket_session(
    body: CloseSessionRequest,
    session_id: str = Path(..., min_length=32, max_length=32),
) -> CloseSessionResponse:
    record = await SESSION_REGISTRY.get(session_id)

    record.state = WsConnectionState.CLOSING

    if record.websocket is not None:
        try:
            await asyncio.wait_for(
                record.websocket.close(body.status_code, body.reason),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass

    if record.listener_task and not record.listener_task.done():
        record.listener_task.cancel()
        try:
            await asyncio.wait_for(record.listener_task, timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    record.state = WsConnectionState.CLOSED
    uptime = time.monotonic() - record.opened_at
    total_frames = record.frames_sent + record.frames_received

    snapshot = CloseSessionResponse(
        session_id=session_id,
        target_url=record.target_url,
        final_state=record.state,
        frames_sent=record.frames_sent,
        frames_received=record.frames_received,
        terminal_entropy=round(record.current_entropy, 6),
        schema_violations=record.schema_violations,
        uptime_seconds=round(uptime, 3),
        message=(
            f"Session '{session_id}' closed with status {body.status_code}. "
            f"Total frames exchanged: {total_frames}. "
            "session_id is permanently deallocated."
        ),
    )

    await SESSION_REGISTRY.remove(session_id)
    return snapshot

# --- NEXUS: servidor MCP real montado en el mismo proceso (inyectado por forge_agent) ---
# Reemplaza el wrapper Node/TypeScript separado -- un solo deploy, sin
# segundo servicio, sin salto de red interno. Ver mcp_wrapper_generator.py
# (v2.0) para el razonamiento completo, incluido el gotcha de
# session_manager que explica el patron startup/shutdown de abajo.

from typing import Annotated, Any, Literal
from contextlib import AsyncExitStack as _NexusMcpExitStack

import os
import httpx
from pydantic import Field
from mcp.server.fastmcp import FastMCP as _NexusFastMCP
from mcp.server.transport_security import TransportSecuritySettings

# --- NEXUS: PATCH fix_mcp_dns_rebinding_host ---
# FastMCP() sin host/transport_security explicito activa proteccion
# anti DNS-rebinding con allowlist localhost-only por default del SDK,
# rechazando con 421 "Invalid Host header" cualquier request real
# contra el dominio publico de Railway (bug real confirmado en
# produccion 2026-07-09, ver docstring del generador). Se pasa
# transport_security explicito leyendo RAILWAY_PUBLIC_DOMAIN en
# runtime -- Railway lo inyecta automaticamente en cada servicio, asi
# que este codigo no necesita conocer su propio dominio al generarse.
_nexus_railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "*")

_nexus_mcp = _NexusFastMCP(
    'nexus-npm-package-ws-has-241560546-weekly-down',
    stateless_http=True,
    host="0.0.0.0",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        # --- PATCH fix_mcp_dns_rebinding_bare_host ---
        # Railway (como cualquier proxy HTTPS estandar) manda el Host
        # header SIN puerto explicito -- "dominio:*" nunca matchea eso,
        # solo matchea "dominio:443". Se agrega tambien el dominio
        # pelado para cubrir ambos casos (bug real confirmado en
        # produccion 2026-07-09: primer fix desplegado, /mcp seguia
        # devolviendo 421 tras el redeploy).
        allowed_hosts=[
            "localhost:*",
            "127.0.0.1:*",
            _nexus_railway_domain,
            _nexus_railway_domain + ":*",
        ],
        allowed_origins=[
            "http://localhost:*",
            "http://127.0.0.1:*",
            "https://" + _nexus_railway_domain,
        ],
    ),
)


async def _nexus_mcp_call_core(method: str, path: str, params: dict) -> Any:
    """
    Llama al endpoint real del core -- via ASGI in-process (sin red
    real, sin segundo proceso), no un HTTP call externo. `app` ya
    existe en este mismo modulo (es el FastAPI que FORGE genero).
    """
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://nexus-internal") as client:
        if method == "GET":
            resp = await client.get(path, params=params)
        else:
            resp = await client.post(path, json=params)
        resp.raise_for_status()
        return resp.json()


@_nexus_mcp.tool(name='nexus_npm_package_ws_has_241560546_weekly_down_open_websocket_session', description='Establishes a persistent WebSocket connection to a target URL and registers it in the stateful session registry. Returns a session_id for all subsequent operations. Use when an agent needs to initiate a long-lived connection before sending or receiving frames. Do NOT use to reconnect an already-open session — call close_websocket_session first, or use the session_id of an existing session.')
async def open_websocket_session(target_url: Annotated[str, Field(..., description='Full WebSocket URL to connect to (must start with ws:// or wss://). Example: wss://stream.example.com/feed', min_length=6, max_length=2048)], headers: Annotated[list[str], Field(None, description="Optional HTTP headers to send on handshake, formatted as 'Key: Value' strings. Use for Authorization, Sec-WebSocket-Protocol, or custom headers.")], frame_schema_json: Annotated[str, Field(None, description='Optional JSON Schema (as a JSON string) describing expected frame payloads. Used to baseline entropy scoring. If omitted, entropy is computed against empirical baseline from first 10 frames.', min_length=2, max_length=8192)], connect_timeout_ms: Annotated[float, Field(5000, description='Maximum milliseconds to wait for the WebSocket handshake to complete before returning an error.', ge=500, le=30000)]) -> dict[str, Any]:
    """Open WebSocket Session"""
    params = {"target_url": target_url, "headers": headers, "frame_schema_json": frame_schema_json, "connect_timeout_ms": connect_timeout_ms}
    return await _nexus_mcp_call_core('POST', '/ws-sessions/open', params)

@_nexus_mcp.tool(name='nexus_npm_package_ws_has_241560546_weekly_down_send_typed_ws_frame', description="Sends a single text or binary frame over an open session and returns the frame's Shannon entropy delta relative to the session baseline — confirming delivery and quantifying how novel this frame is versus the established message distribution. Use for request-response patterns or publishing commands to a WebSocket server. Do NOT use to send a sequence of frames in bulk — call this tool once per frame to preserve per-frame entropy tracking. Fails if session_id does not exist or connection is not in OPEN state.")
async def send_typed_ws_frame(session_id: Annotated[str, Field(..., description='UUID of an open session returned by open_websocket_session.', min_length=36, max_length=36)], payload: Annotated[str, Field(..., description="Frame payload. For binary frames, encode as Base64 and set frame_type to 'binary'.", min_length=0, max_length=65536)], frame_type: Annotated[str, Field('text', description="Either 'text' or 'binary'. Determines wire encoding. Default is 'text'.", min_length=4, max_length=6)]) -> dict[str, Any]:
    """Send Typed WebSocket Frame"""
    params = {"session_id": session_id, "payload": payload, "frame_type": frame_type}
    return await _nexus_mcp_call_core('POST', '/ws-sessions/{session_id}/send-frame', params)

@_nexus_mcp.tool(name='nexus_npm_package_ws_has_241560546_weekly_down_drain_ws_frame_buffer', description='Returns up to `max_frames` buffered inbound frames received since the last drain (or session open), each annotated with its Shannon entropy delta and schema validation result. Use to poll for incoming messages without holding a blocking connection. Do NOT use as a real-time streaming mechanism — it returns only frames already buffered; for high-frequency streams increase poll cadence or reduce max_frames. Returns empty list if no frames have arrived since last call.')
async def drain_ws_frame_buffer(session_id: Annotated[str, Field(..., description='UUID of the open session to drain frames from.', min_length=36, max_length=36)], max_frames: Annotated[float, Field(20, description='Maximum number of buffered frames to return in a single call. Frames are returned in arrival order (FIFO). Remaining frames stay in buffer.', ge=1, le=200)], entropy_anomaly_threshold: Annotated[float, Field(0, description='If set, only return frames whose absolute Shannon entropy delta exceeds this threshold (bits). Use to surface only anomalous frames. Set to 0 or omit to return all frames.', ge=0, le=16)]) -> dict[str, Any]:
    """Drain WebSocket Frame Buffer"""
    params = {"session_id": session_id, "max_frames": max_frames, "entropy_anomaly_threshold": entropy_anomaly_threshold}
    return await _nexus_mcp_call_core('POST', '/ws-sessions/{session_id}/drain-frames', params)

@_nexus_mcp.tool(name='nexus_npm_package_ws_has_241560546_weekly_down_inspect_ws_session_telemetry', description='Returns real-time telemetry for a session: connection state, frame counts (sent/received), cumulative and rolling Shannon entropy statistics, schema violation rate, and uptime. Use to diagnose whether a stream is diverging from its expected schema or to verify a session is still alive before sending. Do NOT use as the primary liveness check in a tight loop — it performs entropy aggregation on each call; poll at most once per second.')
async def inspect_ws_session_telemetry(session_id: Annotated[str, Field(..., description='UUID of the session to inspect.', min_length=36, max_length=36)], entropy_window_frames: Annotated[float, Field(50, description='Number of most-recent frames to include in the rolling entropy window calculation. Larger windows give more stable estimates; smaller windows are more sensitive to recent drift.', ge=5, le=500)]) -> dict[str, Any]:
    """Inspect WebSocket Session Telemetry"""
    params = {"session_id": session_id, "entropy_window_frames": entropy_window_frames}
    return await _nexus_mcp_call_core('POST', '/ws-sessions/{session_id}/telemetry', params)

@_nexus_mcp.tool(name='nexus_npm_package_ws_has_241560546_weekly_down_close_websocket_session', description='Sends a WebSocket close frame with the specified status code, waits for the server close handshake, and removes the session from the registry. Returns a final telemetry snapshot including total frames exchanged and terminal entropy statistics. Use when a session is no longer needed or before re-opening a connection to the same URL. Do NOT use to temporarily pause a session — the session_id is permanently deallocated after this call and cannot be reused.')
async def close_websocket_session(session_id: Annotated[str, Field(..., description='UUID of the session to close.', min_length=36, max_length=36)], close_code: Annotated[float, Field(1000, description='WebSocket close status code per RFC 6455. Standard values: 1000 (normal closure), 1001 (going away), 1008 (policy violation). Defaults to 1000.', ge=1000, le=4999)], close_reason: Annotated[str, Field(None, description='Optional UTF-8 close reason string sent in the close frame. Must be 123 bytes or fewer per RFC 6455.', min_length=0, max_length=123)], drain_before_close: Annotated[bool, Field(True, description='If true, all buffered inbound frames are included in the final telemetry snapshot before the session is destroyed. If false, buffered frames are discarded immediately.')]) -> dict[str, Any]:
    """Close WebSocket Session"""
    params = {"session_id": session_id, "close_code": close_code, "close_reason": close_reason, "drain_before_close": drain_before_close}
    return await _nexus_mcp_call_core('POST', '/ws-sessions/{session_id}/close', params)


# Crea el sub-app ASGI de streamable HTTP -- DEBE llamarse antes de
# poder acceder a _nexus_mcp.session_manager (se crea de forma
# perezosa, ver docstring del modulo).
# Se monta en "/" (no en "/mcp"): streamable_http_app() YA expone su
# propia ruta interna en "/mcp" -- montarlo de nuevo en "/mcp" duplica
# el path a "/mcp/mcp" y da 404 (bug real encontrado probando esto en
# runtime con un cliente MCP de verdad, no algo teorico).
_nexus_mcp_asgi_app = _nexus_mcp.streamable_http_app()
_nexus_mcp_stack = _NexusMcpExitStack()


@app.on_event("startup")
async def _nexus_mcp_startup():
    await _nexus_mcp_stack.enter_async_context(_nexus_mcp.session_manager.run())


@app.on_event("shutdown")
async def _nexus_mcp_shutdown():
    await _nexus_mcp_stack.aclose()


app.mount("/", _nexus_mcp_asgi_app)

# --- NEXUS: reporte de uso real a Stripe (inyectado por forge_output_saver_v6) ---
_NEXUS_BILLING_EXCLUDED_PATHS = {'/docs', '/favicon.ico', '/openapi.json', '/mcp', '/redoc', '/health', '/'}
@app.middleware("http")
async def _nexus_usage_middleware(request, call_next):
    response = await call_next(request)
    try:
        _nexus_route = request.scope.get('route')
        _nexus_route_path = getattr(_nexus_route, 'path', None)
        if (
            request.url.path not in _NEXUS_BILLING_EXCLUDED_PATHS
            and _nexus_route_path not in _NEXUS_BILLING_EXCLUDED_PATHS
            and response.status_code < 400
        ):
            import os as _nexus_os
            import stripe as _nexus_stripe
            _customer_id = _nexus_os.environ.get("STRIPE_CUSTOMER_ID")
            _event_name = _nexus_os.environ.get("STRIPE_EVENT_NAME")
            _secret_key = _nexus_os.environ.get("STRIPE_SECRET_KEY")
            if _customer_id and _event_name and _secret_key:
                _nexus_stripe.api_key = _secret_key
                _nexus_stripe.billing.MeterEvent.create(
                    event_name=_event_name,
                    payload={
                        "stripe_customer_id": _customer_id,
                        "value": "1",
                    },
                )
    except Exception:
        pass  # nunca romper la response real por un fallo de billing
    return response
