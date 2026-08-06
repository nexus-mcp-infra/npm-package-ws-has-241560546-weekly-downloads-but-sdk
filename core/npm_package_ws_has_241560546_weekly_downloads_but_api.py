import asyncio
import hashlib
import os
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

# --- NEXUS: PATCH ws_asset_session_limits_2026-07-25 ---
# _nexus_meta.json documentaba "TTL eviction... hasta 256 sesiones"
# como decision de diseno, pero no habia codigo real detras -- sin
# auth en este asset, una sesion nunca cerrada agotaba memoria/fds sin
# limite. Ambos valores son configurables sin redeploy via env vars
# Railway (mismo patron que NEXUS_RATE_LIMIT_PER_MINUTE en los otros
# 2 assets).
MAX_CONCURRENT_SESSIONS = int(os.getenv("NEXUS_WS_MAX_SESSIONS", "256"))
SESSION_IDLE_TTL_SECONDS = float(os.getenv("NEXUS_WS_SESSION_TTL_SECONDS", "3600"))


class WsSessionRegistry:
    def __init__(self) -> None:
        self._sessions: dict[str, WsSessionRecord] = {}
        self._lock = asyncio.Lock()

    async def create(self, target_url: str) -> WsSessionRecord:
        async with self._lock:
            await self._evict_stale_locked()
            if len(self._sessions) >= MAX_CONCURRENT_SESSIONS:
                raise HTTPException(
                    status_code=503,
                    detail=(
                        f"Session registry at capacity ({MAX_CONCURRENT_SESSIONS} "
                        "concurrent sessions). Close idle sessions or retry later."
                    ),
                )
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

    async def _evict_stale_locked(self) -> None:
        """Caller must already hold self._lock. Removes CLOSED sessions
        and any session past SESSION_IDLE_TTL_SECONDS, closing sockets
        and cancelling listener tasks best-effort."""
        now = time.monotonic()
        stale_ids = [
            sid for sid, rec in self._sessions.items()
            if rec.state == WsConnectionState.CLOSED
            or (now - rec.opened_at) > SESSION_IDLE_TTL_SECONDS
        ]
        for sid in stale_ids:
            rec = self._sessions.pop(sid, None)
            if rec is None:
                continue
            if rec.listener_task and not rec.listener_task.done():
                rec.listener_task.cancel()
            if rec.websocket is not None:
                try:
                    await rec.websocket.close(1001)
                except Exception:
                    pass


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
    contact={"email": "dasaanrod@gmail.com"},
    description=(
        "Stateful WebSocket session registry with per-connection Shannon entropy "
        "delta tracking for schema divergence detection in agentic workflows."
    ),
    lifespan=lifespan,
)

# --- NEXUS: x402 (pago por llamada en USDC, Base Sepolia testnet) ---
from x402.http import FacilitatorConfig, HTTPFacilitatorClient, PaymentOption
from x402.http.middleware.fastapi import PaymentMiddlewareASGI
from x402.http.types import RouteConfig
from x402.mechanisms.evm.exact import ExactEvmServerScheme
from x402.schemas import Network
from x402.server import x402ResourceServer
# --- NEXUS PATCH cdp_facilitator_bazaar_ws ---
from cdp.x402 import create_facilitator_config as _nexus_cdp_create_facilitator_config
from x402.extensions.bazaar import (
    OutputConfig as _NexusBazaarOutputConfig,
    declare_discovery_extension as _nexus_declare_discovery_extension,
)

_NEXUS_X402_EVM_ADDRESS = "0x70e9f8057bb50e31b6ee06958bcbbe7de9daa98f"
_NEXUS_X402_NETWORK: Network = "eip155:84532"  # Base Sepolia (testnet) -- cambiar a eip155:8453 + facilitator mainnet para produccion
_NEXUS_X402_PRICE = "$0.01"

# x402.org/facilitator nunca cataloga nada en el Bazaar de Coinbase -- el CDP
# Facilitator si lo hace (indexa en el primer verify/settle real que pasa por
# el, Base Sepolia incluido). create_facilitator_config() sin argumentos lee
# CDP_API_KEY_ID/CDP_API_KEY_SECRET del entorno (Railway) -- deben estar
# seteadas antes de este deploy. A diferencia de BuyWhere, "ws" no llama
# initialize()/get_supported() a nivel de modulo, asi que sin credenciales el
# proceso igual bootea -- solo el primer verify/settle real contra
# POST /ws-sessions/open fallaria con 401 de CDP.
_nexus_x402_facilitator = HTTPFacilitatorClient(
    _nexus_cdp_create_facilitator_config()
)
_nexus_x402_server = x402ResourceServer(_nexus_x402_facilitator)
_nexus_x402_server.register(_NEXUS_X402_NETWORK, ExactEvmServerScheme())

# --- PATCH x402_revenue_events_hook ---
# Cierra CLAUDE.md SS9.57 pendiente #1 / SS9.58: settle_payment() real
# (REST o MCP, ambos llaman sobre esta MISMA instancia de
# x402ResourceServer) no dejaba ninguna fila en revenue_events. Portado
# a mano del mismo fix ya aplicado a mcp_wrapper_generator.py -- ver
# patch_x402_revenue_events_hook.py y CLAUDE.md SS9.58 para el diseno
# completo (por que on_after_settle() y no un punto proxy pre-settlement).
_NEXUS_SUPABASE_URL = os.getenv("SUPABASE_URL")
_NEXUS_SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
_NEXUS_ASSET_NAME = os.getenv("NEXUS_ASSET")


async def _nexus_supabase_insert(table, payload):
    if not _NEXUS_SUPABASE_URL or not _NEXUS_SUPABASE_ANON_KEY:
        return
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            await client.post(
                f"{_NEXUS_SUPABASE_URL}/rest/v1/{table}",
                json=payload,
                headers={
                    "apikey": _NEXUS_SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {_NEXUS_SUPABASE_ANON_KEY}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
            )
    except Exception:
        pass  # nunca romper el flujo real por un fallo de telemetria


async def _nexus_log_x402_revenue_event(ctx) -> None:
    """
    AfterSettleHook real -- dispara SOLO cuando x402ResourceServer
    confirma settle_result.success=True (ver _settle_payment_core en
    x402/server_base.py). Duck-typed, nunca levanta: un bug aca no debe
    poder romper la respuesta de pago real que el caller ya esta por
    recibir.
    """
    try:
        result = getattr(ctx, "result", None)
        requirements = getattr(ctx, "requirements", None)
        if result is None or requirements is None or not getattr(result, "success", False):
            return
        # amount en unidades atomicas de USDC (6 decimales) -- amount_eur
        # no es EUR, es el valor numerico crudo, sin conversion FX (mismo
        # mismatch de schema ya documentado en el generador).
        raw_amount = getattr(requirements, "amount", None)
        amount_eur = int(raw_amount) / 1_000_000 if raw_amount is not None else None
        await _nexus_supabase_insert("revenue_events", {
            "asset_name": _NEXUS_ASSET_NAME,
            "amount_eur": amount_eur,
            "pricing_model": "x402",
            "stripe_event_id": None,
            "customer_id": getattr(result, "payer", None),
        })
    except Exception:
        pass


def _nexus_register_x402_revenue_logging(x402_server) -> None:
    """Registrado UNA vez, cubre settle_payment() real de REST y MCP
    por igual -- ambos llaman sobre la misma instancia compartida."""
    x402_server.on_after_settle(_nexus_log_x402_revenue_event)


_nexus_register_x402_revenue_logging(_nexus_x402_server)

_NEXUS_BAZAAR_WS_OPEN_EXTENSION = _nexus_declare_discovery_extension(
    input={
        "target_url": "wss://echo.websocket.org",
        "connect_timeout_seconds": 10.0,
    },
    input_schema={
        "properties": {
            "target_url": {"type": "string", "description": "WebSocket URL to connect to (ws:// or wss://)"},
            "connect_timeout_seconds": {"type": "number", "minimum": 0.5, "maximum": 60.0},
            "extra_headers": {"type": "object", "description": "Optional HTTP headers for the upgrade request"},
        },
        "required": ["target_url"],
    },
    output=_NexusBazaarOutputConfig(
        example={"session_id": "sess_abc123", "target_url": "wss://echo.websocket.org", "state": "open", "opened_at_unix": 1785168800.0, "message": "connected"},
    ),
)

_NEXUS_X402_ROUTES: dict[str, RouteConfig] = {
    "POST /ws-sessions/open": RouteConfig(
        accepts=[PaymentOption(scheme="exact", pay_to=_NEXUS_X402_EVM_ADDRESS, price=_NEXUS_X402_PRICE, network=_NEXUS_X402_NETWORK)],
        mime_type="application/json",
        description="Abre una sesion WebSocket administrada con tracking de entropia por frame",
        service_name="WS-Session-Registry",
        tags=["websocket", "session", "entropy", "agentic"],
        extensions=_NEXUS_BAZAAR_WS_OPEN_EXTENSION,
    ),
}

app.add_middleware(PaymentMiddlewareASGI, routes=_NEXUS_X402_ROUTES, server=_nexus_x402_server)


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

# --- NEXUS: PATCH ws_asset_mcp_regenerated_2026-07-25 ---
# --- NEXUS: servidor MCP real montado en el mismo proceso (inyectado por forge_agent) ---
# Reemplaza el wrapper Node/TypeScript separado -- un solo deploy, sin
# segundo servicio, sin salto de red interno. Ver mcp_wrapper_generator.py
# (v2.0) para el razonamiento completo, incluido el gotcha de
# session_manager que explica el patron startup/shutdown de abajo.

from typing import Annotated, Any, Literal
from contextlib import asynccontextmanager

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


# --- PATCH ws_mcp_internal_call_isolation ---
# Ver patch_mcp_internal_call_isolation.py (mcp_wrapper_generator.py) y
# CLAUDE.md SS9.51/SS9.5x para el razonamiento completo. `_nexus_internal_app`
# comparte las mismas rutas/dependencias que `app` (misma resolucion real
# de FastAPI DI) pero SIN el x402 PaymentMiddlewareASGI (registrado solo en
# `app`, filtrado por metodo+ruta REST -- gateaba esta misma llamada interna
# a POST /ws-sessions/open con un 402 espurio incondicional, sin importar si
# el caller MCP ya habia pagado la request real a /mcp).
_nexus_internal_app = FastAPI(routes=list(app.routes))


async def _nexus_mcp_call_core(method: str, path: str, params: dict, headers: dict | None = None) -> Any:
    """
    Llama al endpoint real del core -- via ASGI in-process (sin red
    real, sin segundo proceso) contra _nexus_internal_app (ver arriba),
    NO contra `app` directamente.
    """
    transport = httpx.ASGITransport(app=_nexus_internal_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://nexus-internal") as client:
        if method == "GET":
            resp = await client.get(path, params=params, headers=headers)
        else:
            resp = await client.post(path, json=params, headers=headers)
        resp.raise_for_status()
        return resp.json()


# --- PATCH ws_mcp_x402_auth_gate ---
# create_payment_wrapper() con las MISMAS PaymentOption/price/network/pay_to
# que ya usa la ruta REST (_NEXUS_X402_ROUTES / _nexus_x402_server, ambos
# definidos mas arriba) -- mismo patron ya probado en produccion en
# similarity-search-api (archive/patches/patch_mcp_x402_auth_gate_similarity_search.py),
# con una guarda adicional: si initialize()/build_payment_requirements()
# fallan (ej. credenciales CDP invalidas), el wrapper cae a identity en vez
# de crashear el boot completo -- ver docstring de patch_ws_mcp_x402_auth_gate.py
# para el razonamiento (CDP, a diferencia de x402.org, exige JWT real).
from x402.mcp import create_payment_wrapper as _nexus_mcp_x402_wrapper_factory
from x402.schemas.config import ResourceConfig as _NexusX402ResourceConfig

try:
    if not getattr(_nexus_x402_server, "_initialized", False):
        _nexus_x402_server.initialize()
    _NEXUS_MCP_X402_RESOURCE_CONFIG = _NexusX402ResourceConfig(
        scheme="exact",
        pay_to=_NEXUS_X402_EVM_ADDRESS,
        price=_NEXUS_X402_PRICE,
        network=_NEXUS_X402_NETWORK,
    )
    _NEXUS_MCP_X402_ACCEPTS = _nexus_x402_server.build_payment_requirements(_NEXUS_MCP_X402_RESOURCE_CONFIG)
    _nexus_mcp_x402_wrapper = _nexus_mcp_x402_wrapper_factory(_nexus_x402_server, accepts=_NEXUS_MCP_X402_ACCEPTS)
except Exception as _nexus_x402_mcp_init_error:
    print(
        f"[NEXUS x402-mcp] init failed, open_websocket_session MCP tool falls back to "
        f"UNGATED (REST via PaymentMiddlewareASGI stays gated normally): {_nexus_x402_mcp_init_error}",
        flush=True,
    )
    _nexus_mcp_x402_wrapper = lambda fn: fn


@_nexus_mcp.tool(name='nexus_npm_package_ws_has_241560546_weekly_down_open_websocket_session', description='Establishes a persistent WebSocket connection to a target URL and registers it in the stateful session registry. Returns a session_id for all subsequent operations. Use when an agent needs to initiate a long-lived connection before sending or receiving frames. Do NOT use to reconnect an already-open session -- call close_websocket_session first, or use the session_id of an existing session.')
@_nexus_mcp_x402_wrapper
async def open_websocket_session(target_url: Annotated[str, Field(..., description='Full WebSocket URL to connect to (must start with ws:// or wss://).', min_length=6)], connect_timeout_seconds: Annotated[float, Field(10.0, description='Maximum seconds to wait for the WebSocket handshake to complete.', ge=0.5, le=60.0)]) -> dict[str, Any]:
    """Open WebSocket Session"""
    _nexus_path = '/ws-sessions/open'.format()
    params = {"target_url": target_url, "connect_timeout_seconds": connect_timeout_seconds}
    return await _nexus_mcp_call_core('POST', _nexus_path, params)

@_nexus_mcp.tool(name='nexus_npm_package_ws_has_241560546_weekly_down_send_typed_ws_frame', description="Sends a single text or binary frame over an open session and returns the frame's Shannon entropy delta relative to the session baseline. Use for request-response patterns or publishing commands to a WebSocket server. Do NOT use to send a sequence of frames in bulk -- call this tool once per frame. Fails if session_id does not exist or connection is not in OPEN state.")
async def send_typed_ws_frame(session_id: Annotated[str, Field(..., description='32-character session id (uuid4 hex, no dashes) returned by open_websocket_session.', min_length=32, max_length=32)], payload: Annotated[str, Field(..., description="Frame payload. For binary frames, encode as hex and set frame_type to 'binary'.", min_length=0, max_length=131072)], frame_type: Annotated[Literal['text', 'binary'], Field('text', description="Either 'text' or 'binary'. Determines wire encoding.")]) -> dict[str, Any]:
    """Send Typed WebSocket Frame"""
    _nexus_path = '/ws-sessions/{session_id}/send-frame'.format(session_id=session_id)
    params = {"payload": payload, "frame_type": frame_type}
    return await _nexus_mcp_call_core('POST', _nexus_path, params)

@_nexus_mcp.tool(name='nexus_npm_package_ws_has_241560546_weekly_down_drain_ws_frame_buffer', description='Returns up to max_frames buffered inbound frames received since the last drain (or session open), each annotated with its Shannon entropy delta and schema validation result. Use to poll for incoming messages without holding a blocking connection. Do NOT use as a real-time streaming mechanism -- it returns only frames already buffered.')
async def drain_ws_frame_buffer(session_id: Annotated[str, Field(..., description='32-character session id of the open session to drain frames from.', min_length=32, max_length=32)], max_frames: Annotated[float, Field(32, description='Maximum number of buffered frames to return in a single call.', ge=1, le=256)]) -> dict[str, Any]:
    """Drain WebSocket Frame Buffer"""
    _nexus_path = '/ws-sessions/{session_id}/drain-frames'.format(session_id=session_id)
    params = {"max_frames": max_frames}
    return await _nexus_mcp_call_core('POST', _nexus_path, params)

@_nexus_mcp.tool(name='nexus_npm_package_ws_has_241560546_weekly_down_inspect_ws_session_telemetry', description='Returns real-time telemetry for a session: connection state, frame counts, cumulative and rolling Shannon entropy statistics, schema violation rate, and uptime. Use to diagnose whether a stream is diverging from its expected schema or to verify a session is still alive before sending. Do NOT use as the primary liveness check in a tight loop.')
async def inspect_ws_session_telemetry(session_id: Annotated[str, Field(..., description='32-character session id of the session to inspect.', min_length=32, max_length=32)]) -> dict[str, Any]:
    """Inspect WebSocket Session Telemetry"""
    _nexus_path = '/ws-sessions/{session_id}/telemetry'.format(session_id=session_id)
    params = {}
    return await _nexus_mcp_call_core('POST', _nexus_path, params)

@_nexus_mcp.tool(name='nexus_npm_package_ws_has_241560546_weekly_down_close_websocket_session', description='Sends a WebSocket close frame with the specified status code, waits for the server close handshake, and removes the session from the registry. Returns a final telemetry snapshot. Use when a session is no longer needed. Do NOT use to temporarily pause a session -- the session_id is permanently deallocated after this call and cannot be reused.')
async def close_websocket_session(session_id: Annotated[str, Field(..., description='32-character session id of the session to close.', min_length=32, max_length=32)], status_code: Annotated[float, Field(1000, description='WebSocket close status code per RFC 6455 (1000-4999). Defaults to 1000.', ge=1000, le=4999)], reason: Annotated[str, Field('', description='Optional UTF-8 close reason string, max 123 bytes per RFC 6455.', max_length=123)]) -> dict[str, Any]:
    """Close WebSocket Session"""
    _nexus_path = '/ws-sessions/{session_id}/close'.format(session_id=session_id)
    params = {"status_code": status_code, "reason": reason}
    return await _nexus_mcp_call_core('POST', _nexus_path, params)


# Crea el sub-app ASGI de streamable HTTP -- DEBE llamarse antes de
# poder acceder a _nexus_mcp.session_manager (se crea de forma
# perezosa, ver docstring del modulo).
# Se monta en "/" (no en "/mcp"): streamable_http_app() YA expone su
# propia ruta interna en "/mcp" -- montarlo de nuevo en "/mcp" duplica
# el path a "/mcp/mcp" y da 404 (bug real encontrado probando esto en
# runtime con un cliente MCP de verdad, no algo teorico).
_nexus_mcp_asgi_app = _nexus_mcp.streamable_http_app()

# --- NEXUS: PATCH mcp_lifespan_composition_fix ---
# @app.on_event() SOLO se ejecuta si Starlette uso _DefaultLifespan --
# eso pasa unicamente cuando el `app = FastAPI(...)` que genero el LLM
# NO paso su propio parametro `lifespan=`. Si el LLM SI definio uno
# (tipico en assets con estado -- ej. cleanup de conexiones WebSocket
# al shutdown), Starlette usa ESE callable exclusivamente y los
# handlers @app.on_event quedan sin ejecutarse -- sin warning, sin
# error, el server bootea limpio ("Application startup complete") y
# el primer request a /mcp explota con "RuntimeError: Task group is
# not initialized." (confirmado contra Router.__init__ de Starlette:
# lifespan=None -> _DefaultLifespan(self) [dispara on_event],
# lifespan=<callable> -> se usa ESE, on_event nunca corre). Bug real
# encontrado en produccion 2026-07-25 (asset "ws", que define su
# propio lifespan para cerrar sesiones WebSocket abiertas).
#
# Fix: envolver el lifespan_context que Starlette YA construyo (sea
# _DefaultLifespan o el custom del LLM) en vez de competir con el
# via @app.on_event. Funciona en ambos casos sin parsear ni tocar
# el `app = FastAPI(...)` original.
_nexus_prev_lifespan_context = app.router.lifespan_context


@asynccontextmanager
async def _nexus_combined_lifespan(app):
    async with _nexus_mcp.session_manager.run():
        async with _nexus_prev_lifespan_context(app):
            yield


app.router.lifespan_context = _nexus_combined_lifespan


# --- NEXUS: A2A Agent Card (spec v0.3.0/v1.0) para discovery ---
# Ruta correcta del spec vigente -- NO /agent.json (deprecado en
# v0.1.0), NO /v1/agent-card.json ni /v2/agent-card.json. Debe
# registrarse ANTES del app.mount("/", ...) de mas abajo: Starlette
# matchea rutas en el orden en que se agregan a app.routes, y un Mount
# en "/" intercepta cualquier path si se agrega primero.
@app.get("/.well-known/agent-card.json", include_in_schema=False)
async def _nexus_a2a_agent_card() -> dict:
    return {
        "name": "WebSocket Session Manager API",
        "description": "Stateful WebSocket session registry with per-connection Shannon entropy delta tracking for schema divergence detection in agentic workflows.",
        "url": "https://npm-package-ws-has-241560546-w-production.up.railway.app",
        "version": "1.0.0",
        "documentationUrl": "https://npm-package-ws-has-241560546-w-production.up.railway.app/docs",
        "provider": {
            "organization": "nexus-mcp-infra",
            "url": "https://github.com/nexus-mcp-infra/npm-package-ws-has-241560546-weekly-downloads-but-sdk",
        },
        "capabilities": {
            "streaming": False,
            "pushNotifications": False,
            "stateTransitionHistory": False,
        },
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
        "additionalInterfaces": [
            {"url": "https://npm-package-ws-has-241560546-w-production.up.railway.app/mcp", "transport": "MCP"},
        ],
        "skills": [
            {
                "id": "nexus_npm_package_ws_has_241560546_weekly_down_open_websocket_session",
                "name": "Open WebSocket Session",
                "description": "Establishes a persistent WebSocket connection to a target URL and registers it in the stateful session registry. Returns a session_id for all subsequent operations. Do NOT use to reconnect an already-open session -- call close_websocket_session first.",
                "tags": ["websocket", "session-management"],
            },
            {
                "id": "nexus_npm_package_ws_has_241560546_weekly_down_send_typed_ws_frame",
                "name": "Send Typed WebSocket Frame",
                "description": "Sends a single text or binary frame over an open session and returns the frame's Shannon entropy delta relative to the session baseline. Do NOT use to send a sequence of frames in bulk -- call this tool once per frame.",
                "tags": ["websocket", "entropy"],
            },
            {
                "id": "nexus_npm_package_ws_has_241560546_weekly_down_drain_ws_frame_buffer",
                "name": "Drain WebSocket Frame Buffer",
                "description": "Returns up to max_frames buffered inbound frames received since the last drain (or session open), each annotated with its Shannon entropy delta and schema validation result. Do NOT use as a real-time streaming mechanism -- it returns only frames already buffered.",
                "tags": ["websocket", "polling"],
            },
            {
                "id": "nexus_npm_package_ws_has_241560546_weekly_down_inspect_ws_session_telemetry",
                "name": "Inspect WebSocket Session Telemetry",
                "description": "Returns real-time telemetry for a session: connection state, frame counts, cumulative and rolling Shannon entropy statistics, schema violation rate, and uptime. Do NOT use as the primary liveness check in a tight loop.",
                "tags": ["websocket", "telemetry"],
            },
            {
                "id": "nexus_npm_package_ws_has_241560546_weekly_down_close_websocket_session",
                "name": "Close WebSocket Session",
                "description": "Sends a WebSocket close frame with the specified status code, waits for the server close handshake, and removes the session from the registry. Do NOT use to temporarily pause a session -- the session_id is permanently deallocated after this call.",
                "tags": ["websocket", "session-management"],
            },
        ],
        "metadata": {
            "protocol_note": (
                "This service implements the Model Context Protocol (MCP) at /mcp, "
                "not A2A's own JSONRPC/gRPC/HTTP+JSON task methods (message/send, "
                "tasks/get, etc.). This Agent Card is provided for discovery/indexing "
                "purposes; A2A-conformant task orchestration is not implemented. No "
                "API key is required by this asset. See payment_info below for the "
                "per-route x402 payment status."
            ),
            # --- NEXUS PATCH agent_card_payment_status_ws ---
            # Grounded 1:1 contra _NEXUS_X402_ROUTES / _NEXUS_X402_PRICE (mas arriba
            # en este mismo archivo) -- no se inventa ningun precio/ruta nuevo aca.
            # Corrige el hallazgo de la auditoria Fase 0 (docs/fase0_auditoria_
            # 2026-07-28.md SS3): protocol_note afirmaba "no payment required" pese a
            # que POST /ws-sessions/open ya devolvia 402 real en produccion.
            "_NEXUS_AGENT_CARD_PAYMENT_INFO": True,
            "payment_info": {
                "POST /ws-sessions/open": {
                    "payment_required": True,
                    "scheme": "x402",
                    "amount": _NEXUS_X402_PRICE,
                    "asset": "USDC",
                    "network": _NEXUS_X402_NETWORK,
                    "note": "Opens a new managed WebSocket session; the 4 operations below act on an already-open session and are not separately charged.",
                },
                "POST /ws-sessions/{session_id}/send-frame": {"payment_required": False},
                "POST /ws-sessions/{session_id}/drain-frames": {"payment_required": False},
                "POST /ws-sessions/{session_id}/telemetry": {"payment_required": False},
                "POST /ws-sessions/{session_id}/close": {"payment_required": False},
            },
        },
    }


# --- NEXUS: favicon.ico real para checklist de x402scan ---
# x402scan.com/resources/register senala /favicon.ico ausente al leer
# el listado de otro asset (2026-07-26); aplicado aca tambien para
# consistencia entre los 3. Icono generico solido, sin decision de
# branding -- generado con stdlib puro (struct+zlib), sin Pillow. Debe
# registrarse ANTES del app.mount("/", ...) de mas abajo, mismo motivo
# que el Agent Card: Starlette matchea rutas en el orden en que se
# agregan a app.routes.
import base64 as _nexus_favicon_base64

_NEXUS_FAVICON_ICO = _nexus_favicon_base64.b64decode(
    "AAABAAEAICAAAAEAIABpAAAAFgAAAIlQTkcNChoKAAAADUlIRFIAAAAgAAAAIAgGAAAAc3p69AAAADBJREFUeNrtziEBAAAIAzCS4MlA/1wQ42ZiftWzl1QCAgICAgICAgICAgICAgLpwAO9cwRbSHXMRQAAAABJRU5ErkJggg=="
)


@app.get("/favicon.ico", include_in_schema=False)
async def _nexus_favicon():
    from fastapi import Response
    return Response(content=_NEXUS_FAVICON_ICO, media_type="image/x-icon")


# --- NEXUS: 402index.io domain claim verification file ---
# POST /api/v1/claim (domain=npm-package-ws-has-241560546-w-production.up.railway.app,
# 2026-07-26) pide este archivo estatico con el hash exacto, sin espacios
# ni saltos de linea extra. No es DNS TXT -- es un archivo servido por la
# propia app. Debe registrarse ANTES del app.mount("/", ...) de mas abajo,
# mismo motivo que favicon.ico/agent-card.json: Starlette matchea rutas en
# el orden en que se agregan a app.routes.
_NEXUS_402INDEX_VERIFY_HASH = "904ccf92916ad0d0a4f739059885c80250a1c1c9d51b009695ea83f704c66d3b"


@app.get("/.well-known/402index-verify.txt", include_in_schema=False)
async def _nexus_402index_verify():
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(content=_NEXUS_402INDEX_VERIFY_HASH)


# --- NEXUS PATCH ws_health_endpoint_2026_07_28 ---
# GET /health no existia (404 real, ver CLAUDE.md SS9.8/9.12) -- "ws" no
# depende de ningun upstream fijo (target_url lo define el caller en cada
# request, ver OpenSessionRequest mas arriba), asi que el unico chequeo
# real disponible es liveness del proceso + saturacion del registro de
# sesiones concurrentes ya existente (SESSION_REGISTRY.count() /
# MAX_CONCURRENT_SESSIONS, ver PATCH ws_asset_session_limits_2026-07-25
# mas arriba). "degraded" al 90% de MAX_CONCURRENT_SESSIONS -- mismo
# umbral de alerta temprana antes del 503 real que ya dispara
# WsSessionRegistry.create() al 100% de capacidad. status NO esta
# hardcodeado (bug ya documentado en similarity-search-api y en el
# /health original de BuyWhere, ver CLAUDE.md SS9.8) -- es funcion real
# de active_sessions vs. capacity. Debe registrarse ANTES de
# app.mount("/", ...) de mas abajo, mismo motivo que
# agent-card.json/favicon.ico/402index-verify.txt.
@app.get(
    "/health",
    summary="Liveness probe - no auth required",
    openapi_extra={"security": []},
)
async def _nexus_health_check() -> dict:
    active = SESSION_REGISTRY.count()
    capacity = MAX_CONCURRENT_SESSIONS
    near_capacity = active >= int(capacity * 0.9)
    return {
        "status": "degraded" if near_capacity else "ok",
        "active_sessions": active,
        "max_concurrent_sessions": capacity,
    }


app.mount("/", _nexus_mcp_asgi_app)


# --- NEXUS: reporte de uso real a Stripe (inyectado por forge_output_saver_v6) ---
_NEXUS_BILLING_EXCLUDED_PATHS = {'/docs', '/favicon.ico', '/openapi.json', '/mcp', '/redoc', '/health', '/', '/.well-known/agent-card.json', '/ws-sessions/open', '/.well-known/402index-verify.txt'}  # x402 cubre /ws-sessions/open -- Stripe no debe cobrarla de nuevo; 402index-verify.txt es discovery/verificacion, no negocio
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
