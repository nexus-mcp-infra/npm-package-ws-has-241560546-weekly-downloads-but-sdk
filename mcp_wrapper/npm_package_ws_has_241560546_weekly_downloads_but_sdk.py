import httpx
from typing import Any, Optional, Union
import json


class WebSocketSessionError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class WebSocketFrameError(WebSocketSessionError):
    pass


class AuthenticationError(WebSocketSessionError):
    pass


class SessionNotFoundError(WebSocketSessionError):
    pass


class Client:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.ws-mcp.nexus.ai/v1",
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        if not api_key or not isinstance(api_key, str):
            raise AuthenticationError("api_key must be a non-empty string")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._http = httpx.Client(
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "ws-mcp-python-sdk/1.0.0",
            },
        )

    def main_method(self, data: dict[str, Any]) -> dict[str, Any]:
        if not data or not isinstance(data, dict):
            raise WebSocketSessionError("data must be a non-empty dict")
        action = data.get("action")
        if not action or not isinstance(action, str):
            raise WebSocketSessionError("data must contain a non-empty 'action' key")
        dispatch = {
            "open_session": lambda: self.open_session(
                target_url=data["target_url"],
                schema_hint=data.get("schema_hint"),
                headers=data.get("headers"),
            ),
            "send_typed_frame": lambda: self.send_typed_frame(
                session_id=data["session_id"],
                payload=data["payload"],
                frame_type=data.get("frame_type", "text"),
            ),
            "inspect_session_entropy": lambda: self.inspect_session_entropy(
                session_id=data["session_id"],
                last_n_frames=data.get("last_n_frames"),
            ),
            "subscribe_topic_stream": lambda: self.subscribe_topic_stream(
                session_id=data["session_id"],
                topic_filter=data["topic_filter"],
                max_frames=data.get("max_frames", 50),
                entropy_alert_threshold=data.get("entropy_alert_threshold"),
            ),
            "close_session": lambda: self.close_session(
                session_id=data["session_id"],
                reason=data.get("reason"),
            ),
        }
        handler = dispatch.get(action)
        if handler is None:
            raise WebSocketSessionError(
                f"Unknown action '{action}'. Valid actions: {list(dispatch.keys())}"
            )
        return handler()

    def open_session(
        self,
        target_url: str,
        schema_hint: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        if not target_url or not isinstance(target_url, str):
            raise WebSocketSessionError("target_url must be a non-empty string")
        if not target_url.startswith(("ws://", "wss://")):
            raise WebSocketSessionError(
                f"target_url must use ws:// or wss:// scheme, got: {target_url!r}"
            )
        if schema_hint is not None and not isinstance(schema_hint, dict):
            raise WebSocketSessionError("schema_hint must be a dict or None")
        if headers is not None and not isinstance(headers, dict):
            raise WebSocketSessionError("headers must be a dict or None")
        body: dict[str, Any] = {"target_url": target_url}
        if schema_hint is not None:
            body["schema_hint"] = schema_hint
        if headers is not None:
            body["headers"] = headers
        return self._post("/sessions/open", body)

    def send_typed_frame(
        self,
        session_id: str,
        payload: Union[str, dict[str, Any], bytes],
        frame_type: str = "text",
    ) -> dict[str, Any]:
        if not session_id or not isinstance(session_id, str):
            raise WebSocketFrameError("session_id must be a non-empty string")
        if payload is None:
            raise WebSocketFrameError("payload must not be None")
        if frame_type not in ("text", "binary", "json"):
            raise WebSocketFrameError(
                f"frame_type must be 'text', 'binary', or 'json', got: {frame_type!r}"
            )
        if isinstance(payload, bytes):
            import base64
            encoded_payload: Any = base64.b64encode(payload).decode("ascii")
            frame_type = "binary"
        elif isinstance(payload, dict):
            encoded_payload = payload
            if frame_type == "text":
                frame_type = "json"
        elif isinstance(payload, str):
            encoded_payload = payload
        else:
            raise WebSocketFrameError(
                f"payload must be str, dict, or bytes, got: {type(payload).__name__}"
            )
        body: dict[str, Any] = {
            "session_id": session_id,
            "payload": encoded_payload,
            "frame_type": frame_type,
        }
        return self._post("/frames/send", body)

    def inspect_session_entropy(
        self,
        session_id: str,
        last_n_frames: Optional[int] = None,
    ) -> dict[str, Any]:
        if not session_id or not isinstance(session_id, str):
            raise WebSocketSessionError("session_id must be a non-empty string")
        if last_n_frames is not None:
            if not isinstance(last_n_frames, int) or last_n_frames < 1:
                raise WebSocketSessionError(
                    "last_n_frames must be a positive integer or None"
                )
            if last_n_frames > 10000:
                raise WebSocketSessionError(
                    "last_n_frames must not exceed 10000"
                )
        params: dict[str, Any] = {"session_id": session_id}
        if last_n_frames is not None:
            params["last_n_frames"] = last_n_frames
        return self._get("/sessions/entropy", params)

    def subscribe_topic_stream(
        self,
        session_id: str,
        topic_filter: str,
        max_frames: int = 50,
        entropy_alert_threshold: Optional[float] = None,
    ) -> dict[str, Any]:
        if not session_id or not isinstance(session_id, str):
            raise WebSocketSessionError("session_id must be a non-empty string")
        if not topic_filter or not isinstance(topic_filter, str):
            raise WebSocketSessionError("topic_filter must be a non-empty string")
        if not isinstance(max_frames, int) or max_frames < 1:
            raise WebSocketSessionError("max_frames must be a positive integer")
        if max_frames > 500:
            raise WebSocketSessionError("max_frames must not exceed 500 per call")
        if entropy_alert_threshold is not None:
            if not isinstance(entropy_alert_threshold, (int, float)):
                raise WebSocketSessionError(
                    "entropy_alert_threshold must be a float or None"
                )
            if not (0.0 <= float(entropy_alert_threshold) <= 1.0):
                raise WebSocketSessionError(
                    "entropy_alert_threshold must be in [0.0, 1.0]"
                )
        body: dict[str, Any] = {
            "session_id": session_id,
            "topic_filter": topic_filter,
            "max_frames": max_frames,
        }
        if entropy_alert_threshold is not None:
            body["entropy_alert_threshold"] = float(entropy_alert_threshold)
        return self._post("/topics/subscribe", body)

    def close_session(
        self,
        session_id: str,
        reason: Optional[str] = None,
    ) -> dict[str, Any]:
        if not session_id or not isinstance(session_id, str):
            raise WebSocketSessionError("session_id must be a non-empty string")
        if reason is not None and not isinstance(reason, str):
            raise WebSocketSessionError("reason must be a string or None")
        if reason is not None and len(reason) > 123:
            raise WebSocketSessionError(
                "reason must not exceed 123 characters (WebSocket close frame limit)"
            )
        body: dict[str, Any] = {"session_id": session_id}
        if reason is not None:
            body["reason"] = reason
        return self._post("/sessions/close", body)

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        try:
            response = self._http.post(url, content=json.dumps(body))
        except httpx.TimeoutException as exc:
            raise WebSocketSessionError(
                f"Request to {path} timed out after {self._timeout}s"
            ) from exc
        except httpx.RequestError as exc:
            raise WebSocketSessionError(
                f"Network error contacting {path}: {exc}"
            ) from exc
        return self._parse_response(response, path)

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        try:
            response = self._http.get(url, params=params)
        except httpx.TimeoutException as exc:
            raise WebSocketSessionError(
                f"Request to {path} timed out after {self._timeout}s"
            ) from exc
        except httpx.RequestError as exc:
            raise WebSocketSessionError(
                f"Network error contacting {path}: {exc}"
            ) from exc
        return self._parse_response(response, path)

    def _parse_response(self, response: httpx.Response, path: str) -> dict[str, Any]:
        raw = response.text
        if response.status_code == 401:
            raise AuthenticationError(
                "Invalid or missing API key", status_code=401, response_body=raw
            )
        if response.status_code == 404:
            raise SessionNotFoundError(
                f"Resource not found at {path}", status_code=404, response_body=raw
            )
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "unknown")
            raise WebSocketSessionError(
                f"Rate limit exceeded. Retry after {retry_after}s",
                status_code=429,
                response_body=raw,
            )
        if response.status_code >= 500:
            raise WebSocketSessionError(
                f"Server error {response.status_code} from {path}",
                status_code=response.status_code,
                response_body=raw,
            )
        if response.status_code >= 400:
            try:
                error_body = response.json()
                detail = error_body.get("detail") or error_body.get("message") or raw
            except Exception:
                detail = raw
            raise WebSocketSessionError(
                f"Client error {response.status_code} from {path}: {detail}",
                status_code=response.status_code,
                response_body=raw,
            )
        if not raw:
            raise WebSocketSessionError(
                f"Empty response body from {path}",
                status_code=response.status_code,
                response_body=raw,
            )
        try:
            result = response.json()
        except json.JSONDecodeError as exc:
            raise WebSocketSessionError(
                f"Invalid JSON in response from {path}: {exc}",
                status_code=response.status_code,
                response_body=raw,
            ) from exc
        if not isinstance(result, dict):
            raise WebSocketSessionError(
                f"Expected JSON object from {path}, got {type(result).__name__}",
                status_code=response.status_code,
                response_body=raw,
            )
        return result

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()