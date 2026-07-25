import unittest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from typing import Any


WS_API_BASE = "http://localhost:8000"
MOCK_SESSION_ID = "sess_7f3a2b1c"
MOCK_API_KEY = "test-key-nexus-ws-001"


class MockWebSocketSession:
    def __init__(self, session_id: str, uri: str, state: str = "connected"):
        self.session_id = session_id
        self.uri = uri
        self.state = state
        self.frames_sent: list[dict] = []
        self.frames_received: list[dict] = []
        self.subscriptions: list[str] = []

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "uri": self.uri,
            "state": self.state,
            "frames_sent": self.frames_sent,
            "frames_received": self.frames_received,
            "subscriptions": self.subscriptions,
        }


class MockWsSessionManager:
    """In-memory session store simulating the stateful WS session backend."""

    def __init__(self, api_key: str | None = None):
        self._api_key = api_key
        self._sessions: dict[str, MockWebSocketSession] = {}

    def _assert_auth(self):
        if not self._api_key:
            raise PermissionError(
                "API key required: set api_key on WsSessionManager before calling any method"
            )

    def connect_websocket_session(self, uri: str, headers: dict | None = None) -> dict:
        self._assert_auth()
        if not isinstance(uri, str):
            raise TypeError(
                f"uri must be a str, got {type(uri).__name__}"
            )
        if not uri.strip():
            raise ValueError("uri must be a non-empty string")
        if len(uri) > 2048:
            raise ValueError("uri exceeds maximum allowed length of 2048 characters")
        if not uri.startswith(("ws://", "wss://")):
            raise ValueError("uri must start with ws:// or wss://")
        session = MockWebSocketSession(
            session_id=MOCK_SESSION_ID, uri=uri, state="connected"
        )
        self._sessions[MOCK_SESSION_ID] = session
        return session.to_dict()

    def send_typed_frame(
        self, session_id: str, payload: Any, frame_type: str = "text"
    ) -> dict:
        self._assert_auth()
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id must be a non-empty string")
        if session_id not in self._sessions:
            raise KeyError(f"No active session with id '{session_id}'")
        if frame_type not in ("text", "binary", "ping", "pong"):
            raise ValueError(
                f"frame_type '{frame_type}' is not valid; use text, binary, ping, or pong"
            )
        if payload is None:
            raise ValueError("payload cannot be None; use an empty string for empty frames")
        session = self._sessions[session_id]
        frame = {"type": frame_type, "payload": payload, "seq": len(session.frames_sent) + 1}
        session.frames_sent.append(frame)
        return {"status": "sent", "frame": frame}

    def subscribe_topic_filter(self, session_id: str, topic: str) -> dict:
        self._assert_auth()
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id must be a non-empty string")
        if session_id not in self._sessions:
            raise KeyError(f"No active session with id '{session_id}'")
        if not isinstance(topic, str) or not topic.strip():
            raise ValueError("topic must be a non-empty string")
        session = self._sessions[session_id]
        if topic not in session.subscriptions:
            session.subscriptions.append(topic)
        return {"session_id": session_id, "subscribed_topics": session.subscriptions}

    def inspect_websocket_session(self, session_id: str) -> dict:
        self._assert_auth()
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id must be a non-empty string")
        if session_id not in self._sessions:
            raise KeyError(f"No active session with id '{session_id}'")
        return self._sessions[session_id].to_dict()

    def close_websocket_session(self, session_id: str, code: int = 1000) -> dict:
        self._assert_auth()
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id must be a non-empty string")
        if session_id not in self._sessions:
            raise KeyError(f"No active session with id '{session_id}'")
        if not isinstance(code, int) or code < 1000 or code > 4999:
            raise ValueError("close code must be an integer between 1000 and 4999")
        session = self._sessions.pop(session_id)
        session.state = "closed"
        return {"session_id": session_id, "state": "closed", "close_code": code}


class TestWsSessionManagerHappyPath(unittest.TestCase):

    def setUp(self):
        self.manager = MockWsSessionManager(api_key=MOCK_API_KEY)

    def test_happy_path_connect_returns_connected_session_dict(self):
        """connect_websocket_session returns dict with state=connected and a session_id."""
        result = self.manager.connect_websocket_session("wss://echo.example.com/ws")
        self.assertEqual(result["state"], "connected")
        self.assertIn("session_id", result)
        self.assertEqual(result["uri"], "wss://echo.example.com/ws")

    def test_happy_path_send_typed_frame_increments_sequence_number(self):
        """send_typed_frame on a connected session returns sent frame with seq=1 then seq=2."""
        self.manager.connect_websocket_session("ws://stream.example.com")
        r1 = self.manager.send_typed_frame(MOCK_SESSION_ID, '{"action":"ping"}')
        r2 = self.manager.send_typed_frame(MOCK_SESSION_ID, '{"action":"subscribe"}')
        self.assertEqual(r1["frame"]["seq"], 1)
        self.assertEqual(r2["frame"]["seq"], 2)
        self.assertEqual(r1["status"], "sent")

    def test_happy_path_subscribe_topic_filter_deduplicates_topics(self):
        """subscribe_topic_filter adds topic once even when called twice with the same topic."""
        self.manager.connect_websocket_session("wss://feeds.example.com")
        self.manager.subscribe_topic_filter(MOCK_SESSION_ID, "market.btcusd.tick")
        self.manager.subscribe_topic_filter(MOCK_SESSION_ID, "market.btcusd.tick")
        result = self.manager.subscribe_topic_filter(MOCK_SESSION_ID, "market.ethusd.tick")
        self.assertEqual(len(result["subscribed_topics"]), 2)
        self.assertIn("market.btcusd.tick", result["subscribed_topics"])


class TestWsSessionManagerEdgeCases(unittest.TestCase):

    def setUp(self):
        self.manager = MockWsSessionManager(api_key=MOCK_API_KEY)

    def test_edge_case_uri_at_maximum_length_boundary_is_accepted(self):
        """A URI exactly 2048 characters long must be accepted without error."""
        long_path = "wss://example.com/" + "x" * (2048 - len("wss://example.com/"))
        self.assertEqual(len(long_path), 2048)
        result = self.manager.connect_websocket_session(long_path)
        self.assertEqual(result["state"], "connected")

    def test_edge_case_uri_exceeds_maximum_length_raises_value_error(self):
        """A URI of 2049 chars must raise ValueError mentioning the 2048 limit."""
        over_limit = "wss://example.com/" + "x" * (2049 - len("wss://example.com/"))
        with self.assertRaises(ValueError) as ctx:
            self.manager.connect_websocket_session(over_limit)
        self.assertIn("2048", str(ctx.exception))

    def test_edge_case_close_with_non_standard_code_4001_is_accepted(self):
        """Application-defined close code 4001 (valid range 4000-4999) must succeed."""
        self.manager.connect_websocket_session("ws://internal.example.com")
        result = self.manager.close_websocket_session(MOCK_SESSION_ID, code=4001)
        self.assertEqual(result["close_code"], 4001)
        self.assertEqual(result["state"], "closed")


class TestWsSessionManagerInvalidInputs(unittest.TestCase):

    def setUp(self):
        self.manager = MockWsSessionManager(api_key=MOCK_API_KEY)

    def test_invalid_input_connect_with_non_string_uri_raises_type_error(self):
        """Passing an integer as uri must raise TypeError with the offending type in the message."""
        with self.assertRaises(TypeError) as ctx:
            self.manager.connect_websocket_session(12345)
        self.assertIn("int", str(ctx.exception))

    def test_invalid_input_send_frame_with_none_payload_raises_value_error(self):
        """None payload must raise ValueError; silent null frames corrupt the wire protocol."""
        self.manager.connect_websocket_session("wss://echo.example.com")
        with self.assertRaises(ValueError) as ctx:
            self.manager.send_typed_frame(MOCK_SESSION_ID, None)
        self.assertIn("None", str(ctx.exception))

    def test_invalid_input_send_frame_with_unknown_frame_type_raises_value_error(self):
        """frame_type='blob' is not in the allowed set; must raise ValueError naming the bad value."""
        self.manager.connect_websocket_session("wss://echo.example.com")
        with self.assertRaises(ValueError) as ctx:
            self.manager.send_typed_frame(MOCK_SESSION_ID, "data", frame_type="blob")
        self.assertIn("blob", str(ctx.exception))


class TestWsSessionManagerRateLimit(unittest.TestCase):

    def setUp(self):
        self.manager = MockWsSessionManager(api_key=MOCK_API_KEY)

    def test_rate_limit_100_consecutive_send_calls_do_not_raise(self):
        """Sending 100 frames in tight succession must not raise or corrupt sequence numbering."""
        self.manager.connect_websocket_session("wss://throughput.example.com")
        results = []
        for i in range(100):
            r = self.manager.send_typed_frame(MOCK_SESSION_ID, f"frame-{i}")
            results.append(r["frame"]["seq"])
        self.assertEqual(results[-1], 100)
        self.assertEqual(len(set(results)), 100)


class TestWsSessionManagerAuth(unittest.TestCase):

    def test_auth_missing_api_key_on_connect_raises_permission_error(self):
        """Manager with no api_key must raise PermissionError with a message naming 'api_key'."""
        manager = MockWsSessionManager(api_key=None)
        with self.assertRaises(PermissionError) as ctx:
            manager.connect_websocket_session("wss://secure.example.com")
        self.assertIn("api_key", str(ctx.exception))


class TestWsSessionManagerIdempotency(unittest.TestCase):

    def setUp(self):
        self.manager = MockWsSessionManager(api_key=MOCK_API_KEY)

    def test_idempotency_inspect_session_returns_identical_dict_on_repeated_calls(self):
        """inspect_websocket_session called twice on the same session must return identical dicts."""
        self.manager.connect_websocket_session("wss://stable.example.com")
        self.manager.send_typed_frame(MOCK_SESSION_ID, "hello")
        result_a = self.manager.inspect_websocket_session(MOCK_SESSION_ID)
        result_b = self.manager.inspect_websocket_session(MOCK_SESSION_ID)
        self.assertEqual(result_a, result_b)


if __name__ == "__main__":
    unittest.main()