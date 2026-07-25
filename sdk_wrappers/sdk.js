const axios = require('axios');

const DEFAULT_BASE_URL = 'https://npm-package-ws-has-241560546-w-production.up.railway.app';
const DEFAULT_TIMEOUT_MS = 30000;

class WsSessionManagerError extends Error {
  constructor(message, statusCode, body) {
    super(message);
    this.name = 'WsSessionManagerError';
    this.statusCode = statusCode || null;
    this.body = body || null;
  }
}

class ValidationError extends WsSessionManagerError {
  constructor(message, body) {
    super(message, 422, body);
    this.name = 'ValidationError';
  }
}

class SessionNotFoundError extends WsSessionManagerError {
  constructor(sessionId, body) {
    super(`session_id '${sessionId}' not found in registry`, 404, body);
    this.name = 'SessionNotFoundError';
    this.sessionId = sessionId;
  }
}

class RegistryAtCapacityError extends WsSessionManagerError {
  constructor(body) {
    super('Session registry at capacity — close idle sessions or retry later', 503, body);
    this.name = 'RegistryAtCapacityError';
  }
}

function assertNonEmptyString(value, label) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new ValidationError(`${label} must be a non-empty string`);
  }
}

function buildHttpClient(baseUrl, timeoutMs) {
  return axios.create({
    baseURL: baseUrl,
    timeout: timeoutMs,
    headers: { 'Content-Type': 'application/json' },
  });
}

function parseApiError(error) {
  if (!error.response) {
    throw new WsSessionManagerError(`Network error: ${error.message}`, null, null);
  }
  const { status, data } = error.response;
  if (status === 404) {
    const sid = (data && data.detail && /'([^']+)'/.exec(data.detail)) || null;
    throw new SessionNotFoundError(sid ? sid[1] : '?', data);
  }
  if (status === 503) {
    throw new RegistryAtCapacityError(data);
  }
  if (status === 422) {
    const detail = (data && data.detail)
      ? (Array.isArray(data.detail) ? data.detail.map((d) => d.msg || JSON.stringify(d)).join('; ') : String(data.detail))
      : 'Unprocessable entity';
    throw new ValidationError(detail, data);
  }
  throw new WsSessionManagerError(`API error ${status}: ${JSON.stringify(data)}`, status, data);
}

/**
 * Thin HTTP client for the deployed WebSocket Session Manager API
 * (REST control-plane — the server holds the real WebSocket connection,
 * this client only calls its /ws-sessions/* endpoints). Every method
 * maps 1:1 to a real deployed route; see AGENTS.md in this repo for the
 * live-verified request/response shapes.
 */
class WsSessionManagerClient {
  constructor(options) {
    const opts = options || {};
    this._baseUrl = (opts.baseUrl || DEFAULT_BASE_URL).replace(/\/$/, '');
    this._timeoutMs = Number.isInteger(opts.timeoutMs) ? opts.timeoutMs : DEFAULT_TIMEOUT_MS;
    this._http = buildHttpClient(this._baseUrl, this._timeoutMs);
  }

  /**
   * Opens a WebSocket session against targetUrl and registers it in the
   * server-side session registry. Returns { session_id, target_url,
   * state, opened_at_unix, message }.
   */
  async openSession(targetUrl, options) {
    assertNonEmptyString(targetUrl, 'targetUrl');
    if (!/^wss?:\/\//i.test(targetUrl)) {
      throw new ValidationError('targetUrl must start with ws:// or wss://');
    }
    const opts = options || {};
    const body = { target_url: targetUrl };
    if (opts.connectTimeoutSeconds !== undefined) {
      body.connect_timeout_seconds = opts.connectTimeoutSeconds;
    }
    try {
      const res = await this._http.post('/ws-sessions/open', body);
      return res.data;
    } catch (err) {
      parseApiError(err);
    }
  }

  /**
   * Sends a single text or binary frame over an open session. Returns
   * { session_id, frame_type, payload_bytes, entropy_after,
   * entropy_delta, schema_valid, sent_at_unix }.
   */
  async sendFrame(sessionId, payload, options) {
    assertNonEmptyString(sessionId, 'sessionId');
    if (payload === null || payload === undefined) {
      throw new ValidationError('payload must not be null or undefined');
    }
    const opts = options || {};
    const body = { payload, frame_type: opts.frameType || 'text' };
    try {
      const res = await this._http.post(`/ws-sessions/${sessionId}/send-frame`, body);
      return res.data;
    } catch (err) {
      parseApiError(err);
    }
  }

  /**
   * Returns up to maxFrames buffered inbound frames since the last
   * drain. Returns { session_id, frames_returned, frames[],
   * buffer_remaining }.
   */
  async drainFrames(sessionId, options) {
    assertNonEmptyString(sessionId, 'sessionId');
    const opts = options || {};
    const body = {};
    if (opts.maxFrames !== undefined) body.max_frames = opts.maxFrames;
    try {
      const res = await this._http.post(`/ws-sessions/${sessionId}/drain-frames`, body);
      return res.data;
    } catch (err) {
      parseApiError(err);
    }
  }

  /**
   * Returns real-time telemetry for a session (state, frame counts,
   * entropy stats, schema violation rate, uptime). Takes no body — the
   * real endpoint accepts none.
   */
  async getTelemetry(sessionId) {
    assertNonEmptyString(sessionId, 'sessionId');
    try {
      const res = await this._http.post(`/ws-sessions/${sessionId}/telemetry`, {});
      return res.data;
    } catch (err) {
      parseApiError(err);
    }
  }

  /**
   * Closes a session and permanently deallocates its session_id.
   * Returns a final telemetry snapshot.
   */
  async closeSession(sessionId, options) {
    assertNonEmptyString(sessionId, 'sessionId');
    const opts = options || {};
    const body = {};
    if (opts.statusCode !== undefined) body.status_code = opts.statusCode;
    if (opts.reason !== undefined) body.reason = opts.reason;
    try {
      const res = await this._http.post(`/ws-sessions/${sessionId}/close`, body);
      return res.data;
    } catch (err) {
      parseApiError(err);
    }
  }
}

module.exports = {
  WsSessionManagerClient,
  WsSessionManagerError,
  ValidationError,
  SessionNotFoundError,
  RegistryAtCapacityError,
};
