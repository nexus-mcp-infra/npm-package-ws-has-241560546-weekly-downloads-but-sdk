
```javascript
"use strict";

const WebSocket = require("ws");
const { EventEmitter } = require("events");
const crypto = require("crypto");

class WebSocketMCPClientError extends Error {
  constructor(message, code, details) {
    super(message);
    this.name = "WebSocketMCPClientError";
    this.code = code || "UNKNOWN_ERROR";
    this.details = details || null;
  }
}

class SessionEntropyTracker {
  constructor() {
    this.tokenFrequencies = new Map();
    this.totalTokens = 0;
    this.lastEntropy = 0;
  }

  updateWithTokens(tokens) {
    for (const token of tokens) {
      const key = String(token);
      this.tokenFrequencies.set(key, (this.tokenFrequencies.get(key) || 0) + 1);
      this.totalTokens++;
    }
  }

  computeShannonEntropy() {
    if (this.totalTokens === 0) return 0;
    let h = 0;
    for (const freq of this.tokenFrequencies.values()) {
      const p = freq / this.totalTokens;
      if (p > 0) {
        h -= p * Math.log2(p);
      }
    }
    return h;
  }

  computeDelta(tokens) {
    const prevEntropy = this.lastEntropy;
    this.updateWithTokens(tokens);
    const newEntropy = this.computeShannonEntropy();
    const delta = newEntropy - prevEntropy;
    this.lastEntropy = newEntropy;
    return { currentEntropy: newEntropy, delta, prevEntropy };
  }

  snapshot() {
    return {
      uniqueTokens: this.tokenFrequencies.size,
      totalTokens: this.totalTokens,
      currentEntropy: this.lastEntropy,
    };
  }
}

function extractFrameTokens(data) {
  if (Buffer.isBuffer(data)) {
    return Array.from(data);
  }
  if (typeof data === "string") {
    try {
      const parsed = JSON.parse(data);
      return extractJsonLeafValues(parsed);
    } catch {
      return data.split(/\s+/).filter(Boolean);
    }
  }
  return [];
}

function extractJsonLeafValues(obj, depth) {
  const maxDepth = 8;
  const d = depth || 0;
  if (d > maxDepth) return [];
  const leaves = [];
  if (obj === null || obj === undefined) return ["__null__"];
  if (typeof obj !== "object") return [String(obj)];
  if (Array.isArray(obj)) {
    for (const item of obj) {
      leaves.push(...extractJsonLeafValues(item, d + 1));
    }
    return leaves;
  }
  for (const [key, val] of Object.entries(obj)) {
    leaves.push(key);
    leaves.push(...extractJsonLeafValues(val, d + 1));
  }
  return leaves;
}

class WebSocketSessionRegistry extends EventEmitter {
  constructor() {
    super();
    this.sessions = new Map();
    this.messageBufferMaxSize = 100;
  }

  allocateSession(url, options) {
    const sessionId = crypto.randomBytes(8).toString("hex");
    const record = {
      sessionId,
      url,
      options: options || {},
      socket: null,
      state: "pending",
      connectedAt: null,
      framesSent: 0,
      framesReceived: 0,
      entropy: new SessionEntropyTracker(),
      messageBuffer: [],
      subscribers: new Map(),
      lastAnomalyDelta: null,
    };
    this.sessions.set(sessionId, record);
    return sessionId;
  }

  getSession(sessionId) {
    if (!sessionId || typeof sessionId !== "string") {
      throw new WebSocketMCPClientError(
        "sessionId must be a non-empty string",
        "INVALID_SESSION_ID"
      );
    }
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new WebSocketMCPClientError(
        `Session '${sessionId}' not found in registry`,
        "SESSION_NOT_FOUND",
        { sessionId }
      );
    }
    return session;
  }

  removeSession(sessionId) {
    this.sessions.delete(sessionId);
  }

  recordFrame(sessionId, direction, data) {
    const session = this.sessions.get(sessionId);
    if (!session) return null;

    const tokens = extractFrameTokens(data);
    const entropyResult = session.entropy.computeDelta(tokens);

    const frameRecord = {
      direction,
      timestamp: Date.now(),
      size: Buffer.isBuffer(data) ? data.length : Buffer.byteLength(String(data)),
      entropyDelta: entropyResult.delta,
      currentEntropy: entropyResult.currentEntropy,
      isAnomaly: Math.abs(entropyResult.delta) > 1.5,
    };

    if (direction === "inbound") session.framesReceived++;
    if (direction === "outbound") session.framesSent++;

    if (frameRecord.isAnomaly) {
      session.lastAnomalyDelta = entropyResult.delta;
    }

    session.messageBuffer.push({ ...frameRecord, data });
    if (session.messageBuffer.length > this.messageBufferMaxSize) {
      session.messageBuffer.shift();
    }

    return frameRecord;
  }

  listSessionIds() {
    return Array.from(this.sessions.keys());
  }
}

const globalRegistry = new WebSocketSessionRegistry();

async function connectWebSocketSession(url, options) {
  if (!url || typeof url !== "string") {
    throw new WebSocketMCPClientError(
      "url must be a non-empty string",
      "INVALID_URL"
    );
  }
  if (!/^wss?:\/\//i.test(url)) {
    throw new WebSocketMCPClientError(
      `url must begin with ws:// or wss://, received: '${url}'`,
      "INVALID_URL_SCHEME",
      { url }
    );
  }

  const opts = options || {};
  const sessionId = globalRegistry.allocateSession(url, opts);
  const session = globalRegistry.getSession(sessionId);

  return new Promise((resolve, reject) => {
    const wsOptions = {};
    if (opts.headers) wsOptions.headers = opts.headers;
    if (opts.protocols) wsOptions.protocols = opts.protocols;
    const connectTimeoutMs = opts.connectTimeoutMs || 15000;

    let settled = false;
    const socket = new WebSocket(url, wsOptions);

    const timeout = setTimeout(() => {
      if (!settled) {
        settled = true;
        session.state = "error";
        socket.terminate();
        globalRegistry.removeSession(sessionId);
        reject(
          new WebSocketMCPClientError(
            `Connection to '${url}' timed out after ${connectTimeoutMs}ms`,
            "CONNECT_TIMEOUT",
            { url, connectTimeoutMs }
          )
        );
      }
    }, connectTimeoutMs);

    socket.on("open", () => {
      clearTimeout(timeout);
      if (!settled) {
        settled = true;
        session.socket = socket;
        session.state = "connected";
        session.connectedAt = Date.now();
        resolve({
          sessionId,
          url,
          state: "connected",
          connectedAt: session.connectedAt,
        });
      }
    });

    socket.on("error", (err) => {
      clearTimeout(timeout);
      if (!settled) {
        settled = true;
        session.state = "error";
        globalRegistry.removeSession(sessionId);
        reject(
          new WebSocketMCPClientError(
            `WebSocket connection error: ${err.message}`,
            "CONNECTION_ERROR",
            { url, originalMessage: err.message }
          )
        );
      }
    });

    socket.on("message", (data) => {
      const frameRecord = globalRegistry.recordFrame(sessionId, "inbound", data);
      let parsedData = null;
      if (typeof data === "string") {
        try {
          parsedData = JSON.parse(data);
        } catch {
          parsedData = data;
        }
      } else {
        parsedData = data;
      }

      for (const [topicFilter, callbackList] of session.subscribers.entries()) {
        let matches = false;
        if (topicFilter === "*") {
          matches = true;
        } else if (
          typeof parsedData === "object" &&
          parsedData !== null &&
          parsedData.topic === topicFilter
        ) {
          matches = true;
        } else if (typeof parsedData === "string" && parsedData.includes(topicFilter)) {
          matches = true;
        }

        if (matches) {
          for (const cb of callbackList) {
            try {
              cb({ data: parsedData, frameMetadata: frameRecord, sessionId });
            } catch {
              // subscriber errors must not crash the session
            }
          }
        }
      }

      globalRegistry.emit("frame", { sessionId, direction: "inbound", frameRecord, data: parsedData });
    });

    socket.on("close", (code, reason) => {
      session.state = "closed";
      globalRegistry.emit("close", { sessionId, code, reason: reason ? reason.toString() : null });
    });
  });
}

async function sendWebSocketFrame(sessionId, payload, options) {
  if (payload === null || payload === undefined) {
    throw new WebSocketMCPClientError(
      "payload must not be null or undefined",
      "INVALID_PAYLOAD"
    );
  }

  const session = globalRegistry.getSession(sessionId);

  if (session.state !== "connected") {
    throw new WebSocketMCPClientError(
      `Cannot send frame: session '${sessionId}' is in state '${session.state}', expected 'connected'`,
      "SESSION_NOT_CONNECTED",
      { sessionId, state: session.state }
    );
  }

  const opts = options || {};
  let rawPayload;

  if (opts.encoding === "binary") {
    if (!Buffer.isBuffer(payload)) {
      throw new WebSocketMCPClientError(
        "payload must be a Buffer when encoding is 'binary'",
        "INVALID_PAYLOAD_TYPE",
        { encoding: "binary" }
      );
    }
    rawPayload = payload;
  } else if (typeof payload === "object" && !Buffer.isBuffer(payload)) {
    rawPayload = JSON.stringify(payload);
  } else {
    rawPayload = String(payload);
  }

  return new Promise((resolve, reject) => {
    session.socket.send(rawPayload, (err) => {
      if (err) {
        return reject(
          new WebSocketMCPClientError(
            `Frame send failed on session '${sessionId}': ${err.message}`,
            "SEND_FAILED",
            { sessionId, originalMessage: err.message }
          )
        );
      }
      const frameRecord = globalRegistry.recordFrame(sessionId, "outbound", rawPayload);
      resolve({
        sessionId,
        framesSent: session.framesSent,
        size: frameRecord.size,
        entropyDelta: frameRecord.entropyDelta,
        currentEntropy: frameRecord.currentEntropy,
      });
    });
  });
}

function subscribeWebSocketTopic(sessionId, topic, callback) {
  if (!topic || typeof topic !== "string") {
    throw new WebSocketMCPClientError(
      "topic must be a non-empty string",
      "INVALID_TOPIC"
    );
  }
  if (typeof callback !== "function") {
    throw new WebSocketMCPClientError(
      "callback must be a function",
      "INVALID_CALLBACK"
    );
  }

  const session = globalRegistry.getSession(sessionId);

  if (!["connected", "pending"].includes(session.state)) {
    throw new WebSocketMCPClientError(
      `Cannot subscribe: session '${sessionId}' is in state '${session.state}'`,
      "SESSION_NOT_CONNECTABLE",
      { sessionId, state: session.state }
    );
  }

  if (!session.subscribers.has(topic)) {
    session.subscribers.set(topic, []);
  }
  session.subscribers.get(topic).push(callback);

  const subscriptionId = `${sessionId}:${topic}:${crypto.randomBytes(4).toString("hex")}`;

  return {
    subscriptionId,
    sessionId,
    topic,
    unsubscribe() {
      const list = session.subscribers.get(topic);
      if (list) {
        const idx = list.indexOf(callback);
        if (idx !== -1) list.splice(idx, 1);
        if (list.length === 0) session.subscribers.delete(topic);
      }
    },
  };
}

function inspectWebSocketSession(sessionId) {
  const session = globalRegistry.getSession(sessionId);

  const recentFrames = session.messageBuffer.slice(-20).map((f) => ({
    direction: f.direction,
    timestamp: f.timestamp,
    size: f.size,
    entropyDelta: f.entropyDelta,
    currentEntropy: f.currentEntropy,
    isAnomaly: f.isAnomaly,
  }));

  return {
    sessionId: session.sessionId,
    url: session.url,
    state: session.state,
    connectedAt: session.connectedAt,
    uptimeMs: session.connectedAt ? Date.now() - session.connectedAt : null,
    framesSent: session.framesSent,
    framesReceived: session.framesReceived,
    activeSubscriptions: Array.from(session.subscribers.keys()),
    entropySnapshot: session.entropy.snapshot(),
    lastAnomalyDelta: session.lastAnomalyDelta,
    recentFrames,
  };
}

async function closeWebSocketSession(sessionId, options) {
  const session = globalRegistry.getSession(sessionId);

  if (session.state === "closed") {
    return { sessionId, state: "closed", alreadyClosed: true };
  }

  if (!session.socket) {
    globalRegistry.removeSession(sessionId);
    return { sessionId, state: "closed", alreadyClosed: false };
  }

  const opts = options || {};
  const code = opts.code !== undefined ? opts.code : 1000;
  const reason = opts.reason || "normal closure";

  if (typeof code !== "number" || code < 1000 || code > 4999) {
    throw new WebSocketMCPClientError(
      `close code must be a number between 1000 and 4999, received: ${code}`,
      "INVALID_CLOSE_CODE",
      { code }
    );
  }

  return new Promise((resolve) => {
    const drainTimeoutMs = opts.drainTimeoutMs || 5000;

    const timeout = setTimeout(() => {
      session.socket.terminate();
      session.state = "closed";
      globalRegistry.removeSession(sessionId);
      resolve({ sessionId, state: "closed", timedOut: true });
    }, drainTimeoutMs);

    session.socket.once("close", () => {
      clearTimeout(timeout);
      session.state = "closed";
      globalRegistry.removeSession(sessionId);
      resolve({ sessionId, state: "closed", timedOut: false });
    });

    session.socket.close(code, reason);
  });
}

module.exports = {
  connectWebSocketSession,
  sendWebSocketFrame,
  subscribeWebSocketTopic,
  inspectWebSocketSession,
  closeWebSocketSession,
  WebSocketMCPClientError,
  _registry: globalRegistry,
};
```