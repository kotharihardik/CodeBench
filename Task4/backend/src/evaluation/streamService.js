/**
 * Server-Sent Events (SSE) Live Execution Streaming Service
 * Provides real-time push streaming of test-case progress and execution outputs.
 *
 * Concepts:
 * - Unidirectional HTTP chunked transfer (`text/event-stream`).
 * - Decoupled Pub/Sub event bridge via Redis channel `submission:stream:<submissionId>`.
 * - Heartbeat keep-alives to prevent proxy/NAT gateway timeouts.
 * - Automatic resource cleanup on client socket disconnect.
 */

const redisConnection = require("../shared/redis");
const logger = require("../shared/logger");

// Local event emitter fallback for non-Redis or test environments
const localSubscribers = new Map();

function getStreamChannel(submissionId) {
  return `submission:stream:${String(submissionId)}`;
}

async function publishSubmissionProgress(submissionId, eventData = {}) {
  const channel = getStreamChannel(submissionId);
  const payload = JSON.stringify({
    submissionId: String(submissionId),
    timestamp: new Date().toISOString(),
    ...eventData
  });

  try {
    if (redisConnection.status === "ready" || redisConnection.status === "connect") {
      await redisConnection.publish(channel, payload);
    }
  } catch (err) {
    logger.warn("stream.publish_error", { submissionId, error: err.message });
  }

  // Notify local in-process subscribers
  const callbacks = localSubscribers.get(String(submissionId));
  if (callbacks && callbacks.size > 0) {
    for (const cb of callbacks) {
      try {
        cb(payload);
      } catch (_) {}
    }
  }
}

function isFinalStatus(status) {
  const clean = String(status || "").toUpperCase();
  return clean === "COMPLETED" || clean === "FAILED";
}

function buildFinishedFrame(sid, state) {
  return `data: ${JSON.stringify({
    event: "FINISHED",
    submissionId: sid,
    status: String(state.status || "").toUpperCase(),
    score: state.score,
    passed: state.passed,
    timestamp: new Date().toISOString()
  })}\n\n`;
}

/**
 * @param initialState   DB snapshot read before subscribing ({ status, score, passed })
 * @param refetchState   async () => fresh DB snapshot; called once the Redis subscription is
 *                       confirmed to close the gap between "read DB" and "subscribed" (an event
 *                       published in that window would otherwise be lost: pub/sub has no replay).
 */
function handleSseConnection(req, res, submissionId, initialState = null, refetchState = null) {
  const sid = String(submissionId);
  const channel = getStreamChannel(sid);

  // Set SSE mandatory headers
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders?.();

  // Send initial handshake frame
  res.write(
    `data: ${JSON.stringify({ event: "CONNECTED", submissionId: sid, timestamp: new Date().toISOString() })}\n\n`
  );

  // Late subscriber: the job may have finished before the client connected (pub/sub has no
  // replay), so emit the final state from the database snapshot and close immediately.
  if (initialState && isFinalStatus(initialState.status)) {
    res.write(buildFinishedFrame(sid, initialState));
    res.end();
    return;
  }

  let closed = false;
  function writeAndMaybeClose(frame, isFinal) {
    if (closed) {
      return;
    }
    res.write(frame);
    if (isFinal) {
      closed = true;
      cleanup();
      res.end();
    }
  }

  let subClient = null;
  const keepAliveInterval = setInterval(() => {
    res.write(":keepalive\n\n");
  }, 15000);

  // In-process callback listener
  const localCallback = (rawPayload) => {
    try {
      const parsed = JSON.parse(rawPayload);
      writeAndMaybeClose(`data: ${rawPayload}\n\n`, isFinalStatus(parsed.status) || parsed.event === "FINISHED");
    } catch (_) {}
  };

  if (!localSubscribers.has(sid)) {
    localSubscribers.set(sid, new Set());
  }
  localSubscribers.get(sid).add(localCallback);

  // Redis Pub/Sub subscription (skip in test environment to avoid open network handles)
  try {
    if (process.env.NODE_ENV !== "test" && redisConnection && typeof redisConnection.duplicate === "function") {
      subClient = redisConnection.duplicate();
      subClient.on("error", () => {});
      subClient.subscribe(channel, async (err) => {
        if (err) {
          logger.warn("stream.subscribe_error", { channel, error: err.message });
          return;
        }

        // Subscribe-then-recheck: catch a FINISHED event published before we were listening.
        if (typeof refetchState === "function") {
          try {
            const latest = await refetchState();
            if (latest && isFinalStatus(latest.status)) {
              writeAndMaybeClose(buildFinishedFrame(sid, latest), true);
            }
          } catch (_) {}
        }
      });

      subClient.on("message", (ch, message) => {
        if (ch === channel && message) {
          try {
            const parsed = JSON.parse(message);
            writeAndMaybeClose(`data: ${message}\n\n`, isFinalStatus(parsed.status) || parsed.event === "FINISHED");
          } catch (_) {}
        }
      });
    }
  } catch (err) {
    logger.warn("stream.redis_setup_failed", { error: err.message });
  }

  function cleanup() {
    clearInterval(keepAliveInterval);

    const subs = localSubscribers.get(sid);
    if (subs) {
      subs.delete(localCallback);
      if (subs.size === 0) {
        localSubscribers.delete(sid);
      }
    }

    if (subClient) {
      try {
        subClient.unsubscribe(channel);
        subClient.quit().catch(() => {});
      } catch (_) {}
      subClient = null;
    }
  }

  req.on("close", () => {
    closed = true;
    cleanup();
  });
}

module.exports = {
  publishSubmissionProgress,
  handleSseConnection
};

