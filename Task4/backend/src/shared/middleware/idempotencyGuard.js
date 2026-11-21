/**
 * Distributed Idempotency Guard
 * Prevents race conditions and duplicate operations using Redis atomic locks.
 *
 * Pattern:
 * 1. Checks X-Idempotency-Key header.
 * 2. Uses atomic `SET key "IN_PROGRESS" NX EX <ttl>` in Redis.
 * 3. If lock exists, safely returns the existing or in-progress operation.
 * 4. Once operation completes, saves the finalized result with extended TTL.
 */

const redisConnection = require("../redis");
const logger = require("../logger");

function createIdempotencyGuard(options = {}) {
  const lockTtlSec = Math.max(10, Number(options.lockTtlSec || 120));
  const resultTtlSec = Math.max(60, Number(options.resultTtlSec || 86400)); // 24h

  return async function idempotencyMiddleware(req, res, next) {
    const rawKey = req.headers["x-idempotency-key"];
    if (!rawKey) {
      return next();
    }

    const cleanKey = String(rawKey).trim();
    if (!cleanKey) {
      return next();
    }

    const userId = req.user?.userId || "anon";
    const redisKey = `idempotency:${userId}:${cleanKey}`;

    try {
      if (redisConnection.status !== "ready" && redisConnection.status !== "connect") {
        return next();
      }

      // Try acquiring atomic lock
      const acquired = await redisConnection.set(
        redisKey,
        JSON.stringify({ status: "IN_PROGRESS", createdAt: Date.now() }),
        "NX",
        "EX",
        lockTtlSec
      );

      if (!acquired) {
        // Lock already held or previous result exists
        const existingData = await redisConnection.get(redisKey);
        if (existingData) {
          try {
            const parsed = JSON.parse(existingData);
            if (parsed.status === "COMPLETED" || parsed.submissionId) {
              logger.info("idempotency.replayed_from_cache", { userId, key: cleanKey });
              return res.status(200).json({
                submissionId: parsed.submissionId,
                status: parsed.status,
                idempotentReplay: true
              });
            }

            if (parsed.status === "IN_PROGRESS") {
              return res.status(409).json({
                error: "A request with this idempotency key is currently being processed. Please wait.",
                idempotencyKey: cleanKey
              });
            }
          } catch (_) {
            // Fall through if parsing fails
          }
        }
      }

      // If the request fails (validation error, 404, 5xx) release the lock, otherwise a
      // corrected retry with the same key would be rejected with 409 until the lock TTL expires.
      let resultSaved = false;
      res.on("finish", () => {
        if (!resultSaved && res.statusCode >= 400) {
          redisConnection.del(redisKey).catch(() => {});
        }
      });

      // Attach callback to finalize idempotency cache once route completes
      res.saveIdempotencyResult = async (resultPayload) => {
        resultSaved = true;
        try {
          if (redisConnection.status === "ready" || redisConnection.status === "connect") {
            await redisConnection.set(
              redisKey,
              JSON.stringify({
                status: resultPayload.status || "COMPLETED",
                submissionId: resultPayload.submissionId,
                finalizedAt: Date.now()
              }),
              "EX",
              resultTtlSec
            );
          }
        } catch (saveErr) {
          logger.warn("idempotency.save_failed", { key: cleanKey, error: saveErr.message });
        }
      };

      return next();
    } catch (err) {
      logger.warn("idempotency.middleware_error", { error: err.message, key: cleanKey });
      return next();
    }
  };
}

module.exports = {
  createIdempotencyGuard
};

