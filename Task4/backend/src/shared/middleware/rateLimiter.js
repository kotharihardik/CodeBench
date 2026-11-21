/**
 * Distributed Token Bucket Rate Limiter
 * Implements token bucket algorithm backed by Redis with local fail-open protection.
 *
 * Concepts:
 * - Capacity: Maximum tokens (burst capacity).
 * - Refill Rate: Tokens added per second.
 * - Cost: Tokens deducted per request.
 * - Fail-open: If Redis is temporarily unreachable, requests pass through gracefully.
 */

const redisConnection = require("../redis");
const logger = require("../logger");

// KEYS[1] = bucket key; ARGV = capacity, refillRatePerSec, cost, nowMs, ttlSec
// Returns { allowed (1/0), tokens remaining (as string to keep the fraction) }
const TOKEN_BUCKET_LUA = `
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local state = redis.call("HMGET", KEYS[1], "tokens", "lastRefill")
local tokens = tonumber(state[1])
local lastRefill = tonumber(state[2])
if tokens == nil then
  tokens = capacity
  lastRefill = now
end

local elapsed = math.max(0, now - lastRefill) / 1000
tokens = math.min(capacity, tokens + elapsed * rate)

local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

redis.call("HSET", KEYS[1], "tokens", tostring(tokens), "lastRefill", tostring(now))
redis.call("EXPIRE", KEYS[1], ttl)
return { allowed, tostring(tokens) }
`;

function createTokenBucketLimiter(options = {}) {
  const name = options.name || "default";
  const capacity = Math.max(1, Number(options.capacity || 5));
  const refillRatePerSec = Math.max(0.01, Number(options.refillRatePerSec || 0.5)); // 1 token every 2 sec
  const cost = Math.max(1, Number(options.cost || 1));
  const ttlSec = Math.ceil(capacity / refillRatePerSec) + 10;

  return async function tokenBucketMiddleware(req, res, next) {
    const identifier = req.user?.userId || req.ip || "anonymous";
    const redisKey = `ratelimit:v2:${name}:${identifier}`;

    try {
      if (redisConnection.status !== "ready" && redisConnection.status !== "connect") {
        // Fail-open if Redis is not connected yet
        return next();
      }

      // Atomic read-refill-consume in a single Lua script: concurrent requests from the same
      // user (across all API replicas) cannot read the same token count and all pass.
      const now = Date.now();
      const [allowed, tokensLeft] = await redisConnection.eval(
        TOKEN_BUCKET_LUA,
        1,
        redisKey,
        capacity,
        refillRatePerSec,
        cost,
        now,
        ttlSec
      );
      const tokens = Number(tokensLeft);

      if (Number(allowed) === 1) {
        res.setHeader("X-RateLimit-Limit", capacity);
        res.setHeader("X-RateLimit-Remaining", Math.max(0, Math.floor(tokens)));
        return next();
      }

      // Bucket exhausted - Rate limit hit
      const neededTokens = cost - tokens;
      const retryAfterSec = Math.max(1, Math.ceil(neededTokens / refillRatePerSec));

      res.setHeader("X-RateLimit-Limit", capacity);
      res.setHeader("X-RateLimit-Remaining", 0);
      res.setHeader("Retry-After", retryAfterSec);

      logger.warn("ratelimit.exceeded", {
        limiter: name,
        identifier,
        retryAfterSec
      });

      return res.status(429).json({
        error: "Rate limit exceeded. Please wait before submitting again.",
        retryAfterSeconds: retryAfterSec
      });
    } catch (err) {
      // Fail-open on unexpected Redis error so legitimate traffic is never blocked
      logger.warn("ratelimit.fail_open", { error: err.message, limiter: name });
      return next();
    }
  };
}

module.exports = {
  createTokenBucketLimiter
};

