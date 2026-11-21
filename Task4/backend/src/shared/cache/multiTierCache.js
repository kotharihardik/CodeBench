/**
 * Multi-Tier Cache Layer (L1 In-Memory + L2 Distributed Redis)
 *
 * Concepts:
 * - L1: Ultra-fast in-memory Map (<1ms) for local process hits.
 * - L2: Shared Redis Cluster (<5ms) across scaled API/worker instances.
 * - Cache-Aside: Reads populate L1/L2 on miss. Writes invalidate both tiers.
 * - Bus Invalidation: On write/invalidation, publishes event to Redis Pub/Sub
 *   so all distributed instances evict their local L1 cache.
 */

const redisConnection = require("../redis");
const logger = require("../logger");

class MultiTierCache {
  constructor(options = {}) {
    this.name = options.name || "default";
    this.l1 = new Map();
    this.maxL1Entries = Math.max(100, Number(options.maxL1Entries || 2000));
    this.defaultL1TtlMs = Math.max(1000, Number(options.defaultL1TtlMs || 5000));
    this.defaultL2TtlSec = Math.max(5, Number(options.defaultL2TtlSec || 30));
    this.redisPrefix = `cache:${this.name}:`;
    this.pubsubChannel = `cache:invalidation:${this.name}`;

    this.metrics = {
      l1Hits: 0,
      l2Hits: 0,
      misses: 0,
      invalidations: 0
    };

    this.setupPubSubListener();
  }

  setupPubSubListener() {
    try {
      if (process.env.NODE_ENV === "test") {
        return;
      }

      if (!redisConnection || typeof redisConnection.duplicate !== "function") {
        return;
      }

      this.subClient = redisConnection.duplicate();
      this.subClient.on("error", () => {});

      this.subClient.subscribe(this.pubsubChannel, (err) => {
        if (!err) {
          logger.info("cache.pubsub.subscribed", { channel: this.pubsubChannel });
        }
      });

      this.subClient.on("message", (channel, message) => {
        if (channel === this.pubsubChannel && message) {
          try {
            const payload = JSON.parse(message);
            if (payload.action === "invalidate" && payload.key) {
              this.l1.delete(payload.key);
            } else if (payload.action === "invalidatePrefix" && payload.prefix) {
              this.evictL1Prefix(payload.prefix);
            } else if (payload.action === "clear") {
              this.l1.clear();
            }
          } catch (_) {
            this.l1.delete(message);
          }
        }
      });
    } catch (err) {
      logger.warn("cache.pubsub.setup_failed", { error: err.message });
    }
  }

  pruneExpiredL1() {
    const now = Date.now();
    for (const [key, item] of this.l1.entries()) {
      if (item && item.expiresAt <= now) {
        this.l1.delete(key);
      }
    }
  }

  trimL1Capacity() {
    if (this.l1.size <= this.maxL1Entries) {
      return;
    }
    const toRemove = this.l1.size - this.maxL1Entries;
    let count = 0;
    for (const key of this.l1.keys()) {
      this.l1.delete(key);
      count += 1;
      if (count >= toRemove) {
        break;
      }
    }
  }

  async get(key) {
    const safeKey = String(key);
    const now = Date.now();

    // 1. Check L1 In-Memory Cache
    const l1Item = this.l1.get(safeKey);
    if (l1Item && l1Item.expiresAt > now) {
      this.metrics.l1Hits += 1;
      return { value: l1Item.value, tier: "L1" };
    }
    if (l1Item) {
      this.l1.delete(safeKey);
    }

    // 2. Check L2 Redis Cache
    try {
      if (redisConnection.status === "ready" || redisConnection.status === "connect") {
        const rawL2 = await redisConnection.get(`${this.redisPrefix}${safeKey}`);
        if (rawL2 !== null) {
          const envelope = JSON.parse(rawL2);
          const parsed = envelope && "value" in envelope ? envelope.value : envelope;
          // Backfill L1 only until the entry's original expiry, so L1 never outlives L2's TTL.
          const expiresAt = Math.min(
            Number(envelope?.expiresAt || now + this.defaultL1TtlMs),
            now + this.defaultL1TtlMs
          );
          this.metrics.l2Hits += 1;

          if (expiresAt > now) {
            this.l1.set(safeKey, { value: parsed, expiresAt });
            this.trimL1Capacity();
          }

          return { value: parsed, tier: "L2" };
        }
      }
    } catch (err) {
      logger.warn("cache.l2_read_failed", { key: safeKey, error: err.message });
    }

    this.metrics.misses += 1;
    return { value: null, tier: "MISS" };
  }

  async set(key, value, options = {}) {
    const safeKey = String(key);
    const now = Date.now();
    const l1TtlMs = Math.max(500, Number(options.l1TtlMs || this.defaultL1TtlMs));
    const l2TtlSec = Math.max(1, Number(options.l2TtlSec || this.defaultL2TtlSec));

    // Store in L1
    this.pruneExpiredL1();
    this.l1.set(safeKey, {
      value,
      expiresAt: now + l1TtlMs
    });
    this.trimL1Capacity();

    // Store in L2 Redis
    try {
      if (redisConnection.status === "ready" || redisConnection.status === "connect") {
        await redisConnection.set(
          `${this.redisPrefix}${safeKey}`,
          JSON.stringify({ value, expiresAt: now + l2TtlSec * 1000 }),
          "EX",
          l2TtlSec
        );
      }
    } catch (err) {
      logger.warn("cache.l2_write_failed", { key: safeKey, error: err.message });
    }
  }

  async invalidate(key) {
    const safeKey = String(key);
    this.metrics.invalidations += 1;

    // 1. Evict from local L1
    this.l1.delete(safeKey);

    // 2. Evict from L2 Redis
    try {
      if (redisConnection.status === "ready" || redisConnection.status === "connect") {
        await redisConnection.del(`${this.redisPrefix}${safeKey}`);
        // 3. Publish invalidation event across nodes
        await redisConnection.publish(
          this.pubsubChannel,
          JSON.stringify({ action: "invalidate", key: safeKey })
        );
      }
    } catch (err) {
      logger.warn("cache.invalidate_failed", { key: safeKey, error: err.message });
    }
  }

  evictL1Prefix(prefix) {
    for (const key of this.l1.keys()) {
      if (key.startsWith(prefix)) {
        this.l1.delete(key);
      }
    }
  }

  // Deletes matching L2 keys with SCAN (non-blocking, unlike KEYS which is O(N) and blocks Redis).
  async deleteL2ByPattern(pattern) {
    let cursor = "0";
    do {
      const [nextCursor, keys] = await redisConnection.scan(cursor, "MATCH", pattern, "COUNT", 200);
      cursor = nextCursor;
      if (keys.length > 0) {
        await redisConnection.del(...keys);
      }
    } while (cursor !== "0");
  }

  // Prefix invalidation works across processes: every instance evicts its own L1 via pub/sub,
  // so a worker finishing a job can invalidate list caches held in API instances.
  async invalidatePrefix(prefix) {
    const safePrefix = String(prefix || "");
    this.metrics.invalidations += 1;
    this.evictL1Prefix(safePrefix);

    try {
      if (redisConnection.status === "ready" || redisConnection.status === "connect") {
        await this.deleteL2ByPattern(`${this.redisPrefix}${safePrefix}*`);
        await redisConnection.publish(
          this.pubsubChannel,
          JSON.stringify({ action: "invalidatePrefix", prefix: safePrefix })
        );
      }
    } catch (err) {
      logger.warn("cache.invalidate_prefix_failed", { prefix: safePrefix, error: err.message });
    }
  }

  async clear() {
    this.l1.clear();
    try {
      if (redisConnection.status === "ready" || redisConnection.status === "connect") {
        await this.deleteL2ByPattern(`${this.redisPrefix}*`);
        await redisConnection.publish(this.pubsubChannel, JSON.stringify({ action: "clear" }));
      }
    } catch (err) {
      logger.warn("cache.clear_failed", { error: err.message });
    }
  }

  getMetrics() {
    const totalRequests = this.metrics.l1Hits + this.metrics.l2Hits + this.metrics.misses;
    const hitRate = totalRequests > 0 ? (this.metrics.l1Hits + this.metrics.l2Hits) / totalRequests : 0;

    return {
      name: this.name,
      l1Size: this.l1.size,
      maxL1Entries: this.maxL1Entries,
      l1Hits: this.metrics.l1Hits,
      l2Hits: this.metrics.l2Hits,
      misses: this.metrics.misses,
      hitRate: Number((hitRate * 100).toFixed(2)),
      invalidations: this.metrics.invalidations
    };
  }
}

module.exports = MultiTierCache;
