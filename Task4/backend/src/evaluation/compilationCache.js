/**
 * Content-Addressable Storage (CAS) Compilation Cache
 * Avoids redundant recompilation of identical or boilerplate code submissions
 * using SHA-256 cryptographic content hashes.
 *
 * Concepts:
 * - Deterministic SHA-256 hashing of normalized source code + compiler profile.
 * - In-Memory (L1) + Redis (L2) binary artifact and compilation result cache.
 * - Compute Conservation: Slashes compiler CPU consumption during contest re-submissions.
 */

const crypto = require("crypto");
const redisConnection = require("../shared/redis");
const logger = require("../shared/logger");

class CompilationCache {
  constructor(options = {}) {
    this.memoryCache = new Map();
    this.maxMemoryEntries = Math.max(50, Number(options.maxEntries || 500));
    this.ttlSec = Math.max(60, Number(options.ttlSec || 3600)); // 1 hour

    this.metrics = {
      hits: 0,
      misses: 0,
      savedCpuDurationMs: 0
    };
  }

  computeHash(language, code) {
    const normalized = String(code || "").trim().replace(/\r\n/g, "\n");
    return crypto
      .createHash("sha256")
      .update(`${String(language).toLowerCase()}::${normalized}`)
      .digest("hex");
  }

  async get(language, code) {
    const hash = this.computeHash(language, code);
    const key = `cas:compile:${hash}`;

    // 1. Check in-memory L1
    const memoryHit = this.memoryCache.get(hash);
    if (memoryHit && memoryHit.expiresAt > Date.now()) {
      this.metrics.hits += 1;
      this.metrics.savedCpuDurationMs += memoryHit.compileDurationMs || 0;
      return { hit: true, tier: "L1", result: memoryHit.result };
    }

    // 2. Check Redis L2
    try {
      if (redisConnection.status === "ready" || redisConnection.status === "connect") {
        const raw = await redisConnection.get(key);
        if (raw) {
          const parsed = JSON.parse(raw);
          this.metrics.hits += 1;
          this.metrics.savedCpuDurationMs += parsed.compileDurationMs || 0;

          // Backfill L1
          this.setMemory(hash, parsed.result, parsed.compileDurationMs);
          return { hit: true, tier: "L2", result: parsed.result };
        }
      }
    } catch (err) {
      logger.warn("cas.cache_read_error", { error: err.message });
    }

    this.metrics.misses += 1;
    return { hit: false, hash };
  }

  setMemory(hash, result, compileDurationMs) {
    if (this.memoryCache.size >= this.maxMemoryEntries) {
      const firstKey = this.memoryCache.keys().next().value;
      this.memoryCache.delete(firstKey);
    }

    this.memoryCache.set(hash, {
      result,
      compileDurationMs,
      expiresAt: Date.now() + this.ttlSec * 1000
    });
  }

  async set(language, code, result, compileDurationMs = 0) {
    const hash = this.computeHash(language, code);
    const key = `cas:compile:${hash}`;

    // Store in L1
    this.setMemory(hash, result, compileDurationMs);

    // Store in Redis L2
    try {
      if (redisConnection.status === "ready" || redisConnection.status === "connect") {
        await redisConnection.set(
          key,
          JSON.stringify({ result, compileDurationMs }),
          "EX",
          this.ttlSec
        );
      }
    } catch (err) {
      logger.warn("cas.cache_write_error", { error: err.message });
    }
  }

  getMetrics() {
    const total = this.metrics.hits + this.metrics.misses;
    const hitRate = total > 0 ? (this.metrics.hits / total) * 100 : 0;

    return {
      casHits: this.metrics.hits,
      casMisses: this.metrics.misses,
      casHitRatePct: Number(hitRate.toFixed(1)),
      savedCpuDurationMs: this.metrics.savedCpuDurationMs,
      cachedEntriesCount: this.memoryCache.size
    };
  }

  clear() {
    this.memoryCache.clear();
    this.metrics.hits = 0;
    this.metrics.misses = 0;
    this.metrics.savedCpuDurationMs = 0;
  }
}

const globalCompilationCache = new CompilationCache();

module.exports = {
  CompilationCache,
  compilationCache: globalCompilationCache
};

