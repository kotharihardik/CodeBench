const { CircuitBreaker, CircuitBreakerOpenError } = require("../src/shared/circuitBreaker");
const MultiTierCache = require("../src/shared/cache/multiTierCache");

describe("Distributed System Patterns Unit Tests", () => {
  describe("Circuit Breaker Pattern", () => {
    test("starts in CLOSED state and passes normal executions", async () => {
      const breaker = new CircuitBreaker("test-service", { failureThreshold: 2 });
      expect(breaker.state).toBe("CLOSED");

      const result = await breaker.execute(async () => "success-value");
      expect(result).toBe("success-value");
      expect(breaker.state).toBe("CLOSED");
      expect(breaker.consecutiveFailures).toBe(0);
    });

    test("trips to OPEN state when failure threshold is reached", async () => {
      const breaker = new CircuitBreaker("flaky-service", {
        failureThreshold: 2,
        recoveryTimeoutMs: 500
      });

      // Failure 1
      await expect(
        breaker.execute(async () => {
          throw new Error("Downstream down");
        })
      ).rejects.toThrow("Downstream down");
      expect(breaker.state).toBe("CLOSED");
      expect(breaker.consecutiveFailures).toBe(1);

      // Failure 2 -> Threshold reached -> Trips to OPEN
      await expect(
        breaker.execute(async () => {
          throw new Error("Downstream still down");
        })
      ).rejects.toThrow("Downstream still down");
      expect(breaker.state).toBe("OPEN");

      // Next call fails immediately with CircuitBreakerOpenError (Fast fail)
      await expect(breaker.execute(async () => "never called")).rejects.toThrow(
        /Circuit \[flaky-service\] is OPEN/
      );
    });

    test("transitions to HALF_OPEN after recovery timeout and resets to CLOSED upon success", async () => {
      const breaker = new CircuitBreaker("recovering-service", {
        failureThreshold: 1,
        recoveryTimeoutMs: 50,
        successThreshold: 1
      });

      // Trip to OPEN
      await expect(
        breaker.execute(async () => {
          throw new Error("Initial failure");
        })
      ).rejects.toThrow();
      expect(breaker.state).toBe("OPEN");

      // Wait for recovery timeout
      await new Promise((resolve) => setTimeout(resolve, 60));

      // Next execution probes health in HALF_OPEN
      const probeResult = await breaker.execute(async () => "healthy again");
      expect(probeResult).toBe("healthy again");
      expect(breaker.state).toBe("CLOSED");
      expect(breaker.consecutiveFailures).toBe(0);
    });
  });

  test("does not trip on errors classified as caller faults (e.g. user code timeout)", async () => {
    const breaker = new CircuitBreaker("sandbox", {
      failureThreshold: 2,
      isFailure: (err) => !err.isUserCodeTimeout
    });

    for (let i = 0; i < 5; i += 1) {
      const timeout = new Error("Docker execution timed out");
      timeout.isUserCodeTimeout = true;
      await expect(breaker.execute(async () => { throw timeout; })).rejects.toThrow("timed out");
    }

    expect(breaker.state).toBe("CLOSED");
  });

  describe("Multi-Tier Cache (L1/L2)", () => {
    test("populates L1 and serves cache hit with low latency", async () => {
      const cache = new MultiTierCache({
        name: "test-cache",
        maxL1Entries: 50,
        defaultL1TtlMs: 500
      });

      await cache.set("user:123", { name: "Alice" });

      const read = await cache.get("user:123");
      expect(read.tier).toBe("L1");
      expect(read.value).toEqual({ name: "Alice" });

      const metrics = cache.getMetrics();
      expect(metrics.l1Hits).toBe(1);
      expect(metrics.hitRate).toBe(100);
    });

    test("evicts all keys under a prefix", async () => {
      const cache = new MultiTierCache({ name: "prefix-cache" });
      await cache.set("submissions:list:user:1:page:1", { rows: 1 });
      await cache.set("submissions:list:user:1:page:2", { rows: 2 });
      await cache.set("submissions:list:user:2:page:1", { rows: 3 });

      await cache.invalidatePrefix("submissions:list:user:1:");

      expect((await cache.get("submissions:list:user:1:page:1")).value).toBeNull();
      expect((await cache.get("submissions:list:user:1:page:2")).value).toBeNull();
      expect((await cache.get("submissions:list:user:2:page:1")).value).toEqual({ rows: 3 });
    });

    test("evicts key on invalidation", async () => {
      const cache = new MultiTierCache({ name: "inval-cache" });
      await cache.set("question:42", { title: "Two Sum" });

      await cache.invalidate("question:42");

      const read = await cache.get("question:42");
      expect(read.tier).toBe("MISS");
      expect(read.value).toBeNull();
    });
  });
});

