/**
 * Universal Circuit Breaker Pattern
 * Protects downstream systems from cascading failures by failing fast
 * when failure thresholds are exceeded.
 *
 * States:
 *  - CLOSED: Normal operation. All calls pass through.
 *  - OPEN: Failure threshold exceeded. Calls fail immediately without executing action.
 *  - HALF_OPEN: Trial period after recovery timeout. Allows probe requests to test service health.
 */

class CircuitBreakerOpenError extends Error {
  constructor(message, circuitName, retryAfterSec) {
    super(message);
    this.name = "CircuitBreakerOpenError";
    this.circuitName = circuitName;
    this.retryAfterSec = retryAfterSec;
    this.isCircuitBreaker = true;
  }
}

class CircuitBreaker {
  constructor(name, options = {}) {
    this.name = name || "default";
    this.failureThreshold = Math.max(1, Number(options.failureThreshold || 3));
    this.recoveryTimeoutMs = Math.max(10, Number(options.recoveryTimeoutMs || 15000));
    this.successThreshold = Math.max(1, Number(options.successThreshold || 2));
    // Decides which errors indicate an unhealthy dependency. Errors caused by the caller's input
    // (e.g. user code timing out) should not trip the breaker for everyone else.
    this.isFailure = typeof options.isFailure === "function" ? options.isFailure : () => true;

    this.state = "CLOSED";
    this.consecutiveFailures = 0;
    this.consecutiveSuccesses = 0;
    this.lastFailureTime = null;
    this.totalCalls = 0;
    this.totalFailures = 0;
    this.totalSuccesses = 0;
  }

  isOpen() {
    if (this.state === "OPEN") {
      const elapsed = Date.now() - (this.lastFailureTime || 0);
      if (elapsed > this.recoveryTimeoutMs) {
        this.state = "HALF_OPEN";
        this.consecutiveSuccesses = 0;
        return false;
      }
      return true;
    }
    return false;
  }

  async execute(action, fallback = null) {
    this.totalCalls += 1;

    if (this.isOpen()) {
      const remainingSec = Math.max(
        1,
        Math.ceil((this.recoveryTimeoutMs - (Date.now() - (this.lastFailureTime || 0))) / 1000)
      );

      if (typeof fallback === "function") {
        return fallback(new CircuitBreakerOpenError(
          `Circuit [${this.name}] is OPEN. Action blocked to prevent cascading failure.`,
          this.name,
          remainingSec
        ));
      }

      throw new CircuitBreakerOpenError(
        `Circuit [${this.name}] is OPEN. Service temporarily degraded. Retry after ${remainingSec}s.`,
        this.name,
        remainingSec
      );
    }

    try {
      const result = await action();
      this.recordSuccess();
      return result;
    } catch (err) {
      if (this.isFailure(err)) {
        this.recordFailure(err);
      } else {
        this.recordSuccess();
      }
      throw err;
    }
  }

  recordSuccess() {
    this.totalSuccesses += 1;

    if (this.state === "HALF_OPEN") {
      this.consecutiveSuccesses += 1;
      if (this.consecutiveSuccesses >= this.successThreshold) {
        this.state = "CLOSED";
        this.consecutiveFailures = 0;
        this.consecutiveSuccesses = 0;
      }
    } else if (this.state === "CLOSED") {
      this.consecutiveFailures = 0;
    }
  }

  recordFailure(err) {
    this.totalFailures += 1;
    this.consecutiveFailures += 1;
    this.lastFailureTime = Date.now();

    if (this.state === "HALF_OPEN" || this.consecutiveFailures >= this.failureThreshold) {
      this.state = "OPEN";
      this.consecutiveSuccesses = 0;
    }
  }

  getStatus() {
    return {
      name: this.name,
      state: this.state,
      consecutiveFailures: this.consecutiveFailures,
      consecutiveSuccesses: this.consecutiveSuccesses,
      lastFailureTime: this.lastFailureTime ? new Date(this.lastFailureTime).toISOString() : null,
      totalCalls: this.totalCalls,
      totalFailures: this.totalFailures,
      totalSuccesses: this.totalSuccesses
    };
  }

  reset() {
    this.state = "CLOSED";
    this.consecutiveFailures = 0;
    this.consecutiveSuccesses = 0;
    this.lastFailureTime = null;
  }
}

module.exports = {
  CircuitBreaker,
  CircuitBreakerOpenError
};
