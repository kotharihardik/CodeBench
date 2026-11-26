const app = require("./app");
const { connectDb } = require("./shared/db");
const { warmReadCaches } = require("./shared/cacheWarmup");
const { port, shutdownTimeoutMs } = require("./shared/config");
const redisConnection = require("./shared/redis");
const mongoose = require("mongoose");
const { getSubmissionQueue, getDeadLetterQueue, closeQueues } = require("./evaluation/queue");
const logger = require("./shared/logger");

let shutdownPromise;

// The shared Redis client is lazyConnect. Rate limiting, idempotency locks and the L2 cache all
// skip Redis unless the client is "ready", so connect eagerly instead of waiting for the first
// queue/health call. Failure is non-fatal: those features fail open and the API still serves.
async function connectRedis() {
  try {
    if (redisConnection.status === "wait") {
      await redisConnection.connect();
    }
    logger.info("redis.connected", { status: redisConnection.status });
  } catch (err) {
    logger.warn("redis.connect_failed", { error: err.message });
  }
}

async function startServer() {
  await connectDb();
  await connectRedis();
  await warmReadCaches();
  return app.listen(port, () => {
    logger.info("api.started", { port });
  });
}

function closeHttpServer(httpServer) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      logger.warn("api.shutdown.force_close", { timeoutMs: shutdownTimeoutMs });
      httpServer.closeAllConnections();
    }, Math.max(0, shutdownTimeoutMs));

    httpServer.close((err) => {
      clearTimeout(timeout);
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

async function runShutdownStep(step, operation) {
  try {
    await operation();
  } catch (err) {
    process.exitCode = 1;
    logger.error("api.shutdown_step_failed", { step, error: err.message });
  }
}

function shutdownServer(signal, httpServer) {
  if (shutdownPromise) {
    return shutdownPromise;
  }

  app.locals.shuttingDown = true;
  logger.info("api.shutdown_started", { signal });

  shutdownPromise = (async () => {
    if (httpServer?.listening) {
      await runShutdownStep("http", () => closeHttpServer(httpServer));
    }

    await runShutdownStep("queues", closeQueues);
    await runShutdownStep("mongo", () => mongoose.disconnect());
    await runShutdownStep("redis", async () => {
      if (redisConnection.status === "ready") {
        await redisConnection.quit();
      } else {
        redisConnection.disconnect();
      }
    });

    logger.info("api.shutdown_completed", { signal });
  })();

  return shutdownPromise;
}

async function main() {
  const httpServer = await startServer();
  const handleSignal = (signal) => {
    void shutdownServer(signal, httpServer);
  };

  process.once("SIGINT", () => handleSignal("SIGINT"));
  process.once("SIGTERM", () => handleSignal("SIGTERM"));
}

if (require.main === module) {
  main().catch((err) => {
    logger.error("api.start_failed", { error: err.message });
    process.exit(1);
  });
}

module.exports = {
  startServer,
  shutdownServer
};
