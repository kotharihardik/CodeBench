const app = require("./app");
const { connectDb } = require("./shared/db");
const { warmReadCaches } = require("./shared/cacheWarmup");
const { port } = require("./shared/config");
const redisConnection = require("./shared/redis");
const logger = require("./shared/logger");

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
  app.listen(port, () => {
    logger.info("api.started", { port });
  });
}

startServer().catch((err) => {
  logger.error("api.start_failed", { error: err.message });
  process.exit(1);
});
