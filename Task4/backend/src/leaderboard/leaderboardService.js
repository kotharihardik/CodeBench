/**
 * Distributed Real-Time Contest Leaderboard Engine
 * Backed by Redis Sorted Sets (ZSET) with sub-millisecond dynamic ranking.
 *
 * Mathematical Properties:
 * - O(log N) updates via ZADD
 * - O(log N) individual rank lookups via ZREVRANK
 * - O(log N + M) Top-K range queries via ZREVRANGE
 * - Composite tie-breaking formula (exact integer packing, no floating-point drift):
 *     compositeScore = score * 10^7 + (10^7 - 1 - penaltySeconds)
 *   Higher score always ranks higher; equal scores are broken by lower penalty (faster finish).
 *   Max exact double is 2^53 (~9e15), so scores up to ~9e8 are represented exactly.
 */

const redisConnection = require("../shared/redis");
const logger = require("../shared/logger");

// In-memory fallback if Redis is unavailable
const memoryLeaderboard = new Map();

const PENALTY_SLOTS = 10000000; // 10^7 seconds (~115 days) of penalty resolution

function computeCompositeScore(score, penaltySeconds = 0) {
  const safeScore = Math.max(0, Math.round(Number(score || 0)));
  const safePenalty = Math.min(PENALTY_SLOTS - 1, Math.max(0, Math.round(Number(penaltySeconds || 0))));
  return safeScore * PENALTY_SLOTS + (PENALTY_SLOTS - 1 - safePenalty);
}

function parseScoreAndPenalty(compositeScore) {
  const value = Math.max(0, Math.round(Number(compositeScore || 0)));
  const score = Math.floor(value / PENALTY_SLOTS);
  const penaltySeconds = PENALTY_SLOTS - 1 - (value % PENALTY_SLOTS);
  return { score, penaltySeconds };
}

function getLeaderboardKey(contestId) {
  return `leaderboard:contest:${String(contestId || "global")}`;
}

function getMetaKey(contestId) {
  return `leaderboard:contest:${String(contestId || "global")}:meta`;
}

async function recordContestScore(contestId, userId, data = {}) {
  const uid = String(userId);
  const compositeScore = computeCompositeScore(data.score, data.penaltySeconds);
  const { score, penaltySeconds } = parseScoreAndPenalty(compositeScore);
  const metadata = {
    userId: uid,
    username: data.username || "Candidate",
    score,
    penaltySeconds,
    problemsSolved: Number(data.problemsSolved || 0),
    updatedAt: new Date().toISOString()
  };

  const zKey = getLeaderboardKey(contestId);
  const mKey = getMetaKey(contestId);

  try {
    if (redisConnection.status === "ready" || redisConnection.status === "connect") {
      await redisConnection.zadd(zKey, compositeScore, uid);
      await redisConnection.hset(mKey, uid, JSON.stringify(metadata));
      return { ok: true, compositeScore, rank: await getUserRank(contestId, uid) };
    }
  } catch (err) {
    logger.warn("leaderboard.redis_error", { error: err.message });
  }

  // In-memory fallback
  if (!memoryLeaderboard.has(zKey)) {
    memoryLeaderboard.set(zKey, new Map());
  }
  memoryLeaderboard.get(zKey).set(uid, { compositeScore, metadata });
  return { ok: true, compositeScore, rank: await getUserRank(contestId, uid) };
}

async function getUserRank(contestId, userId) {
  const uid = String(userId);
  const zKey = getLeaderboardKey(contestId);
  const mKey = getMetaKey(contestId);

  try {
    if (redisConnection.status === "ready" || redisConnection.status === "connect") {
      const rankZeroBased = await redisConnection.zrevrank(zKey, uid);
      if (rankZeroBased === null) {
        return null;
      }

      const compositeScore = await redisConnection.zscore(zKey, uid);
      const rawMeta = await redisConnection.hget(mKey, uid);
      const meta = rawMeta ? JSON.parse(rawMeta) : {};
      const { score, penaltySeconds } = parseScoreAndPenalty(Number(compositeScore || 0));

      return {
        rank: rankZeroBased + 1,
        userId: uid,
        score,
        penaltySeconds,
        compositeScore: Number(compositeScore),
        ...meta
      };
    }
  } catch (err) {
    logger.warn("leaderboard.rank_redis_error", { error: err.message });
  }

  // In-memory fallback
  const contestMap = memoryLeaderboard.get(zKey);
  if (!contestMap || !contestMap.has(uid)) {
    return null;
  }

  const sorted = Array.from(contestMap.entries()).sort(
    (a, b) => b[1].compositeScore - a[1].compositeScore
  );
  const idx = sorted.findIndex(([id]) => id === uid);
  const entry = contestMap.get(uid);

  return {
    rank: idx + 1,
    userId: uid,
    compositeScore: entry.compositeScore,
    ...entry.metadata
  };
}

async function getTopLeaderboard(contestId, options = {}) {
  const limit = Math.min(100, Math.max(1, Number(options.limit || 20)));
  const offset = Math.max(0, Number(options.offset || 0));
  const zKey = getLeaderboardKey(contestId);
  const mKey = getMetaKey(contestId);

  try {
    if (redisConnection.status === "ready" || redisConnection.status === "connect") {
      const rawEntries = await redisConnection.zrevrange(
        zKey,
        offset,
        offset + limit - 1,
        "WITHSCORES"
      );

      if (!rawEntries || rawEntries.length === 0) {
        return { totalParticipants: 0, leaderboard: [] };
      }

      const totalParticipants = await redisConnection.zcard(zKey);
      const userIds = [];
      const scoreMap = new Map();

      for (let i = 0; i < rawEntries.length; i += 2) {
        const id = rawEntries[i];
        const compScore = Number(rawEntries[i + 1]);
        userIds.push(id);
        scoreMap.set(id, compScore);
      }

      const rawMetas = await redisConnection.hmget(mKey, ...userIds);
      const leaderboard = userIds.map((id, index) => {
        const compScore = scoreMap.get(id);
        const { score, penaltySeconds } = parseScoreAndPenalty(compScore);
        const meta = rawMetas[index] ? JSON.parse(rawMetas[index]) : {};

        return {
          rank: offset + index + 1,
          userId: id,
          score,
          penaltySeconds,
          compositeScore: compScore,
          username: meta.username || "Candidate",
          problemsSolved: meta.problemsSolved || 0
        };
      });

      return {
        totalParticipants,
        limit,
        offset,
        leaderboard
      };
    }
  } catch (err) {
    logger.warn("leaderboard.top_redis_error", { error: err.message });
  }

  // In-memory fallback
  const contestMap = memoryLeaderboard.get(zKey) || new Map();
  const sorted = Array.from(contestMap.entries()).sort(
    (a, b) => b[1].compositeScore - a[1].compositeScore
  );

  const page = sorted.slice(offset, offset + limit);
  const leaderboard = page.map(([id, entry], index) => ({
    rank: offset + index + 1,
    userId: id,
    compositeScore: entry.compositeScore,
    ...entry.metadata
  }));

  return {
    totalParticipants: sorted.length,
    limit,
    offset,
    leaderboard
  };
}

async function resetContestLeaderboard(contestId) {
  const zKey = getLeaderboardKey(contestId);
  const mKey = getMetaKey(contestId);
  memoryLeaderboard.delete(zKey);

  try {
    if (redisConnection.status === "ready" || redisConnection.status === "connect") {
      await redisConnection.del(zKey, mKey);
    }
  } catch (_) {}

  return { ok: true };
}

module.exports = {
  recordContestScore,
  getUserRank,
  getTopLeaderboard,
  resetContestLeaderboard,
  computeCompositeScore,
  parseScoreAndPenalty
};

