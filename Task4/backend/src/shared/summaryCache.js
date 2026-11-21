const {
  summaryCacheTtlMs,
  summaryCacheMaxEntries,
  submissionsListCacheTtlMs,
  questionTopicsCacheTtlMs
} = require("./config");
const MultiTierCache = require("./cache/multiTierCache");

const multiTierCache = new MultiTierCache({
  name: "summaries",
  maxL1Entries: summaryCacheMaxEntries,
  defaultL1TtlMs: summaryCacheTtlMs,
  defaultL2TtlSec: Math.ceil(summaryCacheTtlMs / 1000)
});

// Single cache store: L1 (per-process Map) + L2 (Redis) with a pub/sub invalidation bus.
// Reads check L1 then L2; invalidations evict L1 on every API/worker instance.

async function getCached(key) {
  const read = await multiTierCache.get(String(key));
  return read.value;
}

function setCached(key, value, ttlMs = summaryCacheTtlMs) {
  const safeTtl = Number(ttlMs || summaryCacheTtlMs);
  return multiTierCache.set(String(key), value, {
    l1TtlMs: safeTtl,
    l2TtlSec: Math.max(1, Math.ceil(safeTtl / 1000))
  });
}

function clearByPrefix(prefix) {
  return multiTierCache.invalidatePrefix(String(prefix || ""));
}

function buildSubmissionScope(role, userId) {
  if (String(role || "") === "admin") {
    return "admin";
  }

  return `user:${String(userId || "")}`;
}

function submissionsListCacheKey({ role, userId, page, limit, type }) {
  const scope = buildSubmissionScope(role, userId);
  return [
    "submissions:list",
    scope,
    `page:${Math.max(1, Number(page || 1))}`,
    `limit:${Math.max(1, Number(limit || 10))}`,
    `type:${String(type || "all") || "all"}`
  ].join(":");
}

function questionTopicsCacheKey({ role, includeInactive }) {
  const scope = String(role || "student") === "admin" ? "admin" : "student";
  const inactive = includeInactive ? "with-inactive" : "active-only";
  return `questions:topics:${scope}:${inactive}`;
}

function invalidateSubmissionListCacheForUser(userId) {
  return Promise.all([
    clearByPrefix("submissions:list:admin:"),
    clearByPrefix(`submissions:list:user:${String(userId || "")}:`)
  ]);
}

function invalidateSubmissionListCacheAll() {
  return clearByPrefix("submissions:list:");
}

function invalidateQuestionTopicsCache() {
  return clearByPrefix("questions:topics:");
}

function cacheTtlMs() {
  return {
    submissionsList: Number(submissionsListCacheTtlMs || 8000),
    questionTopics: Number(questionTopicsCacheTtlMs || 60000)
  };
}

function studentSummaryKey(userId) {
  return `student-summary:${String(userId || "")}`;
}

function adminOverviewKey() {
  return "admin-overview";
}

function invalidateStudentSummary(userId) {
  return multiTierCache.invalidate(studentSummaryKey(userId));
}

function invalidateAdminOverview() {
  return multiTierCache.invalidate(adminOverviewKey());
}

function getMultiTierCacheMetrics() {
  return multiTierCache.getMetrics();
}

module.exports = {
  getCached,
  setCached,
  clearByPrefix,
  cacheTtlMs,
  studentSummaryKey,
  adminOverviewKey,
  submissionsListCacheKey,
  questionTopicsCacheKey,
  invalidateStudentSummary,
  invalidateAdminOverview,
  invalidateSubmissionListCacheForUser,
  invalidateSubmissionListCacheAll,
  invalidateQuestionTopicsCache,
  getMultiTierCacheMetrics
};
