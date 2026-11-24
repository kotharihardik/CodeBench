const express = require("express");
const mongoose = require("mongoose");
const Question = require("../models/Question");
const Submission = require("../models/Submission");
const TestSession = require("../models/TestSession");
const requireAuth = require("../shared/middleware/requireAuth");
const { workerConcurrency } = require("../shared/config");
const { getWorkerTelemetrySnapshot } = require("../shared/workerTelemetry");
const { invalidateSubmissionListCacheForUser } = require("../shared/summaryCache");
const logger = require("../shared/logger");
const { getSubmissionQueue } = require("../evaluation/queue");
const { createTokenBucketLimiter } = require("../shared/middleware/rateLimiter");
const { createIdempotencyGuard } = require("../shared/middleware/idempotencyGuard");
const {
  isQueueConnectionError,
  getSubmissionListWithCache
} = require("./submissionReadService");
const { handleSseConnection } = require("../evaluation/streamService");

const router = express.Router();

const submissionRateLimiter = createTokenBucketLimiter({
  name: "submissions",
  capacity: 10,
  refillRatePerSec: 1
});

const submissionIdempotencyGuard = createIdempotencyGuard({
  lockTtlSec: 60,
  resultTtlSec: 86400
});

function estimateQueueSeconds(waitingCount, avgEvaluationMs) {
  const safeWaiting = Math.max(0, Number(waitingCount || 0));
  const safeAvgEvalMs = Math.max(1000, Number(avgEvaluationMs || 1000));
  const safeWorkers = Math.max(1, Number(workerConcurrency || 1));
  return Math.round((safeWaiting * safeAvgEvalMs) / (safeWorkers * 1000));
}

router.post("/", requireAuth, submissionRateLimiter, submissionIdempotencyGuard, async (req, res) => {
  try {
    const { questionId, language, code, answer } = req.body;
    const idempotencyKey = String(req.headers["x-idempotency-key"] || "").trim();

    if (!questionId) {
      return res.status(400).json({ error: "questionId is required" });
    }

    if (idempotencyKey) {
      const existing = await Submission.findOne({
        userId: req.user.userId,
        idempotencyKey
      }).lean();

      if (existing) {
        return res.status(200).json({
          submissionId: existing._id,
          status: existing.status,
          idempotentReplay: true
        });
      }
    }

    const question = await Question.findOne({ _id: questionId, isActive: true });
    if (!question) {
      return res.status(404).json({ error: "Question not found" });
    }

    const activeTimedTest = await TestSession.findOne({
      userId: req.user.userId,
      status: "ACTIVE",
      questionIds: question._id,
      endsAt: { $gt: new Date() }
    })
      .select({ _id: 1 })
      .lean();

    if (activeTimedTest) {
      return res.status(409).json({
        error: "This question belongs to an active timed test. Submit using timed test flow."
      });
    }

    if (question.type === "code" && (!language || !code)) {
      return res.status(400).json({ error: "language and code are required for code questions" });
    }

    if ((question.type === "mcq" || question.type === "sql") && !answer) {
      return res.status(400).json({ error: "answer is required for this question type" });
    }

    const submission = await Submission.create({
      userId: req.user.userId,
      questionId: question._id,
      type: question.type,
      topic: question.topic,
      difficulty: question.difficulty,
      idempotencyKey: idempotencyKey || undefined,
      language: language || undefined,
      code: code || undefined,
      answer: answer || undefined,
      status: "QUEUED"
    });

    await invalidateSubmissionListCacheForUser(req.user.userId);

    const submissionQueue = getSubmissionQueue();

    await submissionQueue.add(
      "evaluate-submission",
      {
        submissionId: String(submission._id)
      },
      {
        jobId: String(submission._id)
      }
    );

    logger.info("submission.queued", {
      requestId: req.requestId || null,
      submissionId: String(submission._id),
      userId: String(req.user.userId),
      questionId: String(question._id)
    });

    if (typeof res.saveIdempotencyResult === "function") {
      await res.saveIdempotencyResult({
        submissionId: submission._id,
        status: submission.status
      });
    }

    return res.status(202).json({
      submissionId: submission._id,
      status: submission.status
    });
  } catch (err) {
    // Lost the race on the unique {userId, idempotencyKey} index (e.g. Redis guard unavailable):
    // the first writer wins, so replay its submission instead of failing.
    const idempotencyKey = String(req.headers["x-idempotency-key"] || "").trim();
    if (err && err.code === 11000 && idempotencyKey) {
      const existing = await Submission.findOne({ userId: req.user.userId, idempotencyKey }).lean();
      if (existing) {
        return res.status(200).json({
          submissionId: existing._id,
          status: existing.status,
          idempotentReplay: true
        });
      }
    }

    if (isQueueConnectionError(err)) {
      return res.status(503).json({
        error: "Queue service unavailable. Start Redis and worker then retry submission."
      });
    }

    return res.status(500).json({ error: err.message });
  }
});

router.get("/", requireAuth, async (req, res) => {
  try {
    const freshQuery = String(req.query.fresh || req.query.noCache || "").trim().toLowerCase();
    const bypassCache = ["1", "true", "yes", "y"].includes(freshQuery);

    const read = await getSubmissionListWithCache({
      role: req.user.role,
      userId: req.user.userId,
      page: req.query.page,
      limit: req.query.limit,
      type: req.query.type,
      bypassCache
    });

    res.setHeader("Cache-Control", "no-store");
    res.setHeader("x-read-cache", read.cacheHit ? "hit" : `miss:${read.source}`);
    return res.json(read.payload);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

router.get("/queue/status", requireAuth, async (req, res) => {
  try {
    const [queueCounts, workerTelemetry] = await Promise.all([
      getSubmissionQueue().getJobCounts("waiting", "active", "completed", "failed", "delayed"),
      getWorkerTelemetrySnapshot()
    ]);

    const expectedQueueWaitSec = estimateQueueSeconds(
      Number(queueCounts.waiting || 0),
      Number(workerTelemetry?.evaluation?.avgMs || 1000)
    );

    return res.json({
      queue: {
        ...queueCounts,
        expectedQueueWaitSec
      },
      worker: {
        ready: Boolean(workerTelemetry.ready),
        evaluation: workerTelemetry.evaluation,
        queueWait: workerTelemetry.queueWait
      }
    });
  } catch (err) {
    if (isQueueConnectionError(err)) {
      return res.status(503).json({
        error: "Queue service unavailable. Start Redis and worker to fetch queue status."
      });
    }

    return res.status(500).json({ error: err.message });
  }
});

router.get("/:id/stream", requireAuth, async (req, res) => {
  try {
    if (!mongoose.Types.ObjectId.isValid(String(req.params.id || ""))) {
      return res.status(400).json({ error: "Invalid submission id" });
    }

    const submission = await Submission.findById(req.params.id)
      .select({ userId: 1, status: 1, score: 1, passed: 1 })
      .lean();
    if (!submission) {
      return res.status(404).json({ error: "Submission not found" });
    }

    const isOwner = String(submission.userId) === String(req.user.userId);
    if (req.user.role !== "admin" && !isOwner) {
      return res.status(403).json({ error: "Access denied" });
    }

    const refetchState = () =>
      Submission.findById(req.params.id).select({ status: 1, score: 1, passed: 1 }).lean();

    return handleSseConnection(req, res, req.params.id, submission, refetchState);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

router.get("/:id", requireAuth, async (req, res) => {
  try {
    const submission = await Submission.findById(req.params.id).lean();

    if (!submission) {
      return res.status(404).json({ error: "Submission not found" });
    }

    const isOwner = String(submission.userId) === String(req.user.userId);
    if (req.user.role !== "admin" && !isOwner) {
      return res.status(403).json({ error: "Access denied" });
    }

    return res.json({ submission });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

module.exports = router;
