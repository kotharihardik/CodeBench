const express = require("express");
const mongoose = require("mongoose");
const requireAuth = require("../shared/middleware/requireAuth");
const requireRole = require("../shared/middleware/requireRole");
const Submission = require("../models/Submission");
const { compareSubmissions, auditSubmissionsBatch } = require("./plagiarismEngine");

const router = express.Router();

// POST /api/plagiarism/compare - Compare two code strings directly
router.post("/compare", requireAuth, async (req, res) => {
  try {
    const { codeA, codeB, threshold } = req.body;

    if (!codeA || !codeB) {
      return res.status(400).json({ error: "Both codeA and codeB are required for comparison" });
    }

    const comparison = compareSubmissions(codeA, codeB, {
      threshold: Number(threshold || 0.75)
    });

    return res.json(comparison);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// POST /api/plagiarism/audit-question/:questionId - Admin audit of all submissions for a question
router.post("/audit-question/:questionId", requireAuth, requireRole("admin"), async (req, res) => {
  try {
    const { questionId } = req.params;
    if (!mongoose.Types.ObjectId.isValid(String(questionId || ""))) {
      return res.status(400).json({ error: "Invalid questionId" });
    }

    const threshold = Number(req.body?.threshold || 0.75);

    const submissions = await Submission.find({
      questionId,
      status: "COMPLETED",
      code: { $exists: true, $ne: "" }
    })
      .select({ _id: 1, userId: 1, code: 1, language: 1 })
      .populate("userId", "name email")
      .lean();

    if (submissions.length < 2) {
      return res.json({
        totalSubmissionsAnalyzed: submissions.length,
        flaggedPairsCount: 0,
        message: "Not enough submissions to run collusion audit",
        flaggedPairs: []
      });
    }

    const formatted = submissions.map((sub) => ({
      id: String(sub._id),
      userId: String(sub.userId?._id || sub.userId),
      username: sub.userId?.name || "Student",
      code: sub.code
    }));

    const auditResult = auditSubmissionsBatch(formatted, { threshold });
    return res.json({
      questionId,
      ...auditResult
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

module.exports = router;

