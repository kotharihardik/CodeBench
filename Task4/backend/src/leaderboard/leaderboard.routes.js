const express = require("express");
const requireAuth = require("../shared/middleware/requireAuth");
const requireRole = require("../shared/middleware/requireRole");
const {
  recordContestScore,
  getUserRank,
  getTopLeaderboard,
  resetContestLeaderboard
} = require("./leaderboardService");

const router = express.Router();

// GET /api/leaderboard/:contestId - Fetch Top K Leaderboard
router.get("/:contestId", requireAuth, async (req, res) => {
  try {
    const { contestId } = req.params;
    const limit = parseInt(req.query.limit, 10) || 20;
    const offset = parseInt(req.query.offset, 10) || 0;

    const data = await getTopLeaderboard(contestId, { limit, offset });
    return res.json(data);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// GET /api/leaderboard/:contestId/me - Fetch Current User's Live Rank
router.get("/:contestId/me", requireAuth, async (req, res) => {
  try {
    const { contestId } = req.params;
    const rankInfo = await getUserRank(contestId, req.user.userId);

    if (!rankInfo) {
      return res.status(404).json({ error: "Candidate has not participated in this contest yet" });
    }

    return res.json(rankInfo);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// GET /api/leaderboard/:contestId/rank/:userId - Fetch Specific User Rank
router.get("/:contestId/rank/:userId", requireAuth, async (req, res) => {
  try {
    const { contestId, userId } = req.params;
    const rankInfo = await getUserRank(contestId, userId);

    if (!rankInfo) {
      return res.status(404).json({ error: "Candidate rank not found" });
    }

    return res.json(rankInfo);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// POST /api/leaderboard/:contestId/score - Record or Update Candidate Score (Admin / server only)
// Scores must come from the grader, never from the candidate's own request; otherwise any
// student could POST {score: 999999} for themselves.
router.post("/:contestId/score", requireAuth, requireRole("admin"), async (req, res) => {
  try {
    const { contestId } = req.params;
    const { score, penaltySeconds, username, problemsSolved, targetUserId } = req.body;

    if (!targetUserId) {
      return res.status(400).json({ error: "targetUserId is required" });
    }

    const userId = targetUserId;

    const result = await recordContestScore(contestId, userId, {
      score,
      penaltySeconds,
      username: username || "Candidate",
      problemsSolved
    });

    return res.status(200).json(result);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// DELETE /api/leaderboard/:contestId - Reset Contest Leaderboard (Admin only)
router.delete("/:contestId", requireAuth, async (req, res) => {
  try {
    if (req.user.role !== "admin") {
      return res.status(403).json({ error: "Only admins can reset contest leaderboards" });
    }

    await resetContestLeaderboard(req.params.contestId);
    return res.json({ ok: true, message: "Leaderboard reset successfully" });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

module.exports = router;

