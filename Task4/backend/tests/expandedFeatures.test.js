const {
  recordContestScore,
  getUserRank,
  getTopLeaderboard,
  resetContestLeaderboard,
  computeCompositeScore
} = require("../src/leaderboard/leaderboardService");
const {
  publishSubmissionProgress,
  handleSseConnection
} = require("../src/evaluation/streamService");
const {
  tokenize,
  generateFingerprints,
  compareSubmissions,
  auditSubmissionsBatch
} = require("../src/plagiarism/plagiarismEngine");
const { CompilationCache } = require("../src/evaluation/compilationCache");

describe("Expanded Distributed System Features", () => {
  describe("1. Real-Time Leaderboard Engine (ZSET Dynamic Ranking)", () => {
    const contestId = "test-contest-2026";

    beforeEach(async () => {
      await resetContestLeaderboard(contestId);
    });

    test("correctly applies composite score and breaks ties by faster completion", async () => {
      // User A and User B have the same score (300), but User A completed faster (600s vs 1200s)
      const scoreA = computeCompositeScore(300, 600);
      const scoreB = computeCompositeScore(300, 1200);

      expect(scoreA).toBeGreaterThan(scoreB);

      await recordContestScore(contestId, "userA", {
        score: 300,
        penaltySeconds: 600,
        username: "Alice"
      });

      await recordContestScore(contestId, "userB", {
        score: 300,
        penaltySeconds: 1200,
        username: "Bob"
      });

      // User C scored higher (400)
      await recordContestScore(contestId, "userC", {
        score: 400,
        penaltySeconds: 1500,
        username: "Charlie"
      });

      const rankC = await getUserRank(contestId, "userC");
      const rankA = await getUserRank(contestId, "userA");
      const rankB = await getUserRank(contestId, "userB");

      expect(rankC.rank).toBe(1); // Charlie highest score
      expect(rankA.rank).toBe(2); // Alice tied score with Bob, but faster
      expect(rankB.rank).toBe(3); // Bob tied score, but slower
    });

    test("fetches Top-K leaderboard with accurate pagination and rankings", async () => {
      for (let i = 1; i <= 5; i += 1) {
        await recordContestScore(contestId, `user_${i}`, {
          score: i * 50,
          penaltySeconds: 100 * (6 - i),
          username: `Student_${i}`
        });
      }

      const topResult = await getTopLeaderboard(contestId, { limit: 3, offset: 0 });
      expect(topResult.leaderboard.length).toBe(3);
      expect(topResult.leaderboard[0].userId).toBe("user_5");
      expect(topResult.leaderboard[0].rank).toBe(1);
      expect(topResult.leaderboard[0].score).toBe(250);
    });
  });

  describe("2. Live Execution Output Streaming (SSE Service)", () => {
    test("streams intermediate progress events to connected SSE client", (done) => {
      const submissionId = "sub_sse_123";

      const mockReq = {
        on: jest.fn()
      };

      const emittedChunks = [];
      const mockRes = {
        setHeader: jest.fn(),
        flushHeaders: jest.fn(),
        write: jest.fn((chunk) => {
          emittedChunks.push(chunk);
        }),
        end: jest.fn(() => {
          expect(emittedChunks.length).toBeGreaterThanOrEqual(2);
          const firstChunk = emittedChunks[0];
          expect(firstChunk).toContain("CONNECTED");

          const finalChunk = emittedChunks[emittedChunks.length - 1];
          expect(finalChunk).toContain("COMPLETED");
          done();
        })
      };

      handleSseConnection(mockReq, mockRes, submissionId);

      // Simulate worker publishing progress
      setTimeout(() => {
        publishSubmissionProgress(submissionId, {
          event: "TEST_CASE_RESULT",
          testCaseIndex: 1,
          passed: true
        });
      }, 20);

      // Simulate worker completion
      setTimeout(() => {
        publishSubmissionProgress(submissionId, {
          event: "FINISHED",
          status: "COMPLETED",
          score: 100
        });
      }, 50);
    });
  });

  describe("3. AST Structural Plagiarism Engine (Winnowing Algorithm)", () => {
    test("detects high similarity between obfuscated / renamed code submissions", () => {
      const codeOriginal = `
        function findMax(numbers) {
          let maxVal = numbers[0];
          for (let i = 1; i < numbers.length; i++) {
            if (numbers[i] > maxVal) {
              maxVal = numbers[i];
            }
          }
          return maxVal;
        }
      `;

      // Code with renamed variables, altered comments, and different spacing
      const codeObfuscated = `
        // Copied solution with variable renaming
        function findMax(arr) {
          let highest = arr[0];
          for (let idx = 1; idx < arr.length; idx++) {
            /* Check if current is larger */
            if (arr[idx] > highest) {
              highest = arr[idx];
            }
          }
          return highest;
        }
      `;

      const result = compareSubmissions(codeOriginal, codeObfuscated, { threshold: 0.75 });
      expect(result.similarityPct).toBeGreaterThanOrEqual(85);
      expect(result.isSuspicious).toBe(true);
    });

    test("reports low similarity for distinctly different algorithmic logic", () => {
      const codeA = `
        function isEven(n) {
          return n % 2 === 0;
        }
      `;

      const codeB = `
        function binarySearch(arr, target) {
          let left = 0;
          let right = arr.length - 1;
          while (left <= right) {
            let mid = Math.floor((left + right) / 2);
            if (arr[mid] === target) return mid;
            if (arr[mid] < target) left = mid + 1;
            else right = mid - 1;
          }
          return -1;
        }
      `;

      const result = compareSubmissions(codeA, codeB, { threshold: 0.75 });
      expect(result.similarityPct).toBeLessThan(30);
      expect(result.isSuspicious).toBe(false);
    });

    test("audits submission batch and flags collusion pairs", () => {
      const batch = [
        { id: "1", userId: "u1", username: "Alice", code: "for(let i=0; i<10; i++) { sum += i; }" },
        { id: "2", userId: "u2", username: "Bob", code: "for(let k=0; k<10; k++) { sum += k; }" },
        { id: "3", userId: "u3", username: "Charlie", code: "while(running) { doSomethingElse(); }" }
      ];

      const audit = auditSubmissionsBatch(batch, { threshold: 0.70 });
      expect(audit.totalSubmissionsAnalyzed).toBe(3);
      expect(audit.flaggedPairsCount).toBe(1);
      expect(audit.flaggedPairs[0].submissionA.userId).toBe("u1");
      expect(audit.flaggedPairs[0].submissionB.userId).toBe("u2");
      expect(audit.flaggedPairs[0].verdict).toBe("HIGH_PROBABILITY_COLLUSION");
    });
  });

  describe("4. Content-Addressable Storage (CAS) Compilation Cache", () => {
    test("serves compilation cache hit on identical code and computes saved CPU duration", async () => {
      const cas = new CompilationCache();
      const code = "int main() { return 0; }";
      const language = "cpp";

      // Initial read -> Miss
      const initialRead = await cas.get(language, code);
      expect(initialRead.hit).toBe(false);

      // Store compiled binary result (took 800ms)
      await cas.set(language, code, { binaryPath: "/tmp/main_bin", exitCode: 0 }, 800);

      // Subsequent read -> Hit
      const cachedRead = await cas.get(language, code);
      expect(cachedRead.hit).toBe(true);
      expect(cachedRead.tier).toBe("L1");
      expect(cachedRead.result.binaryPath).toBe("/tmp/main_bin");

      const metrics = cas.getMetrics();
      expect(metrics.casHits).toBe(1);
      expect(metrics.casMisses).toBe(1);
      expect(metrics.savedCpuDurationMs).toBe(800);
      expect(metrics.casHitRatePct).toBe(50.0);
    });
  });
});

