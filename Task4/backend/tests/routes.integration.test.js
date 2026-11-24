jest.mock("../src/evaluation/queue", () => {
  const add = jest.fn().mockResolvedValue({ id: "job-1" });
  const getJobCounts = jest.fn().mockResolvedValue({
    waiting: 2,
    active: 1,
    completed: 10,
    failed: 0,
    delayed: 0
  });

  return {
    getSubmissionQueue: () => ({
      add,
      getJobCounts
    }),
    getDeadLetterQueue: () => ({
      count: jest.fn().mockResolvedValue(0)
    })
  };
});

const request = require("supertest");
const app = require("../src/app");
const Question = require("../src/models/Question");
const Submission = require("../src/models/Submission");
const { connectTestDb, clearTestDb, disconnectTestDb } = require("./helpers/testDb");

async function signupUser(role = "student") {
  const payload = {
    name: role === "admin" ? "Admin User" : "Student User",
    email: `${role}_${Date.now()}_${Math.floor(Math.random() * 10000)}@mail.com`,
    password: "pass1234",
    role
  };

  if (role === "admin") {
    payload.adminKey = process.env.ADMIN_SIGNUP_KEY || "team27";
  }

  const response = await request(app).post("/api/auth/signup").send(payload);
  return {
    token: response.body.token,
    user: response.body.user
  };
}

describe("Expanded Routes Integration Tests", () => {
  beforeAll(async () => {
    await connectTestDb();
  });

  afterEach(async () => {
    await clearTestDb();
  });

  afterAll(async () => {
    await disconnectTestDb();
  });

  describe("Leaderboard HTTP API", () => {
    test("records score and returns Top-K leaderboard over HTTP", async () => {
      const admin = await signupUser("admin");
      const student1 = await signupUser("student");
      const student2 = await signupUser("student");
      // Unique contest id: leaderboard lives in Redis, which persists across test runs.
      const contest = `contest_${Date.now()}`;

      // Scores are written by the grader/admin on behalf of a candidate
      const res1 = await request(app)
        .post(`/api/leaderboard/${contest}/score`)
        .set("Authorization", `Bearer ${admin.token}`)
        .send({ targetUserId: student1.user.id, score: 250, penaltySeconds: 300, problemsSolved: 3 });

      expect(res1.statusCode).toBe(200);
      expect(res1.body.ok).toBe(true);

      const res2 = await request(app)
        .post(`/api/leaderboard/${contest}/score`)
        .set("Authorization", `Bearer ${admin.token}`)
        .send({ targetUserId: student2.user.id, score: 300, penaltySeconds: 0, problemsSolved: 4 });

      expect(res2.statusCode).toBe(200);

      const listRes = await request(app)
        .get(`/api/leaderboard/${contest}?limit=10`)
        .set("Authorization", `Bearer ${student1.token}`);

      expect(listRes.statusCode).toBe(200);
      expect(listRes.body.leaderboard.length).toBe(2);
      expect(listRes.body.leaderboard[0].score).toBe(300);
      expect(listRes.body.leaderboard[0].penaltySeconds).toBe(0);
      expect(listRes.body.leaderboard[0].rank).toBe(1);

      const meRes = await request(app)
        .get(`/api/leaderboard/${contest}/me`)
        .set("Authorization", `Bearer ${student1.token}`);

      expect(meRes.statusCode).toBe(200);
      expect(meRes.body.rank).toBe(2);
      expect(meRes.body.score).toBe(250);
      expect(meRes.body.penaltySeconds).toBe(300);
    });

    test("rejects a student writing their own leaderboard score", async () => {
      const student = await signupUser("student");
      const res = await request(app)
        .post(`/api/leaderboard/contest_${Date.now()}/score`)
        .set("Authorization", `Bearer ${student.token}`)
        .send({ score: 999999 });

      expect(res.statusCode).toBe(403);
    });
  });

  describe("Plagiarism HTTP API", () => {
    test("compares two code submissions and detects high similarity", async () => {
      const student = await signupUser("student");

      const codeA = "function add(a, b) { return a + b; }";
      const codeB = "function add(x, y) { return x + y; }";

      const compareRes = await request(app)
        .post("/api/plagiarism/compare")
        .set("Authorization", `Bearer ${student.token}`)
        .send({ codeA, codeB, threshold: 0.70 });

      expect(compareRes.statusCode).toBe(200);
      expect(compareRes.body.similarityPct).toBeGreaterThanOrEqual(70);
      expect(compareRes.body.isSuspicious).toBe(true);
    });

    test("allows admin to run collusion audit across question submissions", async () => {
      const admin = await signupUser("admin");
      const student1 = await signupUser("student");
      const student2 = await signupUser("student");

      const question = await Question.create({
        title: "Two Sum",
        description: "Find indices",
        type: "code",
        topic: "Arrays",
        difficulty: "easy",
        isActive: true
      });

      // Create two identical completed submissions
      await Submission.create({
        userId: student1.user.id,
        questionId: question._id,
        type: "code",
        topic: "Arrays",
        difficulty: "easy",
        code: "function solve(arr) { for(let i=0; i<arr.length; i++) { sum += arr[i]; } }",
        status: "COMPLETED"
      });

      await Submission.create({
        userId: student2.user.id,
        questionId: question._id,
        type: "code",
        topic: "Arrays",
        difficulty: "easy",
        code: "function solve(nums) { for(let k=0; k<nums.length; k++) { sum += nums[k]; } }",
        status: "COMPLETED"
      });

      const auditRes = await request(app)
        .post(`/api/plagiarism/audit-question/${question._id}`)
        .set("Authorization", `Bearer ${admin.token}`)
        .send({ threshold: 0.60 });

      expect(auditRes.statusCode).toBe(200);
      expect(auditRes.body.totalSubmissionsAnalyzed).toBe(2);
      expect(auditRes.body.flaggedPairsCount).toBe(1);
    });
  });

  describe("Monitor Dashboard Telemetry API", () => {
    test("exposes complete runtime metrics including cache, circuit breaker, and CAS", async () => {
      const admin = await signupUser("admin");

      const res = await request(app)
        .get("/api/monitor/dashboard")
        .set("Authorization", `Bearer ${admin.token}`);

      expect(res.statusCode).toBe(200);
      expect(res.body.queue).toBeDefined();
      expect(res.body.cache).toBeDefined();
      expect(res.body.circuitBreaker).toBeDefined();
      expect(res.body.circuitBreaker.state).toBe("CLOSED");
      expect(res.body.compilationCache).toBeDefined();
    });
  });

  describe("Submissions SSE Stream Endpoint", () => {
    test("rejects stream access without authentication", async () => {
      const res = await request(app).get("/api/submissions/6af37936b8dc2d311b2e2243/stream");
      expect(res.statusCode).toBe(401);
    });

    test("returns 404 if submission does not exist", async () => {
      const student = await signupUser("student");
      const fakeId = "6af37936b8dc2d311b2e2243";

      const res = await request(app)
        .get(`/api/submissions/${fakeId}/stream`)
        .set("Authorization", `Bearer ${student.token}`);

      expect(res.statusCode).toBe(404);
    });
  });
});

