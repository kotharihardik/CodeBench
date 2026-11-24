const { runCodeInDocker } = require("../dockerRunner");
const { publishSubmissionProgress } = require("../streamService");

function normalizeOutput(text) {
  return String(text || "").trim().replace(/\r\n/g, "\n");
}

class CodeStrategy {
  async evaluate({ submission, question }) {
    let testCases = [];

    if (Array.isArray(question.testCases) && question.testCases.length > 0) {
      testCases = question.testCases;
    } else if (Array.isArray(question.hiddenTestCases) && question.hiddenTestCases.length > 0) {
      testCases = question.hiddenTestCases;
    } else if (Array.isArray(question.sampleTestCases) && question.sampleTestCases.length > 0) {
      testCases = question.sampleTestCases;
    }

    if (!submission.code || !submission.language) {
      return {
        passed: false,
        score: 0,
        output: {
          stdout: "",
          stderr: "Missing code or language",
          details: "Code submission is incomplete"
        }
      };
    }

    if (testCases.length === 0) {
      const runResult = await runCodeInDocker(submission.language, submission.code);
      return {
        passed: runResult.exitCode === 0,
        score: runResult.exitCode === 0 ? 100 : 0,
        output: {
          stdout: runResult.stdout,
          stderr: runResult.stderr,
          details: "Executed without test cases"
        }
      };
    }

    const caseResults = [];
    let passCount = 0;

    for (let i = 0; i < testCases.length; i += 1) {
      const testCase = testCases[i];
      const caseStartAt = Date.now();

      if (submission._id) {
        void publishSubmissionProgress(submission._id, {
          event: "TEST_CASE_START",
          testCaseIndex: i + 1,
          totalTestCases: testCases.length
        });
      }

      const runResult = await runCodeInDocker(
        submission.language,
        submission.code,
        String(testCase.input || "")
      );
      const actual = normalizeOutput(runResult.stdout);
      const expected = normalizeOutput(testCase.expectedOutput);
      const passed = runResult.exitCode === 0 && actual === expected;
      const durationMs = Date.now() - caseStartAt;

      if (passed) {
        passCount += 1;
      }

      if (submission._id) {
        void publishSubmissionProgress(submission._id, {
          event: "TEST_CASE_RESULT",
          testCaseIndex: i + 1,
          totalTestCases: testCases.length,
          passed,
          durationMs
        });
      }

      caseResults.push({
        expected,
        actual,
        passed,
        stderr: runResult.stderr
      });
    }

    const passed = passCount === testCases.length;
    const score = Math.round((passCount / testCases.length) * 100);

    return {
      passed,
      score,
      output: {
        stdout: "",
        stderr: "",
        details: JSON.stringify(caseResults)
      }
    };
  }
}

module.exports = CodeStrategy;
