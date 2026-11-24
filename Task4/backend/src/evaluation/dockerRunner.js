const { spawn } = require("child_process");
const { dockerTimeoutSec, dockerPullTimeoutSec } = require("../shared/config");
const { CircuitBreaker } = require("../shared/circuitBreaker");

const dockerCircuitBreaker = new CircuitBreaker("docker-sandbox", {
  failureThreshold: 5,
  recoveryTimeoutMs: 20000,
  successThreshold: 2,
  // A user's infinite loop timing out is a verdict (TLE), not a sandbox outage.
  isFailure: (err) => !(err && err.isUserCodeTimeout)
});

const languageProfiles = {
  javascript: {
    image: "node:20-alpine",
    command:
      "printf '%s' \"$CODE_B64\" | base64 -d > /tmp/main.js && printf '%s' \"$INPUT_B64\" | base64 -d > /tmp/input.txt && node /tmp/main.js < /tmp/input.txt"
  },
  python: {
    image: "python:3.12-alpine",
    command:
      "printf '%s' \"$CODE_B64\" | base64 -d > /tmp/main.py && printf '%s' \"$INPUT_B64\" | base64 -d > /tmp/input.txt && python /tmp/main.py < /tmp/input.txt"
  },
  cpp: {
    image: "gcc:14",
    command:
      "printf '%s' \"$CODE_B64\" | base64 -d > /tmp/main.cpp && printf '%s' \"$INPUT_B64\" | base64 -d > /tmp/input.txt && g++ /tmp/main.cpp -O2 -std=c++17 -o /tmp/main && /tmp/main < /tmp/input.txt"
  }
};

function runDockerCommand(args, timeoutSec) {
  return new Promise((resolve, reject) => {
    const child = spawn("docker", args, { windowsHide: true });

    let stdout = "";
    let stderr = "";
    let finished = false;

    const timer = setTimeout(() => {
      if (finished) {
        return;
      }

      finished = true;
      child.kill("SIGKILL");
      reject(new Error(`Docker command timed out: docker ${args.join(" ")}`));
    }, Math.max(1, timeoutSec) * 1000);

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (err) => {
      if (finished) {
        return;
      }

      finished = true;
      clearTimeout(timer);
      reject(new Error(`Docker command failed: ${err.message}`));
    });

    child.on("close", (exitCode) => {
      if (finished) {
        return;
      }

      finished = true;
      clearTimeout(timer);

      if (exitCode !== 0) {
        return reject(
          new Error(`Docker command exited ${exitCode}: ${stderr.trim() || stdout.trim()}`)
        );
      }

      return resolve({
        stdout: stdout.trim(),
        stderr: stderr.trim(),
        exitCode
      });
    });
  });
}

function getSupportedDockerImages() {
  return [...new Set(Object.values(languageProfiles).map((profile) => profile.image))];
}

async function prePullDockerImages() {
  const images = getSupportedDockerImages();

  try {
    for (const image of images) {
      await runDockerCommand(["pull", image], dockerPullTimeoutSec);
    }

    return {
      ok: true,
      images
    };
  } catch (error) {
    return {
      ok: false,
      images,
      error: error.message
    };
  }
}

function executeDockerSandbox(language, code, input = "") {
  const profile = languageProfiles[String(language || "").toLowerCase()];

  if (!profile) {
    throw new Error("Unsupported language for docker runner");
  }

  const codeB64 = Buffer.from(String(code || ""), "utf8").toString("base64");
  const inputB64 = Buffer.from(String(input || ""), "utf8").toString("base64");
  const containerName = `sandbox-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`;

  return new Promise((resolve, reject) => {
    const args = [
      "run",
      "--name",
      containerName,
      "--rm",
      "--network",
      "none",
      "--cpus",
      "1",
      "--memory",
      "256m",
      "--pids-limit",
      "64",
      "--tmpfs",
      "/tmp:rw,nosuid,size=32m",
      "-e",
      `CODE_B64=${codeB64}`,
      "-e",
      `INPUT_B64=${inputB64}`,
      profile.image,
      "sh",
      "-c",
      profile.command
    ];

    const child = spawn("docker", args, { windowsHide: true });

    let stdout = "";
    let stderr = "";
    let finished = false;

    const timer = setTimeout(() => {
      if (finished) {
        return;
      }

      finished = true;
      // Explicitly kill the container by name so dockerd frees orphaned resources
      try {
        spawn("docker", ["kill", containerName], { windowsHide: true });
      } catch (_) {}
      child.kill("SIGKILL");
      const timeoutError = new Error("Docker execution timed out");
      timeoutError.isUserCodeTimeout = true;
      reject(timeoutError);
    }, dockerTimeoutSec * 1000);

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (err) => {
      if (finished) {
        return;
      }

      finished = true;
      clearTimeout(timer);
      reject(new Error(`Docker command failed: ${err.message}`));
    });

    child.on("close", (exitCode) => {
      if (finished) {
        return;
      }

      finished = true;
      clearTimeout(timer);
      resolve({
        exitCode,
        stdout: stdout.trim(),
        stderr: stderr.trim()
      });
    });

  });
}

function runCodeInDocker(language, code, input = "") {
  return dockerCircuitBreaker.execute(() => executeDockerSandbox(language, code, input));
}

function getDockerCircuitBreakerStatus() {
  return dockerCircuitBreaker.getStatus();
}

module.exports = {
  runCodeInDocker,
  prePullDockerImages,
  getSupportedDockerImages,
  getDockerCircuitBreakerStatus
};
