/**
 * AST Structural Code Plagiarism Detection Engine
 * Implements the Stanford MOSS-style Winnowing Algorithm with Token Normalization
 * and Rabin-Karp Rolling Hashes.
 *
 * Concepts:
 * 1. Token Normalization: Strips comments, string/numeric literals, and normalizes
 *    variable/function identifiers to '$ID' to defeat trivial obfuscation (renaming).
 * 2. K-Gram Generation: Groups structural tokens into continuous sequences of length k.
 * 3. Rolling Hash: Computes polynomial rolling hashes for each k-gram.
 * 4. Winnowing: Selects rightmost minimum hashes in sliding windows of size w.
 * 5. Jaccard Similarity: Calculates overlap coefficient between fingerprint sets.
 */

const KEYWORDS = new Set([
  "if", "else", "for", "while", "do", "return", "def", "class", "function",
  "switch", "case", "try", "catch", "finally", "throw", "import", "include",
  "var", "let", "const", "int", "float", "double", "char", "bool", "void",
  "public", "private", "protected", "static", "new", "this", "null", "true", "false"
]);

function stripCommentsAndLiterals(code) {
  let clean = String(code || "");

  // Remove multi-line comments /* ... */
  clean = clean.replace(/\/\*[\s\S]*?\*\//g, " ");

  // Remove single-line comments // ... and # ...
  clean = clean.replace(/(\/\/|#).*$/gm, " ");

  // Remove string literals "..." and '...'
  clean = clean.replace(/(["'])(?:(?=(\\?))\2[\s\S])*?\1/g, " $LIT ");

  // Remove numeric literals
  clean = clean.replace(/\b\d+(\.\d+)?\b/g, " $LIT ");

  return clean;
}

function tokenize(code) {
  const sanitized = stripCommentsAndLiterals(code);
  const rawTokens = sanitized.match(/[a-zA-Z_][a-zA-Z0-9_]*|[^\s\w]/g) || [];

  return rawTokens.map((token) => {
    const lower = token.toLowerCase();
    if (KEYWORDS.has(lower)) {
      return lower.toUpperCase();
    }
    if (token === "$LIT") {
      return "$LIT";
    }
    if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(token)) {
      return "$ID"; // Normalize all variable and function names
    }
    return token;
  });
}

function hashKgram(tokens, start, k) {
  const P = 31;
  const M = 2147483647; // 2^31 - 1 (Mersenne prime)
  let hash = 0;

  for (let i = 0; i < k; i += 1) {
    const str = tokens[start + i];
    for (let j = 0; j < str.length; j += 1) {
      hash = (hash * P + str.charCodeAt(j)) % M;
    }
  }

  return hash;
}

/**
 * Implements the Winnowing Algorithm
 * @param {Array<number>} hashes - array of k-gram hashes
 * @param {number} windowSize - sliding window size w
 * @returns {Set<number>} fingerprint set
 */
function winnow(hashes, windowSize = 4) {
  const fingerprints = new Set();
  const n = hashes.length;

  if (n <= windowSize) {
    if (n > 0) {
      let minHash = hashes[0];
      for (const h of hashes) {
        if (h <= minHash) minHash = h;
      }
      fingerprints.add(minHash);
    }
    return fingerprints;
  }

  let minIndex = -1;

  for (let i = 0; i <= n - windowSize; i += 1) {
    let windowMin = hashes[i];
    let windowMinIdx = i;

    for (let j = 1; j < windowSize; j += 1) {
      const idx = i + j;
      // In Winnowing, if there is a tie, pick the rightmost minimum
      if (hashes[idx] <= windowMin) {
        windowMin = hashes[idx];
        windowMinIdx = idx;
      }
    }

    if (windowMinIdx !== minIndex) {
      fingerprints.add(windowMin);
      minIndex = windowMinIdx;
    }
  }

  return fingerprints;
}

function generateFingerprints(code, options = {}) {
  const k = Math.max(2, Number(options.k || 4));
  const w = Math.max(2, Number(options.w || 4));

  const tokens = tokenize(code);
  if (tokens.length < k) {
    return new Set(tokens.map((t) => t.charCodeAt(0)));
  }

  const hashes = [];
  for (let i = 0; i <= tokens.length - k; i += 1) {
    hashes.push(hashKgram(tokens, i, k));
  }

  return winnow(hashes, w);
}

function calculateJaccardSimilarity(setA, setB) {
  if (setA.size === 0 && setB.size === 0) {
    return 1.0;
  }
  if (setA.size === 0 || setB.size === 0) {
    return 0.0;
  }

  let intersectionCount = 0;
  for (const item of setA) {
    if (setB.has(item)) {
      intersectionCount += 1;
    }
  }

  const unionCount = setA.size + setB.size - intersectionCount;
  return unionCount > 0 ? Number((intersectionCount / unionCount).toFixed(4)) : 0;
}

function compareSubmissions(codeA, codeB, options = {}) {
  const fpA = generateFingerprints(codeA, options);
  const fpB = generateFingerprints(codeB, options);

  const similarity = calculateJaccardSimilarity(fpA, fpB);
  const similarityPct = Math.round(similarity * 100);

  return {
    similarity,
    similarityPct,
    fingerprintCountA: fpA.size,
    fingerprintCountB: fpB.size,
    isSuspicious: similarity >= Number(options.threshold || 0.75)
  };
}

function auditSubmissionsBatch(submissions, options = {}) {
  const threshold = Number(options.threshold || 0.75);
  const results = [];
  const processed = [];

  for (const sub of submissions) {
    if (sub.code) {
      processed.push({
        id: sub.id || sub._id,
        userId: sub.userId,
        username: sub.username,
        code: sub.code,
        fingerprints: generateFingerprints(sub.code, options)
      });
    }
  }

  // Cross-compare all pairs O(N^2)
  for (let i = 0; i < processed.length; i += 1) {
    for (let j = i + 1; j < processed.length; j += 1) {
      const a = processed[i];
      const b = processed[j];

      if (String(a.userId) === String(b.userId)) {
        continue; // Skip comparing same candidate's submissions
      }

      const similarity = calculateJaccardSimilarity(a.fingerprints, b.fingerprints);
      if (similarity >= threshold) {
        results.push({
          submissionA: { id: a.id, userId: a.userId, username: a.username },
          submissionB: { id: b.id, userId: b.userId, username: b.username },
          similarity,
          similarityPct: Math.round(similarity * 100),
          verdict: similarity >= 0.9 ? "HIGH_PROBABILITY_COLLUSION" : "SUSPICIOUS_SIMILARITY"
        });
      }
    }
  }

  results.sort((a, b) => b.similarity - a.similarity);

  return {
    totalSubmissionsAnalyzed: processed.length,
    flaggedPairsCount: results.length,
    flaggedPairs: results
  };
}

module.exports = {
  tokenize,
  generateFingerprints,
  calculateJaccardSimilarity,
  compareSubmissions,
  auditSubmissionsBatch
};

