const mongoose = require("mongoose");
const { mongoUri } = require("./config");
const logger = require("./logger");

const LEGACY_SUBMISSION_INDEXES = ["userId_1_idempotencyKey_1"];

// One-time migration: the old sparse unique index on {userId, idempotencyKey} rejected every
// key-less submission after a user's first one. Drop it and build the partial index instead.
async function migrateSubmissionIndexes() {
  const Submission = require("../models/Submission");

  try {
    const existing = await Submission.collection.indexes();
    for (const index of existing) {
      if (LEGACY_SUBMISSION_INDEXES.includes(index.name)) {
        await Submission.collection.dropIndex(index.name);
        logger.info("mongo.index.dropped_legacy", { index: index.name });
      }
    }
  } catch (err) {
    // Collection may not exist yet on a fresh database.
    if (err.codeName !== "NamespaceNotFound") {
      logger.warn("mongo.index.migration_failed", { error: err.message });
    }
  }

  await Submission.createIndexes();
}

async function connectDb() {
  await mongoose.connect(mongoUri);
  logger.info("mongo.connected", {
    host: mongoose.connection.host || null,
    name: mongoose.connection.name || null
  });
  await migrateSubmissionIndexes();
}

module.exports = {
  connectDb,
  migrateSubmissionIndexes
};
