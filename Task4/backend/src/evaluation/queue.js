const { Queue } = require("bullmq");
const redisConnection = require("../shared/redis");
const { queueName, deadLetterQueueName } = require("../shared/config");

let submissionQueue;
let deadLetterQueue;

function getSubmissionQueue() {
  if (!submissionQueue) {
    submissionQueue = new Queue(queueName, {
      connection: redisConnection,
      defaultJobOptions: {
        attempts: 3,
        // BullMQ v5 built-in exponential backoff with jitter (1s, 2s, ... each randomised by up
        // to 50%) so retries from many failed jobs don't hit Docker/Mongo in synchronized waves.
        // (A custom type name would need Worker settings.backoffStrategy; an unknown type throws
        // "Unknown backoff strategy" at retry time and the job fails without being retried.)
        backoff: {
          type: "exponential",
          delay: 1000,
          jitter: 0.5
        },
        removeOnComplete: 200,
        removeOnFail: 500
      }
    });
  }

  return submissionQueue;
}

function getDeadLetterQueue() {
  if (!deadLetterQueue) {
    deadLetterQueue = new Queue(deadLetterQueueName, {
      connection: redisConnection,
      defaultJobOptions: {
        removeOnComplete: 2000,
        removeOnFail: 2000
      }
    });
  }

  return deadLetterQueue;
}

module.exports = {
  getSubmissionQueue,
  getDeadLetterQueue
};
