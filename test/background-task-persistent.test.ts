import assert from "node:assert/strict";
import { describe, expect, it, beforeEach, afterAll } from "bun:test";
import { sql } from "bun";

const { BackgroundTaskManager } = await import("../src/background-task.js");
const {
  cleanBackgroundJobsTableQuery,
  getJobByIdQuery: findJobByIdQuery,
  getCompletedJobsCountQuery,
  popNextBackgroundJobQuery,
  enqueueJobQuery,
  requeueInterruptedJobQuery,
  resetStuckRunningJobsQuery,
} = await import("../src/queries.js");

/** These tests inspect jobs they have just inserted; absence is a fixture failure. */
async function getJobByIdQuery(id: string) {
  const job = await findJobByIdQuery(id);
  assert(job, `Missing fixture job ${id}`);
  return job;
}

// Needs a PostgreSQL with the `background_jobs` schema (see test/schema.sql); skipped otherwise.
describe.skipIf(!process.env.DATABASE_URL)("BackgroundTaskManager - Persistent Job Queue", () => {
  beforeEach(async () => {
    BackgroundTaskManager.stopWorker();
    BackgroundTaskManager.resetForTest();
    BackgroundTaskManager.configure({ silent: true });

    // Clean up background_jobs table before each test
    await cleanBackgroundJobsTableQuery("test-persistent-%");
  });

  afterAll(async () => {
    // Clean up background_jobs table after all tests
    await cleanBackgroundJobsTableQuery("test-persistent-%");
  });

  it("should successfully enqueue and execute a persistent task immediately", async () => {
    let executedPayload: any = null;

    BackgroundTaskManager.registerRunner(
      "test-persistent-immediate",
      async (payload) => {
        executedPayload = payload;
      },
    );

    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-immediate",
      { message: "hello persistent queue" },
    );

    expect(jobId).toBeDefined();

    // Verify it's in 'pending' state
    const jobBefore = await getJobByIdQuery(jobId);
    expect(jobBefore).toBeDefined();
    expect(jobBefore.status).toBe("pending");
    expect(jobBefore.attempts).toBe(0);

    // Start worker with 50ms polling
    BackgroundTaskManager.startWorker(50);

    // Wait for task execution and resolution
    await new Promise((r) => setTimeout(r, 150));

    // Verify it was executed with payload
    expect(executedPayload).toEqual({ message: "hello persistent queue" });

    // Verify DB state updated to 'completed'
    const jobAfter = await getJobByIdQuery(jobId);
    expect(jobAfter.status).toBe("completed");
    expect(jobAfter.attempts).toBe(1);
    expect(jobAfter.finished_at).toBeDefined();
  });

  it("should support delayed persistent task execution", async () => {
    let executed = false;

    BackgroundTaskManager.registerRunner(
      "test-persistent-delayed",
      async () => {
        executed = true;
      },
    );

    // Enqueue with a 200ms delay
    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-delayed",
      {},
      { delayMs: 200 },
    );

    // Start worker with 50ms polling
    BackgroundTaskManager.startWorker(50);

    // Wait 100ms (less than delay)
    await new Promise((r) => setTimeout(r, 100));
    expect(executed).toBe(false);

    // Verify status is still pending in DB
    const jobMiddle = await getJobByIdQuery(jobId);
    expect(jobMiddle.status).toBe("pending");

    // Wait another 200ms (total 300ms, past the 200ms delay)
    await new Promise((r) => setTimeout(r, 200));

    expect(executed).toBe(true);

    // Verify completed in DB
    const jobAfter = await getJobByIdQuery(jobId);
    expect(jobAfter.status).toBe("completed");
  });

  it("should handle runner errors and execute retries with backoff, eventually transitioning to failed", async () => {
    let attemptsCount = 0;

    BackgroundTaskManager.registerRunner(
      "test-persistent-failing",
      async () => {
        attemptsCount++;
        throw new Error("Runner simulated error");
      },
    );

    // Retries: 1, initialDelay: 50ms
    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-failing",
      {},
      { retries: 1, initialDelayMs: 50 },
    );

    // Start worker
    BackgroundTaskManager.startWorker(40);

    // 1. Initial attempt: starts immediately, fails, transitions to pending with run_at delay of 50ms
    await new Promise((r) => setTimeout(r, 100));

    // Verify first attempt failure and scheduling of retry
    const jobAfterFirst = await getJobByIdQuery(jobId);
    expect(jobAfterFirst.status).toBe("pending");
    expect(jobAfterFirst.attempts).toBe(1);
    expect(jobAfterFirst.error_message).toBe("Runner simulated error");

    // 2. Wait for retry to fire (at least another 50ms, let's wait 120ms total)
    await new Promise((r) => setTimeout(r, 120));

    // Verify final state is failed after 2 attempts (1 initial + 1 retry)
    const jobAfterFinal = await getJobByIdQuery(jobId);
    expect(jobAfterFinal.status).toBe("failed");
    expect(jobAfterFinal.attempts).toBe(2);
    expect(jobAfterFinal.error_message).toBe("Runner simulated error");
    expect(attemptsCount).toBe(2);
  });

  it("should support concurrent processing safety via SKIP LOCKED (Stress Test)", async () => {
    const taskCount = 10;
    const executionLogs: string[] = [];

    BackgroundTaskManager.registerRunner(
      "test-persistent-stress",
      async (payload) => {
        // Simulate task duration
        await new Promise((r) => setTimeout(r, 15));
        // Narrow the persisted payload before consuming it.
        assert(
          typeof payload === "object" && payload !== null && "index" in payload,
        );
        assert(typeof payload.index === "string");
        executionLogs.push(payload.index);
      },
    );

    // Enqueue 10 tasks
    const enqueuePromises = [];
    for (let i = 0; i < taskCount; i++) {
      enqueuePromises.push(
        BackgroundTaskManager.enqueue("test-persistent-stress", {
          index: `job-${i}`,
        }),
      );
    }
    await Promise.all(enqueuePromises);

    // Simulate multiple worker instances by starting 3 workers in parallel
    BackgroundTaskManager.startWorker(10);

    // We can also trigger another poll loop to simulate a second node polling simultaneously
    const secondNodeTimer = setInterval(async () => {
      // Simulate worker tick of second node calling pollAndExecute
      // @ts-ignore
      await BackgroundTaskManager.pollAndExecute();
    }, 12);

    // Wait for all 10 tasks to be completed
    await new Promise((r) => setTimeout(r, 600));

    clearInterval(secondNodeTimer);

    // Verify all 10 tasks completed successfully
    const completedJobsCount = await getCompletedJobsCountQuery(
      "test-persistent-stress",
    );
    expect(completedJobsCount).toBe(taskCount);

    // Verify each task was processed exactly once (no duplicates)
    expect(executionLogs.length).toBe(taskCount);
    const uniqueLogs = new Set(executionLogs);
    expect(uniqueLogs.size).toBe(taskCount);
  });

  it("should support graceful shutdown and resume (state persistence across restarts)", async () => {
    let executed = false;
    BackgroundTaskManager.registerRunner(
      "test-persistent-restart",
      async () => {
        executed = true;
      },
    );

    // Enqueue a job while worker is stopped
    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-restart",
      {},
    );

    // Verify it is pending in DB
    const jobBefore = await getJobByIdQuery(jobId);
    expect(jobBefore.status).toBe("pending");

    // Wait a bit to ensure it is NOT executed
    await new Promise((r) => setTimeout(r, 60));
    expect(executed).toBe(false);

    // Start worker
    BackgroundTaskManager.startWorker(20);

    // Wait for execution
    await new Promise((r) => setTimeout(r, 100));
    expect(executed).toBe(true);

    const jobAfter = await getJobByIdQuery(jobId);
    expect(jobAfter.status).toBe("completed");
  });

  it("should leave jobs pending if no runner is registered", async () => {
    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-norunner",
      {},
    );

    BackgroundTaskManager.startWorker(20);

    // A process without runners must leave the shared queue untouched.
    await new Promise((r) => setTimeout(r, 80));

    const job = await getJobByIdQuery(jobId);
    expect(job.status).toBe("pending");
    expect(job.attempts).toBe(0);
    expect(job.started_at).toBeNull();
    expect(job.finished_at).toBeNull();
    expect(job.error_message).toBeNull();
  });

  it("should skip unregistered jobs and preserve them for their runner", async () => {
    const foreignName = "test-persistent-foreign";
    const localName = "test-persistent-local";
    const executions: string[] = [];
    const foreignId = await BackgroundTaskManager.enqueue(foreignName, {
      source: "async",
    });
    const foreignBefore = await getJobByIdQuery(foreignId);
    const localId = await BackgroundTaskManager.enqueue(localName, {});

    BackgroundTaskManager.registerRunner(localName, async () => {
      executions.push(localName);
    });

    expect(await (BackgroundTaskManager as any).pollAndExecute()).toBe(true);
    await BackgroundTaskManager.waitForAll();

    expect((await getJobByIdQuery(localId)).status).toBe("completed");
    expect(await getJobByIdQuery(foreignId)).toEqual(foreignBefore);
    expect(await (BackgroundTaskManager as any).pollAndExecute()).toBe(false);

    // The original job remains available when its runner becomes available.
    BackgroundTaskManager.registerRunner(foreignName, async (payload) => {
      expect(payload).toEqual({ source: "async" });
      executions.push(foreignName);
    });
    expect(await (BackgroundTaskManager as any).pollAndExecute()).toBe(true);
    await BackgroundTaskManager.waitForAll();

    const foreignAfter = await getJobByIdQuery(foreignId);
    expect(foreignAfter.status).toBe("completed");
    expect(foreignAfter.attempts).toBe(1);
    expect(foreignAfter.error_message).toBeNull();
    expect(executions).toEqual([localName, foreignName]);
  });

  it("should isolate concurrent claims with different runner sets", async () => {
    const firstName = "test-persistent-worker-first";
    const secondName = "test-persistent-worker-second";
    const unknownId = await BackgroundTaskManager.enqueue(
      "test-persistent-worker-unknown",
      {},
    );
    const firstId = await BackgroundTaskManager.enqueue(firstName, {});
    const secondId = await BackgroundTaskManager.enqueue(secondName, {});

    const claims = await Promise.all([
      popNextBackgroundJobQuery([secondName]),
      popNextBackgroundJobQuery([firstName]),
      popNextBackgroundJobQuery([secondName]),
      popNextBackgroundJobQuery([firstName]),
    ]);

    expect(claims[0]?.id ?? claims[2]?.id).toBe(secondId);
    expect(claims[1]?.id ?? claims[3]?.id).toBe(firstId);
    expect(claims.filter(Boolean)).toHaveLength(2);
    expect((await getJobByIdQuery(firstId)).attempts).toBe(1);
    expect((await getJobByIdQuery(secondId)).attempts).toBe(1);
    expect((await getJobByIdQuery(unknownId)).status).toBe("pending");
    expect((await getJobByIdQuery(unknownId)).attempts).toBe(0);
  });

  it("should recover stuck jobs only for registered runners", async () => {
    const jobs = [
      { name: "test-persistent-stuck-local-retry", retries: 1 },
      { name: "test-persistent-stuck-local-failed", retries: 0 },
      { name: "test-persistent-stuck-foreign-retry", retries: 1 },
      { name: "test-persistent-stuck-foreign-failed", retries: 0 },
    ];
    const ids = await Promise.all(
      jobs.map(({ name, retries }) =>
        BackgroundTaskManager.enqueue(name, {}, { retries }),
      ),
    );
    await sql`
      UPDATE background_jobs
      SET status = 'running', attempts = 1,
          started_at = (now() AT TIME ZONE 'utc') - interval '2 minutes'
      WHERE id = ANY(${sql.array(ids, "uuid")})
    `;
    const before = await Promise.all(ids.map(getJobByIdQuery));

    await resetStuckRunningJobsQuery([]);
    expect(await Promise.all(ids.map(getJobByIdQuery))).toEqual(before);

    await resetStuckRunningJobsQuery(jobs.slice(0, 2).map(({ name }) => name));
    const after = await Promise.all(ids.map(getJobByIdQuery));
    assert(after[0] && after[1]);
    expect(after[0].status).toBe("pending");
    expect(after[1].status).toBe("failed");
    expect(after.slice(2)).toEqual(before.slice(2));
  });

  it("should make an immediate job due right away using the DB clock", async () => {
    // Arrange / Act: no delay, then check due-ness against the DB's own now().
    const jobId = await BackgroundTaskManager.enqueue("test-persistent-due", {});
    const [due] = await sql<{ is_due: boolean }[]>`
      SELECT run_at <= (now() AT TIME ZONE 'utc') AS is_due
      FROM background_jobs WHERE id = ${jobId}
    `;

    // Assert: claimable by the very next poll, independent of host/DB skew.
    expect(due?.is_due).toBe(true);
    const claimed = await popNextBackgroundJobQuery(["test-persistent-due"]);
    expect(claimed?.id).toBe(jobId);
  });

  it("should schedule a delayed job in the future relative to the DB clock", async () => {
    // Arrange / Act
    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-due-delayed",
      {},
      { delayMs: 60_000 },
    );
    const [row] = await sql<{ seconds_ahead: number }[]>`
      SELECT EXTRACT(EPOCH FROM (run_at - (now() AT TIME ZONE 'utc')))::float8 AS seconds_ahead
      FROM background_jobs WHERE id = ${jobId}
    `;

    // Assert: ~60s ahead (tolerance for query latency) and not claimable yet.
    assert(row);
    expect(row.seconds_ahead).toBeGreaterThan(55);
    expect(row.seconds_ahead).toBeLessThanOrEqual(60);
    expect(await popNextBackgroundJobQuery(["test-persistent-due-delayed"])).toBeNull();
  });

  it("should requeue a persistent job interrupted by shutdown without charging the attempt", async () => {
    // Arrange: a long runner that honours the abort signal, and no retries (like cron jobs).
    let started = false;
    BackgroundTaskManager.registerRunner(
      "test-persistent-interrupted",
      async (_payload, signal) => {
        started = true;
        await new Promise((_resolve, reject) => {
          signal.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError")),
          );
        });
      },
    );
    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-interrupted",
      {},
      { retries: 0 },
    );
    expect(await (BackgroundTaskManager as any).pollAndExecute()).toBe(true);
    await new Promise((r) => setTimeout(r, 30));
    expect(started).toBe(true);
    expect((await getJobByIdQuery(jobId)).status).toBe("running");

    // Act
    await BackgroundTaskManager.shutdown();

    // Assert: back in the queue, claimable now, attempt refunded.
    const job = await getJobByIdQuery(jobId);
    expect(job.status).toBe("pending");
    expect(job.attempts).toBe(0);
    expect(job.started_at).toBeNull();
    expect(job.finished_at).toBeNull();
    expect(job.error_message).toBe("Interrupted by shutdown");
    const [due] = await sql<{ is_due: boolean }[]>`
      SELECT run_at <= (now() AT TIME ZONE 'utc') AS is_due
      FROM background_jobs WHERE id = ${jobId}
    `;
    expect(due?.is_due).toBe(true);
  });

  it("should never drive attempts below zero when requeueing", async () => {
    // Arrange: a running row with attempts already at 0 (e.g. manual intervention).
    const jobId = await BackgroundTaskManager.enqueue("test-persistent-requeue-floor", {});
    await sql`UPDATE background_jobs SET status = 'running', attempts = 0 WHERE id = ${jobId}`;

    // Act
    await requeueInterruptedJobQuery(jobId);

    // Assert
    const job = await getJobByIdQuery(jobId);
    expect(job.status).toBe("pending");
    expect(job.attempts).toBe(0);
  });

  it("should not resurrect a job that already settled when requeue is requested", async () => {
    // Arrange: a job that completed just before the interruption was processed.
    const jobId = await BackgroundTaskManager.enqueue("test-persistent-requeue-settled", {});
    await sql`UPDATE background_jobs SET status = 'completed', attempts = 1 WHERE id = ${jobId}`;

    // Act
    await requeueInterruptedJobQuery(jobId);

    // Assert
    expect((await getJobByIdQuery(jobId)).status).toBe("completed");
  });

  it("should hand back a job claimed after shutdown began instead of dropping it", async () => {
    // Arrange: a due job and a manager that is draining (poll tick was mid-flight).
    BackgroundTaskManager.registerRunner("test-persistent-late-claim", async () => {});
    const jobId = await BackgroundTaskManager.enqueue("test-persistent-late-claim", {});
    BackgroundTaskManager.setShuttingDown(true);

    // Act
    const found = await (BackgroundTaskManager as any).pollAndExecute();

    // Assert: not executed, not left 'running', attempt refunded.
    expect(found).toBe(false);
    const job = await getJobByIdQuery(jobId);
    expect(job.status).toBe("pending");
    expect(job.attempts).toBe(0);
  });

  it("should handle job execution timeouts correctly", async () => {
    BackgroundTaskManager.registerRunner(
      "test-persistent-timeout",
      async (payload, signal) => {
        await new Promise((resolve, reject) => {
          const timer = setTimeout(resolve, 150);
          signal.addEventListener("abort", () => {
            clearTimeout(timer);
            reject(new DOMException("Task timed out", "AbortError"));
          });
        });
      },
    );

    // Enqueue with a very short timeout and no retries
    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-timeout",
      {},
      { timeoutMs: 40, retries: 0 },
    );

    BackgroundTaskManager.startWorker(20);

    // Wait for timeout to trigger and status to be updated in DB
    await new Promise((r) => setTimeout(r, 100));

    const job = await getJobByIdQuery(jobId);
    expect(job.status).toBe("failed");
    expect(job.attempts).toBe(1);
    expect(job.error_message).toBeDefined();
    assert(job.error_message);
    expect(
      job.error_message.toLowerCase().includes("timed out") ||
        job.error_message.toLowerCase().includes("aborted") ||
        job.error_message.toLowerCase().includes("abort"),
    ).toBe(true);
  });

  it("should enforce maxConcurrency backpressure and delay database polling", async () => {
    BackgroundTaskManager.configure({ maxConcurrency: 1 });

    const executedOrder: string[] = [];

    BackgroundTaskManager.registerRunner("test-persistent-bp-1", async () => {
      executedOrder.push("bp-1");
      // Keep it active for 150ms
      await new Promise((r) => setTimeout(r, 150));
    });

    BackgroundTaskManager.registerRunner("test-persistent-bp-2", async () => {
      executedOrder.push("bp-2");
    });

    // Enqueue both jobs to run immediately
    const jobId1 = await BackgroundTaskManager.enqueue(
      "test-persistent-bp-1",
      {},
    );
    const jobId2 = await BackgroundTaskManager.enqueue(
      "test-persistent-bp-2",
      {},
    );

    // Start worker
    BackgroundTaskManager.startWorker(25);

    // Wait 50ms - Job 1 should be running/popped, but Job 2 should still be pending
    // because maxConcurrency of 1 is currently active with Job 1
    await new Promise((r) => setTimeout(r, 50));

    const job1Middle = await getJobByIdQuery(jobId1);
    const job2Middle = await getJobByIdQuery(jobId2);

    expect(job1Middle.status).toBe("running");
    expect(job2Middle.status).toBe("pending"); // Not popped yet!
    expect(executedOrder).toEqual(["bp-1"]);

    // Wait for Job 1 to finish and Job 2 to execute (150ms + 100ms extra)
    await new Promise((r) => setTimeout(r, 220));

    const job1Final = await getJobByIdQuery(jobId1);
    const job2Final = await getJobByIdQuery(jobId2);

    expect(job1Final.status).toBe("completed");
    expect(job2Final.status).toBe("completed");
    expect(executedOrder).toEqual(["bp-1", "bp-2"]);
  });

  it("should support programmatic cancellation of running persistent tasks via manager.cancel", async () => {
    let wasAborted = false;

    BackgroundTaskManager.registerRunner(
      "test-persistent-abort",
      async (payload, signal) => {
        try {
          await new Promise((resolve, reject) => {
            const timer = setTimeout(resolve, 300);
            signal.addEventListener("abort", () => {
              clearTimeout(timer);
              reject(new DOMException("Aborted", "AbortError"));
            });
          });
        } catch (err) {
          wasAborted = true;
          throw err;
        }
      },
    );

    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-abort",
      {},
      { retries: 0 },
    );

    BackgroundTaskManager.startWorker(20);

    // Wait for worker to pop and start the task
    await new Promise((r) => setTimeout(r, 50));

    // Confirm it's running in the DB
    const jobRunning = await getJobByIdQuery(jobId);
    expect(jobRunning.status).toBe("running");

    // Cancel all tasks matching the specific runner name
    BackgroundTaskManager.cancel(`persistent-test-persistent-abort-${jobId}`);

    // Wait for abort and cleanup
    await new Promise((r) => setTimeout(r, 100));

    expect(wasAborted).toBe(true);

    const jobCancelled = await getJobByIdQuery(jobId);
    expect(jobCancelled.status).toBe("failed");
    assert(jobCancelled.error_message);
    expect(
      jobCancelled.error_message.toLowerCase().includes("cancelled") ||
        jobCancelled.error_message.toLowerCase().includes("aborted") ||
        jobCancelled.error_message.toLowerCase().includes("abort"),
    ).toBe(true);
  });

  it("should keep polling while idle and pick up a job inserted by another process", async () => {
    // Arrange: an idle worker (no in-process enqueue will wake it up).
    let executed = false;
    BackgroundTaskManager.registerRunner("test-persistent-idle-foreign", async () => {
      executed = true;
    });
    BackgroundTaskManager.startWorker(10);
    await new Promise((r) => setTimeout(r, 100));
    expect(BackgroundTaskManager.getIsWorkerRunning()).toBe(true);

    // Act: insert the row directly, as the other service sharing the table would.
    const jobId = await enqueueJobQuery("test-persistent-idle-foreign", {}, 0, 0, 1000, 30000);
    await new Promise((r) => setTimeout(r, 100));

    // Assert
    expect(executed).toBe(true);
    expect((await getJobByIdQuery(jobId)).status).toBe("completed");
  });

  it("should schedule a retry in the future relative to the DB clock", async () => {
    // Arrange: first attempt fails, backoff of 60s.
    BackgroundTaskManager.registerRunner("test-persistent-retry-clock", async () => {
      throw new Error("transient");
    });
    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-retry-clock",
      {},
      { retries: 1, initialDelayMs: 60_000 },
    );

    // Act
    expect(await (BackgroundTaskManager as any).pollAndExecute()).toBe(true);
    await BackgroundTaskManager.waitForAll();

    // Assert: pending again, ~60s ahead of the DB's now(), not claimable yet.
    const job = await getJobByIdQuery(jobId);
    expect(job.status).toBe("pending");
    expect(job.attempts).toBe(1);
    const [row] = await sql<{ seconds_ahead: number }[]>`
      SELECT EXTRACT(EPOCH FROM (run_at - (now() AT TIME ZONE 'utc')))::float8 AS seconds_ahead
      FROM background_jobs WHERE id = ${jobId}
    `;
    assert(row);
    expect(row.seconds_ahead).toBeGreaterThan(55);
    expect(row.seconds_ahead).toBeLessThanOrEqual(60);
    expect(await popNextBackgroundJobQuery(["test-persistent-retry-clock"])).toBeNull();
  });

  it("should let a job finish within the drain grace period", async () => {
    // Arrange: a short runner already in flight.
    BackgroundTaskManager.registerRunner("test-persistent-drain-ok", async () => {
      await new Promise((r) => setTimeout(r, 50));
    });
    const jobId = await BackgroundTaskManager.enqueue("test-persistent-drain-ok", {});
    expect(await (BackgroundTaskManager as any).pollAndExecute()).toBe(true);

    // Act
    const { activeCount, done } = BackgroundTaskManager.startDrain(2_000);
    const { timedOut } = await done;

    // Assert: completed normally, no new work accepted.
    expect(activeCount).toBe(1);
    expect(timedOut).toBe(false);
    expect((await getJobByIdQuery(jobId)).status).toBe("completed");
    expect(BackgroundTaskManager.getIsShuttingDown()).toBe(true);
    expect(BackgroundTaskManager.getIsWorkerRunning()).toBe(false);
  });

  it("should requeue a job that outlives the drain grace period", async () => {
    // Arrange: a runner that only stops when aborted, no retries (like cron jobs).
    BackgroundTaskManager.registerRunner("test-persistent-drain-late", async (_payload, signal) => {
      await new Promise((_resolve, reject) => {
        signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
      });
    });
    const jobId = await BackgroundTaskManager.enqueue(
      "test-persistent-drain-late",
      {},
      { retries: 0 },
    );
    expect(await (BackgroundTaskManager as any).pollAndExecute()).toBe(true);

    // Act
    const { timedOut } = await BackgroundTaskManager.startDrain(50).done;

    // Assert: interrupted, handed back, attempt refunded.
    expect(timedOut).toBe(true);
    const job = await getJobByIdQuery(jobId);
    expect(job.status).toBe("pending");
    expect(job.attempts).toBe(0);
    expect(job.error_message).toBe("Interrupted by shutdown");
  });
});
