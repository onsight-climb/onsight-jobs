import { describe, expect, it, beforeEach } from "bun:test";
import { BackgroundTaskManager } from "../src/background-task.js";

describe("BackgroundTaskManager", () => {
  beforeEach(() => {
    BackgroundTaskManager.resetForTest();
    BackgroundTaskManager.configure({ silent: true });
  });

  it("should execute a successful background task successfully", async () => {
    let executed = false;
    BackgroundTaskManager.run(async () => {
      executed = true;
    }, "test-success-task");

    await BackgroundTaskManager.waitForAll();
    expect(executed).toBe(true);
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
  });

  it("should contain errors in a failing background task and not crash", async () => {
    let executed = false;

    BackgroundTaskManager.run(async () => {
      executed = true;
      throw new Error("Task failed successfully");
    }, "test-failure-task");

    await BackgroundTaskManager.waitForAll();
    expect(executed).toBe(true);
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
  });

  it("should correctly track active tasks and wait for all to complete", async () => {
    let resolve1: any;
    let resolve2: any;

    const promise1 = new Promise((resolve) => {
      resolve1 = resolve;
    });

    const promise2 = new Promise((resolve) => {
      resolve2 = resolve;
    });

    let task1Finished = false;
    let task2Finished = false;

    BackgroundTaskManager.run(async () => {
      await promise1;
      task1Finished = true;
    }, "test-active-1");

    BackgroundTaskManager.run(async () => {
      await promise2;
      task2Finished = true;
    }, "test-active-2");

    expect(BackgroundTaskManager.getActiveCount()).toBe(2);

    // Resolve first task
    resolve1();
    // Wait slightly to let the event loop process the microtask
    await new Promise((r) => setTimeout(r, 10));

    expect(task1Finished).toBe(true);
    expect(task2Finished).toBe(false);
    expect(BackgroundTaskManager.getActiveCount()).toBe(1);

    // Resolve second task
    resolve2();
    await BackgroundTaskManager.waitForAll();

    expect(task2Finished).toBe(true);
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
  });

  it("should support task cancellation via AbortSignal", async () => {
    let resolveTask: any;
    const promise = new Promise((resolve) => {
      resolveTask = resolve;
    });

    let wasAborted = false;
    let taskCompleted = false;

    BackgroundTaskManager.run(async (signal) => {
      await promise;
      wasAborted = signal.aborted;
      taskCompleted = true;
    }, "cancellable-task");

    expect(BackgroundTaskManager.getActiveCount()).toBe(1);

    // Cancel task by name
    BackgroundTaskManager.cancel("cancellable-task");

    resolveTask();
    await BackgroundTaskManager.waitForAll();

    expect(taskCompleted).toBe(true);
    expect(wasAborted).toBe(true);
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
  });

  it("should cancel subsequent arbitrary tasks when an error occurs in a task", async () => {
    let resolveSubsequent: any;
    const promiseSubsequent = new Promise((resolve) => {
      resolveSubsequent = resolve;
    });

    let taskAFailed = false;
    let taskBAborted = false;

    // Task B is a subsequent task that runs in parallel/queue
    BackgroundTaskManager.run(async (signal) => {
      await promiseSubsequent;
      taskBAborted = signal.aborted;
    }, "task-B");

    // Task A starts, fails, and cancels task B
    BackgroundTaskManager.run(async () => {
      taskAFailed = true;
      // Trigger error scenario, and cancel task B
      BackgroundTaskManager.cancel("task-B");
      throw new Error("Fatal task A failure");
    }, "task-A");

    // Wait slightly to let task A complete and trigger cancellation
    await new Promise((r) => setTimeout(r, 10));

    resolveSubsequent();
    await BackgroundTaskManager.waitForAll();

    expect(taskAFailed).toBe(true);
    expect(taskBAborted).toBe(true);
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
  });

  it("should support exact cancellation of dynamic/interpolated task names", async () => {
    let resolve1: any;
    let resolve2: any;
    const promise1 = new Promise((resolve) => {
      resolve1 = resolve;
    });
    const promise2 = new Promise((resolve) => {
      resolve2 = resolve;
    });

    let task1Aborted = false;
    let task2Aborted = false;

    const id1 = "user-abc-123";
    const id2 = "user-xyz-456";

    BackgroundTaskManager.run(async (signal) => {
      await promise1;
      task1Aborted = signal.aborted;
    }, `send-email-${id1}`);

    BackgroundTaskManager.run(async (signal) => {
      await promise2;
      task2Aborted = signal.aborted;
    }, `send-email-${id2}`);

    // Cancel ONLY task 1
    BackgroundTaskManager.cancel(`send-email-${id1}`);

    resolve1();
    resolve2();
    await BackgroundTaskManager.waitForAll();

    expect(task1Aborted).toBe(true);
    expect(task2Aborted).toBe(false);
  });

  it("should support prefix wildcard cancellation of dynamic task names", async () => {
    let resolve1: any;
    let resolve2: any;
    let resolve3: any;
    const p1 = new Promise((resolve) => {
      resolve1 = resolve;
    });
    const p2 = new Promise((resolve) => {
      resolve2 = resolve;
    });
    const p3 = new Promise((resolve) => {
      resolve3 = resolve;
    });

    let t1Aborted = false;
    let t2Aborted = false;
    let t3Aborted = false;

    BackgroundTaskManager.run(async (signal) => {
      await p1;
      t1Aborted = signal.aborted;
    }, "job-export-csv-123");

    BackgroundTaskManager.run(async (signal) => {
      await p2;
      t2Aborted = signal.aborted;
    }, "job-export-pdf-456");

    BackgroundTaskManager.run(async (signal) => {
      await p3;
      t3Aborted = signal.aborted;
    }, "other-task-789");

    // Cancel all job exports
    BackgroundTaskManager.cancel("job-export-*");

    resolve1();
    resolve2();
    resolve3();
    await BackgroundTaskManager.waitForAll();

    expect(t1Aborted).toBe(true);
    expect(t2Aborted).toBe(true);
    expect(t3Aborted).toBe(false);
  });

  it("should support predicate function cancellation of dynamic task names", async () => {
    let resolve1: any;
    let resolve2: any;
    const p1 = new Promise((resolve) => {
      resolve1 = resolve;
    });
    const p2 = new Promise((resolve) => {
      resolve2 = resolve;
    });

    let t1Aborted = false;
    let t2Aborted = false;

    BackgroundTaskManager.run(async (signal) => {
      await p1;
      t1Aborted = signal.aborted;
    }, "notify-user-10");

    BackgroundTaskManager.run(async (signal) => {
      await p2;
      t2Aborted = signal.aborted;
    }, "notify-user-25");

    // Cancel any notify task where ID is greater than 20
    BackgroundTaskManager.cancel((name) => {
      const match = name.match(/notify-user-(\d+)/);
      if (!match) return false;
      const id = parseInt(match[1]!, 10);
      return id > 20;
    });

    resolve1();
    resolve2();
    await BackgroundTaskManager.waitForAll();

    expect(t1Aborted).toBe(false);
    expect(t2Aborted).toBe(true);
  });

  // ==========================================
  //         PRODUCTION FEATURES TESTS
  // ==========================================

  it("should enforce concurrency limits using a task queue", async () => {
    BackgroundTaskManager.configure({ maxConcurrency: 2 });

    let resolve1: any, resolve2: any, resolve3: any;
    const p1 = new Promise((r) => (resolve1 = r));
    const p2 = new Promise((r) => (resolve2 = r));
    const p3 = new Promise((r) => (resolve3 = r));

    let t1Run = false,
      t2Run = false,
      t3Run = false;

    BackgroundTaskManager.run(async () => {
      t1Run = true;
      await p1;
    }, "task-1");
    BackgroundTaskManager.run(async () => {
      t2Run = true;
      await p2;
    }, "task-2");
    BackgroundTaskManager.run(async () => {
      t3Run = true;
      await p3;
    }, "task-3");

    // Wait for tasks to start
    await new Promise((r) => setTimeout(r, 10));

    expect(t1Run).toBe(true);
    expect(t2Run).toBe(true);
    expect(t3Run).toBe(false); // Should be queued since maxConcurrency is 2
    expect(BackgroundTaskManager.getActiveCount()).toBe(2);
    expect(BackgroundTaskManager.getQueueCount()).toBe(1);

    // Resolve task 1 to free a slot
    resolve1();
    await new Promise((r) => setTimeout(r, 15));

    expect(t3Run).toBe(true); // Should have started now
    expect(BackgroundTaskManager.getActiveCount()).toBe(2);
    expect(BackgroundTaskManager.getQueueCount()).toBe(0);

    resolve2();
    resolve3();
    await BackgroundTaskManager.waitForAll();

    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
  });

  it("should abort tasks that exceed their execution timeout limit", async () => {
    let wasAborted = false;
    let promiseResolved = false;

    BackgroundTaskManager.run(
      async (signal) => {
        try {
          await new Promise((resolve, reject) => {
            const onAbort = () =>
              reject(new DOMException("Aborted", "AbortError"));
            signal.addEventListener("abort", onAbort);
            setTimeout(() => {
              signal.removeEventListener("abort", onAbort);
              resolve(true);
            }, 100); // Takes 100ms
          });
          promiseResolved = true;
        } catch (err: any) {
          if (err.name === "AbortError") wasAborted = true;
        }
      },
      { name: "timeout-task", timeoutMs: 20 }, // Fails because of 20ms timeout limit
    );

    await BackgroundTaskManager.waitForAll();

    expect(wasAborted).toBe(true);
    expect(promiseResolved).toBe(false);
  });

  it("should retry failing tasks with exponential backoff", async () => {
    let attemptsCount = 0;
    const startTime = performance.now();

    BackgroundTaskManager.run(
      async () => {
        attemptsCount++;
        throw new Error("Temporary failure");
      },
      {
        name: "retry-task",
        retries: 2,
        initialDelayMs: 20, // 20ms initial delay, retry 1 after 20ms, retry 2 after 40ms
      },
    );

    await BackgroundTaskManager.waitForAll();
    const duration = performance.now() - startTime;

    expect(attemptsCount).toBe(3); // 1 initial + 2 retries
    expect(duration).toBeGreaterThan(60); // Total backoff delay is at least 20ms + 40ms = 60ms
  });

  it("should not retry tasks if they are cancelled", async () => {
    let attemptsCount = 0;

    BackgroundTaskManager.run(
      async (signal) => {
        attemptsCount++;
        if (attemptsCount === 1) {
          // Cancel it after first failure
          BackgroundTaskManager.cancel("no-retry-cancelled");
        }
        throw new Error("Failed");
      },
      {
        name: "no-retry-cancelled",
        retries: 3,
        initialDelayMs: 10,
      },
    );

    await BackgroundTaskManager.waitForAll();

    expect(attemptsCount).toBe(1); // Should only run 1 time because of cancellation
  });

  it("should execute graceful shutdown successfully", async () => {
    let resolve1: any;
    const p1 = new Promise((r) => (resolve1 = r));

    let activeTaskFinished = false;
    let queuedTaskRun = false;
    let newQueuedTaskRun = false;

    BackgroundTaskManager.configure({ maxConcurrency: 1 });

    // 1. Active task
    BackgroundTaskManager.run(async (signal) => {
      await p1;
      activeTaskFinished = !signal.aborted;
    }, "active-shutdown-task");

    // 2. Queued task (waiting in queue)
    BackgroundTaskManager.run(async () => {
      queuedTaskRun = true;
    }, "queued-shutdown-task");

    expect(BackgroundTaskManager.getActiveCount()).toBe(1);
    expect(BackgroundTaskManager.getQueueCount()).toBe(1);

    // Trigger shutdown
    const shutdownPromise = BackgroundTaskManager.shutdown();

    // 3. Try to add a new task during shutdown
    BackgroundTaskManager.run(async () => {
      newQueuedTaskRun = true;
    }, "new-shutdown-task");

    resolve1();
    await shutdownPromise;

    expect(activeTaskFinished).toBe(false); // Active task is aborted on shutdown
    expect(queuedTaskRun).toBe(false); // Queued task is cleared and not executed
    expect(newQueuedTaskRun).toBe(false); // New task is rejected
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
    expect(BackgroundTaskManager.getQueueCount()).toBe(0);
  });

  it("should handle a massive load of tasks under strict concurrency limits (Stress Test)", async () => {
    const totalTasks = 500;
    const maxConcurrency = 15;
    BackgroundTaskManager.configure({ maxConcurrency });

    const activeCountHistory: number[] = [];
    let completedCount = 0;

    // Launch all 500 tasks simultaneously
    for (let i = 0; i < totalTasks; i++) {
      BackgroundTaskManager.run(
        async () => {
          // Record current active task count to check concurrency limits
          const active = BackgroundTaskManager.getActiveCount();
          activeCountHistory.push(active);

          // Simulate some randomized workload duration (1-5ms)
          const delay = Math.floor(Math.random() * 5) + 1;
          await new Promise((resolve) => setTimeout(resolve, delay));

          completedCount++;
        },
        { name: `stress-task-${i}` },
      );
    }

    // Await all tasks to finish
    await BackgroundTaskManager.waitForAll();

    // 1. Verify all launched tasks completed successfully
    expect(completedCount).toBe(totalTasks);

    // 2. Verify that at no point the number of active tasks exceeded the configured limit
    const exceededLimit = activeCountHistory.some(
      (count) => count > maxConcurrency,
    );
    expect(exceededLimit).toBe(false);

    // 3. Verify that the queue count and active counts are reset to 0
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
    expect(BackgroundTaskManager.getQueueCount()).toBe(0);
  });

  // ==========================================
  //         DELAYED TASKS TESTS
  // ==========================================

  it("should execute a task after a specified delay", async () => {
    let executed = false;
    const startTime = performance.now();

    BackgroundTaskManager.run(
      async () => {
        executed = true;
      },
      { name: "delayed-test-task", delayMs: 40 },
    );

    // Immediately it should not be active, but in the delayed queue
    expect(executed).toBe(false);
    expect(BackgroundTaskManager.getDelayedCount()).toBe(1);
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);

    // Wait slightly but less than the delay
    await new Promise((r) => setTimeout(r, 15));
    expect(executed).toBe(false);

    // Wait for it to complete
    await BackgroundTaskManager.waitForAll();
    const elapsed = performance.now() - startTime;

    expect(executed).toBe(true);
    expect(elapsed).toBeGreaterThanOrEqual(40);
    expect(BackgroundTaskManager.getDelayedCount()).toBe(0);
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
  });

  it("should support cancelling a delayed task before execution", async () => {
    let executed = false;

    BackgroundTaskManager.run(
      async () => {
        executed = true;
      },
      { name: "cancel-delayed-task", delayMs: 50 },
    );

    expect(BackgroundTaskManager.getDelayedCount()).toBe(1);

    // Cancel before the delay runs out
    BackgroundTaskManager.cancel("cancel-delayed-task");

    await BackgroundTaskManager.waitForAll();

    expect(executed).toBe(false);
    expect(BackgroundTaskManager.getDelayedCount()).toBe(0);
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
  });

  it("should clean up delayed tasks on shutdown", async () => {
    let executed = false;

    BackgroundTaskManager.run(
      async () => {
        executed = true;
      },
      { name: "shutdown-delayed-task", delayMs: 100 },
    );

    expect(BackgroundTaskManager.getDelayedCount()).toBe(1);

    // Call shutdown
    await BackgroundTaskManager.shutdown();

    expect(executed).toBe(false);
    expect(BackgroundTaskManager.getDelayedCount()).toBe(0);
    expect(BackgroundTaskManager.getActiveCount()).toBe(0);
  });

  it("should check if tasks exist using has() with exact name, prefix, and predicate matching", async () => {
    // 1. Initially should be false for any non-existent task
    expect(BackgroundTaskManager.has("test-has-*")).toBe(false);
    expect(BackgroundTaskManager.has("test-has-delayed")).toBe(false);

    // 2. Schedule a delayed task
    BackgroundTaskManager.run(async () => {}, {
      name: "test-has-delayed",
      delayMs: 100,
    });

    expect(BackgroundTaskManager.has("test-has-delayed")).toBe(true);
    expect(BackgroundTaskManager.has("test-has-*")).toBe(true);
    expect(BackgroundTaskManager.has((name) => name.includes("delayed"))).toBe(
      true,
    );
    expect(BackgroundTaskManager.has("non-existent")).toBe(false);

    // 3. Setup concurrency limits to queue a task
    BackgroundTaskManager.configure({ maxConcurrency: 1 });

    let resolveActive: any;
    const activePromise = new Promise((resolve) => {
      resolveActive = resolve;
    });

    BackgroundTaskManager.run(async () => {
      await activePromise;
    }, "test-has-active");

    BackgroundTaskManager.run(async () => {}, "test-has-queued");

    // Check active
    expect(BackgroundTaskManager.has("test-has-active")).toBe(true);
    // Check queued
    expect(BackgroundTaskManager.has("test-has-queued")).toBe(true);
    // Check non-existent
    expect(BackgroundTaskManager.has("test-has-other")).toBe(false);

    // Resolve active to finish everything
    resolveActive();
    await BackgroundTaskManager.waitForAll();

    // After completion, all should be false
    expect(BackgroundTaskManager.has("test-has-active")).toBe(false);
    expect(BackgroundTaskManager.has("test-has-queued")).toBe(false);
    expect(BackgroundTaskManager.has("test-has-delayed")).toBe(false);
  });
});

// Arbitrary thrown values must reach the same retry policy as Error instances.
it("retries a null rejection without failing inside error reporting", async () => {
  BackgroundTaskManager.resetForTest();
  BackgroundTaskManager.configure({ silent: true });
  let attempts = 0;
  BackgroundTaskManager.run(
    async () => {
      attempts++;
      throw null;
    },
    { name: "null-rejection", retries: 2, initialDelayMs: 1 },
  );
  await BackgroundTaskManager.waitForAll();
  expect(attempts).toBe(3);
  expect(BackgroundTaskManager.getActiveCount()).toBe(0);
});
