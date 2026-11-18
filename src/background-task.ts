import {
  SHUTDOWN_ABORT_MESSAGE,
  isAbortError,
  taskErrorMessage,
  type BackgroundJob,
} from "./model.js";
import {
  enqueueJobQuery,
  popNextBackgroundJobQuery,
  markJobCompletedQuery,
  scheduleJobRetryQuery,
  markJobFailedQuery,
  resetStuckRunningJobsQuery,
  requeueInterruptedJobQuery,
} from "./queries.js";

/** Persistent payloads cross a serialization boundary and must be validated by each runner. */
export type TaskRunner = (
  payload: unknown,
  signal: AbortSignal,
) => Promise<unknown>;

export interface EnqueueOptions {
  /** Delay in milliseconds before task execution starts. Default: 0 */
  delayMs?: number;
  /** Number of retries in case of failure. Default: 0 */
  retries?: number;
  /** Initial delay for exponential backoff in milliseconds. Default: 1000 */
  initialDelayMs?: number;
  /** Maximum execution time in milliseconds. Default: 30000 */
  timeoutMs?: number;
}

export interface TaskOptions {
  /** Descriptive name of the task for logging/cancellation */
  name?: string;
  /** Maximum execution time in milliseconds. Default: 30000 (30 seconds). Use 0 for no timeout. */
  timeoutMs?: number;
  /** Number of retries in case of failure. Default: 0 */
  retries?: number;
  /** Initial delay for exponential backoff in milliseconds. Default: 1000 */
  initialDelayMs?: number;
  /** Arbitrary delay in milliseconds before task execution starts. Default: 0 */
  delayMs?: number;
}

interface QueuedTask {
  task: (signal: AbortSignal) => Promise<unknown>;
  options: Required<TaskOptions>;
  controller: AbortController;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
}

export interface ActiveTask {
  promise: Promise<unknown>;
  name: string;
  controller: AbortController;
}

/** Outcome of the first (synchronous) half of a drain, plus the pending second half. */
export interface DrainHandle {
  /** Tasks running at the moment polling stopped. */
  activeCount: number;
  /** Resolves once in-flight work finished or was aborted at the end of the grace period. */
  done: Promise<DrainResult>;
}

export interface DrainResult {
  /** True when the grace period elapsed and the remaining tasks had to be aborted. */
  timedOut: boolean;
}

interface DelayedTask {
  name: string;
  controller: AbortController;
  timeoutId: ReturnType<typeof setTimeout> | undefined;
  reject: (reason: unknown) => void;
}

/**
 * BackgroundTaskManager handles safe, production-grade execution of non-blocking asynchronous tasks.
 * It features concurrency limits (queue), automatic retries with exponential backoff,
 * execution timeouts, task cancellation via AbortSignal, and graceful process shutdown.
 */
export class BackgroundTaskManager {
  private static activeTasks = new Set<ActiveTask>();
  private static delayedTasks = new Set<DelayedTask>();
  private static pendingPromises = new Set<Promise<unknown>>();
  private static taskQueue: QueuedTask[] = [];
  private static maxConcurrency = 50; // Default max parallel tasks (increased to 50 for production-grade throughput)
  private static isShuttingDown = false;
  private static silent = false; // Flag to silence task logging (e.g. during test suites)
  private static runners = new Map<string, TaskRunner>();
  private static workerTimer: ReturnType<typeof setTimeout> | null = null;
  private static isWorkerRunning = false;
  private static workerPollIntervalMs = 5000;
  private static workerStarted = false; // Tracks if startWorker was explicitly called
  /** The poll tick currently awaiting the DB, if any: shutdown waits for it (bounded). */
  private static currentTick: Promise<void> | null = null;
  /**
   * Persistent jobs this process claimed ('running' in the DB) whose wrapper has not settled.
   * A job stays here if its runner ignores the abort, or if it was still queued in memory when
   * shutdown rejected the queue: shutdown requeues whatever is left so no row stays 'running'.
   */
  private static claimedJobIds = new Set<string>();
  /** Upper bound on how long shutdown() waits for aborted tasks before requeueing leftovers. */
  private static shutdownHardTimeoutMs = 5000;

  static getIsWorkerRunning(): boolean {
    return this.isWorkerRunning;
  }

  static getIsShuttingDown(): boolean {
    return this.isShuttingDown;
  }

  static setShuttingDown(val: boolean): void {
    this.isShuttingDown = val;
  }

  /**
   * True when `signal` was aborted by `shutdown()` (grace period over), as opposed to a task
   * timeout or an explicit cancel(). Checking the abort reason keeps a timed-out task failing
   * normally even if it happens to time out while the process is draining.
   */
  static isShutdownInterruption(signal: AbortSignal): boolean {
    if (!this.isShuttingDown || !signal.aborted) return false;
    const reason: unknown = signal.reason;
    return (
      typeof reason === "object" &&
      reason !== null &&
      "message" in reason &&
      reason.message === SHUTDOWN_ABORT_MESSAGE
    );
  }

  /**
   * Helper to write logs unless logging has been silenced.
   */
  private static log(
    method: "log" | "warn" | "error",
    ...args: unknown[]
  ): void {
    if (this.silent) return;
    console[method](...args);
  }

  /**
   * Configures the global background task manager options.
   */
  static configure(options: {
    maxConcurrency?: number;
    silent?: boolean;
    /** Max wait (ms) in shutdown() for aborted tasks before requeueing leftovers. Default: 5000 */
    shutdownHardTimeoutMs?: number;
  }): void {
    if (options.maxConcurrency !== undefined) {
      this.maxConcurrency = options.maxConcurrency;
      this.processQueue();
    }
    if (options.shutdownHardTimeoutMs !== undefined) {
      this.shutdownHardTimeoutMs = options.shutdownHardTimeoutMs;
    }
    if (options.silent !== undefined) {
      this.silent = options.silent;
    }
  }

  /**
   * Runs an asynchronous task in the background.
   * Catches all errors, manages retries, enforces timeouts, controls concurrency,
   * and tracks active tasks. Returns void to prevent unhandled promise rejections on the caller side.
   *
   * @param task An async function to execute. It receives an AbortSignal.
   * @param optionsOrName Configuration options or the descriptive task name string.
   */
  static run<T>(
    task: (signal: AbortSignal) => Promise<T>,
    optionsOrName: TaskOptions | string = {},
  ): void {
    const options: Required<TaskOptions> = {
      name:
        typeof optionsOrName === "string"
          ? optionsOrName
          : (optionsOrName.name ?? "unnamed-task"),
      timeoutMs:
        typeof optionsOrName === "object" &&
        optionsOrName.timeoutMs !== undefined
          ? optionsOrName.timeoutMs
          : 30000,
      retries:
        typeof optionsOrName === "object" && optionsOrName.retries !== undefined
          ? optionsOrName.retries
          : 0,
      initialDelayMs:
        typeof optionsOrName === "object" &&
        optionsOrName.initialDelayMs !== undefined
          ? optionsOrName.initialDelayMs
          : 1000,
      delayMs:
        typeof optionsOrName === "object" && optionsOrName.delayMs !== undefined
          ? optionsOrName.delayMs
          : 0,
    };

    if (this.isShuttingDown) {
      this.log(
        "warn",
        `\x1b[33m[BACKGROUND TASK REJECTED]\x1b[0m Cannot accept new task "${options.name}" because manager is shutting down.`,
      );
      return;
    }

    const controller = new AbortController();

    // Wrap in a promise to manage queue resolution/rejection cleanly
    const p = new Promise<unknown>((resolve, reject) => {
      if (options.delayMs > 0) {
        const delayedTask: DelayedTask = {
          name: options.name,
          controller,
          timeoutId: undefined,
          reject,
        };

        const execute = () => {
          this.delayedTasks.delete(delayedTask);
          if (controller.signal.aborted) {
            reject(
              controller.signal.reason ||
                new DOMException("Operation aborted", "AbortError"),
            );
            return;
          }
          this.taskQueue.push({
            task,
            options,
            controller,
            resolve,
            reject,
          });
          this.processQueue();
        };

        delayedTask.timeoutId = setTimeout(execute, options.delayMs);
        this.delayedTasks.add(delayedTask);
      } else {
        this.taskQueue.push({
          task,
          options,
          controller,
          resolve,
          reject,
        });
        this.processQueue();
      }
    });

    this.pendingPromises.add(p);
    p.finally(() => {
      this.pendingPromises.delete(p);
    }).catch(() => {
      // Catch queue promise rejections globally to prevent unhandled promise rejections
    });
  }

  /**
   * Processes queued tasks up to the maximum concurrency limit.
   */
  private static processQueue(): void {
    while (
      this.activeTasks.size < this.maxConcurrency &&
      this.taskQueue.length > 0
    ) {
      const queued = this.taskQueue.shift();
      if (!queued) break;

      const { task, options, controller, resolve, reject } = queued;

      if (controller.signal.aborted) {
        reject(
          controller.signal.reason ||
            new DOMException("Operation aborted", "AbortError"),
        );
        continue;
      }

      const signal = controller.signal;
      const startTime = performance.now();

      this.log("log", `\x1b[36m[BACKGROUND TASK START]\x1b[0m ${options.name}`);

      // Set execution timeout if specified
      let timeoutId: ReturnType<typeof setTimeout> | undefined;
      if (options.timeoutMs > 0) {
        timeoutId = setTimeout(() => {
          this.log(
            "warn",
            `\x1b[33m[BACKGROUND TASK TIMEOUT]\x1b[0m ${options.name} exceeded ${options.timeoutMs}ms limit. Aborting...`,
          );
          controller.abort(new DOMException("Task timed out", "AbortError"));
        }, options.timeoutMs);
      }

      // Declare taskObject shape first
      const taskObject: ActiveTask = {
        name: options.name,
        controller,
        promise: Promise.resolve(),
      };

      // Add to active tasks set BEFORE running the async generator callback
      // This perfectly prevents race conditions when task calls BackgroundTaskManager.cancel() synchronously
      this.activeTasks.add(taskObject);

      taskObject.promise = (async () => {
        let attempt = 0;

        while (true) {
          try {
            if (signal.aborted) {
              throw (
                signal.reason ||
                new DOMException("Operation aborted", "AbortError")
              );
            }

            const result = await task(signal);

            if (timeoutId) clearTimeout(timeoutId);

            if (signal.aborted) {
              this.log(
                "log",
                `\x1b[33m[BACKGROUND TASK CANCELLED]\x1b[0m ${options.name} aborted before final completion`,
              );
              reject(
                signal.reason ||
                  new DOMException("Operation aborted", "AbortError"),
              );
              return;
            }

            const duration = (performance.now() - startTime).toFixed(2);
            this.log(
              "log",
              `\x1b[32m[BACKGROUND TASK SUCCESS]\x1b[0m ${options.name} completed in ${duration}ms`,
            );
            resolve(result);
            return;
          } catch (error: unknown) {
            const isAbort = signal.aborted || isAbortError(error);

            if (isAbort) {
              if (timeoutId) clearTimeout(timeoutId);
              const duration = (performance.now() - startTime).toFixed(2);
              this.log(
                "log",
                `\x1b[33m[BACKGROUND TASK CANCELLED]\x1b[0m ${options.name} aborted after ${duration}ms`,
              );
              reject(error);
              return;
            }

            // Retry logic
            if (attempt < options.retries) {
              if (signal.aborted) {
                throw (
                  signal.reason ||
                  new DOMException("Operation aborted", "AbortError")
                );
              }

              attempt++;
              const delay = options.initialDelayMs * Math.pow(2, attempt - 1);
              this.log(
                "warn",
                `\x1b[33m[BACKGROUND TASK RETRY]\x1b[0m ${options.name} failed (Attempt ${attempt}/${options.retries}). Retrying in ${delay}ms... Error: ${taskErrorMessage(error)}`,
              );

              // Wait for delay or until aborted
              await new Promise<void>((r, rej) => {
                const onAbort = () => {
                  clearTimeout(retryTimer);
                  signal.removeEventListener("abort", onAbort);
                  rej(
                    signal.reason ||
                      new DOMException("Operation aborted", "AbortError"),
                  );
                };

                const retryTimer = setTimeout(() => {
                  signal.removeEventListener("abort", onAbort);
                  r();
                }, delay);

                signal.addEventListener("abort", onAbort);
              }).catch((abortErr) => {
                error = abortErr;
              });

              continue;
            }

            // Final error handling
            if (timeoutId) clearTimeout(timeoutId);
            const duration = (performance.now() - startTime).toFixed(2);
            this.log(
              "error",
              `\x1b[31m[BACKGROUND TASK ERROR]\x1b[0m ${options.name} failed after ${duration}ms (Attempts: ${attempt + 1}):`,
              error,
            );
            reject(error);
            return;
          }
        }
      })().finally(() => {
        if (timeoutId) clearTimeout(timeoutId);
        this.activeTasks.delete(taskObject);
        this.processQueue();
      });
    }
  }

  /**
   * Checks if a background task matching a name, a prefix wildcard, or a predicate function exists.
   *
   * @param match The name, wildcard pattern, or predicate to match tasks.
   * @returns True if a matching task is active, queued, or delayed; false otherwise.
   */
  static has(match: string | ((name: string) => boolean)): boolean {
    const shouldMatch =
      typeof match === "function"
        ? match
        : (name: string) => {
            if (match.endsWith("*")) {
              const prefix = match.slice(0, -1);
              return name.startsWith(prefix);
            }
            return name === match;
          };

    // Check active tasks
    for (const task of this.activeTasks) {
      if (shouldMatch(task.name)) {
        return true;
      }
    }

    // Check delayed tasks
    for (const task of this.delayedTasks) {
      if (shouldMatch(task.name)) {
        return true;
      }
    }

    // Check queued tasks
    for (const queued of this.taskQueue) {
      if (shouldMatch(queued.options.name)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Cancels active background tasks matching a name, a prefix wildcard, or a predicate function.
   *
   * @param match The name, wildcard pattern, or predicate to match tasks for cancellation.
   */
  static cancel(match: string | ((name: string) => boolean)): void {
    let cancelledCount = 0;

    const shouldCancel =
      typeof match === "function"
        ? match
        : (name: string) => {
            if (match.endsWith("*")) {
              const prefix = match.slice(0, -1);
              return name.startsWith(prefix);
            }
            return name === match;
          };

    // 1. Cancel active tasks
    for (const task of this.activeTasks) {
      if (shouldCancel(task.name)) {
        task.controller.abort(
          new DOMException("Task cancelled by manager", "AbortError"),
        );
        cancelledCount++;
      }
    }

    // 2. Cancel delayed tasks (prevent them from running entirely, clear timeout)
    for (const task of this.delayedTasks) {
      if (shouldCancel(task.name)) {
        clearTimeout(task.timeoutId);
        task.controller.abort(
          new DOMException("Task cancelled by manager", "AbortError"),
        );
        task.reject(
          new DOMException("Task cancelled by manager", "AbortError"),
        );
        this.delayedTasks.delete(task);
        cancelledCount++;
      }
    }

    // 3. Cancel queued tasks (they'll be skipped when shifted in processQueue)
    for (const queued of this.taskQueue) {
      if (shouldCancel(queued.options.name)) {
        queued.controller.abort(
          new DOMException("Task cancelled by manager", "AbortError"),
        );
        cancelledCount++;
      }
    }

    if (cancelledCount > 0) {
      const matchDescription =
        typeof match === "function" ? "predicate" : `"${match}"`;
      this.log(
        "log",
        `\x1b[33m[BACKGROUND TASK CANCEL]\x1b[0m Triggered cancellation for ${cancelledCount} task(s) matching ${matchDescription}`,
      );
    }
  }

  /**
   * Returns the count of currently running background tasks.
   */
  static getActiveCount(): number {
    return this.activeTasks.size;
  }

  /**
   * Returns the number of currently queued tasks waiting for execution.
   */
  static getQueueCount(): number {
    return this.taskQueue.length;
  }

  /**
   * Returns the count of currently delayed tasks waiting for execution.
   */
  static getDelayedCount(): number {
    return this.delayedTasks.size;
  }

  /**
   * Awaits all currently active background tasks until they finish.
   */
  static async waitForAll(): Promise<void> {
    while (this.pendingPromises.size > 0) {
      const promises = Array.from(this.pendingPromises).map((p) =>
        p.catch(() => {}),
      );
      await Promise.all(promises);
    }
  }

  /**
   * Registers a task runner for a persistent background task.
   */
  static registerRunner(name: string, runner: TaskRunner): void {
    this.runners.set(name, runner);
  }

  static unregisterRunner(name: string): void {
    this.runners.delete(name);
  }

  static getRegisteredRunnerNames(): string[] {
    return Array.from(this.runners.keys());
  }

  /**
   * Enqueues a persistent background task in PostgreSQL (UTC time).
   *
   * @param name The registered name of the runner.
   * @param payload JSON serializable arguments for the task.
   * @param options Configuration for delay, retries, timeout.
   * @returns The UUID of the created database job.
   */
  static async enqueue(
    name: string,
    payload: Record<string, unknown>,
    options: EnqueueOptions = {},
  ): Promise<string> {
    const delayMs = options.delayMs ?? 0;
    const retries = options.retries ?? 0;
    const initialDelayMs = options.initialDelayMs ?? 1000;
    const timeoutMs = options.timeoutMs ?? 30000;

    try {
      // run_at is derived from the DB clock inside the query: the host clock can drift from
      // the DB's, and the poller compares run_at against the DB's now().
      const jobId = await enqueueJobQuery(
        name,
        payload,
        delayMs,
        retries,
        initialDelayMs,
        timeoutMs,
      );

      this.log(
        "log",
        `\x1b[36m[PERSISTENT TASK ENQUEUED]\x1b[0m ${name} (Job ID: ${jobId}) to run in ${delayMs}ms`,
      );

      // Do not start workers implicitly in processes that only enqueue jobs.
      if (this.workerStarted && !this.workerTimer && !this.isShuttingDown) {
        this.startWorker(this.workerPollIntervalMs);
      }

      return jobId;
    } catch (error) {
      this.log("error", `Failed to enqueue persistent task "${name}":`, error);
      throw error;
    }
  }

  /**
   * Starts the polling worker to fetch and process persistent jobs from the database.
   *
   * @param pollIntervalMs Interval in milliseconds to poll the database. Default: 5000.
   */
  static startWorker(pollIntervalMs = 5000): void {
    this.workerStarted = true;
    if (this.isWorkerRunning || this.workerTimer) return;
    this.isWorkerRunning = true;
    this.workerPollIntervalMs = pollIntervalMs;

    // Backend and async share the queue, but each recovers its own task types.
    resetStuckRunningJobsQuery(Array.from(this.runners.keys())).catch(
      (err: unknown) => {
        this.log(
          "error",
          "Failed to reset stuck running jobs on startup:",
          err,
        );
      },
    );

    const tick = async (): Promise<void> => {
      if (!this.isWorkerRunning || this.isShuttingDown) return;
      this.workerTimer = null;
      try {
        let foundJob = false;
        do {
          if (
            this.activeTasks.size + this.taskQueue.length >=
            this.maxConcurrency
          ) {
            break;
          }
          foundJob = await this.pollAndExecute();
        } while (foundJob && this.isWorkerRunning && !this.isShuttingDown);

        // An empty due-job result can hide delayed work (future run_at, backoff retries) or
        // rows inserted by other processes. Keep polling until explicitly stopped: an idle
        // auto-stop would strand them until the next in-process enqueue.
      } catch (err) {
        this.log("error", "Error in persistent queue worker tick:", err);
      }
      if (this.isWorkerRunning && !this.isShuttingDown) {
        this.workerTimer = setTimeout(runTick, pollIntervalMs);
      }
    };

    // Track the in-flight tick so shutdown can wait for it: a tick that already claimed a job
    // must get to hand it back before the process exits.
    const runTick = (): void => {
      const current = tick();
      this.currentTick = current;
      void current.finally(() => {
        if (this.currentTick === current) this.currentTick = null;
      });
    };

    this.workerTimer = setTimeout(runTick, pollIntervalMs);
    this.log(
      "log",
      `\x1b[32m[PERSISTENT QUEUE WORKER]\x1b[0m Worker started with poll interval of ${pollIntervalMs}ms`,
    );
  }

  /**
   * Stops the polling worker.
   */
  static stopWorker(): void {
    this.workerStarted = false;
    if (this.workerTimer) {
      clearTimeout(this.workerTimer);
      this.workerTimer = null;
    }
    this.isWorkerRunning = false;
    this.log("log", `\x1b[33m[PERSISTENT QUEUE WORKER]\x1b[0m Worker stopped`);
  }

  /**
   * Polls the next pending job from the database, executes it via in-memory run(),
   * and updates its status to 'completed' or 'failed' based on the result.
   */
  private static async pollAndExecute(): Promise<boolean> {
    if (this.activeTasks.size + this.taskQueue.length >= this.maxConcurrency) {
      return false; // Avoid overloading the in-memory queues (backpressure)
    }

    let job: BackgroundJob | null;
    try {
      job = await popNextBackgroundJobQuery(Array.from(this.runners.keys()));
    } catch (popError) {
      this.log(
        "error",
        "Error polling for next persistent background job:",
        popError,
      );
      return false;
    }

    if (!job) {
      return false; // No pending jobs ready
    }

    // A tick that was awaiting the DB when shutdown began can still claim a job. run() would
    // silently drop it and leave the row 'running', so hand it back untouched instead.
    if (this.isShuttingDown) {
      try {
        await requeueInterruptedJobQuery(job.id);
      } catch (requeueError) {
        this.log(
          "error",
          `Failed to requeue job ${job.id} claimed during shutdown:`,
          requeueError,
        );
      }
      return false;
    }

    const {
      id,
      name,
      payload,
      attempts,
      max_retries,
      initial_delay_ms,
      timeout_ms,
    } = job;

    const runner = this.runners.get(name);
    if (!runner) {
      const errorMsg = `No registered TaskRunner found for persistent task "${name}"`;
      this.log("error", errorMsg);

      // Mark the job as failed in the DB
      try {
        await markJobFailedQuery(id, errorMsg);
      } catch (updateError) {
        this.log("error", `Failed to mark job ${id} as failed:`, updateError);
      }
      return false;
    }

    // Tracked until the wrapper below settles; see claimedJobIds.
    this.claimedJobIds.add(id);

    // Run the task using BackgroundTaskManager.run() to inherit all execution logic (concurrency, logging, timeouts)
    BackgroundTaskManager.run(
      async (signal) => {
        try {
          // Execute the registered runner
          await runner(payload, signal);

          // If successful, update the DB status to 'completed'
          try {
            await markJobCompletedQuery(id);
          } catch (updateError) {
            this.log(
              "error",
              `Failed to mark job ${id} as completed:`,
              updateError,
            );
          }
        } catch (err: unknown) {
          const errMsg = taskErrorMessage(err);

          // Shutdown aborted this run: the task did not fail, the process is going away.
          // Requeue it (attempt refunded) so the next worker start picks it up; a cron-triggered
          // job has max_retries = 0 and its cron row already advanced, so failing it here
          // would silently lose that occurrence. A timeout abort still follows retry/fail below.
          if (this.isShutdownInterruption(signal)) {
            this.log(
              "warn",
              `Persistent task "${name}" (${id}) interrupted by shutdown on attempt ${attempts}. Requeueing.`,
            );
            try {
              await requeueInterruptedJobQuery(id);
            } catch (updateError) {
              this.log(
                "error",
                `Failed to requeue interrupted job ${id}:`,
                updateError,
              );
            }
            throw err;
          }

          // Check if we have remaining retries
          if (attempts <= max_retries) {
            // Calculate exponential backoff delay in milliseconds
            const backoffDelay = initial_delay_ms * Math.pow(2, attempts - 1);

            this.log(
              "warn",
              `Persistent task "${name}" (${id}) failed on attempt ${attempts}/${max_retries + 1}. Retrying in ${backoffDelay}ms. Error: ${errMsg}`,
            );

            // Re-schedule the task to 'pending'; the query derives run_at from the DB clock.
            try {
              await scheduleJobRetryQuery(id, backoffDelay, errMsg);
            } catch (updateError) {
              this.log(
                "error",
                `Failed to schedule retry for job ${id}:`,
                updateError,
              );
            }
          } else {
            this.log(
              "error",
              `Persistent task "${name}" (${id}) failed final attempt ${attempts}/${max_retries + 1}. Mark as failed. Error: ${errMsg}`,
            );

            // Final failure: set status to 'failed'
            try {
              await markJobFailedQuery(id, errMsg);
            } catch (updateError) {
              this.log(
                "error",
                `Failed to mark job ${id} as failed:`,
                updateError,
              );
            }
          }
          throw err;
        } finally {
          this.claimedJobIds.delete(id);
        }
      },
      {
        name: `persistent-${name}-${id}`,
        timeoutMs: timeout_ms,
        retries: 0, // Database handles retries itself via exponential backoff, so in-memory retry is 0
      },
    );

    return true;
  }

  /**
   * Single implementation of "stop taking work, let in-flight work finish, abort the rest".
   *
   * Stops queue polling and refuses new tasks right away (otherwise the poller would keep
   * claiming jobs that the abort then cuts short), waits for in-flight tasks up to `graceMs`,
   * then calls `shutdown()`. Persistent jobs still running at that point are aborted and
   * requeued without charging the attempt (see `requeueInterruptedJobQuery`); jobs that finish
   * within the grace period simply complete, so a routine deploy loses and repeats nothing.
   *
   * Callers that own other pollers (e.g. a cron scheduler) must stop those first.
   */
  static startDrain(graceMs: number): DrainHandle {
    // Goes through the public API (not the private fields) so consumers can stub each step.
    this.stopWorker();
    this.setShuttingDown(true);

    const activeCount = this.getActiveCount();

    const done = (async (): Promise<DrainResult> => {
      let graceTimer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<"timeout">((resolve) => {
        graceTimer = setTimeout(() => resolve("timeout"), graceMs);
      });
      const drained = (async () => {
        // A tick mid-claim hands its job back once it sees the flag; wait for it as well.
        if (this.currentTick) await this.currentTick;
        await this.waitForAll();
        return "drained" as const;
      })();
      const outcome = await Promise.race([drained, timeout]);
      clearTimeout(graceTimer);

      // Also runs after a clean drain to release timers and queued/delayed tasks.
      await this.shutdown();
      return { timedOut: outcome === "timeout" };
    })();

    return { activeCount, done };
  }

  /**
   * Gracefully shuts down the background task manager.
   * Stops accepting new tasks, clears the queued tasks, and cancels all currently active tasks.
   */
  static async shutdown(): Promise<void> {
    // No early return when already shutting down: the drain phase sets the flag first (to
    // refuse new work) and this call must still abort whatever outlived the grace period.
    this.isShuttingDown = true;

    this.stopWorker();

    const queuedCount = this.taskQueue.length;
    const delayedCount = this.delayedTasks.size;

    this.log(
      "log",
      `\x1b[33m[BACKGROUND TASK SHUTDOWN]\x1b[0m Commencing graceful shutdown. Rejecting new tasks, clearing ${queuedCount} queued task(s), and ${delayedCount} delayed task(s).`,
    );

    // Reject and clear all delayed tasks
    for (const task of this.delayedTasks) {
      clearTimeout(task.timeoutId);
      task.controller.abort(
        new DOMException(SHUTDOWN_ABORT_MESSAGE, "AbortError"),
      );
      task.reject(new Error(SHUTDOWN_ABORT_MESSAGE));
    }
    this.delayedTasks.clear();

    // Reject all queued tasks
    for (const queued of this.taskQueue) {
      queued.reject(new Error(SHUTDOWN_ABORT_MESSAGE));
    }
    this.taskQueue = [];

    // Cancel all active tasks
    const activeCount = this.activeTasks.size;
    if (activeCount > 0) {
      this.log(
        "log",
        `\x1b[33m[BACKGROUND TASK SHUTDOWN]\x1b[0m Cancelling ${activeCount} active task(s)...`,
      );
      for (const task of this.activeTasks) {
        task.controller.abort(
          new DOMException(SHUTDOWN_ABORT_MESSAGE, "AbortError"),
        );
      }
    }

    // Bounded wait: a runner that ignores its AbortSignal (or a tick stuck on the DB) must not
    // keep the process alive until the platform SIGKILLs it, which would skip the requeue below.
    let hardTimer: ReturnType<typeof setTimeout> | undefined;
    const hardTimeout = new Promise<"timeout">((resolve) => {
      hardTimer = setTimeout(() => resolve("timeout"), this.shutdownHardTimeoutMs);
    });
    const settled = (async () => {
      if (this.currentTick) await this.currentTick;
      await this.waitForAll();
      return "settled" as const;
    })();
    const outcome = await Promise.race([settled, hardTimeout]);
    clearTimeout(hardTimer);

    // Claimed jobs whose wrapper never settled: still running past the hard timeout, or
    // rejected while queued in memory. Hand them back (the status = 'running' guard keeps a
    // job that settled meanwhile untouched).
    const leftovers = Array.from(this.claimedJobIds);
    this.claimedJobIds.clear();
    if (leftovers.length > 0) {
      this.log(
        "warn",
        `\x1b[33m[BACKGROUND TASK SHUTDOWN]\x1b[0m Requeueing ${leftovers.length} claimed job(s) that did not settle${outcome === "timeout" ? ` within ${this.shutdownHardTimeoutMs}ms` : ""}.`,
      );
      const results = await Promise.allSettled(
        leftovers.map((jobId) => requeueInterruptedJobQuery(jobId)),
      );
      results.forEach((result, index) => {
        if (result.status === "rejected") {
          this.log(
            "error",
            `Failed to requeue job ${leftovers[index]} at shutdown:`,
            result.reason,
          );
        }
      });
    }

    this.log(
      "log",
      `\x1b[32m[BACKGROUND TASK SHUTDOWN]\x1b[0m Graceful shutdown complete.`,
    );
  }

  /**
   * Resets the manager state. (Mainly for testing purposes)
   */
  static resetForTest(): void {
    this.stopWorker();
    this.runners.clear();
    this.isShuttingDown = false;
    this.taskQueue = [];
    this.maxConcurrency = 50; // Dynamic default reset to 50
    this.silent = false; // Reset logging silence

    for (const task of this.delayedTasks) {
      clearTimeout(task.timeoutId);
      task.controller.abort();
      task.reject(new DOMException("Operation aborted", "AbortError"));
    }
    this.delayedTasks.clear();

    for (const task of this.activeTasks) {
      task.controller.abort();
    }
    this.activeTasks.clear();

    this.pendingPromises.clear();
    this.claimedJobIds.clear();
    this.currentTick = null;
    this.shutdownHardTimeoutMs = 5000;
  }
}
