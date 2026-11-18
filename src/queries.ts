import { sql } from "bun";
import { INTERRUPTED_BY_SHUTDOWN_MESSAGE, type BackgroundJob } from "./model.js";

/**
 * Enqueues a persistent background task in PostgreSQL (UTC time).
 *
 * `run_at` is computed by the database, never by the host: the claim query compares it with
 * the DB's own `now()`, so a host clock ahead of the DB would make an immediate job look
 * "not yet due". A negative delay yields a past `run_at` (used by tests to seed due jobs).
 */
export async function enqueueJobQuery(
  name: string,
  payload: Record<string, unknown>,
  delayMs: number,
  retries: number,
  initialDelayMs: number,
  timeoutMs: number,
): Promise<string> {
  const [result] = await sql<{ id: string }[]>`
    INSERT INTO background_jobs
      (name, payload, status, run_at, max_retries, initial_delay_ms, timeout_ms)
    VALUES (
      ${name},
      ${payload},
      'pending',
      (now() AT TIME ZONE 'utc') + make_interval(secs => ${delayMs / 1000}::double precision),
      ${retries},
      ${initialDelayMs},
      ${timeoutMs}
    )
    RETURNING id
  `;
  if (!result) throw new Error("Background job insert returned no ID");
  return result.id;
}

/**
 * Atomically claims a due job handled by one of this process's runners.
 * Filter before locking so other services' jobs remain available to them.
 */
export async function popNextBackgroundJobQuery(
  runnerNames: string[],
): Promise<BackgroundJob | null> {
  if (runnerNames.length === 0) return null;

  const [job] = await sql<BackgroundJob[]>`
    UPDATE background_jobs
    SET status = 'running',
        started_at = (now() AT TIME ZONE 'utc'),
        attempts = attempts + 1
    WHERE id = (
      SELECT id
      FROM background_jobs
      WHERE status = 'pending'
        AND run_at <= (now() AT TIME ZONE 'utc')
        AND name = ANY(${sql.array(runnerNames, "TEXT")})
      ORDER BY run_at ASC
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    )
    RETURNING *
  `;
  return job || null;
}

/**
 * Marks a persistent job as completed.
 */
export async function markJobCompletedQuery(id: string): Promise<void> {
  await sql`
    UPDATE background_jobs
    SET status = 'completed', finished_at = (now() at time zone 'utc')
    WHERE id = ${id}
  `;
}

/**
 * Re-schedules a failed job for a retry after exponential backoff.
 */
export async function scheduleJobRetryQuery(
  id: string,
  backoffDelayMs: number,
  errorMessage: string,
): Promise<void> {
  // Same reason as enqueue: run_at is compared with the DB's now(), so the DB computes it.
  await sql`
    UPDATE background_jobs
    SET status = 'pending',
        run_at = (now() AT TIME ZONE 'utc') + make_interval(secs => ${backoffDelayMs / 1000}::double precision),
        error_message = ${errorMessage}
    WHERE id = ${id}
  `;
}

/**
 * Gives an interrupted job back to the queue without charging the attempt it never finished.
 * Shutdown is not a task failure: failing it would lose work for jobs with max_retries = 0
 * (every cron-triggered job), whose cron row has already advanced to the next occurrence.
 * The `AND status = 'running'` guard avoids resurrecting a job that already settled.
 */
export async function requeueInterruptedJobQuery(id: string): Promise<void> {
  await sql`
    UPDATE background_jobs
    SET status = 'pending',
        attempts = GREATEST(attempts - 1, 0),
        run_at = (now() AT TIME ZONE 'utc'),
        started_at = NULL,
        error_message = ${INTERRUPTED_BY_SHUTDOWN_MESSAGE}
    WHERE id = ${id}
      AND status = 'running'
  `;
}

/**
 * Marks a persistent job as permanently failed.
 */
export async function markJobFailedQuery(
  id: string,
  errorMessage: string,
): Promise<void> {
  await sql`
    UPDATE background_jobs
    SET status = 'failed', error_message = ${errorMessage}, finished_at = (now() at time zone 'utc')
    WHERE id = ${id}
  `;
}

// ==========================================
//          TEST HELPERS QUERIES
// ==========================================

/**
 * Deletes all test-specific jobs from the background_jobs table.
 */
export async function cleanBackgroundJobsTableQuery(
  name: string,
): Promise<void> {
  await sql`
    DELETE FROM background_jobs
    WHERE name LIKE ${name}
  `;
}

/**
 * Fetches a background job by ID.
 */
export async function getJobByIdQuery(
  id: string,
): Promise<BackgroundJob | null> {
  const [job] = await sql<BackgroundJob[]>`
    SELECT * FROM background_jobs
    WHERE id = ${id}
  `;
  return job || null;
}

/**
 * Returns the count of completed test-specific jobs.
 */
export async function getCompletedJobsCountQuery(
  name: string,
): Promise<number> {
  const [result] = await sql<{ count: number }[]>`
    SELECT COUNT(*)::integer FROM background_jobs
    WHERE status = 'completed' AND name LIKE ${name}
  `;
  if (!result) throw new Error("Background job count returned no row");
  return result.count;
}

/**
 * Recovers only this process's jobs stuck beyond their timeout limit.
 * If attempts <= max_retries, sets status to 'pending' to retry them.
 * Otherwise, sets status to 'failed'.
 */
export async function resetStuckRunningJobsQuery(
  runnerNames: string[],
): Promise<void> {
  if (runnerNames.length === 0) return;

  await sql`
    WITH stuck_jobs AS (
      SELECT id, attempts, max_retries
      FROM background_jobs
      WHERE status = 'running'
        AND name = ANY(${sql.array(runnerNames, "TEXT")})
        AND started_at < (now() at time zone 'utc') - (timeout_ms + 10000) * interval '1 millisecond'
    ),
    retried_jobs AS (
      UPDATE background_jobs
      SET status = 'pending',
          run_at = (now() at time zone 'utc'),
          error_message = 'Job timed out or worker process crashed (auto-recovered)'
      WHERE id IN (SELECT id FROM stuck_jobs WHERE attempts <= max_retries)
    )
    UPDATE background_jobs
    SET status = 'failed',
        finished_at = (now() at time zone 'utc'),
        error_message = 'Job timed out or worker process crashed (max retries exceeded)'
    WHERE id IN (SELECT id FROM stuck_jobs WHERE attempts > max_retries)
  `;
}
