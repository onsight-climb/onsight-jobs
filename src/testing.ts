import { sql } from "bun";
import type { BackgroundJob } from "./model.js";

// Test-only helpers, exported as `onsight-jobs/testing` and kept out of `onsight-jobs/queries`:
// cleanBackgroundJobsTableQuery deletes by LIKE pattern on the shared queue.

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
