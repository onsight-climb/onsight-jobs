/** Abort reason used when the manager cancels in-flight tasks at the end of the shutdown grace period. */
export const SHUTDOWN_ABORT_MESSAGE = "TaskManager is shutting down";

/** Stored in `error_message` for jobs handed back to the queue by a shutdown. */
export const INTERRUPTED_BY_SHUTDOWN_MESSAGE = "Interrupted by shutdown";

/** Persisted queue row shared by the backend and worker; payload must be narrowed by its runner. */
export interface BackgroundJob {
  id: string;
  name: string;
  payload: unknown;
  status: "pending" | "running" | "completed" | "failed";
  attempts: number;
  max_retries: number;
  initial_delay_ms: number;
  timeout_ms: number;
  run_at: Date | string;
  started_at: Date | string | null;
  finished_at: Date | string | null;
  error_message: string | null;
}

/** Read provider-style error properties without assuming that thrown values are Error instances. */
export function taskErrorMessage(error: unknown): string {
  if (
    typeof error === "object" &&
    error !== null &&
    "message" in error &&
    typeof error.message === "string"
  ) {
    return error.message || String(error);
  }
  return String(error);
}

/** Preserve cancellation handling for both Error instances and plain provider error objects. */
export function isAbortError(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  return (
    ("name" in error && error.name === "AbortError") ||
    ("message" in error &&
      typeof error.message === "string" &&
      error.message.includes("abort"))
  );
}
