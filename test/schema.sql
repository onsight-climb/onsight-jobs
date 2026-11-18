-- Minimal replica of the `background_jobs` schema owned by onsight-backend's Supabase
-- migrations (supabase/migrations/20260912105000_ranking_camps_initial_placement.sql).
-- Used only by this package's CI; keep the columns in sync with the real migration.
CREATE TYPE public.job_status AS ENUM ('pending', 'running', 'completed', 'failed');

CREATE TABLE IF NOT EXISTS public.background_jobs (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    name text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    status public.job_status DEFAULT 'pending'::public.job_status NOT NULL,
    run_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc') NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    max_retries integer DEFAULT 0 NOT NULL,
    initial_delay_ms integer DEFAULT 1000 NOT NULL,
    timeout_ms integer DEFAULT 30000 NOT NULL,
    error_message text,
    created_at timestamp without time zone DEFAULT (now() AT TIME ZONE 'utc') NOT NULL,
    started_at timestamp without time zone,
    finished_at timestamp without time zone
);

CREATE INDEX idx_background_jobs_polling ON public.background_jobs USING btree (status, run_at)
    WHERE status = 'pending'::public.job_status;
