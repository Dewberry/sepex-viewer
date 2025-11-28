-- Create log table if not exists
CREATE TABLE IF NOT EXISTS public.job_status_log (
    id text NOT NULL,
    status text,
    updated timestamp without time zone
);

-- Create trigger function
CREATE OR REPLACE FUNCTION public.log_job_status_update()
RETURNS trigger AS $$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status OR NEW.updated IS DISTINCT FROM OLD.updated THEN
        INSERT INTO public.job_status_log (id, status, updated)
        VALUES (NEW.id, NEW.status, NEW.updated);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'job_status_update_trigger'
    ) THEN
        CREATE TRIGGER job_status_update_trigger
        AFTER UPDATE ON public.jobs
        FOR EACH ROW EXECUTE FUNCTION public.log_job_status_update();
    END IF;
END
$$;