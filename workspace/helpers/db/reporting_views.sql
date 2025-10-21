-- Store job-level reporting rules
DROP TABLE IF EXISTS irp_job_status_rule;
CREATE TABLE irp_job_status_rule (
    id SERIAL PRIMARY KEY,
    skipped BOOLEAN NOT NULL,
    status VARCHAR(50) NOT NULL,
    report_status VARCHAR(50) NOT NULL,
    age_calculation VARCHAR(100) NOT NULL,
    next_best_action VARCHAR(200) NOT NULL,
    is_terminal BOOLEAN DEFAULT FALSE,
    created_ts TIMESTAMP DEFAULT NOW(),
    UNIQUE(skipped, status)
);

-- Populate with rules 
INSERT INTO irp_job_status_rule (skipped, status, report_status, age_calculation, next_best_action, is_terminal) VALUES
(FALSE, 'INITIATED', 'UNSUBMITTED', 'now(UTC) - created_ts', 'Submit Batch', FALSE),
(FALSE, 'SUBMITTED', 'ACTIVE', 'now(UTC) - submitted_ts', 'Track Batch', FALSE),
(FALSE, 'QUEUED', 'ACTIVE', 'now(UTC) - submitted_ts', 'Track Batch', FALSE),
(FALSE, 'PENDING', 'ACTIVE', 'now(UTC) - submitted_ts', 'Track Batch', FALSE),
(FALSE, 'RUNNING', 'ACTIVE', 'now(UTC) - submitted_ts', 'Track Batch', FALSE),
(FALSE, 'CANCEL_REQUESTED', 'ACTIVE', 'now(UTC) - submitted_ts', 'Track Batch', FALSE),
(FALSE, 'CANCELLING', 'ACTIVE', 'now(UTC) - submitted_ts', 'Track Batch', FALSE),
(FALSE, 'FINISHED', 'FINISHED', 'completed_ts - submitted_ts', '', TRUE),
(FALSE, 'FAILED', 'FAILED', 'completed_ts - submitted_ts', 'Resubmit OR Skip after Manual Operation on Moody''s', TRUE),
(FALSE, 'CANCELLED', 'CANCELLED', 'completed_ts - submitted_ts', 'Skip Configuration OR Resubmit', TRUE),
(FALSE, 'ERROR', 'ERROR', 'updated_ts - created_ts', 'Resubmit OR Skip after Manual Operation on Moody''s', TRUE),
(TRUE, 'INITIATED', 'SKIPPED', 'updated_ts - created_ts', 'Recon Batch OR Check for Manually Completed Job on Moody''s', TRUE),
(TRUE, 'SUBMITTED', 'SKIPPED', 'updated_ts - created_ts', 'Recon Batch OR Check for Manually Completed Job on Moody''s', TRUE),
(TRUE, 'QUEUED', 'SKIPPED', 'updated_ts - created_ts', 'Recon Batch OR Check for Manually Completed Job on Moody''s', TRUE),
(TRUE, 'PENDING', 'SKIPPED', 'updated_ts - created_ts', 'Recon Batch OR Check for Manually Completed Job on Moody''s', TRUE),
(TRUE, 'RUNNING', 'SKIPPED', 'updated_ts - created_ts', 'Recon Batch OR Check for Manually Completed Job on Moody''s', TRUE),
(TRUE, 'FINISHED', 'SKIPPED', 'updated_ts - created_ts', 'Recon Batch OR Check for Manually Completed Job on Moody''s', TRUE),
(TRUE, 'FAILED', 'SKIPPED', 'updated_ts - created_ts', 'Recon Batch OR Check for Manually Completed Job on Moody''s', TRUE),
(TRUE, 'CANCELLED', 'SKIPPED', 'updated_ts - created_ts', 'Recon Batch OR Check for Manually Completed Job on Moody''s', TRUE),
(TRUE, 'ERROR', 'SKIPPED', 'updated_ts - created_ts', 'Recon Batch OR Check for Manually Completed Job on Moody''s', TRUE);

DROP VIEW IF EXISTS v_irp_job;
CREATE VIEW v_irp_job AS
SELECT 
    -- Derived reporting fields from rule table
    r.report_status,
    r.next_best_action,
    r.is_terminal,    
    CASE 
        WHEN r.age_calculation = 'now(UTC) - created_ts' THEN 
            EXTRACT(EPOCH FROM (NOW() - j.created_ts)) / 3600.0  -- hours
        WHEN r.age_calculation = 'now(UTC) - submitted_ts' THEN 
            EXTRACT(EPOCH FROM (NOW() - j.submitted_ts)) / 3600.0
        WHEN r.age_calculation = 'completed_ts - submitted_ts' THEN 
            EXTRACT(EPOCH FROM (j.completed_ts - j.submitted_ts)) / 3600.0
        WHEN r.age_calculation = 'updated_ts - created_ts' THEN 
            EXTRACT(EPOCH FROM (j.updated_ts - j.created_ts)) / 3600.0
        ELSE NULL
    END AS age_hours,    
    CASE 
        WHEN j.status IN ('FINISHED') AND NOT j.skipped THEN TRUE
        ELSE FALSE
    END AS is_successful,    
    CASE 
        WHEN j.status IN ('FAILED', 'CANCELLED', 'ERROR') THEN TRUE
        ELSE FALSE
    END AS needs_attention,    
    j.*,
    jc.configuration_id,
    jc.overridden,
    jc.override_reason_txt,
    jc.parent_job_configuration_id
FROM irp_job j
LEFT JOIN irp_job_status_rule r 
    ON j.skipped = r.skipped 
    AND j.status::TEXT = r.status
LEFT JOIN irp_job_configuration jc
    ON j.job_configuration_id = jc.id;
-- Add comment
COMMENT ON VIEW v_irp_job IS 
'Enhanced job view with derived reporting status, age calculations, and actionable recommendations based on job_status_rule table';

-- View for job configuration-level reporting
DROP VIEW IF EXISTS v_irp_job_configuration;
CREATE VIEW v_irp_job_configuration AS
WITH job_stats AS (
    SELECT 
        jc.id AS job_configuration_id,
        jc.batch_id,
        jc.configuration_id,
        jc.skipped AS config_skipped,
        jc.overridden,
        jc.parent_job_configuration_id,
        -- Count jobs by status (excluding skipped jobs)
        COUNT(*) FILTER (WHERE NOT j.skipped) AS total_jobs,
        COUNT(*) FILTER (WHERE NOT j.skipped AND j.status = 'INITIATED') AS unsubmitted_jobs,
        COUNT(*) FILTER (WHERE NOT j.skipped AND j.status = 'FAILED') AS failed_jobs,
        COUNT(*) FILTER (WHERE NOT j.skipped AND j.status = 'CANCELLED') AS cancelled_jobs,
        COUNT(*) FILTER (WHERE NOT j.skipped AND j.status = 'ERROR') AS error_jobs,
        COUNT(*) FILTER (WHERE NOT j.skipped AND j.status = 'FINISHED') AS finished_jobs,
        -- Count skipped jobs (where skipped=True AND override_job_configuration_id IS NULL)
        COUNT(*) FILTER (WHERE j.skipped AND jc.override_job_configuration_id IS NULL) AS skipped_jobs,
        -- Check if any job is finished
        BOOL_OR(j.status = 'FINISHED' AND NOT j.skipped) AS has_finished_job,
        -- Check if all non-skipped jobs exist
        BOOL_AND(j.id IS NOT NULL OR jc.skipped) AS has_jobs
    FROM irp_job_configuration jc
    LEFT JOIN irp_job j ON jc.id = j.job_configuration_id
    GROUP BY jc.id, jc.batch_id, jc.configuration_id, jc.skipped, 
             jc.overridden, jc.parent_job_configuration_id
)
SELECT 
    js.*,
    -- Derive Job Configuration Reporting Status (per Image 2 rules)
    CASE 
        -- Rule 1: Skipped configurations
        WHEN js.config_skipped THEN 'SKIPPED'
        -- Rule 2: Has at least one FINISHED job
        WHEN js.has_finished_job THEN 'FULFILLED'
        -- Rule 3: Has jobs but none are FINISHED
        WHEN js.total_jobs > 0 AND NOT js.has_finished_job THEN 'UNFULFILLED'
        -- Rule 4: No jobs at all for non-skipped config
        WHEN js.total_jobs = 0 AND NOT js.config_skipped THEN 'UNFULFILLED'
        ELSE 'UNKNOWN'
    END AS config_report_status,
    -- Additional derived metrics
    CASE 
        WHEN js.failed_jobs > 0 THEN TRUE
        ELSE FALSE
    END AS has_failures,
    CASE 
        WHEN js.error_jobs > 0 THEN TRUE
        ELSE FALSE
    END AS has_errors,
    CASE 
        WHEN js.unsubmitted_jobs > 0 THEN TRUE
        ELSE FALSE
    END AS has_unsubmitted,
    -- Progress percentage (finished / total non-skipped jobs)
    CASE 
        WHEN js.total_jobs > 0 THEN 
            ROUND((js.finished_jobs::NUMERIC / js.total_jobs::NUMERIC) * 100, 2)
        ELSE 0
    END AS progress_percent,
    jc.job_configuration_data,
    jc.skipped_reason_txt,
    jc.override_reason_txt,
    jc.override_job_configuration_id,
    jc.created_ts,
    jc.updated_ts
FROM job_stats js
JOIN irp_job_configuration jc ON js.job_configuration_id = jc.id;

COMMENT ON VIEW v_irp_job_configuration IS 
'Job configuration view with aggregated job statistics and derived reporting status (FULFILLED/UNFULFILLED/SKIPPED)';

-- View for batch-level reporting
DROP VIEW IF EXISTS v_irp_batch;
CREATE VIEW v_irp_batch AS
WITH batch_stats AS (
    SELECT
        b.id AS batch_id,
        b.step_id,
        b.configuration_id,
        b.batch_type,
        b.status AS batch_status,
        -- Configuration stats
        COUNT(DISTINCT jc.id) AS total_configs,
        COUNT(DISTINCT jc.id) FILTER (WHERE NOT jc.skipped) AS non_skipped_configs,
        COUNT(DISTINCT jc.id) FILTER (WHERE vjc.config_report_status = 'FULFILLED') AS fulfilled_configs,
        COUNT(DISTINCT jc.id) FILTER (WHERE vjc.config_report_status = 'UNFULFILLED') AS unfulfilled_configs,
        COUNT(DISTINCT jc.id) FILTER (WHERE vjc.config_report_status = 'SKIPPED') AS skipped_configs,
        -- Job stats (from v_irp_job_configuration)
        SUM(vjc.total_jobs) AS total_jobs,
        SUM(vjc.unsubmitted_jobs) AS unsubmitted_jobs,
        SUM(vjc.failed_jobs) AS failed_jobs,
        SUM(vjc.cancelled_jobs) AS cancelled_jobs,
        SUM(vjc.error_jobs) AS error_jobs,
        SUM(vjc.finished_jobs) AS finished_jobs,
        SUM(vjc.skipped_jobs) AS skipped_jobs,
        -- Aggregate flags
        BOOL_OR(vjc.has_failures) AS has_any_failures,
        BOOL_OR(vjc.has_errors) AS has_any_errors,
        -- Check if all jobs are UNSUBMITTED (using v_irp_job report_status)
        BOOL_AND(COALESCE(vj.report_status = 'UNSUBMITTED', TRUE)) AS all_jobs_unsubmitted,
        -- Check if at least one job has ERROR reporting status
        BOOL_OR(vj.report_status = 'ERROR') AS has_error_report_status,
        -- Check if at least one job has FAILED reporting status
        BOOL_OR(vj.report_status = 'FAILED') AS has_failed_report_status,
        b.created_ts,
        b.submitted_ts,
        b.completed_ts,
        b.updated_ts
    FROM irp_batch b
    LEFT JOIN irp_job_configuration jc ON b.id = jc.batch_id
    LEFT JOIN v_irp_job_configuration vjc ON jc.id = vjc.job_configuration_id
    LEFT JOIN v_irp_job vj ON jc.id = vj.job_configuration_id
    GROUP BY b.id, b.step_id, b.configuration_id, b.batch_type,
             b.status, b.created_ts, b.submitted_ts, b.completed_ts, b.updated_ts
)
SELECT
    bs.*,
    -- Derived batch reporting status based on configuration and job reporting statuses
    CASE
        -- Rule 1: If all configurations are SKIPPED reporting status, then SKIPPED
        WHEN bs.total_configs > 0 AND bs.skipped_configs = bs.total_configs THEN 'SKIPPED'
        -- Rule 2: If all configurations are FULFILLED, then COMPLETED
        WHEN bs.total_configs > 0 AND bs.fulfilled_configs = bs.total_configs THEN 'COMPLETED'
        -- Rule 3: If all jobs are in UNSUBMITTED reporting status, then UNSUBMITTED
        WHEN bs.all_jobs_unsubmitted AND bs.total_jobs > 0 THEN 'UNSUBMITTED'
        -- Rule 4: If at least one job is in ERROR reporting status, then ERROR
        WHEN bs.has_error_report_status THEN 'ERROR'
        -- Rule 5: If at least one job is in FAILED reporting status, then FAILED
        WHEN bs.has_failed_report_status THEN 'FAILED'
        -- Rule 6: Otherwise INCOMPLETE
        ELSE 'INCOMPLETE'
    END AS reporting_status,
    -- Completion percentage
    CASE
        WHEN bs.non_skipped_configs > 0 THEN
            ROUND((bs.fulfilled_configs::NUMERIC / bs.non_skipped_configs::NUMERIC) * 100, 2)
        ELSE 0
    END AS completion_percent,
    -- Age calculations
    CASE
        WHEN bs.batch_status IN ('INITIATED') THEN
            EXTRACT(EPOCH FROM (NOW() - bs.created_ts)) / 3600.0
        WHEN bs.batch_status IN ('ACTIVE') THEN
            EXTRACT(EPOCH FROM (NOW() - bs.submitted_ts)) / 3600.0
        WHEN bs.batch_status IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN
            EXTRACT(EPOCH FROM (bs.completed_ts - bs.submitted_ts)) / 3600.0
        ELSE NULL
    END AS age_hours,
    -- Recommended action
    CASE
        WHEN bs.batch_status = 'INITIATED' THEN 'Submit Batch'
        WHEN bs.batch_status = 'ACTIVE' AND bs.unfulfilled_configs > 0 THEN 'Track Jobs'
        WHEN bs.batch_status = 'ACTIVE' AND bs.unfulfilled_configs = 0 THEN 'Recon Batch'
        WHEN bs.batch_status IN ('FAILED', 'ERROR') THEN 'Review Failed Jobs'
        WHEN bs.batch_status = 'COMPLETED' THEN 'Proceed to Next Step'
        ELSE 'No Action Required'
    END AS recommended_action
FROM batch_stats bs;