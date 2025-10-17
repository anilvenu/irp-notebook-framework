-- IRP Notebook Framework Database Schema
-- PostgreSQL

-- Drop tables in correct order (if recreating)
DROP TABLE IF EXISTS irp_job_tracking_log CASCADE;
DROP TABLE IF EXISTS irp_batch_recon_log CASCADE;
DROP TABLE IF EXISTS irp_job CASCADE;
DROP TABLE IF EXISTS irp_job_configuration CASCADE;
DROP TABLE IF EXISTS irp_batch CASCADE;
DROP TABLE IF EXISTS irp_step_run CASCADE;
DROP TABLE IF EXISTS irp_step CASCADE;
DROP TABLE IF EXISTS irp_stage CASCADE;
DROP TABLE IF EXISTS irp_configuration CASCADE;
DROP TABLE IF EXISTS irp_cycle CASCADE;

-- Drop types if they exist
DROP TYPE IF EXISTS cycle_status_enum CASCADE;
DROP TYPE IF EXISTS step_status_enum CASCADE;
DROP TYPE IF EXISTS batch_status_enum CASCADE;
DROP TYPE IF EXISTS job_status_enum CASCADE;
DROP TYPE IF EXISTS configuration_status_enum CASCADE;

-- Create custom types
CREATE TYPE cycle_status_enum AS ENUM ('ACTIVE', 'ARCHIVED');
CREATE TYPE step_status_enum AS ENUM ('ACTIVE', 'COMPLETED', 'FAILED', 'SKIPPED');
CREATE TYPE configuration_status_enum AS ENUM ('NEW', 'VALID', 'ACTIVE', 'ERROR');
CREATE TYPE batch_status_enum AS ENUM ('INITIATED', 'ACTIVE', 'COMPLETED', 'FAILED', 'CANCELLED');
CREATE TYPE job_status_enum AS ENUM ('INITIATED', 'SUBMITTED', 'PENDING', 'QUEUED', 'RUNNING', 'FINISHED', 'FAILED', 'CANCEL_REQUESTED', 'CANCELLING', 'CANCELLED', 'FORCED_OK');

-- Core Cycle Management
CREATE TABLE irp_cycle (
    id SERIAL PRIMARY KEY,
    cycle_name VARCHAR(255) UNIQUE NOT NULL,
    status cycle_status_enum DEFAULT 'ACTIVE',
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    archived_ts TIMESTAMPTZ NULL,
    created_by VARCHAR(255) NULL,
    metadata JSONB NULL
);

-- Configuration for Cycle
CREATE TABLE irp_configuration (
    id SERIAL PRIMARY KEY,
    cycle_id INTEGER NOT NULL,
    configuration_file_name VARCHAR(2000) NOT NULL,
    configuration_data JSONB NOT NULL,
    status configuration_status_enum DEFAULT 'NEW',
    file_last_updated_ts  TIMESTAMPTZ NOT NULL,
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    updated_ts TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_configuration_cycle FOREIGN KEY (cycle_id) REFERENCES irp_cycle(id) ON DELETE CASCADE
);

-- Stage Tracking
CREATE TABLE irp_stage (
    id SERIAL PRIMARY KEY,
    cycle_id INTEGER NOT NULL,
    stage_num INTEGER NOT NULL,
    stage_name VARCHAR(255) NOT NULL,
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_stage_cycle FOREIGN KEY (cycle_id) REFERENCES irp_cycle(id) ON DELETE CASCADE,
    CONSTRAINT uq_cycle_stage UNIQUE(cycle_id, stage_num)
);

-- Step Tracking
CREATE TABLE irp_step (
    id SERIAL PRIMARY KEY,
    stage_id INTEGER NOT NULL,
    step_num INTEGER NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    notebook_path VARCHAR(1000) NULL,
    requires_batch BOOLEAN DEFAULT FALSE,
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_step_stage FOREIGN KEY (stage_id) REFERENCES irp_stage(id) ON DELETE CASCADE,
    CONSTRAINT uq_stage_step UNIQUE(stage_id, step_num)
);

-- Step Run History
CREATE TABLE irp_step_run (
    id SERIAL PRIMARY KEY,
    step_id INTEGER NOT NULL,
    run_number INTEGER NOT NULL,
    status step_status_enum DEFAULT 'ACTIVE',
    started_ts TIMESTAMPTZ DEFAULT NOW(),
    completed_ts TIMESTAMPTZ NULL,
    started_by VARCHAR(255) NULL,
    error_message TEXT NULL,
    output_data JSONB NULL,
    CONSTRAINT fk_steprun_step FOREIGN KEY (step_id) REFERENCES irp_step(id) ON DELETE CASCADE
);

-- Batch Management
CREATE TABLE irp_batch (
    id SERIAL PRIMARY KEY,
    step_id INTEGER NOT NULL,
    configuration_id INTEGER NOT NULL,
    batch_type VARCHAR(255) NOT NULL,
    status batch_status_enum DEFAULT 'INITIATED',
    submitted_ts TIMESTAMPTZ NULL,
    completed_ts TIMESTAMPTZ NULL,
    total_jobs INTEGER DEFAULT 0,
    completed_jobs INTEGER DEFAULT 0,
    failed_jobs INTEGER DEFAULT 0,
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    updated_ts TIMESTAMPTZ DEFAULT NOW(),
    metadata JSONB NULL,
    CONSTRAINT fk_batch_step FOREIGN KEY (step_id) REFERENCES irp_step(id),
    CONSTRAINT fk_batch_configuration FOREIGN KEY (configuration_id) REFERENCES irp_configuration(id)
);


-- Configuration for Job
CREATE TABLE irp_job_configuration (
    id SERIAL PRIMARY KEY,    
    batch_id INTEGER NOT NULL,
    configuration_id INTEGER NOT NULL,
    job_configuration_data JSONB NOT NULL,
    skipped BOOLEAN DEFAULT False,
    overridden BOOLEAN DEFAULT False,
    override_reason_txt VARCHAR(1000),
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    updated_ts TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_job_configuration_batch FOREIGN KEY (batch_id) REFERENCES irp_batch(id),
    CONSTRAINT fk_job_configuration_configuration FOREIGN KEY (configuration_id) REFERENCES irp_configuration(id)
);

-- Job Tracking
CREATE TABLE irp_job (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    job_configuration_id INTEGER NOT NULL,
    moodys_workflow_id VARCHAR(50) NULL,
    status job_status_enum DEFAULT 'INITIATED',
    skipped BOOLEAN DEFAULT False,
    last_error TEXT NULL,
    parent_job_id INTEGER,
    submitted_ts TIMESTAMPTZ NULL,
    completed_ts TIMESTAMPTZ NULL,
    last_poll_ts TIMESTAMPTZ NULL,
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    updated_ts TIMESTAMPTZ DEFAULT NOW(),
    submission_request JSONB NULL,
    submission_response JSONB NULL,
    CONSTRAINT fk_job_batch FOREIGN KEY (batch_id) REFERENCES irp_batch(id) ON DELETE CASCADE,
    CONSTRAINT fk_job_configuration FOREIGN KEY (job_configuration_id) REFERENCES irp_job_configuration(id)
);

-- Batch Reconciliation Log
CREATE TABLE irp_batch_recon_log (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    recon_ts TIMESTAMPTZ DEFAULT NOW(),
    recon_result batch_status_enum NOT NULL,
    recon_summary JSONB NOT NULL,
    CONSTRAINT fk_recon_batch FOREIGN KEY (batch_id) REFERENCES irp_batch(id) ON DELETE CASCADE
);

-- Job Tracking Log
CREATE TABLE irp_job_tracking_log (
    id SERIAL PRIMARY KEY,
    job_id INTEGER NOT NULL,
    tracked_ts TIMESTAMPTZ DEFAULT NOW(),
    moodys_workflow_id VARCHAR(50) NOT NULL,
    job_status job_status_enum NOT NULL,
    response_data JSONB NULL,
    CONSTRAINT fk_tracking_job FOREIGN KEY (job_id) REFERENCES irp_job(id) ON DELETE CASCADE
);

-- Create indexes for performance
CREATE INDEX idx_stage_cycle ON irp_stage(cycle_id);
CREATE INDEX idx_step_stage ON irp_step(stage_id);
CREATE INDEX idx_steprun_step ON irp_step_run(step_id);
CREATE INDEX idx_batch_step ON irp_batch(step_id);
CREATE INDEX idx_batch_configuration ON irp_batch(configuration_id);
CREATE INDEX idx_batch_status ON irp_batch(status);
CREATE INDEX idx_batch_type ON irp_batch(batch_type);
CREATE INDEX idx_job_batch ON irp_job(batch_id);
CREATE INDEX idx_job_status ON irp_job(status);
CREATE INDEX idx_job_configuration ON irp_job(job_configuration_id);
CREATE INDEX idx_recon_batch ON irp_batch_recon_log(batch_id);
CREATE INDEX idx_recon_ts ON irp_batch_recon_log(recon_ts DESC);
CREATE INDEX idx_tracking_job ON irp_job_tracking_log(job_id);
CREATE INDEX idx_tracking_ts ON irp_job_tracking_log(tracked_ts DESC);

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO irp_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO irp_user;

SELECT 'Analyst Workflow Database Components Initialized Successfully!' as message;