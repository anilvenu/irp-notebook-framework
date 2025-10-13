-- IRP Notebook Framework Database Schema
-- PostgreSQL Compatible

-- Drop tables in correct order (if recreating)
DROP TABLE IF EXISTS irp_job CASCADE;
DROP TABLE IF EXISTS irp_configuration CASCADE;
DROP TABLE IF EXISTS irp_batch CASCADE;
DROP TABLE IF EXISTS irp_step_run CASCADE;
DROP TABLE IF EXISTS irp_step CASCADE;
DROP TABLE IF EXISTS irp_stage CASCADE;
DROP TABLE IF EXISTS irp_cycle CASCADE;

-- Drop types if they exist
DROP TYPE IF EXISTS cycle_status_enum CASCADE;
DROP TYPE IF EXISTS step_status_enum CASCADE;
DROP TYPE IF EXISTS batch_status_enum CASCADE;
DROP TYPE IF EXISTS job_status_enum CASCADE;

-- Create custom types
CREATE TYPE cycle_status_enum AS ENUM ('active', 'archived', 'failed');
CREATE TYPE step_status_enum AS ENUM ('running', 'completed', 'failed', 'skipped');
CREATE TYPE batch_status_enum AS ENUM ('pending', 'running', 'completed', 'failed', 'cancelled');
CREATE TYPE job_status_enum AS ENUM ('pending', 'submitted', 'queued', 'running', 'completed', 'failed', 'cancelled');

-- Core Cycle Management
CREATE TABLE irp_cycle (
    id SERIAL PRIMARY KEY,
    cycle_name VARCHAR(255) UNIQUE NOT NULL,
    status cycle_status_enum DEFAULT 'active',
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    archived_ts TIMESTAMPTZ NULL,
    created_by VARCHAR(255) NULL,
    metadata JSONB NULL
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
    notebook_path VARCHAR(500) NULL,
    is_idempotent BOOLEAN DEFAULT FALSE,
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
    status step_status_enum DEFAULT 'running',
    started_ts TIMESTAMPTZ DEFAULT NOW(),
    completed_ts TIMESTAMPTZ NULL,
    started_by VARCHAR(255) NULL,
    error_message TEXT NULL,
    output_data JSONB NULL,
    CONSTRAINT fk_steprun_step FOREIGN KEY (step_id) REFERENCES irp_step(id) ON DELETE CASCADE
);

-- Batch Management (for Phase 2)
CREATE TABLE irp_batch (
    id SERIAL PRIMARY KEY,
    step_id INTEGER NOT NULL,
    batch_name VARCHAR(255) NOT NULL,
    status batch_status_enum DEFAULT 'pending',
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    completed_ts TIMESTAMPTZ NULL,
    total_jobs INTEGER DEFAULT 0,
    completed_jobs INTEGER DEFAULT 0,
    failed_jobs INTEGER DEFAULT 0,
    metadata JSONB NULL,
    CONSTRAINT fk_batch_step FOREIGN KEY (step_id) REFERENCES irp_step(id) ON DELETE CASCADE,
    CONSTRAINT uq_step_batch UNIQUE(step_id, batch_name)
);

-- Configuration for Batch
CREATE TABLE irp_configuration (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    config_name VARCHAR(255) NOT NULL,
    config_data JSONB NOT NULL,
    skip BOOLEAN DEFAULT FALSE,
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_config_batch FOREIGN KEY (batch_id) REFERENCES irp_batch(id) ON DELETE CASCADE
);

-- Job Tracking
CREATE TABLE irp_job (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    configuration_id INTEGER NOT NULL,
    workflow_id VARCHAR(255) NULL,
    status job_status_enum DEFAULT 'pending',
    retry_count INTEGER DEFAULT 0,
    last_error TEXT NULL,
    created_ts TIMESTAMPTZ DEFAULT NOW(),
    submitted_ts TIMESTAMPTZ NULL,
    completed_ts TIMESTAMPTZ NULL,
    poll_count INTEGER DEFAULT 0,
    last_poll_ts TIMESTAMPTZ NULL,
    result_data JSONB NULL,
    CONSTRAINT fk_job_batch FOREIGN KEY (batch_id) REFERENCES irp_batch(id) ON DELETE CASCADE,
    CONSTRAINT fk_job_config FOREIGN KEY (configuration_id) REFERENCES irp_configuration(id)
);

-- Create indexes for performance
CREATE INDEX idx_stage_cycle ON irp_stage(cycle_id);
CREATE INDEX idx_step_stage ON irp_step(stage_id);
CREATE INDEX idx_steprun_step ON irp_step_run(step_id);
CREATE INDEX idx_batch_step ON irp_batch(step_id);
CREATE INDEX idx_job_batch ON irp_job(batch_id);
CREATE INDEX idx_job_status ON irp_job(status);

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO irp_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO irp_user;

SELECT 'Analyst Workflow Database Components Initialized Successfully!' as message;