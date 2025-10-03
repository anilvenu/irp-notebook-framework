-- IRP Notebook Framework Database Schema
-- SQL Server Compatible (Also works with Azure SQL Database)

-- Create database if not exists (for local SQL Server)
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'IRP_DB')
BEGIN
    CREATE DATABASE IRP_DB;
END
GO

USE IRP_DB;
GO

-- Drop tables in correct order (if recreating)
IF OBJECT_ID('dbo.irp_job', 'U') IS NOT NULL DROP TABLE dbo.irp_job;
IF OBJECT_ID('dbo.irp_configuration', 'U') IS NOT NULL DROP TABLE dbo.irp_configuration;
IF OBJECT_ID('dbo.irp_batch', 'U') IS NOT NULL DROP TABLE dbo.irp_batch;
IF OBJECT_ID('dbo.irp_step_run', 'U') IS NOT NULL DROP TABLE dbo.irp_step_run;
IF OBJECT_ID('dbo.irp_step', 'U') IS NOT NULL DROP TABLE dbo.irp_step;
IF OBJECT_ID('dbo.irp_stage', 'U') IS NOT NULL DROP TABLE dbo.irp_stage;
IF OBJECT_ID('dbo.irp_system_lock', 'U') IS NOT NULL DROP TABLE dbo.irp_system_lock;
IF OBJECT_ID('dbo.irp_cycle', 'U') IS NOT NULL DROP TABLE dbo.irp_cycle;

-- Core Cycle Management
CREATE TABLE irp_cycle (
    id INT PRIMARY KEY IDENTITY(1,1),
    cycle_name NVARCHAR(255) UNIQUE NOT NULL,
    status VARCHAR(20) CHECK (status IN ('active', 'archived', 'failed')) DEFAULT 'active',
    created_ts DATETIME2 DEFAULT GETUTCDATE(),
    archived_ts DATETIME2 NULL,
    created_by NVARCHAR(255) NULL,
    metadata NVARCHAR(MAX) NULL -- JSON field for flexibility
);

-- Stage Tracking
CREATE TABLE irp_stage (
    id INT PRIMARY KEY IDENTITY(1,1),
    cycle_id INT NOT NULL,
    stage_num INT NOT NULL,
    stage_name NVARCHAR(255) NOT NULL,
    created_ts DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT FK_stage_cycle FOREIGN KEY (cycle_id) REFERENCES irp_cycle(id),
    CONSTRAINT UQ_cycle_stage UNIQUE(cycle_id, stage_num)
);

-- Step Tracking
CREATE TABLE irp_step (
    id INT PRIMARY KEY IDENTITY(1,1),
    stage_id INT NOT NULL,
    step_num INT NOT NULL,
    step_name NVARCHAR(255) NOT NULL,
    notebook_path NVARCHAR(500) NULL,
    is_idempotent BIT DEFAULT 0,
    requires_batch BIT DEFAULT 0,
    created_ts DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT FK_step_stage FOREIGN KEY (stage_id) REFERENCES irp_stage(id),
    CONSTRAINT UQ_stage_step UNIQUE(stage_id, step_num)
);

-- Step Run History
CREATE TABLE irp_step_run (
    id INT PRIMARY KEY IDENTITY(1,1),
    step_id INT NOT NULL,
    run_number INT NOT NULL,
    status VARCHAR(20) CHECK (status IN ('running', 'completed', 'failed', 'skipped')) DEFAULT 'running',
    started_ts DATETIME2 DEFAULT GETUTCDATE(),
    completed_ts DATETIME2 NULL,
    started_by NVARCHAR(255) NULL,
    error_message NVARCHAR(MAX) NULL,
    output_data NVARCHAR(MAX) NULL, -- JSON for step results
    CONSTRAINT FK_steprun_step FOREIGN KEY (step_id) REFERENCES irp_step(id)
);

-- Batch Management (for Phase 2)
CREATE TABLE irp_batch (
    id INT PRIMARY KEY IDENTITY(1,1),
    step_id INT NOT NULL,
    batch_name NVARCHAR(255) NOT NULL,
    status VARCHAR(20) CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')) DEFAULT 'pending',
    created_ts DATETIME2 DEFAULT GETUTCDATE(),
    completed_ts DATETIME2 NULL,
    total_jobs INT DEFAULT 0,
    completed_jobs INT DEFAULT 0,
    failed_jobs INT DEFAULT 0,
    metadata NVARCHAR(MAX) NULL,
    CONSTRAINT FK_batch_step FOREIGN KEY (step_id) REFERENCES irp_step(id),
    CONSTRAINT UQ_step_batch UNIQUE(step_id, batch_name)
);

-- Configuration for Batch
CREATE TABLE irp_configuration (
    id INT PRIMARY KEY IDENTITY(1,1),
    batch_id INT NOT NULL,
    config_name NVARCHAR(255) NOT NULL,
    config_data NVARCHAR(MAX) NOT NULL, -- JSON
    skip BIT DEFAULT 0,
    created_ts DATETIME2 DEFAULT GETUTCDATE(),
    CONSTRAINT FK_config_batch FOREIGN KEY (batch_id) REFERENCES irp_batch(id)
);

-- Job Tracking
CREATE TABLE irp_job (
    id INT PRIMARY KEY IDENTITY(1,1),
    batch_id INT NOT NULL,
    configuration_id INT NOT NULL,
    workflow_id NVARCHAR(255) NULL, -- Moody's workflow ID
    status VARCHAR(20) CHECK (status IN ('pending', 'submitted', 'queued', 'running', 'completed', 'failed', 'cancelled')) DEFAULT 'pending',
    retry_count INT DEFAULT 0,
    last_error NVARCHAR(MAX) NULL,
    created_ts DATETIME2 DEFAULT GETUTCDATE(),
    submitted_ts DATETIME2 NULL,
    completed_ts DATETIME2 NULL,
    poll_count INT DEFAULT 0,
    last_poll_ts DATETIME2 NULL,
    result_data NVARCHAR(MAX) NULL,
    CONSTRAINT FK_job_batch FOREIGN KEY (batch_id) REFERENCES irp_batch(id),
    CONSTRAINT FK_job_config FOREIGN KEY (configuration_id) REFERENCES irp_configuration(id)
);

-- System Lock (ensures single active cycle)
CREATE TABLE irp_system_lock (
    id INT PRIMARY KEY CHECK (id = 1), -- Only one row allowed
    active_cycle_id INT NULL,
    locked_by NVARCHAR(255) NULL,
    locked_at DATETIME2 NULL,
    CONSTRAINT FK_lock_cycle FOREIGN KEY (active_cycle_id) REFERENCES irp_cycle(id)
);

-- Initialize system lock
INSERT INTO irp_system_lock (id, active_cycle_id, locked_by, locked_at) 
VALUES (1, NULL, NULL, NULL);

-- Create indexes for performance
CREATE INDEX IX_stage_cycle ON irp_stage(cycle_id);
CREATE INDEX IX_step_stage ON irp_step(stage_id);
CREATE INDEX IX_steprun_step ON irp_step_run(step_id);
CREATE INDEX IX_batch_step ON irp_batch(step_id);
CREATE INDEX IX_job_batch ON irp_job(batch_id);
CREATE INDEX IX_job_status ON irp_job(status);

PRINT 'IRP Database initialized successfully!';