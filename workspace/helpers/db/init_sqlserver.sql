-- ============================================================================
-- SQL Server Express Test Database Initialization
-- ============================================================================
-- This script creates a test database and sample tables for MSSQL integration
-- testing in the IRP Notebook Framework.
--
-- Database: test_db
-- Tables: test_portfolios, test_risks
-- ============================================================================

-- Create test database if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'test_db')
BEGIN
    CREATE DATABASE test_db;
    PRINT 'Database test_db created successfully';
END
ELSE
BEGIN
    PRINT 'Database test_db already exists';
END
GO

-- Switch to test database
USE test_db;
GO

-- Drop existing tables if they exist (for clean re-initialization)
IF OBJECT_ID('test_risks', 'U') IS NOT NULL
    DROP TABLE test_risks;
GO

IF OBJECT_ID('test_portfolios', 'U') IS NOT NULL
    DROP TABLE test_portfolios;
GO

-- Create test_portfolios table
CREATE TABLE test_portfolios (
    id INT IDENTITY(1,1) PRIMARY KEY,
    portfolio_name NVARCHAR(255) NOT NULL,
    portfolio_value DECIMAL(18,2),
    status NVARCHAR(50) DEFAULT 'ACTIVE',
    created_ts DATETIME2 DEFAULT GETDATE(),
    updated_ts DATETIME2 DEFAULT GETDATE()
);

PRINT 'Table test_portfolios created successfully';
GO

-- Create test_risks table
CREATE TABLE test_risks (
    id INT IDENTITY(1,1) PRIMARY KEY,
    portfolio_id INT NOT NULL,
    risk_type NVARCHAR(100) NOT NULL,
    risk_value DECIMAL(18,4),
    calculated_ts DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT fk_portfolio FOREIGN KEY (portfolio_id)
        REFERENCES test_portfolios(id) ON DELETE CASCADE
);

PRINT 'Table test_risks created successfully';
GO

-- Insert sample portfolio data
INSERT INTO test_portfolios (portfolio_name, portfolio_value, status)
VALUES
    ('Test Portfolio A', 1000000.00, 'ACTIVE'),
    ('Test Portfolio B', 2500000.00, 'ACTIVE'),
    ('Test Portfolio C', 750000.00, 'ACTIVE'),
    ('Test Portfolio D', 5000000.00, 'ACTIVE'),
    ('Test Portfolio E', 125000.00, 'INACTIVE');

PRINT 'Sample portfolios inserted successfully';
GO

-- Insert sample risk data
INSERT INTO test_risks (portfolio_id, risk_type, risk_value)
VALUES
    (1, 'VaR_95', 45000.50),
    (1, 'VaR_99', 67500.75),
    (1, 'CVaR_95', 52000.25),
    (2, 'VaR_95', 112500.25),
    (2, 'VaR_99', 168750.00),
    (2, 'CVaR_95', 135000.50),
    (3, 'VaR_95', 33750.00),
    (3, 'VaR_99', 50625.00),
    (4, 'VaR_95', 225000.00),
    (4, 'VaR_99', 337500.00);

PRINT 'Sample risks inserted successfully';
GO

-- Create indexes for better query performance
CREATE INDEX idx_portfolios_status ON test_portfolios(status);
CREATE INDEX idx_risks_portfolio ON test_risks(portfolio_id);
CREATE INDEX idx_risks_type ON test_risks(risk_type);

PRINT 'Indexes created successfully';
GO

-- Display summary
SELECT
    'Portfolios' AS TableName,
    COUNT(*) AS RecordCount
FROM test_portfolios

UNION ALL

SELECT
    'Risks' AS TableName,
    COUNT(*) AS RecordCount
FROM test_risks;

PRINT 'SQL Server test database initialization completed successfully';
GO