-- ============================================================================
-- Script: sample_query.sql
-- Purpose: Example SQL query demonstrating parameter usage
-- Parameters:
--   {portfolio_id} - Portfolio identifier (integer)
--   {risk_type} - Type of risk metric (string, e.g., 'VaR_95', 'VaR_99')
-- Returns: Portfolio details with associated risk metrics
-- Author: IRP Framework Team
-- Created: 2025-01-15
-- ============================================================================

SELECT
    p.portfolio_name,
    p.portfolio_value,
    p.created_ts,
    r.risk_type,
    r.risk_value,
    r.calculated_ts
FROM test_portfolios p
INNER JOIN test_risks r ON p.id = r.portfolio_id
WHERE p.id = {portfolio_id}
  AND r.risk_type = {risk_type}
ORDER BY r.calculated_ts DESC;