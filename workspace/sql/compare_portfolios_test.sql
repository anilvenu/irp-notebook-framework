-- Minimal test script to diagnose output issue
-- Run this first to verify basic connectivity

SELECT 'TEST 1: Basic SELECT works' AS Test

DECLARE @EDM1 VARCHAR(100) = 'RMS_EDM_202503_Quarterly_USFL_JP_Testing_BOXH'
DECLARE @EDM2 VARCHAR(100) = 'RM_EDM_202503_Testing_USFL_Comm_USFL_LJrF'
DECLARE @Portfolio1 VARCHAR(100) = 'USFL_Comm_Test'
DECLARE @Portfolio2 VARCHAR(100) = 'USFL_Commercial'

SELECT 'TEST 2: Variables declared' AS Test, @EDM1 AS EDM1, @Portfolio1 AS Portfolio1

-- Test dynamic SQL
DECLARE @SQL NVARCHAR(MAX)
SET @SQL = 'SELECT ''TEST 3: Dynamic SQL works'' AS Test'
EXEC sp_executesql @SQL

-- Test cross-database access to EDM1
SET @SQL = 'SELECT ''TEST 4: EDM1 access works'' AS Test, COUNT(*) AS PortCount FROM [' + @EDM1 + ']..portinfo'
EXEC sp_executesql @SQL

-- Test cross-database access to EDM2
SET @SQL = 'SELECT ''TEST 5: EDM2 access works'' AS Test, COUNT(*) AS PortCount FROM [' + @EDM2 + ']..portinfo'
EXEC sp_executesql @SQL

-- Test portfolio lookup in EDM1
SET @SQL = 'SELECT ''TEST 6: Portfolio1 found'' AS Test, PORTINFOID, PORTNAME FROM [' + @EDM1 + ']..portinfo WHERE PORTNAME = ''' + @Portfolio1 + ''''
EXEC sp_executesql @SQL

-- Test portfolio lookup in EDM2
SET @SQL = 'SELECT ''TEST 7: Portfolio2 found'' AS Test, PORTINFOID, PORTNAME FROM [' + @EDM2 + ']..portinfo WHERE PORTNAME = ''' + @Portfolio2 + ''''
EXEC sp_executesql @SQL

SELECT 'ALL TESTS COMPLETED' AS Status
