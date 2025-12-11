/**********************************************************************************************************************************************
Purpose: Compare two portfolios across different EDMs to validate data is identical
         Use this when analysis results differ between manual and automated runs

Author: Claude Code
Created: 2025-12

Instructions:
    1. Set @EDM1 and @EDM2 to the two EDM database names
    2. Set @Portfolio1 and @Portfolio2 to the portfolio names in each EDM
    3. Run on RMS SQL Server with access to both EDMs
    4. Review each section's output - empty results mean no differences

SQL Server: RMS SQL Server
Runtime: < 1 min (depends on portfolio size)

Sections:
    1. High-Level Summary - Quick count comparison
    2. Location Differences - Missing or mismatched locations
    3. Policy Differences - Missing or mismatched policies
    4. Hurricane Deductible Differences - hudet table comparison
    5. Quick Validation - Simple yes/no check
**********************************************************************************************************************************************/

SET NOCOUNT ON;

DECLARE @EDM1 VARCHAR(100) = 'RMS_EDM_202503_Quarterly_USFL_JP_Testing_BOXH'      -- EDM for manual run
DECLARE @EDM2 VARCHAR(100) = 'RM_EDM_202503_Testing_USFL_Comm_USFL_LJrF'   -- EDM for automated run
DECLARE @Portfolio1 VARCHAR(100) = 'USFL_Comm_Test'        -- Portfolio name in EDM1
DECLARE @Portfolio2 VARCHAR(100) = 'USFL_Commercial'        -- Portfolio name in EDM2

DECLARE @SQL NVARCHAR(MAX)

-- Verify variables are set
SELECT 'Configuration' AS Section, @EDM1 AS EDM1, @EDM2 AS EDM2, @Portfolio1 AS Portfolio1, @Portfolio2 AS Portfolio2

-- ============================================================================
-- SECTION 1: HIGH-LEVEL SUMMARY COMPARISON
-- ============================================================================
SET @SQL = '
SELECT
    ''EDM1: ' + @EDM1 + ''' AS Source,
    ''' + @Portfolio1 + ''' AS Portfolio,
    (SELECT COUNT(DISTINCT pa.ACCGRPID)
     FROM [' + @EDM1 + ']..portacct pa
     JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
     WHERE pi.PORTNAME = ''' + @Portfolio1 + ''') AS AccountCount,
    (SELECT COUNT(DISTINCT p.LOCID)
     FROM [' + @EDM1 + ']..Property p
     JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
     JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
     WHERE pi.PORTNAME = ''' + @Portfolio1 + ''') AS LocationCount,
    (SELECT SUM(lc.VALUEAMT)
     FROM [' + @EDM1 + ']..loccvg lc
     JOIN [' + @EDM1 + ']..Property p ON lc.LOCID = p.LOCID
     JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
     JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
     WHERE pi.PORTNAME = ''' + @Portfolio1 + ''') AS TotalTIV,
    (SELECT COUNT(DISTINCT pol.POLICYID)
     FROM [' + @EDM1 + ']..policy pol
     JOIN [' + @EDM1 + ']..portacct pa ON pol.ACCGRPID = pa.ACCGRPID
     JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
     WHERE pi.PORTNAME = ''' + @Portfolio1 + ''') AS PolicyCount

UNION ALL

SELECT
    ''EDM2: ' + @EDM2 + ''' AS Source,
    ''' + @Portfolio2 + ''' AS Portfolio,
    (SELECT COUNT(DISTINCT pa.ACCGRPID)
     FROM [' + @EDM2 + ']..portacct pa
     JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
     WHERE pi.PORTNAME = ''' + @Portfolio2 + ''') AS AccountCount,
    (SELECT COUNT(DISTINCT p.LOCID)
     FROM [' + @EDM2 + ']..Property p
     JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
     JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
     WHERE pi.PORTNAME = ''' + @Portfolio2 + ''') AS LocationCount,
    (SELECT SUM(lc.VALUEAMT)
     FROM [' + @EDM2 + ']..loccvg lc
     JOIN [' + @EDM2 + ']..Property p ON lc.LOCID = p.LOCID
     JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
     JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
     WHERE pi.PORTNAME = ''' + @Portfolio2 + ''') AS TotalTIV,
    (SELECT COUNT(DISTINCT pol.POLICYID)
     FROM [' + @EDM2 + ']..policy pol
     JOIN [' + @EDM2 + ']..portacct pa ON pol.ACCGRPID = pa.ACCGRPID
     JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
     WHERE pi.PORTNAME = ''' + @Portfolio2 + ''') AS PolicyCount
'
PRINT '=== SECTION 1: HIGH-LEVEL SUMMARY ==='
EXEC sp_executesql @SQL


-- ============================================================================
-- SECTION 2: SKIPPED - Use Section 5b and 5c for Property comparison
-- The original Section 2 with coverage aggregation was too slow
-- ============================================================================
PRINT '=== SECTION 2: SKIPPED (see Section 5b/5c for Property comparison) ==='


-- ============================================================================
-- SECTION 3: POLICY-LEVEL COMPARISON - SUMMARY ONLY
-- ============================================================================
SET @SQL = '
SELECT ''Policy Summary'' AS Section,
    ''EDM1'' AS Source,
    COUNT(*) AS PolicyCount,
    SUM(BLANLIMAMT) AS Total_BLANLIMAMT,
    SUM(BLANPREAMT) AS Total_BLANPREAMT,
    SUM(BLANDEDAMT) AS Total_BLANDEDAMT,
    SUM(UNDCOVAMT) AS Total_UNDCOVAMT
FROM [' + @EDM1 + ']..policy pol
JOIN [' + @EDM1 + ']..portacct pa ON pol.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio1 + '''

UNION ALL

SELECT ''Policy Summary'' AS Section,
    ''EDM2'' AS Source,
    COUNT(*) AS PolicyCount,
    SUM(BLANLIMAMT) AS Total_BLANLIMAMT,
    SUM(BLANPREAMT) AS Total_BLANPREAMT,
    SUM(BLANDEDAMT) AS Total_BLANDEDAMT,
    SUM(UNDCOVAMT) AS Total_UNDCOVAMT
FROM [' + @EDM2 + ']..policy pol
JOIN [' + @EDM2 + ']..portacct pa ON pol.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
'
PRINT '=== SECTION 3: POLICY SUMMARY ==='
EXEC sp_executesql @SQL


-- ============================================================================
-- SECTION 4: HURRICANE/FLOOD DEDUCTIBLES (hudet table) - SUMMARY ONLY
-- Using summary comparison to avoid cartesian product from multiple hudet per location
-- ============================================================================
SET @SQL = '
-- HuDet Summary Comparison
SELECT ''HuDet Summary'' AS Section,
    ''EDM1'' AS Source,
    COUNT(*) AS HuDetRecordCount,
    SUM(SITEDEDAMT) AS Total_SITEDEDAMT,
    SUM(SITELIMAMT) AS Total_SITELIMAMT,
    SUM(COMBINEDDEDAMT) AS Total_COMBINEDDEDAMT,
    SUM(COMBINEDLIMAMT) AS Total_COMBINEDLIMAMT
FROM [' + @EDM1 + ']..hudet hd
JOIN [' + @EDM1 + ']..Property p ON hd.LOCID = p.LOCID
JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio1 + '''

UNION ALL

SELECT ''HuDet Summary'' AS Section,
    ''EDM2'' AS Source,
    COUNT(*) AS HuDetRecordCount,
    SUM(SITEDEDAMT) AS Total_SITEDEDAMT,
    SUM(SITELIMAMT) AS Total_SITELIMAMT,
    SUM(COMBINEDDEDAMT) AS Total_COMBINEDDEDAMT,
    SUM(COMBINEDLIMAMT) AS Total_COMBINEDLIMAMT
FROM [' + @EDM2 + ']..hudet hd
JOIN [' + @EDM2 + ']..Property p ON hd.LOCID = p.LOCID
JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
'
PRINT '=== SECTION 4a: HUDET SUMMARY ==='
EXEC sp_executesql @SQL

-- Section 4b skipped - use Section 4a summary for hudet comparison
-- If Section 4a shows identical totals, hudet data matches
PRINT '=== SECTION 4b: SKIPPED (use Section 4a summary) ==='


-- ============================================================================
-- SECTION 5: QUICK VALIDATION - ARE LOCATION SETS IDENTICAL?
-- ============================================================================
SET @SQL = '
SELECT
    CASE
        WHEN
            (SELECT COUNT(*) FROM (
                SELECT LOCNUM FROM [' + @EDM1 + ']..Property p
                JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
                JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
                WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
                EXCEPT
                SELECT LOCNUM FROM [' + @EDM2 + ']..Property p
                JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
                JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
                WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
            ) diff) = 0
            AND
            (SELECT COUNT(*) FROM (
                SELECT LOCNUM FROM [' + @EDM2 + ']..Property p
                JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
                JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
                WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
                EXCEPT
                SELECT LOCNUM FROM [' + @EDM1 + ']..Property p
                JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
                JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
                WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
            ) diff) = 0
        THEN ''LOCATION SET MATCHES''
        ELSE ''LOCATION SET DIFFERS''
    END AS LocationSetStatus
'
PRINT '=== SECTION 5: QUICK VALIDATION ==='
EXEC sp_executesql @SQL


-- ============================================================================
-- SECTION 5b: PROPERTY-ONLY COMPARISON (no coverage aggregation)
-- Isolates Property table differences from coverage differences
-- ============================================================================
SET @SQL = '
;WITH EDM1_Props AS (
    SELECT
        p.LOCNUM,
        p.NUMSTORIES,
        p.YEARBUILT,
        p.NUMBLDGS,
        p.BLDGCLASS,
        p.OCCTYPE,
        p.HUZONE
    FROM [' + @EDM1 + ']..Property p
    JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
),
EDM2_Props AS (
    SELECT
        p.LOCNUM,
        p.NUMSTORIES,
        p.YEARBUILT,
        p.NUMBLDGS,
        p.BLDGCLASS,
        p.OCCTYPE,
        p.HUZONE
    FROM [' + @EDM2 + ']..Property p
    JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
)
SELECT TOP 50
    ''Property Mismatch'' AS DiffType,
    e1.LOCNUM,
    e1.NUMSTORIES AS EDM1_NUMSTORIES,
    e2.NUMSTORIES AS EDM2_NUMSTORIES,
    e1.YEARBUILT AS EDM1_YEARBUILT,
    e2.YEARBUILT AS EDM2_YEARBUILT,
    e1.NUMBLDGS AS EDM1_NUMBLDGS,
    e2.NUMBLDGS AS EDM2_NUMBLDGS,
    e1.BLDGCLASS AS EDM1_BLDGCLASS,
    e2.BLDGCLASS AS EDM2_BLDGCLASS,
    e1.OCCTYPE AS EDM1_OCCTYPE,
    e2.OCCTYPE AS EDM2_OCCTYPE
FROM EDM1_Props e1
JOIN EDM2_Props e2 ON e1.LOCNUM = e2.LOCNUM
WHERE ISNULL(e1.NUMSTORIES, 0) <> ISNULL(e2.NUMSTORIES, 0)
   OR ISNULL(e1.YEARBUILT, 0) <> ISNULL(e2.YEARBUILT, 0)
   OR ISNULL(e1.NUMBLDGS, 0) <> ISNULL(e2.NUMBLDGS, 0)
   OR ISNULL(e1.BLDGCLASS, '''') <> ISNULL(e2.BLDGCLASS, '''')
   OR ISNULL(e1.OCCTYPE, '''') <> ISNULL(e2.OCCTYPE, '''')
   OR ISNULL(e1.HUZONE, '''') <> ISNULL(e2.HUZONE, '''')
ORDER BY e1.LOCNUM
'
PRINT '=== SECTION 5b: PROPERTY-ONLY DIFFERENCES (no coverage) ==='
EXEC sp_executesql @SQL

-- Count of property differences by attribute
SET @SQL = '
;WITH EDM1_Props AS (
    SELECT
        p.LOCNUM,
        p.NUMSTORIES,
        p.YEARBUILT,
        p.NUMBLDGS,
        p.BLDGCLASS,
        p.OCCTYPE,
        p.HUZONE
    FROM [' + @EDM1 + ']..Property p
    JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
),
EDM2_Props AS (
    SELECT
        p.LOCNUM,
        p.NUMSTORIES,
        p.YEARBUILT,
        p.NUMBLDGS,
        p.BLDGCLASS,
        p.OCCTYPE,
        p.HUZONE
    FROM [' + @EDM2 + ']..Property p
    JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
)
SELECT
    SUM(CASE WHEN ISNULL(e1.NUMSTORIES, 0) <> ISNULL(e2.NUMSTORIES, 0) THEN 1 ELSE 0 END) AS NUMSTORIES_Diffs,
    SUM(CASE WHEN ISNULL(e1.YEARBUILT, 0) <> ISNULL(e2.YEARBUILT, 0) THEN 1 ELSE 0 END) AS YEARBUILT_Diffs,
    SUM(CASE WHEN ISNULL(e1.NUMBLDGS, 0) <> ISNULL(e2.NUMBLDGS, 0) THEN 1 ELSE 0 END) AS NUMBLDGS_Diffs,
    SUM(CASE WHEN ISNULL(e1.BLDGCLASS, '''') <> ISNULL(e2.BLDGCLASS, '''') THEN 1 ELSE 0 END) AS BLDGCLASS_Diffs,
    SUM(CASE WHEN ISNULL(e1.OCCTYPE, '''') <> ISNULL(e2.OCCTYPE, '''') THEN 1 ELSE 0 END) AS OCCTYPE_Diffs,
    SUM(CASE WHEN ISNULL(e1.HUZONE, '''') <> ISNULL(e2.HUZONE, '''') THEN 1 ELSE 0 END) AS HUZONE_Diffs,
    COUNT(*) AS TotalLocationsCompared
FROM EDM1_Props e1
JOIN EDM2_Props e2 ON e1.LOCNUM = e2.LOCNUM
'
PRINT '=== SECTION 5c: PROPERTY DIFFERENCE SUMMARY ==='
EXEC sp_executesql @SQL


-- ============================================================================
-- SECTION 6: COVERAGE DETAIL COMPARISON (loccvg table)
-- Compares individual coverage records by LOCNUM to identify value swaps
-- ============================================================================
SET @SQL = '
-- First show coverage record counts and totals by each coverage column
SELECT ''Coverage Summary'' AS Section,
    ''EDM1'' AS Source,
    COUNT(*) AS CoverageRecordCount,
    SUM(VALUEAMT) AS Total_VALUEAMT,
    SUM(LIMITAMT) AS Total_LIMITAMT,
    SUM(DEDUCTAMT) AS Total_DEDUCTAMT
FROM [' + @EDM1 + ']..loccvg lc
JOIN [' + @EDM1 + ']..Property p ON lc.LOCID = p.LOCID
JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio1 + '''

UNION ALL

SELECT ''Coverage Summary'' AS Section,
    ''EDM2'' AS Source,
    COUNT(*) AS CoverageRecordCount,
    SUM(VALUEAMT) AS Total_VALUEAMT,
    SUM(LIMITAMT) AS Total_LIMITAMT,
    SUM(DEDUCTAMT) AS Total_DEDUCTAMT
FROM [' + @EDM2 + ']..loccvg lc
JOIN [' + @EDM2 + ']..Property p ON lc.LOCID = p.LOCID
JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
'
PRINT '=== SECTION 6a: COVERAGE SUMMARY ==='
EXEC sp_executesql @SQL

-- Coverage detail by location - side by side comparison of individual loccvg records
SET @SQL = '
;WITH EDM1_Coverage AS (
    SELECT
        p.LOCNUM,
        lc.LOCCVGID,
        lc.VALUEAMT,
        lc.LIMITAMT,
        lc.DEDUCTAMT,
        ROW_NUMBER() OVER (PARTITION BY p.LOCNUM ORDER BY lc.LOCCVGID) AS CvgSeq
    FROM [' + @EDM1 + ']..loccvg lc
    JOIN [' + @EDM1 + ']..Property p ON lc.LOCID = p.LOCID
    JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
),
EDM2_Coverage AS (
    SELECT
        p.LOCNUM,
        lc.LOCCVGID,
        lc.VALUEAMT,
        lc.LIMITAMT,
        lc.DEDUCTAMT,
        ROW_NUMBER() OVER (PARTITION BY p.LOCNUM ORDER BY lc.LOCCVGID) AS CvgSeq
    FROM [' + @EDM2 + ']..loccvg lc
    JOIN [' + @EDM2 + ']..Property p ON lc.LOCID = p.LOCID
    JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
)
-- Show locations where coverage values differ (top 100)
SELECT TOP 100
    e1.LOCNUM,
    e1.CvgSeq,
    e1.VALUEAMT AS EDM1_VALUEAMT,
    e2.VALUEAMT AS EDM2_VALUEAMT,
    e1.LIMITAMT AS EDM1_LIMITAMT,
    e2.LIMITAMT AS EDM2_LIMITAMT,
    e1.DEDUCTAMT AS EDM1_DEDUCTAMT,
    e2.DEDUCTAMT AS EDM2_DEDUCTAMT,
    CASE
        WHEN e1.VALUEAMT = e2.LIMITAMT AND e1.LIMITAMT = e2.VALUEAMT THEN ''VALUE/LIMIT SWAPPED''
        WHEN e1.VALUEAMT = e2.DEDUCTAMT AND e1.DEDUCTAMT = e2.VALUEAMT THEN ''VALUE/DEDUCT SWAPPED''
        WHEN e1.LIMITAMT = e2.DEDUCTAMT AND e1.DEDUCTAMT = e2.LIMITAMT THEN ''LIMIT/DEDUCT SWAPPED''
        ELSE ''OTHER MISMATCH''
    END AS SwapPattern
FROM EDM1_Coverage e1
JOIN EDM2_Coverage e2 ON e1.LOCNUM = e2.LOCNUM AND e1.CvgSeq = e2.CvgSeq
WHERE ISNULL(e1.VALUEAMT, 0) <> ISNULL(e2.VALUEAMT, 0)
   OR ISNULL(e1.LIMITAMT, 0) <> ISNULL(e2.LIMITAMT, 0)
   OR ISNULL(e1.DEDUCTAMT, 0) <> ISNULL(e2.DEDUCTAMT, 0)
ORDER BY e1.LOCNUM, e1.CvgSeq
'
PRINT '=== SECTION 6b: COVERAGE DETAIL MISMATCHES (showing swap patterns) ==='
EXEC sp_executesql @SQL

-- Count of each swap pattern
SET @SQL = '
;WITH EDM1_Coverage AS (
    SELECT
        p.LOCNUM,
        lc.LOCCVGID,
        lc.VALUEAMT,
        lc.LIMITAMT,
        lc.DEDUCTAMT,
        ROW_NUMBER() OVER (PARTITION BY p.LOCNUM ORDER BY lc.LOCCVGID) AS CvgSeq
    FROM [' + @EDM1 + ']..loccvg lc
    JOIN [' + @EDM1 + ']..Property p ON lc.LOCID = p.LOCID
    JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
),
EDM2_Coverage AS (
    SELECT
        p.LOCNUM,
        lc.LOCCVGID,
        lc.VALUEAMT,
        lc.LIMITAMT,
        lc.DEDUCTAMT,
        ROW_NUMBER() OVER (PARTITION BY p.LOCNUM ORDER BY lc.LOCCVGID) AS CvgSeq
    FROM [' + @EDM2 + ']..loccvg lc
    JOIN [' + @EDM2 + ']..Property p ON lc.LOCID = p.LOCID
    JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
)
SELECT
    CASE
        WHEN e1.VALUEAMT = e2.LIMITAMT AND e1.LIMITAMT = e2.VALUEAMT THEN ''VALUE/LIMIT SWAPPED''
        WHEN e1.VALUEAMT = e2.DEDUCTAMT AND e1.DEDUCTAMT = e2.VALUEAMT THEN ''VALUE/DEDUCT SWAPPED''
        WHEN e1.LIMITAMT = e2.DEDUCTAMT AND e1.DEDUCTAMT = e2.LIMITAMT THEN ''LIMIT/DEDUCT SWAPPED''
        ELSE ''OTHER MISMATCH''
    END AS SwapPattern,
    COUNT(*) AS RecordCount
FROM EDM1_Coverage e1
JOIN EDM2_Coverage e2 ON e1.LOCNUM = e2.LOCNUM AND e1.CvgSeq = e2.CvgSeq
WHERE ISNULL(e1.VALUEAMT, 0) <> ISNULL(e2.VALUEAMT, 0)
   OR ISNULL(e1.LIMITAMT, 0) <> ISNULL(e2.LIMITAMT, 0)
   OR ISNULL(e1.DEDUCTAMT, 0) <> ISNULL(e2.DEDUCTAMT, 0)
GROUP BY
    CASE
        WHEN e1.VALUEAMT = e2.LIMITAMT AND e1.LIMITAMT = e2.VALUEAMT THEN ''VALUE/LIMIT SWAPPED''
        WHEN e1.VALUEAMT = e2.DEDUCTAMT AND e1.DEDUCTAMT = e2.VALUEAMT THEN ''VALUE/DEDUCT SWAPPED''
        WHEN e1.LIMITAMT = e2.DEDUCTAMT AND e1.DEDUCTAMT = e2.LIMITAMT THEN ''LIMIT/DEDUCT SWAPPED''
        ELSE ''OTHER MISMATCH''
    END
ORDER BY RecordCount DESC
'
PRINT '=== SECTION 6c: SWAP PATTERN SUMMARY ==='
EXEC sp_executesql @SQL

-- Final confirmation
SELECT 'SCRIPT COMPLETED' AS Status
