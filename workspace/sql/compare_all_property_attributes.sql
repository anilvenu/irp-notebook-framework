-- Compare ALL Property attributes between EDMs using LOCID-based matching
-- Uses ROW_NUMBER to match records in sequence order within each LOCNUM

DECLARE @EDM1 VARCHAR(100) = 'RMS_EDM_202503_Quarterly_USFL_JP_Testing_BOXH'
DECLARE @EDM2 VARCHAR(100) = 'RM_EDM_202503_Testing_USFL_Comm_USFL_LJrF'
DECLARE @Portfolio1 VARCHAR(100) = 'USFL_Comm_Test'
DECLARE @Portfolio2 VARCHAR(100) = 'USFL_Commercial'

DECLARE @SQL NVARCHAR(MAX)

-- First, let's see all columns in Property table
SET @SQL = '
SELECT TOP 1 *
FROM [' + @EDM1 + ']..Property p
JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
'
PRINT '=== Property Table Structure (showing column names) ==='
EXEC sp_executesql @SQL

-- Compare using ROW_NUMBER within each LOCNUM to handle duplicates
SET @SQL = '
;WITH EDM1_Props AS (
    SELECT
        p.LOCNUM,
        p.LOCID,
        p.NUMSTORIES,
        p.YEARBUILT,
        p.NUMBLDGS,
        p.BLDGSCHEME,
        p.BLDGCLASS,
        p.OCCSCHEME,
        p.OCCTYPE,
        p.HUZONE,
        p.LOCNAME,
        ROW_NUMBER() OVER (PARTITION BY p.LOCNUM ORDER BY p.LOCID) AS RowSeq
    FROM [' + @EDM1 + ']..Property p
    JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
),
EDM2_Props AS (
    SELECT
        p.LOCNUM,
        p.LOCID,
        p.NUMSTORIES,
        p.YEARBUILT,
        p.NUMBLDGS,
        p.BLDGSCHEME,
        p.BLDGCLASS,
        p.OCCSCHEME,
        p.OCCTYPE,
        p.HUZONE,
        p.LOCNAME,
        ROW_NUMBER() OVER (PARTITION BY p.LOCNUM ORDER BY p.LOCID) AS RowSeq
    FROM [' + @EDM2 + ']..Property p
    JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
)
SELECT
    SUM(CASE WHEN ISNULL(e1.NUMSTORIES, 0) <> ISNULL(e2.NUMSTORIES, 0) THEN 1 ELSE 0 END) AS NUMSTORIES_Diffs,
    SUM(CASE WHEN ISNULL(CAST(e1.YEARBUILT AS VARCHAR), '''') <> ISNULL(CAST(e2.YEARBUILT AS VARCHAR), '''') THEN 1 ELSE 0 END) AS YEARBUILT_Diffs,
    SUM(CASE WHEN ISNULL(e1.NUMBLDGS, 0) <> ISNULL(e2.NUMBLDGS, 0) THEN 1 ELSE 0 END) AS NUMBLDGS_Diffs,
    SUM(CASE WHEN ISNULL(e1.BLDGSCHEME, '''') <> ISNULL(e2.BLDGSCHEME, '''') THEN 1 ELSE 0 END) AS BLDGSCHEME_Diffs,
    SUM(CASE WHEN ISNULL(e1.BLDGCLASS, '''') <> ISNULL(e2.BLDGCLASS, '''') THEN 1 ELSE 0 END) AS BLDGCLASS_Diffs,
    SUM(CASE WHEN ISNULL(e1.OCCSCHEME, '''') <> ISNULL(e2.OCCSCHEME, '''') THEN 1 ELSE 0 END) AS OCCSCHEME_Diffs,
    SUM(CASE WHEN ISNULL(e1.OCCTYPE, '''') <> ISNULL(e2.OCCTYPE, '''') THEN 1 ELSE 0 END) AS OCCTYPE_Diffs,
    SUM(CASE WHEN ISNULL(e1.HUZONE, '''') <> ISNULL(e2.HUZONE, '''') THEN 1 ELSE 0 END) AS HUZONE_Diffs,
    SUM(CASE WHEN ISNULL(e1.LOCNAME, '''') <> ISNULL(e2.LOCNAME, '''') THEN 1 ELSE 0 END) AS LOCNAME_Diffs,
    COUNT(*) AS TotalRecordsCompared
FROM EDM1_Props e1
JOIN EDM2_Props e2 ON e1.LOCNUM = e2.LOCNUM AND e1.RowSeq = e2.RowSeq
'
PRINT '=== PROPERTY ATTRIBUTE DIFFERENCE SUMMARY (using RowSeq matching) ==='
EXEC sp_executesql @SQL

-- Check for unmatched records (different number of records per LOCNUM)
SET @SQL = '
;WITH EDM1_Counts AS (
    SELECT p.LOCNUM, COUNT(*) AS Cnt
    FROM [' + @EDM1 + ']..Property p
    JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
    GROUP BY p.LOCNUM
),
EDM2_Counts AS (
    SELECT p.LOCNUM, COUNT(*) AS Cnt
    FROM [' + @EDM2 + ']..Property p
    JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
    GROUP BY p.LOCNUM
)
SELECT ''Record Count Mismatches'' AS Issue,
    COUNT(*) AS LocnumsWithDifferentCounts
FROM EDM1_Counts e1
FULL OUTER JOIN EDM2_Counts e2 ON e1.LOCNUM = e2.LOCNUM
WHERE ISNULL(e1.Cnt, 0) <> ISNULL(e2.Cnt, 0)
'
PRINT '=== RECORD COUNT MISMATCHES ==='
EXEC sp_executesql @SQL

-- Show sample of actual differences if any exist
SET @SQL = '
;WITH EDM1_Props AS (
    SELECT
        p.LOCNUM,
        p.LOCID,
        p.NUMSTORIES,
        p.YEARBUILT,
        p.NUMBLDGS,
        p.BLDGSCHEME,
        p.BLDGCLASS,
        p.OCCSCHEME,
        p.OCCTYPE,
        p.HUZONE,
        ROW_NUMBER() OVER (PARTITION BY p.LOCNUM ORDER BY p.LOCID) AS RowSeq
    FROM [' + @EDM1 + ']..Property p
    JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
),
EDM2_Props AS (
    SELECT
        p.LOCNUM,
        p.LOCID,
        p.NUMSTORIES,
        p.YEARBUILT,
        p.NUMBLDGS,
        p.BLDGSCHEME,
        p.BLDGCLASS,
        p.OCCSCHEME,
        p.OCCTYPE,
        p.HUZONE,
        ROW_NUMBER() OVER (PARTITION BY p.LOCNUM ORDER BY p.LOCID) AS RowSeq
    FROM [' + @EDM2 + ']..Property p
    JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
    JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
    WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
)
SELECT TOP 20
    e1.LOCNUM,
    e1.RowSeq,
    e1.NUMSTORIES AS E1_NUMSTORIES, e2.NUMSTORIES AS E2_NUMSTORIES,
    e1.YEARBUILT AS E1_YEARBUILT, e2.YEARBUILT AS E2_YEARBUILT,
    e1.NUMBLDGS AS E1_NUMBLDGS, e2.NUMBLDGS AS E2_NUMBLDGS,
    e1.BLDGCLASS AS E1_BLDGCLASS, e2.BLDGCLASS AS E2_BLDGCLASS,
    e1.OCCTYPE AS E1_OCCTYPE, e2.OCCTYPE AS E2_OCCTYPE,
    e1.HUZONE AS E1_HUZONE, e2.HUZONE AS E2_HUZONE
FROM EDM1_Props e1
JOIN EDM2_Props e2 ON e1.LOCNUM = e2.LOCNUM AND e1.RowSeq = e2.RowSeq
WHERE ISNULL(e1.NUMSTORIES, 0) <> ISNULL(e2.NUMSTORIES, 0)
   OR ISNULL(CAST(e1.YEARBUILT AS VARCHAR), '''') <> ISNULL(CAST(e2.YEARBUILT AS VARCHAR), '''')
   OR ISNULL(e1.NUMBLDGS, 0) <> ISNULL(e2.NUMBLDGS, 0)
   OR ISNULL(e1.BLDGSCHEME, '''') <> ISNULL(e2.BLDGSCHEME, '''')
   OR ISNULL(e1.BLDGCLASS, '''') <> ISNULL(e2.BLDGCLASS, '''')
   OR ISNULL(e1.OCCSCHEME, '''') <> ISNULL(e2.OCCSCHEME, '''')
   OR ISNULL(e1.OCCTYPE, '''') <> ISNULL(e2.OCCTYPE, '''')
   OR ISNULL(e1.HUZONE, '''') <> ISNULL(e2.HUZONE, '''')
ORDER BY e1.LOCNUM
'
PRINT '=== SAMPLE OF ACTUAL DIFFERENCES (if any) ==='
EXEC sp_executesql @SQL

SELECT 'DONE' AS Status
