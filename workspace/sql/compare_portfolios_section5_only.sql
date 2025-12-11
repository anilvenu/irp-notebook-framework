-- Section 5b and 5c ONLY - Property comparison without coverage aggregation
-- This is the key diagnostic to find Property table differences

DECLARE @EDM1 VARCHAR(100) = 'RMS_EDM_202503_Quarterly_USFL_JP_Testing_BOXH'
DECLARE @EDM2 VARCHAR(100) = 'RM_EDM_202503_Testing_USFL_Comm_USFL_LJrF'
DECLARE @Portfolio1 VARCHAR(100) = 'USFL_Comm_Test'
DECLARE @Portfolio2 VARCHAR(100) = 'USFL_Commercial'

DECLARE @SQL NVARCHAR(MAX)

SELECT 'Starting Section 5c' AS Status

-- Section 5c: Count of property differences by attribute (run this first - it's the summary)
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
EXEC sp_executesql @SQL

SELECT 'Section 5c Complete - Starting Section 5b' AS Status

-- Section 5b: Show first 50 property differences (details)
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
    e1.LOCNUM,
    e1.NUMSTORIES AS EDM1_NUMSTORIES,
    e2.NUMSTORIES AS EDM2_NUMSTORIES,
    e1.YEARBUILT AS EDM1_YEARBUILT,
    e2.YEARBUILT AS EDM2_YEARBUILT,
    e1.NUMBLDGS AS EDM1_NUMBLDGS,
    e2.NUMBLDGS AS EDM2_NUMBLDGS,
    e1.BLDGCLASS AS EDM1_BLDGCLASS,
    e2.BLDGCLASS AS EDM2_BLDGCLASS
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
EXEC sp_executesql @SQL

SELECT 'ALL DONE' AS Status
