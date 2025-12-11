-- Check for duplicate LOCNUMs within each portfolio
-- This will reveal if the same LOCNUM has multiple Property records

DECLARE @EDM1 VARCHAR(100) = 'RMS_EDM_202503_Quarterly_USFL_JP_Testing_BOXH'
DECLARE @EDM2 VARCHAR(100) = 'RM_EDM_202503_Testing_USFL_Comm_USFL_LJrF'
DECLARE @Portfolio1 VARCHAR(100) = 'USFL_Comm_Test'
DECLARE @Portfolio2 VARCHAR(100) = 'USFL_Commercial'

DECLARE @SQL NVARCHAR(MAX)

-- Check EDM1 for duplicate LOCNUMs
SET @SQL = '
SELECT ''EDM1 Duplicate LOCNUMs'' AS Check_Type,
    p.LOCNUM,
    COUNT(*) AS RecordCount,
    COUNT(DISTINCT p.NUMSTORIES) AS DistinctNumStories,
    MIN(p.NUMSTORIES) AS MinStories,
    MAX(p.NUMSTORIES) AS MaxStories
FROM [' + @EDM1 + ']..Property p
JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio1 + '''
GROUP BY p.LOCNUM
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
'
EXEC sp_executesql @SQL

-- Check EDM2 for duplicate LOCNUMs
SET @SQL = '
SELECT ''EDM2 Duplicate LOCNUMs'' AS Check_Type,
    p.LOCNUM,
    COUNT(*) AS RecordCount,
    COUNT(DISTINCT p.NUMSTORIES) AS DistinctNumStories,
    MIN(p.NUMSTORIES) AS MinStories,
    MAX(p.NUMSTORIES) AS MaxStories
FROM [' + @EDM2 + ']..Property p
JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
GROUP BY p.LOCNUM
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
'
EXEC sp_executesql @SQL

-- Summary counts
SET @SQL = '
SELECT ''EDM1 Summary'' AS Source,
    COUNT(*) AS TotalPropertyRecords,
    COUNT(DISTINCT LOCNUM) AS UniqueLocnums,
    COUNT(*) - COUNT(DISTINCT LOCNUM) AS DuplicateRecords
FROM [' + @EDM1 + ']..Property p
JOIN [' + @EDM1 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM1 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio1 + '''

UNION ALL

SELECT ''EDM2 Summary'' AS Source,
    COUNT(*) AS TotalPropertyRecords,
    COUNT(DISTINCT LOCNUM) AS UniqueLocnums,
    COUNT(*) - COUNT(DISTINCT LOCNUM) AS DuplicateRecords
FROM [' + @EDM2 + ']..Property p
JOIN [' + @EDM2 + ']..portacct pa ON p.ACCGRPID = pa.ACCGRPID
JOIN [' + @EDM2 + ']..portinfo pi ON pa.PORTINFOID = pi.PORTINFOID
WHERE pi.PORTNAME = ''' + @Portfolio2 + '''
'
EXEC sp_executesql @SQL

SELECT 'DONE' AS Status
