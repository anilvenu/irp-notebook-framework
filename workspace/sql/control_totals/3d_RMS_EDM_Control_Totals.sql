/**********************************************************************************************************************************************
Purpose:	This script obtains the RMS Exposure Database control totals that are used to compare to the Verisk Exposure Database control totals.
			The outputs are pasted into Spreadsheet "RMS Exposure Control Totals 202412.xlsx"
			This script must be run on the RMS SQL Server.
			Coordinate with the person doing the RMS preprocessing because you won't be able to run this query until that person has actually
			imported the data into RiskLink.
Author: Charlene Chia
Edited By: Claude Code (Rewritten to capture results in Python)
Instructions:
				1. Update quarter e.g. 202212 to 202412. Use Replace all function
				2. Execute the script
				3. Results are now captured in Python via execute_query_from_file()

SQL Server: RMS SQL Server
SQL Database: Various

Input Tables:	All RMS Exposure Database tables except for Flood Solutions and Other Flood
Output Tables:  Temp tables for intermediate storage, then returned as result sets

Runtime: <1 min

Changes from original:
	- Added temp tables to store results during cursor iteration
	- Modified dynamic SQL to INSERT INTO temp tables instead of direct SELECT
	- Added final SELECT statements that Python can capture
	- Results now returned as 10 separate result sets (DataFrames in Python)
**********************************************************************************************************************************************/

Use [{{ WORKSPACE_EDM }}]
 -- Create the DB Schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'asu') --Rename the schema name as desired
BEGIN
    EXEC('CREATE SCHEMA asu') --Rename the schema name as desired
END


--Step 1: Create list of EDMs
Drop Table if exists {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_{{ DATE_VALUE }}
Create Table {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_{{ DATE_VALUE }} (
	DBName VARCHAR(50));
Insert into {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_{{ DATE_VALUE }}
select name from sys.databases where name like '%EDM%' and name like '%{{ DATE_VALUE }}%' and name like '%{{ CYCLE_TYPE }}%' and name not like '%USFL%' ----CHANGE

--Step 1b: Create temp tables to store results from dynamic SQL
-- These temp tables allow Python to capture the results

-- Non-USFL Result Sets
DROP TABLE IF EXISTS #PolicySummary
CREATE TABLE #PolicySummary (
    PORTNAME VARCHAR(100),
    PolicyCount INT,
    PolicyLimit DECIMAL(18,2),
    PolicyPremium DECIMAL(18,2),
    AttachmentPoint DECIMAL(18,2),
    PolicyDeductible DECIMAL(18,2)
)

DROP TABLE IF EXISTS #LocationCounts
CREATE TABLE #LocationCounts (
    PORTNAME VARCHAR(100),
    LocationCountDistinct INT,
    LocationCountCampus INT
)

DROP TABLE IF EXISTS #LocationValues
CREATE TABLE #LocationValues (
    PORTNAME VARCHAR(100),
    TotalReplacementValue DECIMAL(18,2),
    LocationLimit DECIMAL(18,2)
)

DROP TABLE IF EXISTS #LocationDeductibles
CREATE TABLE #LocationDeductibles (
    PORTNAME VARCHAR(100),
    LocationDeductible DECIMAL(18,2)
)

-- USFL Flood Result Sets
DROP TABLE IF EXISTS #FloodAccountControls
CREATE TABLE #FloodAccountControls (
    PORTNAME VARCHAR(100),
    PolicyCount INT,
    PolicyPremium DECIMAL(18,2),
    PolicyLimit_NonCommercialFlood DECIMAL(18,2),
    AttachmentPoint DECIMAL(18,2),
    PolicyDeductible DECIMAL(18,2)
)

DROP TABLE IF EXISTS #FloodCommercialPolicyLimit
CREATE TABLE #FloodCommercialPolicyLimit (
    USFL_Commercial_PolicyLimit DECIMAL(18,2)
)

DROP TABLE IF EXISTS #FloodCommercialSublimit
CREATE TABLE #FloodCommercialSublimit (
    PORTNAME VARCHAR(100),
    Policy_Sublimit DECIMAL(18,2)
)

DROP TABLE IF EXISTS #FloodLocationCounts
CREATE TABLE #FloodLocationCounts (
    PORTNAME VARCHAR(100),
    LocationCountDistinct INT,
    LocationCountCampus INT
)

DROP TABLE IF EXISTS #FloodLocationValues
CREATE TABLE #FloodLocationValues (
    PORTNAME VARCHAR(100),
    TotalReplacementValue DECIMAL(18,2),
    LocationLimit DECIMAL(18,2),
    LocationDeductible_NonCommercialFlood DECIMAL(18,2)
)

DROP TABLE IF EXISTS #FloodCommercialLocationDeductible
CREATE TABLE #FloodCommercialLocationDeductible (
    USFL_Commercial_LocationDeductible DECIMAL(18,2)
)

--Step 2: Process non-USFL databases and store results in temp tables

SET NOCOUNT ON;

DECLARE @DBName		VARCHAR(50)
DECLARE @DBList		VARCHAR(100)
DECLARE	@SQL		VARCHAR(MAX)

DECLARE Database_Cursor CURSOR LOCAL FOR
Select * From {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_{{ DATE_VALUE }} --UPDATE

OPEN Database_Cursor
FETCH NEXT FROM Database_Cursor INTO @DBName
WHILE @@FETCH_STATUS = 0

BEGIN
	SET @DBName = @DBName
	SET @SQL =
	'
/*Geocoding Summary - INSERT into temp table*/
INSERT INTO #PolicySummary (PORTNAME, PolicyCount, PolicyLimit, PolicyPremium, AttachmentPoint, PolicyDeductible)
Select c.PORTNAME
,Count(*) PolicyCount
,SUM(a.BLANLIMAMT) PolicyLimit
,Sum(BLANPREAMT) PolicyPremium
,SUM(UNDCOVAMT) AttachmentPoint
,SUM(BLANDEDAMT) PolicyDeductible
From '+@DBName+'..policy a
Join '+@DBName+'..portacct b on a.ACCGRPID = b.ACCGRPID
Join '+@DBName+'..portinfo c on c.PORTINFOID = b.PORTINFOID
Group by c.PORTNAME

/*	Location File Controls: LocationCountDistinct, LocationCountCampus - INSERT into temp table	*/
INSERT INTO #LocationCounts (PORTNAME, LocationCountDistinct, LocationCountCampus)
Select e.PORTNAME,
Count(distinct(a.LOCNUM)) LocationCountDistinct
,Count(a.LOCNUM) LocationCountCampus
From '+@DBName+'..Property a
Join '+@DBName+'..accgrp c on c.ACCGRPID = a.ACCGRPID
Join '+@DBName+'..portacct d on c.ACCGRPID = d.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by e.PORTNAME

/*	Location File Controls: Total Replacement value, Location Limit - INSERT into temp table	*/
INSERT INTO #LocationValues (PORTNAME, TotalReplacementValue, LocationLimit)
Select e.PORTNAME,  SUM(b.VALUEAMT) TotalReplacementValue, SUM(b.LIMITAMT) LocationLimit
From '+@DBName+'..Property a
Join '+@DBName+'..loccvg b on a.LOCID = b.LOCID
Join '+@DBName+'..accgrp c on c.ACCGRPID = a.ACCGRPID
Join '+@DBName+'..portacct d on c.ACCGRPID = d.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by e.PORTNAME

/* Location Deductibles - INSERT into temp table */
INSERT INTO #LocationDeductibles (PORTNAME, LocationDeductible)
select PORTNAME, sum(LocationDeductible) LocationDeductible
from(
Select e.PORTNAME, SUM(a.SITEDEDAMT) LocationDeductible
From '+@DBName+'..eqdet a
Join '+@DBName+'..Property b on a.LOCID = b.LOCID
Join '+@DBName+'..accgrp c on c.ACCGRPID = b.ACCGRPID
Join '+@DBName+'..portacct d on d.ACCGRPID = c.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by e.PORTNAME

UNION ALL

Select e.PORTNAME, SUM(a.SITEDEDAMT) LocationDeductible
From '+@DBName+'..hudet a
Join '+@DBName+'..Property b on a.LOCID = b.LOCID
Join '+@DBName+'..accgrp c on c.ACCGRPID = b.ACCGRPID
Join '+@DBName+'..portacct d on d.ACCGRPID = c.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by e.PORTNAME

UNION ALL

Select e.PORTNAME Port,
sum(a.SITEDEDAMT)
LocationDeductible
From '+@DBName+'..todet a
Join '+@DBName+'..Property b on a.LOCID = b.LOCID
Join '+@DBName+'..accgrp c on c.ACCGRPID = b.ACCGRPID
Join '+@DBName+'..portacct d on d.ACCGRPID = c.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by e.PORTNAME

UNION ALL

Select e.PORTNAME Port,
sum(a.SITEDEDAMT)
LocationDeductible
From '+@DBName+'..frdet a
Join '+@DBName+'..Property b on a.LOCID = b.LOCID
Join '+@DBName+'..accgrp c on c.ACCGRPID = b.ACCGRPID
Join '+@DBName+'..portacct d on d.ACCGRPID = c.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by e.PORTNAME

UNION ALL

Select e.PORTNAME Port,
SUM(a.DEDUCTAMT)
LocationDeductible
From '+@DBName+'..LocCvg a
Join '+@DBName+'..Property b on a.LOCID = b.LOCID
Join '+@DBName+'..accgrp c on c.ACCGRPID = b.ACCGRPID
Join '+@DBName+'..portacct d on d.ACCGRPID = c.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by e.PORTNAME
)x
group by PORTNAME

'

Exec (@SQL)
	FETCH NEXT FROM Database_Cursor INTO  @DBName
END
CLOSE Database_Cursor
DEALLOCATE Database_Cursor

/*========================================================================================
USFL Totals - separate from other perils due to different calculations for some totals
========================================================================================*/
--Step 3: Create list of USFL EDMs
Drop Table IF EXISTS {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_Flood_{{ DATE_VALUE }}
Create Table {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_Flood_{{ DATE_VALUE }} (
	DBName VARCHAR(50));
Insert into {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_Flood_{{ DATE_VALUE }}
select name from sys.databases where name like '%EDM%' and name like '%{{ DATE_VALUE }}%' and name like '%{{ CYCLE_TYPE }}%' and name like '%USFL%' ----CHANGE

--Step 4: Process USFL databases and store results in temp tables
SET NOCOUNT ON;

DECLARE @DBName_Flood			VARCHAR(50)
DECLARE @DBList_Flood			VARCHAR(100)
DECLARE	@SQL_Flood				VARCHAR(MAX)

DECLARE Database_Cursor CURSOR LOCAL FOR
Select * From {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_Flood_{{ DATE_VALUE }} --UPDATE

OPEN Database_Cursor
FETCH NEXT FROM Database_Cursor INTO @DBName_Flood
WHILE @@FETCH_STATUS = 0

BEGIN
	SET @DBName_Flood = @DBName_Flood
	SET @SQL_Flood =
	'

/*	Account File Controls - INSERT into temp table	*/
INSERT INTO #FloodAccountControls (PORTNAME, PolicyCount, PolicyPremium, PolicyLimit_NonCommercialFlood, AttachmentPoint, PolicyDeductible) -- TODO: Rename PolicyLimit_NonCommercialFlood to PolicyLimit
Select CASE WHEN c.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE c.PORTNAME END AS PORTNAME
,Count(distinct a.ACCGRPID) PolicyCount
,SUM(BLANPREAMT) PolicyPremium
,SUM(a.BLANLIMAMT) PolicyLimit_NonCommercialFlood
,SUM(UNDCOVAMT) AttachmentPoint
,SUM(BLANDEDAMT) PolicyDeductible
From '+@DBName_Flood+'..policy a
Join '+@DBName_Flood+'..portacct b on a.ACCGRPID = b.ACCGRPID
Join '+@DBName_Flood+'..portinfo c on c.PORTINFOID = b.PORTINFOID
Group by CASE WHEN c.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE c.PORTNAME END

/*Commercial Flood Policy Limit - INSERT into temp table*/
INSERT INTO #FloodCommercialPolicyLimit (USFL_Commercial_PolicyLimit) -- TODO: UPDATE #FloodAccountControls.PolicyLimit where portname = "USFL_Commercial"
SELECT (PolicyLimit+PolicyCoverageLimit) as USFL_Commercial_PolicyLimit
FROM (
    Select c.PORTNAME, SUM(a.BLANLIMAMT) PolicyLimit, SUM(d.LIMITAMT) PolicyCoverageLimit
    From '+@DBName_Flood+'..policy a
    Join '+@DBName_Flood+'..portacct b on a.ACCGRPID = b.ACCGRPID
    Join '+@DBName_Flood+'..portinfo c on c.PORTINFOID = b.PORTINFOID
    Left Join '+@DBName_Flood+'..polcvg d on a.POLICYID = d.POLICYID
    WHERE c.PORTNAME Like ''%Commercial%''
    Group by c.PORTNAME
) CommFlood_PolicyLimit

/*	Account File Controls Commercial Sublimit - INSERT into temp table	*/
INSERT INTO #FloodCommercialSublimit (PORTNAME, Policy_Sublimit)
Select CASE WHEN c.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE c.PORTNAME END AS PORTNAME
,SUM(d.LIMIT) Policy_Sublimit
From '+@DBName_Flood+'..policy a
Join '+@DBName_Flood+'..portacct b on a.ACCGRPID = b.ACCGRPID
Join '+@DBName_Flood+'..portinfo c on c.PORTINFOID = b.PORTINFOID
Join '+@DBName_Flood+'..policyconditions d on a.POLICYID = d.CONDITIONID
Group by CASE WHEN c.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE c.PORTNAME END

/*	Location File Controls: LocationCountDistinct, LocationCountCampus - INSERT into temp table	*/
INSERT INTO #FloodLocationCounts (PORTNAME, LocationCountDistinct, LocationCountCampus)
Select CASE WHEN e.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE e.PORTNAME END AS PORTNAME
,Count(distinct(a.LOCNUM)) LocationCountDistinct
,Count(a.LOCNUM) LocationCountCampus
From '+@DBName_Flood+'..Property a
Join '+@DBName_Flood+'..accgrp c on c.ACCGRPID = a.ACCGRPID
Join '+@DBName_Flood+'..portacct d on c.ACCGRPID = d.ACCGRPID
Join '+@DBName_Flood+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by CASE WHEN e.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE e.PORTNAME END

/*	Location File Controls: Total Replacement value, Location Limit - INSERT into temp table	*/
INSERT INTO #FloodLocationValues (PORTNAME, TotalReplacementValue, LocationLimit, LocationDeductible_NonCommercialFlood)
Select CASE WHEN e.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE e.PORTNAME END AS PORTNAME,
SUM(b.VALUEAMT) TotalReplacementValue, SUM(b.LIMITAMT) LocationLimit, SUM(DEDUCTAMT) LocationDeductible_NonCommercialFlood
From '+@DBName_Flood+'..Property a
Join '+@DBName_Flood+'..loccvg b on a.LOCID = b.LOCID
Join '+@DBName_Flood+'..accgrp c on c.ACCGRPID = a.ACCGRPID
Join '+@DBName_Flood+'..portacct d on c.ACCGRPID = d.ACCGRPID
Join '+@DBName_Flood+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by CASE WHEN e.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE e.PORTNAME END

/*Commercial Flood Location Deductible - INSERT into temp table*/
INSERT INTO #FloodCommercialLocationDeductible (USFL_Commercial_LocationDeductible)
SELECT SUM(LocationDeductible_HUDet+LocationDeductible_LocCvg) as USFL_Commercial_LocationDeductible
FROM (
    SELECT e.PORTNAME, SUM(b.DEDUCTAMT) as LocationDeductible_LocCvg
    From '+@DBName_Flood+'..Property a
    Join '+@DBName_Flood+'..loccvg b on a.LOCID = b.LOCID
    Join '+@DBName_Flood+'..accgrp c on c.ACCGRPID = a.ACCGRPID
    Join '+@DBName_Flood+'..portacct d on c.ACCGRPID = d.ACCGRPID
    Join '+@DBName_Flood+'..portinfo e on e.PORTINFOID = d.PORTINFOID
    WHERE e.PORTNAME Like ''%Commercial%''
    Group by e.PORTNAME
) CommFlood_LocationDeductible_LocCvg
JOIN (
    SELECT e.PORTNAME, SUM(a.SITEDEDAMT) LocationDeductible_HUDet
    From '+@DBName_Flood+'..hudet a
    Join '+@DBName_Flood+'..Property b on a.LOCID = b.LOCID
    Join '+@DBName_Flood+'..accgrp c on c.ACCGRPID = b.ACCGRPID
    Join '+@DBName_Flood+'..portacct d on d.ACCGRPID = c.ACCGRPID
    Join '+@DBName_Flood+'..portinfo e on e.PORTINFOID = d.PORTINFOID
    WHERE e.PORTNAME Like ''%Commercial%''
    Group by e.PORTNAME
) CommFlood_LocationDeductible_HUDet
ON CommFlood_LocationDeductible_LocCvg.PORTNAME = CommFlood_LocationDeductible_HUDet.PORTNAME

'
	EXEC (@SQL_Flood)
	FETCH NEXT FROM Database_Cursor INTO  @DBName_Flood
END
CLOSE Database_Cursor
DEALLOCATE Database_Cursor

/*========================================================================================
Step 5: Return results from temp tables (these are captured by Python!)
========================================================================================*/

-- Non-USFL Results (4 result sets)
SELECT * FROM #PolicySummary ORDER BY PORTNAME

SELECT * FROM #LocationCounts ORDER BY PORTNAME

SELECT * FROM #LocationValues ORDER BY PORTNAME

SELECT * FROM #LocationDeductibles ORDER BY PORTNAME

-- USFL Flood Results (6 result sets)
SELECT * FROM #FloodAccountControls ORDER BY PORTNAME

SELECT * FROM #FloodCommercialPolicyLimit

SELECT * FROM #FloodCommercialSublimit ORDER BY PORTNAME

SELECT * FROM #FloodLocationCounts ORDER BY PORTNAME

SELECT * FROM #FloodLocationValues ORDER BY PORTNAME

SELECT * FROM #FloodCommercialLocationDeductible

/*========================================================================================
Step 6: Cleanup temp tables
========================================================================================*/
DROP TABLE IF EXISTS #PolicySummary
DROP TABLE IF EXISTS #LocationCounts
DROP TABLE IF EXISTS #LocationValues
DROP TABLE IF EXISTS #LocationDeductibles
DROP TABLE IF EXISTS #FloodAccountControls
DROP TABLE IF EXISTS #FloodCommercialPolicyLimit
DROP TABLE IF EXISTS #FloodCommercialSublimit
DROP TABLE IF EXISTS #FloodLocationCounts
DROP TABLE IF EXISTS #FloodLocationValues
DROP TABLE IF EXISTS #FloodCommercialLocationDeductible
