/**********************************************************************************************************************************************
Purpose:	This script obtains the RMS Exposure Database control totals that are used to compare to the Verisk Exposure Database control totals.
			The outputs are pasted into Spreadsheet "RMS Exposure Control Totals 202412.xlsx"
			This script must be run on the RMS SQL Server.
			Coordinate with the person doing the RMS preprocessing because you won't be able to run this query until that person has actually
			imported the data into RiskLink.
Author: Charlene Chia
Edited By: 
Instructions: 
				1. Update quarter e.g. 202212 to 202412. Use Replace all function
				2. Execute the script

SQL Server: RMS SQL Server
SQL Database: Various

Input Tables:	All RMS Exposure Database tables except for Flood Solutions and Other Flood
Output Tables:  No output tables

Runtime: <1 min
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

--Step 2: Update the table that you created in Step 1 below. Then select everthing below this line and execute.

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

/*Geocoding Summary */
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
Order by 1

/*	Location File Controls: LocationCountDistinct, LocationCountCampus	*/
Select e.PORTNAME,  
Count(distinct(a.LOCNUM)) LocationCountDistinct
,Count(a.LOCNUM) LocationCountCampus
From '+@DBName+'..Property a 
Join '+@DBName+'..accgrp c on c.ACCGRPID = a.ACCGRPID
Join '+@DBName+'..portacct d on c.ACCGRPID = d.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by e.PORTNAME
Order by 1

/*	Location File Controls: Total Replacement value, Location Limit	*/
Select e.PORTNAME,  SUM(b.VALUEAMT) TotalReplacementValue, SUM(b.LIMITAMT) LocationLimit 
From '+@DBName+'..Property a 
Join '+@DBName+'..loccvg b on a.LOCID = b.LOCID
Join '+@DBName+'..accgrp c on c.ACCGRPID = a.ACCGRPID
Join '+@DBName+'..portacct d on c.ACCGRPID = d.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by e.PORTNAME
Order by 1



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
union all
Select e.PORTNAME Port,
sum(a.SITEDEDAMT)
LocationDeductible
From '+@DBName+'..frdet a
Join '+@DBName+'..Property b on a.LOCID = b.LOCID
Join '+@DBName+'..accgrp c on c.ACCGRPID = b.ACCGRPID
Join '+@DBName+'..portacct d on d.ACCGRPID = c.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by e.PORTNAME

union all
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
Order by 1


'

Exec (@SQL)
	FETCH NEXT FROM Database_Cursor INTO  @DBName
END
CLOSE Database_Cursor
DEALLOCATE Database_Cursor

/*========================================================================================
USFL Totals - separate from other perils due to different calculations for some totals
========================================================================================*/
--Step 1: Create list of EDMs 
Drop Table IF EXISTS {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_Flood_{{ DATE_VALUE }}
Create Table {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_Flood_{{ DATE_VALUE }} (
	DBName VARCHAR(50));
Insert into {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_Flood_{{ DATE_VALUE }}
select name from sys.databases where name like '%EDM%' and name like '%{{ DATE_VALUE }}%' and name like '%{{ CYCLE_TYPE }}%' and name like '%USFL%' ----CHANGE 

--Step 2: Update the table that you created in Step 1 below. Then select everthing below this line and execute.
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

/*	Account File Controls	*/
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
Order by 1

/*Commercial Flood Policy Limit*/
BEGIN WITH CommFlood_PolicyLimit AS (
Select c.PORTNAME, SUM(a.BLANLIMAMT) PolicyLimit, SUM(d.LIMITAMT) PolicyCoverageLimit
From '+@DBName_Flood+'..policy a
Join '+@DBName_Flood+'..portacct b on a.ACCGRPID = b.ACCGRPID
Join '+@DBName_Flood+'..portinfo c on c.PORTINFOID = b.PORTINFOID
Left Join '+@DBName_Flood+'..polcvg d on a.POLICYID = d.POLICYID
WHERE c.PORTNAME Like ''%Commercial%''
Group by c.PORTNAME )
SELECT (PolicyLimit+PolicyCoverageLimit) as USFL_Commercial_PolicyLimit FROM CommFlood_PolicyLimit END

/*	Account File Controls Commercial Sublimit	*/
Select CASE WHEN c.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE c.PORTNAME END AS PORTNAME
,SUM(d.LIMIT) Policy_Sublimit
From '+@DBName_Flood+'..policy a
Join '+@DBName_Flood+'..portacct b on a.ACCGRPID = b.ACCGRPID
Join '+@DBName_Flood+'..portinfo c on c.PORTINFOID = b.PORTINFOID
Join '+@DBName_Flood+'..policyconditions d on a.POLICYID = d.CONDITIONID
Group by CASE WHEN c.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE c.PORTNAME END
Order by 1

/*	Location File Controls: LocationCountDistinct, LocationCountCampus	*/
Select CASE WHEN e.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE e.PORTNAME END AS PORTNAME
,Count(distinct(a.LOCNUM)) LocationCountDistinct
,Count(a.LOCNUM) LocationCountCampus
From '+@DBName_Flood+'..Property a 
Join '+@DBName_Flood+'..accgrp c on c.ACCGRPID = a.ACCGRPID
Join '+@DBName_Flood+'..portacct d on c.ACCGRPID = d.ACCGRPID
Join '+@DBName_Flood+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by CASE WHEN e.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE e.PORTNAME END
Order by 1

/*	Location File Controls: Total Replacement value, Location Limit	*/
Select CASE WHEN e.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE e.PORTNAME END AS PORTNAME,  
SUM(b.VALUEAMT) TotalReplacementValue, SUM(b.LIMITAMT) LocationLimit, SUM(DEDUCTAMT) LocationDeductible_NonCommercialFlood
From '+@DBName_Flood+'..Property a 
Join '+@DBName_Flood+'..loccvg b on a.LOCID = b.LOCID
Join '+@DBName_Flood+'..accgrp c on c.ACCGRPID = a.ACCGRPID
Join '+@DBName_Flood+'..portacct d on c.ACCGRPID = d.ACCGRPID
Join '+@DBName_Flood+'..portinfo e on e.PORTINFOID = d.PORTINFOID
Group by CASE WHEN e.PORTNAME Like ''%Clay%'' THEN ''USFL_Other_Clayton'' ELSE e.PORTNAME END
Order by 1

/*Commercial Flood Location Deductible*/
BEGIN WITH CommFlood_LocationDeductible_LocCvg AS (
SELECT e.PORTNAME, SUM(b.DEDUCTAMT) as LocationDeductible_LocCvg
From '+@DBName_Flood+'..Property a 
Join '+@DBName_Flood+'..loccvg b on a.LOCID = b.LOCID
Join '+@DBName_Flood+'..accgrp c on c.ACCGRPID = a.ACCGRPID
Join '+@DBName_Flood+'..portacct d on c.ACCGRPID = d.ACCGRPID
Join '+@DBName_Flood+'..portinfo e on e.PORTINFOID = d.PORTINFOID
WHERE e.PORTNAME Like ''%Commercial%''
Group by e.PORTNAME ) ,
CommFlood_LocationDeductible_HUDet AS (
SELECT e.PORTNAME, SUM(a.SITEDEDAMT) LocationDeductible_HUDet
From '+@DBName_Flood+'..hudet a
Join '+@DBName_Flood+'..Property b on a.LOCID = b.LOCID
Join '+@DBName_Flood+'..accgrp c on c.ACCGRPID = b.ACCGRPID
Join '+@DBName_Flood+'..portacct d on d.ACCGRPID = c.ACCGRPID
Join '+@DBName_Flood+'..portinfo e on e.PORTINFOID = d.PORTINFOID
WHERE e.PORTNAME Like ''%Commercial%''
Group by e.PORTNAME )
SELECT SUM(LocationDeductible_HUDet+LocationDeductible_LocCvg) as USFL_Commercial_LocationDeductible 
FROM CommFlood_LocationDeductible_LocCvg a
JOIN CommFlood_LocationDeductible_HUDet b on a.PORTNAME = b.PORTNAME
END

'
	EXEC (@SQL_Flood)
	FETCH NEXT FROM Database_Cursor INTO  @DBName_Flood
END
CLOSE Database_Cursor
DEALLOCATE Database_Cursor





