/**************************************************************************************************************************************************************************************
Purpose: This script obtains Geocoding summary for the portfolios after the geocoding stage. 
			Outputs from this query needs to be copied to "RMS_Exposure Control Totals_202306" spreadsheet.
			This script should be run RMS Sql server.
			Coordinate with the person doing the RMS preprocessing because you won't be able to run this query until that person has actually
			imported and geocoded the data into RiskLink.

Auther: Sridevi
Edited by:

Instructions:	1. Update quarter for each Database name from 202306 to 202306.
				2. Execute the script
				3. Step 1 in this query: This table is already existing when "Exposure Control Totals " are created. If not created, run the step 1 here.

SQL Server: RMS SQL Server
SQL Database: Various

Input Tables:	All RMS Exposure Database tables
Output Tables:  No output tables

******************************************************************************************************************************************************/
Use [{{ WORKSPACE_EDM }}]
 -- Create the DB Schema if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'asu') --Rename the schema name as desired
BEGIN
    EXEC('CREATE SCHEMA asu') --Rename the schema name as desired
END


--Step 1: Create list of EDMs 
Drop Table {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_{{ DATE_VALUE }}
Create Table {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_{{ DATE_VALUE }} (
	DBName VARCHAR(50));
Insert into {{ WORKSPACE_EDM }}.asu.EDM_List_{{ CYCLE_TYPE }}_{{ DATE_VALUE }}
select name from sys.databases where name like '%EDM%' and name like '%{{ DATE_VALUE }}%' and name like '%{{ CYCLE_TYPE }}%' ----CHANGE 

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
Select '''+@DBName+''' DBname ,e.PORTNAME, f.GeoResolutionCode, 
	CASE
		WHEN f.GeoResolutionCode = ''1'' THEN ''Coordinate''
		WHEN f.GeoResolutionCode = ''2'' THEN ''Street address''
		WHEN f.GeoResolutionCode =  ''3'' Then ''High Resolution Postcode / Street Block''
		WHEN f.GeoResolutionCode = ''4'' then ''Street Name''
		WHEN f.GeoResolutionCode = ''5'' then ''Postcode''
		WHEN f.GeoResolutionCode =  ''6'' then ''City District/ Admin4''
		WHEN f.GeoResolutionCode =  ''7'' then ''City/Town''
		WHEN f.GeoResolutionCode = ''8'' then ''District / Municipality/Admin3''
		WHEN f.GeoResolutionCode = ''9'' then ''County/Admin2''
		WHEN f.GeoResolutionCode = ''10'' then ''State/Province/Admin1''
		ELSE ''Null''
		END as GeocodeDescription,
Count(distinct(a.LOCNUM)) RiskCount, SUM(b.LIMITAMT) TIV, SUM(b.VALUEAMT) TRV
From '+@DBName+'..Property a 
Join '+@DBName+'..loccvg b on a.LOCID = b.LOCID
Join '+@DBName+'..accgrp c on c.ACCGRPID = a.ACCGRPID
Join '+@DBName+'..portacct d on c.ACCGRPID = d.ACCGRPID
Join '+@DBName+'..portinfo e on e.PORTINFOID = d.PORTINFOID
JOIN '+@DBName+'..Address f on f.AddressID = a.AddressID
where portname in (''USFF'',''USEQ'',''CBHU'',''CBEQ'',''USST'',''USOW'',''USWF'',''USHU_Full'',''USHU_Leak'',''USFL_Other'',''USFL_Excess'',''USFL_Commercial'')
Group by e.PORTNAME, f.GeoResolutionCode
Order by 1, 2

'

Exec (@SQL)
	FETCH NEXT FROM Database_Cursor INTO  @DBName
END
CLOSE Database_Cursor
DEALLOCATE Database_Cursor


/********************************************************************
0 = None

1 = Coordinate

2 = Street address (Building/Parcel/ Street Interpolation)

3 = High Resolution Postcode / Street Block

4 = Street Name

5 = Postcode

6 = City District/ Admin4

7 = City/Town

8 = District / Municipality/Admin3

9 = County/Admin2

10 = State/Province/ Admin1
*/

