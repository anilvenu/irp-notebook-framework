/**********************************************************************************************************************************************
Purpose: This script creates the Risk Modeler Location import file for Other Flood exposures
Author: Charlene Chia
Edited by: Jillian Perkins
Instructions:
				1. Update all input and output tables names to the current quarter. Use replace all.
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Table:	CombinedData_{{ DATE_VALUE }}_Working
Output Tables:
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

-- US Other Flood Location File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location
SELECT
	LocationID AS ACCNTNUM
	,LocationID AS LOCNUM
	,PolicyNumber AS LOCNAME
	,'' AS ADDRESSNUM
	,Street AS STREETNAME
	,'' AS DISTRICT
	,'' AS DSTRCTCODE
	,City AS CITY
	,'' AS CITYCODE
	,'' AS STATE
	,State AS STATECODE
	,ZIPCODE AS POSTALCODE
	,County AS COUNTY
	,CountyNBR AS COUNTYCODE
	,'' AS CRESTA
	,(Case when GeocodingLevel not in ('Zipcode Level','UW_Spectrum_Zipcode Level') then Latitude else '0' end)  AS LATITUDE
	,(Case when GeocodingLevel not in ('Zipcode Level','UW_Spectrum_Zipcode Level') then Longitude else '0' end) AS LONGITUDE
	,Model_SQF AS FLOORAREA
	,'2' AS AREAUNIT
	,'ISO3A' AS CNTRYSCHEME
	,CASE WHEN State IN ('PR') THEN 'PRI' WHEN State IN ('VI') THEN 'VIR' ELSE 'USA' END AS CNTRYCODE
	,1 AS NUMBLDGS
	,'RMS' AS BLDGSCHEME
	,RMS_ConstCode_HU AS BLDGCLASS
	,'ATC' AS OCCSCHEME
	,RMS_OccType_ATC AS OCCTYPE
	,CASE WHEN Model_YearBuilt = '0' THEN '12/31/9999'
		WHEN RTRIM(Model_YearBuilt) <= '1800' THEN '12/31/9999'
		ELSE '12/31/'+Model_YearBuilt END AS YEARBUILT
	,Model_NumberofStories AS NUMSTORIES
	,CovAValue+FloodAttachmentPoint_CovA AS WSCV4VAL
	,'USD' AS WSCV4VCUR
	,CovBValue AS WSCV5VAL
	,'USD' AS WSCV5VCUR
	,CovCValue AS WSCV6VAL
	,'USD' AS WSCV6VCUR
	,CovDValue AS WSCV7VAL
	,'USD' AS WSCV7VCUR
	,CovALimit_Flood AS WSCV4LIMIT
	,'USD' AS WSCV4LCUR
	,CovBLimit_Flood AS WSCV5LIMIT
	,'USD' AS WSCV5LCUR
	,CovCLimit_Flood AS WSCV6LIMIT
	,'USD' AS WSCV6LCUR
	,CovDLimit_Flood AS WSCV7LIMIT
	,'USD' AS WSCV7LCUR
	,CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 THEN 0 ELSE FloodDed_CovA END AS WSCV4DED
	,'USD' AS WSCV4DCUR
	,CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 THEN 0 ELSE FloodDed_CovB END AS WSCV5DED
	,'USD' AS WSCV5DCUR
	,CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 THEN 0 ELSE FloodDed_CovC END AS WSCV6DED
	,'USD' AS WSCV6DCUR
	,CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 THEN 0 ELSE FloodDed_CovD END AS WSCV7DED
	,'USD' AS WSCV7DCUR
	,0 AS WSSITELIM
	,'USD' AS WSSITELCUR
	,0 AS WSSITEDED
	,'USD' AS WSSITEDCUR
	,0 AS WSCOMBINEDLIM
	,'USD' AS WSCOMBINEDLCUR
	,0 AS WSCOMBINEDDED
	,'USD' AS WSCOMBINEDDCUR
	,0 AS RESISTOPEN
	,RMS_RoofAge_Code AS ROOFAGE
	,0 AS ROOFANCH
	,RMS_RoofCovering_Code AS ROOFSYS
	,0 AS CLADRATE
	,RMS_RoofShape_Code AS ROOFGEOM
	,RMS_CladCode_HU AS CLADSYS
	,CRIndicator AS USERTXT1
	,ProductType AS USERTXT2
	,'' AS USERID1
	,'' AS USERID2
	,'' AS PRIMARYBLDG
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE OTHER_FLOOD_IND = 'Y'


SELECT * FROM Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location
