/**********************************************************************************************************************************************
Purpose: This script creates the Risk Modeler Location import file for US Hurricane exposures
Author: Charlene Chia
Edited by: Ben Bailey
Instructions:
				1. Update quarter e.g. 202212 to {{ DATE_VALUE }}
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Table:	CombinedData_{{ DATE_VALUE }}_Working
Output Tables:
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

-- US HU Location File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Location
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
	,'' AS STATE -- why doesn't Aon map the State?
	,State AS STATECODE
	,ZIPCODE AS POSTALCODE
	,County AS COUNTY
	,CountyNBR AS COUNTYCODE
	,'' AS CRESTA
	,(Case when GeocodingLevel not in ('Zipcode Level','UW_Spectrum_Zipcode Level') then Latitude else '0' end)  AS Latitude
	,(Case when GeocodingLevel not in ('Zipcode Level','UW_Spectrum_Zipcode Level') then Longitude else '0' end) AS Longitude
	,Model_SQF AS FLOORAREA
	,'2' AS AREAUNIT
	,'ISO3A' AS CNTRYSCHEME
	,'USA' AS CNTRYCODE
	,1 AS NUMBLDGS
	,'rms' AS BLDGSCHEME
	,rms_ConstCode_HU AS BLDGCLASS
	,'ATC' AS OCCSCHEME
	,rms_OccType_ATC AS OCCTYPE
	,CASE
		WHEN Model_YearBuilt = '0' THEN '12/31/9999'
		WHEN RTRIM(Model_YearBuilt) <= '1800' THEN '12/31/9999'
		ELSE '12/31/'+Model_YearBuilt
	END AS YEARBUILT
	,Model_NumberofStories AS NUMSTORIES
	,CovAValue AS WSCV4VAL
	,'USD' AS WSCV4VCUR
	,CovBValue AS WSCV5VAL
	,'USD' AS WSCV5VCUR
	,CovCValue AS WSCV6VAL
	,'USD' AS WSCV6VCUR
	,CovDValue AS WSCV7VAL
	,'USD' AS WSCV7VCUR
	,CovAlimit_HU AS WSCV4LIMIT
	,'USD' AS WSCV4LCUR
	,CovBlimit_HU AS WSCV5LIMIT
	,'USD' AS WSCV5LCUR
	,CovClimit_HU AS WSCV6LIMIT
	,'USD' AS WSCV6LCUR
	,CovDlimit_HU AS WSCV7LIMIT
	,'USD' AS WSCV7LCUR
	,CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovA END AS WSCV4DED
	,'USD' AS WSCV4DCUR
	,CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovB END AS WSCV5DED
	,'USD' AS WSCV5DCUR
	,CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovC END AS WSCV6DED
	,'USD' AS WSCV6DCUR
	,CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovD END AS WSCV7DED
	,'USD' AS WSCV7DCUR
	,0 AS WSSITELIM
	,'USD' AS WSSITELCUR
	,CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN HurricaneDeductible ELSE 0 END AS WSSITEDED
	,'USD' AS WSSITEDCUR
	,0 AS WSCOMBINEDLIM
	,'USD' AS WSCOMBINEDLCUR
	,0 AS WSCOMBINEDDED
	,'USD' AS WSCOMBINEDDCUR
	,0 AS RESISTOPEN
	,rms_RoofAge_Code AS ROOFAGE
	,0 AS ROOFANCH
	,rms_RoofCovering_Code AS ROOFSYS
	,0 AS CLADRATE
	,rms_RoofShape_Code AS ROOFGEOM
	,rms_CladCode_HU AS CLADSYS
	,CRIndicator AS USERTXT1
	,ProductType AS USERTXT2
	,'' AS USERID1
	,'' AS USERID2
	,'' AS PRIMARYBLDG
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Location
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE State NOT IN ('PR','VI','GU')
	and HUMODELED = 'Y'
	and Flood_Flag_Total = 'Y'

--Export import files to CSV via export wizard
Select * From Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Location
