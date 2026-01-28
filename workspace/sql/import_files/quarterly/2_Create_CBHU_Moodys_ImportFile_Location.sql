/**********************************************************************************************************************************************
Purpose:	This script creates the Risk Modeler Location import file for CB Hurricane exposures
			Note: Risk Modeler does not support either storm surge or precip flood in their Atlantic Hurricane model. Therefore,
			we omit flood risks from the Risk Modeler import file since they would generate zero loss. This is contrary to the Touchstone
			import files where precip flood is supported. For consistency purposes all exposure totals in result exhibits should
			show the same totals. i.e. copy the AIR TIV and Risk Count totals into the RMS exhibits.
Author: Charlene Chia
Edited by: Teryn Mueller-- Put the name of the person updating this script.
Instructions:
				1. Update quarter e.g. 202209 to {{ DATE_VALUE }}
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Table:	CombinedData_{{ DATE_VALUE }}_Working
Output Tables:
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

-- CB HU Location File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Location
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
	,'' AS STATECODE
	,ZIPCODE AS POSTALCODE
	,County AS COUNTY
	,CountyNBR AS COUNTYCODE
	,'' AS CRESTA
	,(Case when GeocodingLevel not in ('Zipcode Level','UW_Spectrum_Zipcode Level') then Latitude else '0' end)  AS Latitude
	,(Case when GeocodingLevel not in ('Zipcode Level','UW_Spectrum_Zipcode Level') then Longitude else '0' end) AS Longitude
	,Model_SQF AS FLOORAREA
	,'2' AS AREAUNIT
	,'ISO3A' AS CNTRYSCHEME
	,CASE
		WHEN State IN ('PR') THEN 'PRI'
		ELSE 'VIR'
	END AS CNTRYCODE
	,1 AS NUMBLDGS
	,'RMS' AS BLDGSCHEME
	,RMS_ConstCode_HU AS BLDGCLASS
	,'ATC' AS OCCSCHEME
	,RMS_OccType_ATC AS OCCTYPE
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
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Location
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE State IN ('PR','VI')
	and HurricaneCoverage = 'Y'
	and HU_Remetrica_RDMF_Bucket <> 'NULL'
	and Main_BU <> 'Clay'
--(2723 rows affected)


--Export import files to CSV
Select * From Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Location
