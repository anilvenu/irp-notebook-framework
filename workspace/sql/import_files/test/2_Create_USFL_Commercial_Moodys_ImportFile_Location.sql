/**********************************************************************************************************************************************
Purpose: This script creates the Risk Modeler Location import file for Flood Solutions exposures
Author: Charlene Chia
Edited by: Ben Bailey
Instructions:
				1. Update all input and output tables names to the current quarter. Use replace all.
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Tables:
				CombinedData_{{ DATE_VALUE }}_Working
				AssociationNumberLookup_{{ DATE_VALUE }}
				ASST_{{ DATE_VALUE }}_FloodSolutions_Elevation_Lookup
Output Tables:
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

--Create Location File:
--First building, which has the actual number of stories and full location details with cov a/b/d assigned
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Location
SELECT * INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Location
FROM (
SELECT
	b.AssociationAcctNum AS ACCNTNUM
	,a.LocationID AS LOCNUM
	,a.LocationID AS SITENAME
	,CASE WHEN CovCValue <> 0 AND Model_NumberofStories > '1' THEN '1' ELSE '' END AS PRIMARYBLDG
	,Street AS STREETNAME
	,County AS COUNTY
	,CountyNBR AS COUNTYCODE
	,City AS CITY
	,State AS STATECODE
	,ZIPCODE AS POSTALCODE
	,'ISO3A' AS CNTRYSCHEME
	,'USA' AS CNTRYCODE
	,Latitude AS LATITUDE
	,Longitude AS LONGITUDE
	,'' AS ADDRMATCH
	,RMS_ConstCode_HU AS BLDGCLASS
	,'RMS' AS BLDGSCHEME
	,RMS_OccType_ATC AS OCCTYPE
	,'ATC' AS OCCSCHEME
	,RMS_RoofAge_Code AS ROOFAGE
	,0 AS ROOFANCH
	,RMS_RoofCovering_Code AS ROOFSYS
	,0 AS CLADRATE
	,RMS_RoofShape_Code AS ROOFGEOM
	,RMS_CladCode_HU AS CLADSYS
	,CASE WHEN Model_YearBuilt = '0' THEN '12/31/9999'
		WHEN RTRIM(Model_YearBuilt) <= '1800' THEN '12/31/9999'
		ELSE '12/31/'+Model_YearBuilt END AS YEARBUILT
	,Model_NumberofStories AS NUMSTORIES
	,CovAValue AS WSCV4VAL
	,'USD' AS WSCV4VCUR
	,CovBValue AS WSCV5VAL
	,'USD' AS WSCV5VCUR
	,CASE WHEN Model_NumberofStories > 1 THEN 1 -- It needs to be non-zero, otherwise RiskLink will drop entire row from the import and the CovCLimit will be lost.
		ELSE CovCValue END AS WSCV6VAL
	,'USD' AS WSCV6VCUR
	,CovDValue AS WSCV7VAL
	,'USD' AS WSCV7VCUR
	,CASE WHEN BLANKET_LIMIT_TYPE = 'Per Building Per Occurrence' AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'Y' THEN a.Blanket_Building_Limit
		ELSE CovAlimit_Flood END AS WSCV4LIMIT
	,'USD' AS WSCV4LCUR
	,CovBlimit_Flood AS WSCV5LIMIT
	,'USD' AS WSCV5LCUR
	,CASE WHEN BLANKET_LIMIT_TYPE = 'Per Building Per Occurrence' AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'Y' THEN a.Blanket_Content_Limit
		ELSE CovClimit_Flood END AS WSCV6LIMIT -- When modeling a campus, all deductibles and limits must be placed on the Primary Building
	,'USD' AS WSCV6LCUR
	,CovDlimit_Flood AS WSCV7LIMIT
	,'USD' AS WSCV7LCUR
	,CASE WHEN BLANKET_DED_TYPE = 'Per Building Per Occurrence' THEN 0
		WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 AND
		(BLANKET_DED_TYPE <> 'Per Building Per Occurrence' OR BLANKET_DED_TYPE IS NULL) THEN 0
		ELSE FloodDed_CovA END AS WSCV4DED
	,'USD' AS WSCV4DCUR
	,CASE WHEN BLANKET_DED_TYPE = 'Per Building Per Occurrence' THEN 0
		WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 AND
		(BLANKET_DED_TYPE <> 'Per Building Per Occurrence' OR BLANKET_DED_TYPE IS NULL) THEN 0
		ELSE FloodDed_CovB END AS WSCV5DED
	,'USD' AS WSCV5DCUR
	,CASE WHEN BLANKET_DED_TYPE = 'Per Building Per Occurrence' THEN 0
		WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 AND
		(BLANKET_DED_TYPE <> 'Per Building Per Occurrence' OR BLANKET_DED_TYPE IS NULL) THEN 0
		ELSE FloodDed_CovC END AS WSCV6DED
	,'USD' AS WSCV6DCUR
	,CASE WHEN BLANKET_DED_TYPE = 'Per Building Per Occurrence' THEN 0
		WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 AND
		(BLANKET_DED_TYPE <> 'Per Building Per Occurrence' OR BLANKET_DED_TYPE IS NULL) THEN 0
		ELSE FloodDed_CovD END AS WSCV7DED
	,'USD' AS WSCV7DCUR
	,0 AS WSSITELIM --Applies to all coverages
	,'USD' AS WSSITELCUR
	,CASE WHEN BLANKET_DED_TYPE = 'Per Building Per Occurrence' THEN FloodDeductible
		WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 AND
		(BLANKET_DED_TYPE <> 'Per Building Per Occurrence' OR BLANKET_DED_TYPE IS NULL) THEN FloodDeductible
		ELSE 0 END AS WSSITEDED --Applies to all coverages
	,'USD' AS WSSITEDCUR
	,CASE WHEN BLANKET_LIMIT_TYPE = 'Per Building Per Occurrence' AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'N' THEN a.BLANKET_COMBINED_LIMIT
		WHEN SUBLIMIT_TYPE = 'Per Building Per Occurrence' THEN SUBLIMIT
		ELSE 0 END AS WSCOMBINEDLIM --Applies to A+C coverages (A+B+C for homeowners)
	,'USD' AS WSCOMBINEDLCUR
	,0 AS WSCOMBINEDDED --Applies to A+C coverages (A+B+C for homeowners)
	,'USD' AS WSCOMBINEDDCUR
	,Model_SQF AS FLOORAREA
	,'2' AS AREAUNIT
	,FLD_Remetrica_RDMF_Bucket AS USERTXT1
	,ProductType AS USERTXT2
	,'' AS USERID1
	,'' AS USERID2
	,c.CustomElevation AS Elevation
	,CASE WHEN c.CustomElevation <> '-999' THEN '13' ELSE 0 END AS ElevMatch
	,c.LFE_Feet AS BuildingElevation
	,CASE WHEN c.LFE_Feet <> '-999' THEN '13' ELSE 0 END AS BuildingElevationMatch
FROM CombinedData_{{ DATE_VALUE }}_Working a
JOIN AssociationNumberLookup_{{ DATE_VALUE }} b ON a.AssociationName = b.AssociationName
JOIN ASST_{{ DATE_VALUE }}_FloodSolutions_Elevation_Lookup c ON a.LocationID = c.LocationID
WHERE ProductType in ('FB')
UNION ALL
SELECT
	b.AssociationAcctNum AS ACCNTNUM
	,a.LocationID AS LOCNUM
	,a.LocationID AS SITENAME
	,0 AS PRIMARYBLDG
	,Street AS STREETNAME
	,County AS COUNTY
	,CountyNBR AS COUNTYCODE
	,City AS CITY
	,State AS STATECODE
	,ZIPCODE AS POSTALCODE
	,'ISO3A' AS CNTRYSCHEME
	,'USA' AS CNTRYCODE
	,Latitude AS LATITUDE
	,Longitude AS LONGITUDE
	,'' AS ADDRMATCH
	,RMS_ConstCode_HU AS BLDGCLASS
	,'RMS' AS BLDGSCHEME
	,RMS_OccType_ATC AS OCCTYPE
	,'ATC' AS OCCSCHEME
	,RMS_RoofAge_Code AS ROOFAGE
	,0 AS ROOFANCH
	,RMS_RoofCovering_Code AS ROOFSYS
	,0 AS CLADRATE
	,RMS_RoofShape_Code AS ROOFGEOM
	,RMS_CladCode_HU AS CLADSYS
	,CASE WHEN Model_YearBuilt = '0' THEN '12/31/9999'
		WHEN RTRIM(Model_YearBuilt) <= '1800' THEN '12/31/9999'
		ELSE '12/31/'+Model_YearBuilt END AS YEARBUILT
	,1 AS NUMSTORIES
	,0 AS WSCV4VAL
	,'USD' AS WSCV4VCUR
	,0 AS WSCV5VAL
	,'USD' AS WSCV5VCUR
	,CovCValue AS WSCV6VAL
	,'USD' AS WSCV6VCUR
	,0 AS WSCV7VAL
	,'USD' AS WSCV7VCUR
	,0 AS WSCV4LIMIT
	,'USD' AS WSCV4LCUR
	,0 AS WSCV5LIMIT
	,'USD' AS WSCV5LCUR
	,0 AS WSCV6LIMIT -- When modeling a campus, all deductibles and limits must be placed on the Primary Building
	,'USD' AS WSCV6LCUR
	,0 AS WSCV7LIMIT
	,'USD' AS WSCV7LCUR
	,0 AS WSCV4DED
	,'USD' AS WSCV4DCUR
	,0 WSCV5DED
	,'USD' AS WSCV5DCUR
	,0 AS WSCV6DED
	,'USD' AS WSCV6DCUR
	,0 AS WSCV7DED
	,'USD' AS WSCV7DCUR
	,0 AS WSSITELIM
	,'USD' AS WSSITELCUR
	,0 AS WSSITEDED -- When modeling a campus, all deductibles and limits must be placed on the Primary Building
	,'USD' AS WSSITEDCUR
	,0 AS WSCOMBINEDLIM
	,'USD' AS WSCOMBINEDLCUR
	,0 AS WSCOMBINEDDED
	,'USD' AS WSCOMBINEDDCUR
	,Model_SQF AS FLOORAREA
	,'2' AS AREAUNIT
	,FLD_Remetrica_RDMF_Bucket AS USERTXT1
	,ProductType AS USERTXT2
	,'' AS USERID1
	,'' AS USERID2
	,c.CustomElevation AS Elevation
	,CASE WHEN c.CustomElevation <> '-999' THEN '13' ELSE 0 END AS ElevMatch
	,c.LFE_Feet AS BuildingElevation
	,CASE WHEN c.LFE_Feet <> '-999' THEN '13' ELSE 0 END AS BuildingElevationMatch
FROM CombinedData_{{ DATE_VALUE }}_Working a
JOIN AssociationNumberLookup_{{ DATE_VALUE }} b ON a.AssociationName = b.AssociationName
JOIN ASST_{{ DATE_VALUE }}_FloodSolutions_Elevation_Lookup c ON a.LocationID = c.LocationID
WHERE ProductType in ('FB')
AND CovCValue <> 0
AND Model_NumberofStories > 1
) AS Z


SELECT * FROM Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Location
