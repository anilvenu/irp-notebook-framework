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
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Excess_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

/*=======================================================
	Excess Flood
=======================================================*/
--Create Location File:
--First building, which has the actual number of stories and full location details with cov a/b/d assigned
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Excess_Location
SELECT * INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Excess_Location
FROM (
SELECT
	b.AssociationAcctNum AS ACCNTNUM
	,a.LocationID AS LOCNUM
	,a.LocationID AS SITENAME
	,1 AS PRIMARYBLDG
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
	,CovAValue AS FLCV4VAL
	,'USD' AS FLCV4VCUR
	,CovBValue AS FLCV5VAL
	,'USD' AS FLCV5VCUR
	,CASE WHEN Model_NumberofStories > 1 THEN 1 -- It needs to be non-zero, otherwise RiskLink will drop entire row from the import and the CovCLimit will be lost.
		ELSE CovCValue END AS FLCV6VAL
	,'USD' AS FLCV6VCUR
	,CovDValue AS FLCV7VAL
	,'USD' AS FLCV7VCUR
	,CovAlimit_Flood AS FLCV4LIMIT
	,'USD' AS FLCV4LCUR
	,CovBlimit_Flood AS FLCV5LIMIT
	,'USD' AS FLCV5LCUR
	,CovClimit_Flood AS FLCV6LIMIT -- When modeling a campus, all deductibles and limits must be placed on the Primary Building
	,'USD' AS FLCV6LCUR
	,CovDlimit_Flood AS FLCV7LIMIT
	,'USD' AS FLCV7LCUR
	,FloodAttachmentPoint_CovA AS FLCV4DED
	,FloodAttachmentPoint_CovC AS FLCV6DED
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
WHERE ProductType in ('EF','EG')
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
	,0 AS FLCV4VAL
	,'USD' AS FLCV4VCUR
	,0 AS FLCV5VAL
	,'USD' AS FLCV5VCUR
	,CovCValue AS FLCV6VAL
	,'USD' AS FLCV6VCUR
	,0 AS FLCV7VAL
	,'USD' AS FLCV7VCUR
	,0 AS FLCV4LIMIT
	,'USD' AS FLCV4LCUR
	,0 AS FLCV5LIMIT
	,'USD' AS FLCV5LCUR
	,0 AS FLCV6LIMIT -- When modeling a campus, all deductibles and limits must be placed on the Primary Building
	,'USD' AS FLCV6LCUR
	,0 AS FLCV7LIMIT
	,'USD' AS FLCV7LCUR
	,0 AS FLCV4DED
	,0 AS FLCV6DED
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
WHERE ProductType in ('EF','EG')
AND CovCValue <> 0
AND Model_NumberofStories > 1
) AS Z

SELECT * FROM Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Excess_Location
