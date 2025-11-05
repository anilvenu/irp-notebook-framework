/**********************************************************************************************************************************************
Purpose: This script creates the Risk Modeler import files for Flood Solutions exposures
Author: Charlene Chia
Edited by: Jillian Perkins
Instructions: 
				1. Update all input and output tables names to the current quarter. Use replace all.
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Tables:	
				CombinedData_{DATE_VALUE}_Working
				AssociationNumberLookup_{DATE_VALUE}
				ASST_{DATE_VALUE}_FloodSolutions_Elevation_Lookup
Output Tables:
				Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Commercial_Account
				Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Commercial_Location
				Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Excess_Account
				Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Excess_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

/*=======================================================
	Commercial Flood
=======================================================*/
--Create Contract File
DROP TABLE IF EXISTS dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Commercial_Account
SELECT
	b.AssociationAcctNum AS ACCNTNUM
	,b.AssociationAcctNum AS POLICYNUM
	,2 AS POLICYTYPE
	,CASE WHEN BLANKET_LIMIT_TYPE IN ('Policy Level Per Occurrence','Annual Aggregate') AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'Y' THEN 0
		WHEN BLANKET_LIMIT_TYPE IN ('Policy Level Per Occurrence','Annual Aggregate') AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'N' THEN MAX(a.BLANKET_COMBINED_LIMIT)
		WHEN (BLANKET_LIMIT_TYPE IS NULL OR BLANKET_LIMIT_TYPE = '') THEN SUM(PolicyLimit) ELSE '' END AS BLANLIMAMT
	,CASE WHEN BLANKET_LIMIT_TYPE IN ('Policy Level Per Occurrence','Annual Aggregate') AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'Y' THEN MAX(a.Blanket_Building_Limit)
		ELSE '' END AS COV1LIMIT --Building
	,CASE WHEN BLANKET_LIMIT_TYPE IN ('Policy Level Per Occurrence','Annual Aggregate') AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'Y' THEN Max(a.Blanket_Content_Limit)
		ELSE '' END AS COV2LIMIT --Contents
	,SUM(PolicyPremium) AS BLANPREAMT
	,CASE WHEN BLANKET_DED_TYPE = 'Per Occurrence' THEN MAX(FloodDeductible) ELSE 0 END AS BLANDEDAMT 
	,CASE WHEN SUBLIMIT_TYPE = 'Policy Level Per Occurrence' THEN 2 ELSE '' END AS COND1TYPE --(2 = Sublimit) Used to indicate the presence of a policy special condition. You can import up to five policy special conditions
	,CASE WHEN SUBLIMIT_TYPE = 'Policy Level Per Occurrence' THEN 'Policy_Sublimit' ELSE '' END AS COND1NAME -- The name associated with the condition. Required if there is a Condition Type.
	,CASE WHEN SUBLIMIT_TYPE = 'Policy Level Per Occurrence' THEN MAX(SUBLIMIT) ELSE '' END AS COND1LIMIT
	,'FLD Comm' AS LOBNAME
	,MIN(EffectiveDate) AS INCEPTDATE
	,MAX(ExpirationDate) AS EXPIREDATE
	,a.AssociationName AS USERDEF1
	,QEM_Product_Group AS USERDEF2
	,QS_Indicator AS USERDEF3
	,ProductType AS USERDEF4 
	,NetLegalEntity AS USERTXT1
	,'CommercialFlood' AS USERTXT2
	,'USD' AS UNDCOVCUR
	,'ASST' AS CEDANTID
	,'ASST' AS CEDANTNAME
INTO dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Commercial_Account
FROM CombinedData_{DATE_VALUE}_Working a
JOIN AssociationNumberLookup_{DATE_VALUE} b ON a.AssociationName = b.AssociationName
WHERE ProductType in ('FB')
GROUP BY 
	b.AssociationAcctNum,
	a.AssociationName,
	a.QEM_Product_Group,
	a.QS_Indicator,
	a.ProductType,
	a.NetLegalEntity,
	a.BLANKET_LIMIT_TYPE,
	a.SEPERATE_BLANKET_BUILDING_AND_CONTENT,
	a.BLANKET_DED_TYPE,
	a.SUBLIMIT_TYPE

--Create Location File:
--First building, which has the actual number of stories and full location details with cov a/b/d assigned
DROP TABLE IF EXISTS dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Commercial_Location
SELECT * INTO dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Commercial_Location
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
FROM CombinedData_{DATE_VALUE}_Working a
JOIN AssociationNumberLookup_{DATE_VALUE} b ON a.AssociationName = b.AssociationName 
JOIN ASST_{DATE_VALUE}_FloodSolutions_Elevation_Lookup c ON a.LocationID = c.LocationID
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
FROM CombinedData_{DATE_VALUE}_Working a
JOIN AssociationNumberLookup_{DATE_VALUE} b ON a.AssociationName = b.AssociationName 
JOIN ASST_{DATE_VALUE}_FloodSolutions_Elevation_Lookup c ON a.LocationID = c.LocationID
WHERE ProductType in ('FB')
AND CovCValue <> 0 
AND Model_NumberofStories > 1
) AS Z

/*=======================================================
	Excess Flood
=======================================================*/
--Create Contract File
DROP TABLE IF EXISTS dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Excess_Account
SELECT
	b.AssociationAcctNum AS ACCNTNUM
	,b.AssociationAcctNum AS POLICYNUM
	,2 AS POLICYTYPE
	,SUM(PolicyLimit) AS BLANLIMAMT
	,SUM(PolicyPremium) AS BLANPREAMT
	,0 AS BLANDEDAMT
	,'FLD Excess' AS LOBNAME
	,MIN(EffectiveDate) AS INCEPTDATE
	,MAX(ExpirationDate) AS EXPIREDATE
	,a.AssociationName AS USERDEF1
	,QEM_Product_Group AS USERDEF2
	,QS_Indicator AS USERDEF3
	,ProductType AS USERDEF4 
	,NetLegalEntity AS USERTXT1
	,'ExcessFlood' AS USERTXT2
	,'USD' AS UNDCOVCUR
	,'ASST' AS CEDANTID
	,'ASST' AS CEDANTNAME
INTO dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Excess_Account
FROM CombinedData_{DATE_VALUE}_Working a
JOIN AssociationNumberLookup_{DATE_VALUE} b ON a.AssociationName = b.AssociationName
WHERE ProductType in ('EF','EG')
GROUP BY 
	b.AssociationAcctNum,
	a.AssociationName,
	a.QEM_Product_Group,
	a.QS_Indicator,
	a.ProductType,
	a.NetLegalEntity

--Create Location File:
--First building, which has the actual number of stories and full location details with cov a/b/d assigned
DROP TABLE IF EXISTS dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Excess_Location
SELECT * INTO dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USFL_Excess_Location
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
	,CovAValue AS WSCV4VAL
	,'USD' AS WSCV4VCUR
	,CovBValue AS WSCV5VAL
	,'USD' AS WSCV5VCUR
	,CASE WHEN Model_NumberofStories > 1 THEN 1 -- It needs to be non-zero, otherwise RiskLink will drop entire row from the import and the CovCLimit will be lost.
		ELSE CovCValue END AS WSCV6VAL
	,'USD' AS WSCV6VCUR
	,CovDValue AS WSCV7VAL
	,'USD' AS WSCV7VCUR	
	,CovAlimit_Flood AS WSCV4LIMIT 
	,'USD' AS WSCV4LCUR
	,CovBlimit_Flood AS WSCV5LIMIT 
	,'USD' AS WSCV5LCUR
	,CovClimit_Flood AS WSCV6LIMIT -- When modeling a campus, all deductibles and limits must be placed on the Primary Building
	,'USD' AS WSCV6LCUR
	,CovDlimit_Flood AS WSCV7LIMIT 
	,'USD' AS WSCV7LCUR
	,FloodAttachmentPoint_CovA AS WSCV4DED
	,FloodAttachmentPoint_CovC AS WSCV6DED
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
FROM CombinedData_{DATE_VALUE}_Working a
JOIN AssociationNumberLookup_{DATE_VALUE} b ON a.AssociationName = b.AssociationName 
JOIN ASST_{DATE_VALUE}_FloodSolutions_Elevation_Lookup c ON a.LocationID = c.LocationID
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
	,0 AS WSCV6DED
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
FROM CombinedData_{DATE_VALUE}_Working a
JOIN AssociationNumberLookup_{DATE_VALUE} b ON a.AssociationName = b.AssociationName 
JOIN ASST_{DATE_VALUE}_FloodSolutions_Elevation_Lookup c ON a.LocationID = c.LocationID
WHERE ProductType in ('EF','EG')
AND CovCValue <> 0 
AND Model_NumberofStories > 1
) AS Z