/**********************************************************************************************************************************************
Purpose: This script creates the RiskLink import files for Other Flood exposures
Author: Charlene Chia
Edited by: Jillian Perkins
Instructions: 
				1. Update all input and output tables names to the current quarter. Use replace all.
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Table:	CombinedData_{{ DATE_VALUE }}_Working
Output Tables:
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

-- US Other Flood Account File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account
SELECT
	LocationID AS ACCNTNUM
	,Product_Group_ROE AS ACCNTNAME
	,Product_Group_ROE AS ACCGRPNAME
	,'' AS UWRITRNAME
	,'' AS PRODID
	,PolicyNumber AS BRANCHNAME
	,'' AS PRODNAME
	,'ASST' AS CEDANTID
	,'ASST' AS CEDANTNAME
	,LocationID AS POLICYNUM
	,CASE WHEN State IN ('PR','VI') THEN 'FLD Other CB' WHEN MAIN_BU = 'Clay' THEN 'FLD Other Clay' ELSE 'FLD Other' END AS	LOBNAME			
	,EffectiveDate AS INCEPTDATE
	,ExpirationDate AS EXPIREDATE
	,FloodAttachmentPoint_CovA AS UNDCOVAMT
	,'USD' AS UNDCOVCUR
	,MODELED_TIV_FLOOD AS PARTOF
	,'USD' AS PARTOFCUR
	,2 AS POLICYTYPE -- Windstorm = 2
	,1 AS POLICYSTRUCTURE -- 1 = Standard
	,0 AS MINDEDAMT
	,'USD' AS MINDEDCUR
	,0 AS MAXDEDAMT
	,'USD' AS MAXDEDCUR
	,CASE WHEN FLOODDEDUCTIBLE IS NULL THEN FLOODDED_COVA ELSE FLOODDEDUCTIBLE END AS BLANDEDAMT
	,'USD' AS BLANDEDCUR
	,MODELED_TIV_FLOOD AS BLANLIMAMT
	,'USD' AS BLANLIMCUR
	,PolicyPremium AS BLANPREAMT
	,'USD' AS BLANPRECUR
	,0 AS COMBINEDLIM
	,'USD' AS COMBINEDLCUR
	,0 AS COMBINEDDED
	,'USD' AS COMBINEDDCUR
	,0 AS COMBINEDPREM
	,'USD' AS COMBINEDPCUR
	,0 AS COVBASE
	,0 AS LIMITGU
	,FLD_Remetrica_RDMF_Bucket AS USERDEF1
	,FHCFIndicator AS USERDEF2
	,QS_Indicator AS USERDEF3 
	,ProductType AS USERDEF4 
	,QEM_Product_Group AS USERTXT1 
	,BusinessUnit AS USERTXT2
	,CASE WHEN BusinessUnit = 'Clay' THEN ClientName ELSE AccountNumber END AS POLICYUSERTXT1
	,AssurantGroupedLOB AS POLICYUSERTXT2
	,NetLegalEntity AS POLICYUSERTXT3
	,LegalEntity AS POLICYUSERTXT4
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE OTHER_FLOOD_IND = 'Y'

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

SELECT * FROM Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account
SELECT * FROM Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location
