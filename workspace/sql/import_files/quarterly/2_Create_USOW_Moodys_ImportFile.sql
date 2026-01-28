/**********************************************************************************************************************************************
Purpose: This script creates the RiskLink import files for US Severe Convective Storm exposures
Author: Charlene Chia
Edited by: Teryn Mueller-- Put the name of the person updating this script.
Instructions: 
				1. Update quarter e.g. 202409 to {{ DATE_VALUE }}
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Table:	 CombinedData_{{ DATE_VALUE }}_Working
Output Tables:
				 Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Account
				 Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

-- US SCS Account File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Account
SELECT
	LocationID AS ACCNTNUM
	,Product_Group_ROE AS ACCNTNAME --varchar(40), we are at the maximum limit. This was previously AccountNumber.
	,Product_Group_ROE AS ACCGRPNAME
	,'' AS UWRITRNAME
	,'' AS PRODID
	,PolicyNumber AS BRANCHNAME
	,'' AS PRODNAME
	,'ASST' AS CEDANTID
	,'ASST' AS CEDANTNAME
	,LocationID AS POLICYNUM
	,CASE
		WHEN BusinessUnit = 'Lend' THEN 'Lend'
		WHEN BusinessUnit = 'Prop' THEN 'Prop'
		ELSE 'LOB'
	END AS LOBNAME
	,EffectiveDate AS INCEPTDATE
	,ExpirationDate AS EXPIREDATE
	,0 AS UNDCOVAMT
	,'USD' AS UNDCOVCUR
	,PolicyLimit AS PARTOF
	,'USD' AS PARTOFCUR
	,3 AS POLICYTYPE -- Severe Convective Storm = 3
	,1 AS POLICYSTRUCTURE -- 1 = Standard
	,0 AS MINDEDAMT
	,'USD' AS MINDEDCUR
	,0 AS MAXDEDAMT
	,'USD' AS MAXDEDCUR
	,0 AS BLANDEDAMT
	,'USD' AS BLANDEDCUR
	,PolicyLimit AS BLANLIMAMT
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
	,OW_Remetrica_RDMF_Bucket AS USERDEF1
	,FHCFIndicator AS USERDEF2 
	,QS_Indicator AS USERDEF3  
	,ProductType AS USERDEF4 
	,QEM_Product_Group AS USERTXT1 -- was main_bu
	,BusinessUnit AS USERTXT2
	,CASE WHEN BusinessUnit = 'Clay' THEN ClientName ELSE AccountNumber END AS POLICYUSERTXT1 --varchar(20)
	,AssurantGroupedLOB AS POLICYUSERTXT2 --varchar(20)
	,NetLegalEntity AS POLICYUSERTXT3 --varchar(20)
	,LegalEntity AS POLICYUSERTXT4 --varchar(20)
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Account
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE State NOT IN ('PR','VI','GU')
	and THMODELED = 'Y'  
--(3706526 rows affected)

-- US SCS Location File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Location
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
	,(Case when GeocodingLevel not in ('Zipcode Level','UW_Spectrum_Zipcode Level') then Latitude else '0' end)  AS Latitude
	,(Case when GeocodingLevel not in ('Zipcode Level','UW_Spectrum_Zipcode Level') then Longitude else '0' end) AS Longitude
	,Model_SQF AS FLOORAREA
	,'2' AS AREAUNIT
	,'ISO3A' AS CNTRYSCHEME
	,'USA' AS CNTRYCODE
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
	,CovAValue AS TOCV4VAL
	,'USD' AS TOCV4VCUR
	,CovBValue AS TOCV5VAL
	,'USD' AS TOCV5VCUR
	,CovCValue AS TOCV6VAL
	,'USD' AS TOCV6VCUR
	,CovDValue AS TOCV7VAL
	,'USD' AS TOCV7VCUR
	,CovAlimit_TH AS TOCV4LIMIT 
	,'USD' AS TOCV4LCUR
	,CovBlimit_TH AS TOCV5LIMIT 
	,'USD' AS TOCV5LCUR
	,CovClimit_TH AS TOCV6LIMIT 
	,'USD' AS TOCV6LCUR
	,CovDlimit_TH AS TOCV7LIMIT 
	,'USD' AS TOCV7LCUR	
	,CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovA END AS TOCV4DED 
	,'USD' AS TOCV4DCUR
	,CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovB END AS TOCV5DED 
	,'USD' AS TOCV5DCUR
	,CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovC END AS TOCV6DED 
	,'USD' AS TOCV6DCUR
	,CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovD END AS TOCV7DED 
	,'USD' AS TOCV7DCUR
	,0 AS TOSITELIM
	,'USD' AS TOSITELCUR
	,CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN TornadoHailDeductible ELSE 0 END AS TOSITEDED 
	,'USD' AS TOSITEDCUR
	,0 AS TOCOMBINEDLIM
	,'USD' AS TOCOMBINEDLCUR
	,0 AS TOCOMBINEDDED
	,'USD' AS TOCOMBINEDDCUR
	,0 AS RESISTOPEN
	,RMS_RoofAge_Code AS ROOFAGE
	,0 AS ROOFANCH
	,RMS_RoofCovering_Code AS ROOFSYS
	,0 AS CLADRATE
	,RMS_RoofShape_Code AS ROOFGEOM
	,RMS_CladCode_HU AS CLADSYS
	,CRIndicator AS USERTXT1
	,ProductType AS USERTXT2
	,RM_Skylights as ARCHITECT		-- Added for Zesty
	,'' AS USERID1
	,'' AS USERID2
	,'' AS PRIMARYBLDG
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Location
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE State NOT IN ('PR','VI','GU')
	and THMODELED = 'Y'  
--(3706526 rows affected)


--Export import files to CSV via export wizard
Select * From Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Account --3795604
Select * From Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Location --3795604