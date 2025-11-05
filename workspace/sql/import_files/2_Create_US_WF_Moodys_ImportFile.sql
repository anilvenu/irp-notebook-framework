/**********************************************************************************************************************************************
Purpose: This script creates the Risk Modeler import files for US Fire Following exposures, modeled for Wildfire
Author: Charlene Chia
Edited by: Teryn Mueller-- Put the name of the person updating this script.
Instructions: 
				1. Update quarter e.g. 202212 to {DATE_VALUE}
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Table:	CombinedData_{DATE_VALUE}_Working
Output Tables:
				 Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USWF_Account
				 Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USWF_Location -- includes supression

Runtime: 00:00:25
**********************************************************************************************************************************************/

--Export import files to CSV via export wizard after table creation
--Select COUNT(*) Count From Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USWF_Account --3783832
--Select COUNT(*) Count From Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USWF_Location --3783832

-- US WF Account File:
DROP TABLE IF EXISTS dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USWF_Account
SELECT
	LocationID AS ACCNTNUM
	,Product_Group_ROE AS ACCNTNAME
	,Product_Group_ROE AS ACCGRPNAME --***new field added - ACCNTNAME only imports to a temp table --mapped to None and looked to still be in the EDM
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
	,5 AS POLICYTYPE -- 5 = FR = Fire
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
	,FF_Remetrica_RDMF_Bucket AS USERDEF1
	,FHCFIndicator AS USERDEF2
	,QS_Indicator AS USERDEF3
	,ProductType AS USERDEF4
	,QEM_Product_Group AS USERTXT1 --UPDATED FROM "Main_BU" since this field can still break out Clayton
	,BusinessUnit AS USERTXT2
	,CASE WHEN BusinessUnit = 'Clay' THEN ClientName ELSE AccountNumber END AS POLICYUSERTXT1 --varchar(20) --UPDATED FROM "AccountNumber"
	,AssurantGroupedLOB AS POLICYUSERTXT2 --varchar(20)
	,NetLegalEntity AS POLICYUSERTXT3 --varchar(20)
	,LegalEntity AS POLICYUSERTXT4 --varchar(20)
INTO dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USWF_Account
FROM CombinedData_{DATE_VALUE}_Working
WHERE State NOT IN ('PR','VI','GU') and FFMODELED = 'Y'
--(3816610 rows affected)

-------Location Files
/*
WF Secondary Risk Characteristics:
***Roof System Covering  --Combined file field: RMS_ROOFCOVERING_CODE        -- map to WFROOFSYS
***Roof Shape            --Combined file field: RMS_ROOFSHAPE_CODE           -- map to WFROOFGEOM
***Roof Age / Condition  --Combined file field: RMS_ROOFAGE_CODE             -- map to WFROOFAGE
***Wall Cladding Type    --Combined file field: RMS_CLADCODE_EQ              -- map to WFCLADSYS
***Suppression           --Combined file field: case statement for CA risks  -- map to WFSUPPRESS

WFSUPPRESS field:
-- 0 = Unknown             ***coding 0 for all non CA WF risks
-- 1 = Active Suppression  ***coding 1 for all CA WF risks including Clayton 
-- 2 = Passive Suppression						
-- 3 = None                 
*/
--All codes look correct comparing to the SRC coding Moodys document
--SELECT DISTINCT RMS_ROOFCOVERING_CODE FROM CombinedData_{DATE_VALUE}_Working WHERE FFModeled = 'Y' AND State NOT IN ('PR','VI','GU')
--SELECT DISTINCT RMS_ROOFSHAPE_CODE FROM CombinedData_{DATE_VALUE}_Working WHERE FFModeled = 'Y' AND State NOT IN ('PR','VI','GU')
--SELECT DISTINCT RMS_ROOFAGE_CODE FROM CombinedData_{DATE_VALUE}_Working WHERE FFModeled = 'Y' AND State NOT IN ('PR','VI','GU')
--SELECT DISTINCT RMS_CLADCODE_EQ FROM CombinedData_{DATE_VALUE}_Working WHERE FFModeled = 'Y' AND State NOT IN ('PR','VI','GU')

-- US WF Location File including Suppression:
DROP TABLE IF EXISTS dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USWF_Location
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
	,RMS_ConstCode_EQ AS BLDGCLASS
	,'ATC' AS OCCSCHEME
	,RMS_OccType_ATC AS OCCTYPE
	,CASE
		WHEN Model_YearBuilt = '0' THEN '12/31/9999'
		WHEN RTRIM(Model_YearBuilt) <= '1800' THEN '12/31/9999'
		ELSE '12/31/'+Model_YearBuilt 
	END AS YEARBUILT
	,Model_NumberofStories AS NUMSTORIES
	,CovAValue AS FRCV4VAL
	,'USD' AS FRCV4VCUR
	,CovBValue AS FRCV5VAL
	,'USD' AS FRCV5VCUR
	,CovCValue AS FRCV6VAL
	,'USD' AS FRCV6VCUR
	,CovDValue AS FRCV7VAL
	,'USD' AS FRCV7VCUR
	,CovAlimit_FF AS FRCV4LIMIT
	,'USD' AS FRCV4LCUR
	,CovBlimit_FF AS FRCV5LIMIT 
	,'USD' AS FRCV5LCUR
	,CovClimit_FF AS FRCV6LIMIT 
	,'USD' AS FRCV6LCUR
	,CovDlimit_FF AS FRCV7LIMIT
	,'USD' AS FRCV7LCUR
	,CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovA END AS FRCV4DED 
	,'USD' AS FRCV4DCUR
	,CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovB END AS FRCV5DED
	,'USD' AS FRCV5DCUR
	,CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovC END AS FRCV6DED 
	,'USD' AS FRCV6DCUR
	,CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovD END AS FRCV7DED
	,'USD' AS FRCV7DCUR
	,0 AS FRSITELIM
	,'USD' AS FRSITELCUR
	,CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN FireFollowingDeductible ELSE 0 END AS FRSITEDED
	,'USD' AS FRSITEDCUR
	,0 AS FRCOMBINEDLIM
	,'USD' AS FRCOMBINEDLCUR
	,0 AS FRCOMBINEDDED
	,'USD' AS FRCOMBINEDDCUR
	,RMS_ROOFCOVERING_CODE AS WFROOFSYS
	,RMS_ROOFSHAPE_CODE AS WFROOFGEOM
	,RMS_ROOFAGE_CODE AS WFROOFAGE
	,RMS_CladCode_EQ AS CLADDING
	,CASE WHEN State IN ('CA') AND FireFollowingCoverage = 'Y' AND PROPERTYTYPE_GENERAL in ('Mobile Home','Single Family Dwelling')     
			AND PRODUCTTYPE NOT IN ('09','69') AND BUSINESSUNIT NOT IN ('CLAY') 
			AND PolicyNumber not in ('PSM 0263792 04','PSM 0263792 05','PSM 0330347 02','21C 1042540 00') THEN 1
		WHEN State IN ('CA') AND FirefollowingCoverage = 'Y' and PROPERTYTYPE_GENERAL in ('Mobile Home','Single Family Dwelling')
			AND PRODUCTTYPE NOT IN ('09','69') AND BUSINESSUNIT IN ('CLAY')
			AND PolicyNumber not in ('PSM 0263792 04','PSM 0263792 05','PSM 0330347 02','21C 1042540 00') AND EFFECTIVEDATE >= '2025-02-01' THEN 1
		ELSE 0 END AS WFSUPPRESS --1 = Active Suppression & 0 = Unknown
	,CRIndicator AS USERTXT1
	,ProductType AS USERTXT2
	,'' AS USERID1
	,'' AS USERID2
	,'' AS PRIMARYBLDG
INTO dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_USWF_Location
FROM CombinedData_{DATE_VALUE}_Working
WHERE State NOT IN ('PR','VI','GU')	and FFMODELED = 'Y'
--(3816610 rows affected)