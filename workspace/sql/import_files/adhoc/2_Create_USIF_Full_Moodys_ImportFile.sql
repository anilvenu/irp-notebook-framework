/**********************************************************************************************************************************************
Purpose: This script creates the RiskLink import files for US Hurricane exposures
Author: Charlene Chia
Edited by: Ben Bailey
Instructions: 
				1. Update quarter e.g. 202212 to {{ DATE_VALUE }}
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Table:	CombinedData_{{ DATE_VALUE }}_Working
Output Tables:
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Full_Account
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Full_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

-- US HU Account File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Full_Account
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
		WHEN HU_Remetrica_RDMF_Bucket like '%ABIC%' THEN 'ABIC'
		WHEN HU_Remetrica_RDMF_Bucket like '%ASIC%' THEN 'ASIC'
		WHEN HU_Remetrica_RDMF_Bucket like '%GH - HU - FLxFHCF%' THEN 'GH FLxFHCF'
		WHEN HU_Remetrica_RDMF_Bucket like '%GH - HU - xFL%' THEN 'GH xFL'
		WHEN HU_Remetrica_RDMF_Bucket like '%Geico HO - GH - HU - xFL%' THEN 'GH xFL GCO'
		WHEN HU_Remetrica_RDMF_Bucket like '%GS - HU - FLxFHCF%' THEN 'GS FLxFHCF'
		WHEN HU_Remetrica_RDMF_Bucket like '%GS - HU - xFL%' THEN 'GS xFL'
		ELSE 'LOB'
		END AS LOBNAME
	,EffectiveDate AS INCEPTDATE
	,ExpirationDate AS EXPIREDATE
	,0 AS UNDCOVAMT
	,'USD' AS UNDCOVCUR
	,PolicyLimit AS PARTOF
	,'USD' AS PARTOFCUR
	,4 AS POLICYTYPE -- Flood = 4
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
	,HU_Remetrica_RDMF_Bucket AS USERDEF1
	,FHCFIndicator AS USERDEF2 
	,QS_Indicator AS USERDEF3  
	,ProductType AS USERDEF4 
	,QEM_Product_Group AS USERTXT1 -- was main_bu
	,BusinessUnit AS USERTXT2
	,CASE WHEN BusinessUnit = 'Clay' THEN ClientName ELSE AccountNumber END AS POLICYUSERTXT1 --varchar(20)
	,NetLegalEntity AS POLICYUSERTXT3 --varchar(20)
	,LegalEntity AS POLICYUSERTXT4 --varchar(20)
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Full_Account
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE State NOT IN ('PR','VI','GU')
	and HUMODELED = 'Y'
	and Flood_Flag_Total = 'Y'
--(322867 rows affected)

-- US HU Location File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Full_Location
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
	,CovAValue AS FLCV4VAL
	,'USD' AS FLCV4VCUR
	,CovBValue AS FLCV5VAL
	,'USD' AS FLCV5VCUR
	,CovCValue AS FLCV6VAL
	,'USD' AS FLCV6VCUR
	,CovDValue AS FLCV7VAL
	,'USD' AS FLCV7VCUR
	,CovAlimit_HU AS FLCV4LIMIT 
	,'USD' AS FLCV4LCUR
	,CovBlimit_HU AS FLCV5LIMIT 
	,'USD' AS FLCV5LCUR
	,CovClimit_HU AS FLCV6LIMIT 
	,'USD' AS FLCV6LCUR
	,CovDlimit_HU AS FLCV7LIMIT 
	,'USD' AS FLCV7LCUR
	,CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovA END AS FLCV4DED 
	,'USD' AS FLCV4DCUR
	,CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovB END AS FLCV5DED 
	,'USD' AS FLCV5DCUR
	,CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovC END AS FLCV6DED 
	,'USD' AS FLCV6DCUR
	,CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovD END AS FLCV7DED 
	,'USD' AS FLCV7DCUR
	,0 AS FLSITELIM
	,'USD' AS FLSITELCUR
	,CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN HurricaneDeductible ELSE 0 END AS FLSITEDED 
	,'USD' AS FLSITEDCUR
	,0 AS FLCOMBINEDLIM
	,'USD' AS FLCOMBINEDLCUR
	,0 AS FLCOMBINEDDED
	,'USD' AS FLCOMBINEDDCUR
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
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Full_Location
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE State NOT IN ('PR','VI','GU')
	and HUMODELED = 'Y'
	and Flood_Flag_Total = 'Y'
--(322867 rows affected)


--Export import files to CSV via export wizard
Select * From Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Full_Account --327435
Select * From Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Full_Location --327435
