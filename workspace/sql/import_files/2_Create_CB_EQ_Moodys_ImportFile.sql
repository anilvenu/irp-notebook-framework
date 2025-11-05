/**********************************************************************************************************************************************
Purpose: This script creates the RiskLink import files for CB Earthquake exposures
Author: Charlene Chia
Edited by: Teryn Mueller-- Put the name of the person updating this script.
Instructions: 
				1. Update quarter e.g. 202212 to {DATE_VALUE}
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Table:	CombinedData_202209_Working
Output Tables:
				Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_CBEQ_Account
				Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_CBEQ_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

-- CB EQ Account File:
DROP TABLE IF EXISTS dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_CBEQ_Account
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
		WHEN EQ_Remetrica_RDMF_Bucket like '%Banco%' THEN 'Banco Popular'
		WHEN EQ_Remetrica_RDMF_Bucket like '%First%' THEN 'First Bank'
		WHEN EQ_Remetrica_RDMF_Bucket like '%Oriental%' THEN 'Oriental'
		WHEN EQ_Remetrica_RDMF_Bucket like '%lend%' THEN 'xScotia xLending'
		ELSE 'LOB'
	END AS LOBNAME
	,EffectiveDate AS INCEPTDATE
	,ExpirationDate AS EXPIREDATE
	,0 AS UNDCOVAMT
	,'USD' AS UNDCOVCUR
	,PolicyLimit AS PARTOF
	,'USD' AS PARTOFCUR
	,1 AS POLICYTYPE -- Earthquake = 1
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
	,EQ_Remetrica_RDMF_Bucket AS USERDEF1
	,FHCFIndicator AS USERDEF2 
	,QS_Indicator AS USERDEF3  
	,ProductType AS USERDEF4 
	,QEM_Product_Group AS USERTXT1 -- was main_bu
	,BusinessUnit AS USERTXT2
	,CASE WHEN BusinessUnit = 'Clay' THEN ClientName ELSE AccountNumber END AS POLICYUSERTXT1 --varchar(20)
	,AssurantGroupedLOB AS POLICYUSERTXT2 --varchar(20)
	,NetLegalEntity AS POLICYUSERTXT3 --varchar(20)
	,LegalEntity AS POLICYUSERTXT4 --varchar(20)
INTO dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_CBEQ_Account
FROM CombinedData_{DATE_VALUE}_Working
WHERE State IN ('PR','VI')
	and EarthquakeCoverage = 'Y'  
	and EQ_Remetrica_RDMF_Bucket <> 'NULL'
	and Main_BU <> 'Clay'
--(9 rows affected)

-- CB EQ Location File:
DROP TABLE IF EXISTS dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_CBEQ_Location
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
	,RMS_ConstCode_EQ AS BLDGCLASS
	,'ATC' AS OCCSCHEME
	,RMS_OccType_ATC AS OCCTYPE
	,CASE
		WHEN Model_YearBuilt = '0' THEN '12/31/9999'
		WHEN RTRIM(Model_YearBuilt) <= '1800' THEN '12/31/9999'
		ELSE '12/31/'+Model_YearBuilt 
	END AS YEARBUILT
	,Model_NumberofStories AS NUMSTORIES
	,CovAValue AS EQCV4VAL
	,'USD' AS EQCV4VCUR
	,CovBValue AS EQCV5VAL
	,'USD' AS EQCV5VCUR
	,CovCValue AS EQCV6VAL
	,'USD' AS EQCV6VCUR
	,CovDValue AS EQCV7VAL
	,'USD' AS EQCV7VCUR
	,CovAlimit_EQ AS EQCV4LIMIT 
	,'USD' AS EQCV4LCUR
	,CovBlimit_EQ AS EQCV5LIMIT
	,'USD' AS EQCV5LCUR
	,CovClimit_EQ AS EQCV6LIMIT 
	,'USD' AS EQCV6LCUR
	,CovDlimit_EQ AS EQCV7LIMIT 
	,'USD' AS EQCV7LCUR	
	,CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovA END AS EQCV4DED 
	,'USD' AS EQCV4DCUR
	,CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovB END AS EQCV5DED 
	,'USD' AS EQCV5DCUR
	,CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovC END AS EQCV6DED 
	,'USD' AS EQCV6DCUR
	,CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovD END AS EQCV7DED 
	,'USD' AS EQCV7DCUR
	,0 AS EQSITELIM
	,'USD' AS EQSITELCUR
	,CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN EarthquakeDeductible ELSE 0 END AS EQSITEDED 
	,'USD' AS EQSITEDCUR
	,0 AS EQCOMBINEDLIM
	,'USD' AS EQCOMBINEDLCUR
	,0 AS EQCOMBINEDDED
	,'USD' AS EQCOMBINEDDCUR
	,RMS_CladCode_EQ AS CLADDING
	,CRIndicator AS USERTXT1
	,ProductType AS USERTXT2
	,'' AS USERID1
	,'' AS USERID2
	,'' AS PRIMARYBLDG
INTO dbo.Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_CBEQ_Location
FROM CombinedData_{DATE_VALUE}_Working
WHERE State IN ('PR','VI')
	and EarthquakeCoverage = 'Y' 
	and EQ_Remetrica_RDMF_Bucket <> 'NULL'
	and Main_BU <> 'Clay'
--(9 rows affected)


--Export import files to CSV
--Select * From Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_CBEQ_Account --4
--Select * From Modeling_{DATE_VALUE}_Moodys_{CYCLE_TYPE}_CBEQ_Location --4
