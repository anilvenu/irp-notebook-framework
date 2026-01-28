/**********************************************************************************************************************************************
Purpose: This script creates the Risk Modeler Location import file for US Fire Following exposures
Author: Charlene Chia
Edited by: Teryn Mueller-- Put the name of the person updating this script.
Instructions:
				1. Update quarter e.g. 202212 to 202409
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Table:	CombinedData_{{ DATE_VALUE }}_Working
Output Tables:
				 Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Location

Runtime: 00:00:25
**********************************************************************************************************************************************/

-- US FF Location File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Location
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
	,CovAValue AS EQCV4VAL
	,'USD' AS EQCV4VCUR
	,CovBValue AS EQCV5VAL
	,'USD' AS EQCV5VCUR
	,CovCValue AS EQCV6VAL
	,'USD' AS EQCV6VCUR
	,CovDValue AS EQCV7VAL
	,'USD' AS EQCV7VCUR
	,CovAlimit_FF AS EQCV4LIMIT
	,'USD' AS EQCV4LCUR
	,CovBlimit_FF AS EQCV5LIMIT
	,'USD' AS EQCV5LCUR
	,CovClimit_FF AS EQCV6LIMIT
	,'USD' AS EQCV6LCUR
	,CovDlimit_FF AS EQCV7LIMIT
	,'USD' AS EQCV7LCUR
	,CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovA END AS EQCV4DED
	,'USD' AS EQCV4DCUR
	,CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovB END AS EQCV5DED
	,'USD' AS EQCV5DCUR
	,CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovC END AS EQCV6DED
	,'USD' AS EQCV6DCUR
	,CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovD END AS EQCV7DED
	,'USD' AS EQCV7DCUR
	,0 AS EQSITELIM
	,'USD' AS EQSITELCUR
	,CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN FireFollowingDeductible ELSE 0 END AS EQSITEDED
	,'USD' AS EQSITEDCUR
	,0 AS EQCOMBINEDLIM
	,'USD' AS EQCOMBINEDLCUR
	,0 AS EQCOMBINEDDED
	,'USD' AS EQCOMBINEDDCUR
	,RMS_CladCode_EQ AS CLADDING
	,CRIndicator AS USERTXT1
	,ProductType AS USERTXT2
	,RM_Chimney AS URMCHIMNEY		-- Added for Zesty
	,'' AS USERID1
	,'' AS USERID2
	,'' AS PRIMARYBLDG
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Location
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE State NOT IN ('PR','VI','GU')
	and FFMODELED = 'Y'
--(3816610 rows affected)


--Export import files to CSV via export wizard
Select * From Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Location
