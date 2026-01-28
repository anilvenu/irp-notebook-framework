/**********************************************************************************************************************************************
Purpose:	This script creates the Risk Modeler Account import file for CB Hurricane exposures
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
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Account

Runtime: 00:00:25
**********************************************************************************************************************************************/

-- CB HU Account File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Account
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
		WHEN HU_Remetrica_RDMF_Bucket like '%Banco%' THEN 'Banco Popular'
		WHEN HU_Remetrica_RDMF_Bucket like '%First%' THEN 'First Bank'
		WHEN HU_Remetrica_RDMF_Bucket like '%Oriental%' THEN 'Oriental'
		WHEN HU_Remetrica_RDMF_Bucket like '%lend%' THEN 'xScotia xLending'
		ELSE 'LOB'
	END AS LOBNAME
	,EffectiveDate AS INCEPTDATE
	,ExpirationDate AS EXPIREDATE
	,0 AS UNDCOVAMT
	,'USD' AS UNDCOVCUR
	,PolicyLimit AS PARTOF
	,'USD' AS PARTOFCUR
	,2 AS POLICYTYPE -- Windstorm = 2
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
	,AssurantGroupedLOB AS POLICYUSERTXT2 --varchar(20)
	,NetLegalEntity AS POLICYUSERTXT3 --varchar(20)
	,LegalEntity AS POLICYUSERTXT4 --varchar(20)
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Account
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE State IN ('PR','VI')
	and HurricaneCoverage = 'Y'
	and HU_Remetrica_RDMF_Bucket <> 'NULL'
	and Main_BU <> 'Clay'
--(2723 rows affected)


--Export import files to CSV
Select * From Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Account
