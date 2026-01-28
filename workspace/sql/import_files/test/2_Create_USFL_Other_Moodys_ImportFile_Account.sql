/**********************************************************************************************************************************************
Purpose: This script creates the Risk Modeler Account import file for Other Flood exposures
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


SELECT * FROM Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account
