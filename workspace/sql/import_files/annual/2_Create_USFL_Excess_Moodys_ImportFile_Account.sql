/**********************************************************************************************************************************************
Purpose: This script creates the Risk Modeler Account import file for Flood Solutions exposures
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
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Account

Runtime: 00:00:25
**********************************************************************************************************************************************/

/*=======================================================
	Excess Flood
=======================================================*/
--Create Contract File
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Account
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
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Account
FROM CombinedData_{{ DATE_VALUE }}_Working a
JOIN AssociationNumberLookup_{{ DATE_VALUE }} b ON a.AssociationName = b.AssociationName
WHERE ProductType in ('EF','EG')
GROUP BY
	b.AssociationAcctNum,
	a.AssociationName,
	a.QEM_Product_Group,
	a.QS_Indicator,
	a.ProductType,
	a.NetLegalEntity


SELECT * FROM Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Account
