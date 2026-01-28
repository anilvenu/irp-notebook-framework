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
				Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Account

Runtime: 00:00:25
**********************************************************************************************************************************************/

/*=======================================================
	Commercial Flood
=======================================================*/
--Create Contract File
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Account
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
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Account
FROM CombinedData_{{ DATE_VALUE }}_Working a
JOIN AssociationNumberLookup_{{ DATE_VALUE }} b ON a.AssociationName = b.AssociationName
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


SELECT * FROM Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Account
