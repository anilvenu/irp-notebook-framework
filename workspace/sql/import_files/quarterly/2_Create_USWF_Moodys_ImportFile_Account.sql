/**********************************************************************************************************************************************
Purpose: This script creates the Risk Modeler Account import file for US Fire Following exposures, modeled for Wildfire
Author: Charlene Chia
Edited by: Teryn Mueller-- Put the name of the person updating this script.
Instructions:
				1. Update quarter e.g. 202212 to {{ DATE_VALUE }}
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Table:	CombinedData_{{ DATE_VALUE }}_Working
Output Tables:
				 Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Account

Runtime: 00:00:25
**********************************************************************************************************************************************/

-- US WF Account File:
DROP TABLE IF EXISTS dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Account
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
INTO dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Account
FROM CombinedData_{{ DATE_VALUE }}_Working
WHERE State NOT IN ('PR','VI','GU') and FFMODELED = 'Y'
--(3816610 rows affected)


SELECT * FROM Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Account
