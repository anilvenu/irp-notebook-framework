/**********************************************************************************************************************************************
Purpose: This script creates the Risk Modeler Account import file for US Hurricane exposures
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

--Export import files to CSV via export wizard
Select * From Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USIF_Full_Account
