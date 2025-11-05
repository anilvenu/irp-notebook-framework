/**********************************************************************************************************************************************
Purpose:	This script obtains the Working_AIR table control totals that are used to compare to the import files
			The outputs are pasted into Spreadsheet "AIR Exposure Control Totals {DATE_VALUE}_QEM_Group_AllPeril_xFlood"
Author: Teryn Mueller
Edited By: Teryn Mueller
Instructions: 
				1. Update quarter e.g. 202403 to {DATE_VALUE}. Use Replace all function
				2. Execute the script

SQL Server: AIZOVSQLP100001.CEAD.PRD
SQL Database: DW_EXP_MGMT_USER

Input Table:	dbo.CombinedData_{DATE_VALUE}_Working, dbo].[Just_Product_Group_Roe_Power_BI]
Output Tables:  No output tables

Runtime: <1 min
**********************************************************************************************************************************************/

/*===========================================
	CB EQ
===========================================*/
Select Product_group, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_EQ = 0 THEN CovALimit ELSE CovAlimit_EQ END)+
(CASE WHEN Covblimit_EQ = 0 THEN CovbLimit ELSE Covblimit_EQ END)+
(CASE WHEN Covclimit_EQ = 0 THEN CovcLimit ELSE Covclimit_EQ END)+
(CASE WHEN Covdlimit_EQ = 0 THEN CovdLimit ELSE Covdlimit_EQ END)) LocationLimit,
SUM((CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN EarthquakeDeductible ELSE EQDed_CovA END) +
(CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovB END)	+				
(CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovC END)	+					
(CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State IN ('PR','VI')
and EQModeled = 'Y'
Group by Product_group
Order by 1

/*===========================================
	CB HU
===========================================*/
Select Product_group, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_hu = 0 THEN CovALimit ELSE CovAlimit_hu END)+
(CASE WHEN Covblimit_hu = 0 THEN CovbLimit ELSE Covblimit_hu END)+
(CASE WHEN Covclimit_hu = 0 THEN CovcLimit ELSE Covclimit_hu END)+
(CASE WHEN Covdlimit_hu = 0 THEN CovdLimit ELSE Covdlimit_hu END)) LocationLimit,
SUM((CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN HurricaneDeductible ELSE HUDed_CovA END) +
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovB END)	+				
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovC END)	+					
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State IN ('PR','VI')
and HUModeled = 'Y'
Group by Product_group
Order by 1

/*===========================================
	US EQ
===========================================*/
Select Product_group, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_EQ = 0 THEN CovALimit ELSE CovAlimit_EQ END)+
(CASE WHEN Covblimit_EQ = 0 THEN CovbLimit ELSE Covblimit_EQ END)+
(CASE WHEN Covclimit_EQ = 0 THEN CovcLimit ELSE Covclimit_EQ END)+
(CASE WHEN Covdlimit_EQ = 0 THEN CovdLimit ELSE Covdlimit_EQ END)) LocationLimit,
SUM((CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN EarthquakeDeductible ELSE EQDed_CovA END) +
(CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovB END)	+				
(CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovC END)	+					
(CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State NOT IN ('PR','VI','GU')
and EQModeled = 'Y'
and main_bu <> 'Clay'
Group by Product_group

union all 
Select 
CLIENTNAME, SUM(Policypremium) PolicyPremium,
SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_EQ = 0 THEN CovALimit ELSE CovAlimit_EQ END)+
(CASE WHEN Covblimit_EQ = 0 THEN CovbLimit ELSE Covblimit_EQ END)+
(CASE WHEN Covclimit_EQ = 0 THEN CovcLimit ELSE Covclimit_EQ END)+
(CASE WHEN Covdlimit_EQ = 0 THEN CovdLimit ELSE Covdlimit_EQ END)) LocationLimit,
SUM((CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN EarthquakeDeductible ELSE EQDed_CovA END) +
(CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovB END)	+				
(CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovC END)	+					
(CASE WHEN EQDed_CovA+EQDed_CovB+EQDed_CovC+EQDed_CovD = 0 THEN 0 ELSE EQDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working
Where State NOT IN ('PR','VI','GU')
and EQModeled = 'Y'
and main_bu = 'Clay'
Group by CLIENTNAME
Order by 1	

/*===========================================
	US FF
===========================================*/
Select Product_group, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_ff = 0 THEN CovALimit ELSE CovAlimit_ff END)+
(CASE WHEN Covblimit_ff = 0 THEN CovbLimit ELSE Covblimit_ff END)+
(CASE WHEN Covclimit_ff = 0 THEN CovcLimit ELSE Covclimit_ff END)+
(CASE WHEN Covdlimit_ff = 0 THEN CovdLimit ELSE Covdlimit_ff END)) LocationLimit,
SUM((CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN FireFollowingDeductible ELSE FFDed_CovA END) +
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovB END)	+				
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovC END)	+					
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State NOT IN ('PR','VI','GU')
and FFModeled = 'Y'
and main_bu <> 'Clay'
and Product_group <> 'Vol. HO (HIP)'
Group by Product_group

union all 
Select a.Product_group_roe, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_ff = 0 THEN CovALimit ELSE CovAlimit_ff END)+
(CASE WHEN Covblimit_ff = 0 THEN CovbLimit ELSE Covblimit_ff END)+
(CASE WHEN Covclimit_ff = 0 THEN CovcLimit ELSE Covclimit_ff END)+
(CASE WHEN Covdlimit_ff = 0 THEN CovdLimit ELSE Covdlimit_ff END)) LocationLimit,
SUM((CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN FireFollowingDeductible ELSE FFDed_CovA END) +
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovB END)	+				
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovC END)	+					
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State NOT IN ('PR','VI','GU')
and FFModeled = 'Y'
and main_bu <> 'Clay'
and Product_group = 'Vol. HO (HIP)'
Group by a.Product_group_roe

union all 
Select 
CLIENTNAME, SUM(Policypremium) PolicyPremium,
SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_ff = 0 THEN CovALimit ELSE CovAlimit_ff END)+
(CASE WHEN Covblimit_ff = 0 THEN CovbLimit ELSE Covblimit_ff END)+
(CASE WHEN Covclimit_ff = 0 THEN CovcLimit ELSE Covclimit_ff END)+
(CASE WHEN Covdlimit_ff = 0 THEN CovdLimit ELSE Covdlimit_ff END)) LocationLimit,
SUM((CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN FireFollowingDeductible ELSE FFDed_CovA END) +
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovB END)	+				
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovC END)	+					
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working
Where State NOT IN ('PR','VI','GU')
and FFModeled = 'Y'
and main_bu = 'Clay'
Group by CLIENTNAME
Order by 1

/*===========================================
	US ST
===========================================*/
Select Product_group, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_th = 0 THEN CovALimit ELSE CovAlimit_th END)+
(CASE WHEN Covblimit_th = 0 THEN CovbLimit ELSE Covblimit_th END)+
(CASE WHEN Covclimit_th = 0 THEN CovcLimit ELSE Covclimit_th END)+
(CASE WHEN Covdlimit_th = 0 THEN CovdLimit ELSE Covdlimit_th END)) LocationLimit,
SUM((CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN TORNADOHAILDEDUCTIBLE ELSE THDed_CovA END) +
(CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovB END)	+				
(CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovC END)	+					
(CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State NOT IN ('PR','VI','GU')
and THModeled = 'Y'
and main_bu <> 'Clay'
and Product_group <> 'Vol. HO (HIP)'
Group by Product_group

union all 
Select a.Product_group_roe, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_th = 0 THEN CovALimit ELSE CovAlimit_th END)+
(CASE WHEN Covblimit_th = 0 THEN CovbLimit ELSE Covblimit_th END)+
(CASE WHEN Covclimit_th = 0 THEN CovcLimit ELSE Covclimit_th END)+
(CASE WHEN Covdlimit_th = 0 THEN CovdLimit ELSE Covdlimit_th END)) LocationLimit,
SUM((CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN TORNADOHAILDEDUCTIBLE ELSE THDed_CovA END) +
(CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovB END)	+				
(CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovC END)	+					
(CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State NOT IN ('PR','VI','GU')
and THModeled = 'Y'
and main_bu <> 'Clay'
and Product_group = 'Vol. HO (HIP)'
Group by a.Product_group_roe

union all 
Select 
CLIENTNAME, SUM(Policypremium) PolicyPremium,
SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_th = 0 THEN CovALimit ELSE CovAlimit_th END)+
(CASE WHEN Covblimit_th = 0 THEN CovbLimit ELSE Covblimit_th END)+
(CASE WHEN Covclimit_th = 0 THEN CovcLimit ELSE Covclimit_th END)+
(CASE WHEN Covdlimit_th = 0 THEN CovdLimit ELSE Covdlimit_th END)) LocationLimit,
SUM((CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN TORNADOHAILDEDUCTIBLE ELSE THDed_CovA END) +
(CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovB END)	+				
(CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovC END)	+					
(CASE WHEN THDed_CovA+THDed_CovB+THDed_CovC+THDed_CovD = 0 THEN 0 ELSE THDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working
Where State NOT IN ('PR','VI','GU')
and THModeled = 'Y'
and main_bu = 'Clay'
Group by CLIENTNAME
Order by 1

/*===========================================
	US HU Leak
===========================================*/
Select Product_group, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_hu = 0 THEN CovALimit ELSE CovAlimit_hu END)+
(CASE WHEN Covblimit_hu = 0 THEN CovbLimit ELSE Covblimit_hu END)+
(CASE WHEN Covclimit_hu = 0 THEN CovcLimit ELSE Covclimit_hu END)+
(CASE WHEN Covdlimit_hu = 0 THEN CovdLimit ELSE Covdlimit_hu END)) LocationLimit,
SUM((CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN HurricaneDeductible ELSE HUDed_CovA END) +
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovB END)	+				
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovC END)	+					
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State NOT IN ('PR','VI','GU')
and HUModeled = 'Y'
and main_bu <> 'Clay'
and Product_group <> 'Vol. HO (HIP)'
and FLOOD_FLAG_TOTAL = 'n'
Group by Product_group

union all 
Select a.Product_group_roe, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_hu = 0 THEN CovALimit ELSE CovAlimit_hu END)+
(CASE WHEN Covblimit_hu = 0 THEN CovbLimit ELSE Covblimit_hu END)+
(CASE WHEN Covclimit_hu = 0 THEN CovcLimit ELSE Covclimit_hu END)+
(CASE WHEN Covdlimit_hu = 0 THEN CovdLimit ELSE Covdlimit_hu END)) LocationLimit,
SUM((CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN HurricaneDeductible ELSE HUDed_CovA END) +
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovB END)	+				
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovC END)	+					
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State NOT IN ('PR','VI','GU')
and HUModeled = 'Y'
and main_bu <> 'Clay'
and Product_group = 'Vol. HO (HIP)'
and FLOOD_FLAG_TOTAL = 'n'
Group by a.Product_group_roe

union all 
Select 
CLIENTNAME, SUM(Policypremium) PolicyPremium,
SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_hu = 0 THEN CovALimit ELSE CovAlimit_hu END)+
(CASE WHEN Covblimit_hu = 0 THEN CovbLimit ELSE Covblimit_hu END)+
(CASE WHEN Covclimit_hu = 0 THEN CovcLimit ELSE Covclimit_hu END)+
(CASE WHEN Covdlimit_hu = 0 THEN CovdLimit ELSE Covdlimit_hu END)) LocationLimit,
SUM((CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN HurricaneDeductible ELSE HUDed_CovA END) +
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovB END)	+				
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovC END)	+					
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working
Where State NOT IN ('PR','VI','GU')
and HUModeled = 'Y'
and main_bu = 'Clay'
and FLOOD_FLAG_TOTAL = 'n'
Group by CLIENTNAME
Order by 1

/*===========================================
	US HU Full
===========================================*/
Select Product_group, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_hu = 0 THEN CovALimit ELSE CovAlimit_hu END)+
(CASE WHEN Covblimit_hu = 0 THEN CovbLimit ELSE Covblimit_hu END)+
(CASE WHEN Covclimit_hu = 0 THEN CovcLimit ELSE Covclimit_hu END)+
(CASE WHEN Covdlimit_hu = 0 THEN CovdLimit ELSE Covdlimit_hu END) +
(CASE WHEN CovAlimit_Flood <> 0 THEN CovAlimit_Flood ELSE 0 END) +	
(CASE WHEN CovBlimit_Flood <> 0 THEN CovBlimit_Flood ELSE 0 END) + 
(CASE WHEN CovClimit_Flood <> 0 THEN CovClimit_Flood ELSE 0 END) + 
(CASE WHEN CovDlimit_Flood <> 0 THEN CovDlimit_Flood ELSE 0 END)) LocationLimit,
SUM((CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN HurricaneDeductible ELSE HUDed_CovA END) +
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovB END) +				
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovC END) +					
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovD END)) LocationDeductible --+
--(CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 THEN FloodDeductible ELSE 0 END) +
--(CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD <> 0 THEN FloodDed_CovA ELSE 0 END) +	
--(CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD <> 0 THEN FloodDed_CovB ELSE 0 END) + 
--(CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD <> 0 THEN FloodDed_CovC ELSE 0 END) + 
--(CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD <> 0 THEN FloodDed_CovD ELSE 0 END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State NOT IN ('PR','VI','GU')
and HUModeled = 'Y'
and main_bu <> 'Clay'
and FLOOD_FLAG_TOTAL = 'y'
Group by Product_group

union all 
Select 
CLIENTNAME, SUM(Policypremium) PolicyPremium,
SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_hu = 0 THEN CovALimit ELSE CovAlimit_hu END)+
(CASE WHEN Covblimit_hu = 0 THEN CovbLimit ELSE Covblimit_hu END)+
(CASE WHEN Covclimit_hu = 0 THEN CovcLimit ELSE Covclimit_hu END)+
(CASE WHEN Covdlimit_hu = 0 THEN CovdLimit ELSE Covdlimit_hu END) +
(CASE WHEN CovAlimit_Flood <> 0 THEN CovAlimit_Flood ELSE 0 END) +	
(CASE WHEN CovBlimit_Flood <> 0 THEN CovBlimit_Flood ELSE 0 END) + 
(CASE WHEN CovClimit_Flood <> 0 THEN CovClimit_Flood ELSE 0 END) + 
(CASE WHEN CovDlimit_Flood <> 0 THEN CovDlimit_Flood ELSE 0 END)) LocationLimit,
SUM((CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN HurricaneDeductible ELSE HUDed_CovA END) +
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovB END) +				
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovC END) +					
(CASE WHEN HUDed_CovA+HUDed_CovB+HUDed_CovC+HUDed_CovD = 0 THEN 0 ELSE HUDed_CovD END)) LocationDeductible-- +
--(CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 THEN FloodDeductible ELSE 0 END) +
--(CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD <> 0 THEN FloodDed_CovA ELSE 0 END) +
--(CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD <> 0 THEN FloodDed_CovB ELSE 0 END) + 
--(CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD <> 0 THEN FloodDed_CovC ELSE 0 END) + 
--(CASE WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD <> 0 THEN FloodDed_CovD ELSE 0 END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working
Where State NOT IN ('PR','VI','GU')
and HUModeled = 'Y'
and main_bu = 'Clay'
and FLOOD_FLAG_TOTAL = 'y'
Group by CLIENTNAME
Order by 1

/*===========================================
	US WF
===========================================*/
Select Product_group, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_ff = 0 THEN CovALimit ELSE CovAlimit_ff END)+
(CASE WHEN Covblimit_ff = 0 THEN CovbLimit ELSE Covblimit_ff END)+
(CASE WHEN Covclimit_ff = 0 THEN CovcLimit ELSE Covclimit_ff END)+
(CASE WHEN Covdlimit_ff = 0 THEN CovdLimit ELSE Covdlimit_ff END)) LocationLimit,
SUM((CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN FireFollowingDeductible ELSE FFDed_CovA END) +
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovB END)	+				
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovC END)	+					
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State NOT IN ('PR','VI','GU')
and FFModeled = 'Y'
and main_bu <> 'Clay'
and Product_group <> 'Vol. HO (HIP)'
Group by Product_group

union all 
Select a.Product_group_roe, SUM(Policypremium) PolicyPremium, SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_ff = 0 THEN CovALimit ELSE CovAlimit_ff END)+
(CASE WHEN Covblimit_ff = 0 THEN CovbLimit ELSE Covblimit_ff END)+
(CASE WHEN Covclimit_ff = 0 THEN CovcLimit ELSE Covclimit_ff END)+
(CASE WHEN Covdlimit_ff = 0 THEN CovdLimit ELSE Covdlimit_ff END)) LocationLimit,
SUM((CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN FireFollowingDeductible ELSE FFDed_CovA END) +
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovB END)	+				
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovC END)	+					
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State NOT IN ('PR','VI','GU')
and FFModeled = 'Y'
and main_bu <> 'Clay'
and Product_group = 'Vol. HO (HIP)'
Group by a.Product_group_roe

union all 
Select 
CLIENTNAME, SUM(Policypremium) PolicyPremium,
SUM(PolicyLimit) PolicyLimit, SUM(Gross_Exposed_Limit) GrossExposedLimit, Count(*) LocationCount,
SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue,
SUM((CASE WHEN CovAlimit_ff = 0 THEN CovALimit ELSE CovAlimit_ff END)+
(CASE WHEN Covblimit_ff = 0 THEN CovbLimit ELSE Covblimit_ff END)+
(CASE WHEN Covclimit_ff = 0 THEN CovcLimit ELSE Covclimit_ff END)+
(CASE WHEN Covdlimit_ff = 0 THEN CovdLimit ELSE Covdlimit_ff END)) LocationLimit,
SUM((CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN FireFollowingDeductible ELSE FFDed_CovA END) +
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovB END)	+				
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovC END)	+					
(CASE WHEN FFDed_CovA+FFDed_CovB+FFDed_CovC+FFDed_CovD = 0 THEN 0 ELSE FFDed_CovD END)) LocationDeductible
From dbo.CombinedData_{DATE_VALUE}_Working
Where State NOT IN ('PR','VI','GU')
and FFModeled = 'Y'
and main_bu = 'Clay'
Group by CLIENTNAME
Order by 1


/*===========================================
	US FL Commercial Flood
===========================================*/
WITH CommercialFlood_ByCov_PolControlTotals AS (
SELECT Product_Group_ROE as Portfolio, b.AssociationAcctNum
	,CASE WHEN BLANKET_LIMIT_TYPE = 'Policy Level Per Occurrence' AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'Y' THEN MAX(a.Blanket_Building_Limit)+Max(a.Blanket_Content_Limit) --coded in the sublimit, here for totals
		WHEN BLANKET_LIMIT_TYPE = 'Policy Level Per Occurrence' AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'N' THEN MAX(a.BLANKET_COMBINED_LIMIT)
		WHEN (BLANKET_LIMIT_TYPE IS NULL OR BLANKET_LIMIT_TYPE = '') THEN SUM(PolicyLimit) ELSE '' END AS Limit1
	,CASE WHEN BLANKET_LIMIT_TYPE = 'Annual Aggregate' AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'Y' THEN MAX(a.Blanket_Building_Limit)+MAX(a.Blanket_Content_Limit) 
		WHEN BLANKET_LIMIT_TYPE = 'Annual Aggregate' AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'N' THEN MAX(a.BLANKET_COMBINED_LIMIT)
		ELSE '' END AS AggregateLimit
	,CASE WHEN BLANKET_DED_TYPE = 'Per Occurrence' AND FloodDeductible IS NOT NULL AND FloodDeductible > 0 THEN FloodDeductible ELSE 0 END AS DedAmt1
	,CASE WHEN SUBLIMIT_TYPE = 'Policy Level Per Occurrence' THEN MAX(Sublimit) ELSE 0 END AS Policy_Sublimit
FROM dbo.CombinedData_{DATE_VALUE}_Working a
JOIN dbo.AssociationNumberLookup_{DATE_VALUE} b on a.AssociationName = b.AssociationName
WHERE ProductType in ('FB')
GROUP BY Product_Group_ROE, b.AssociationAcctNum, BLANKET_LIMIT_TYPE, SEPERATE_BLANKET_BUILDING_AND_CONTENT, BLANKET_DED_TYPE, FloodDeductible, SUBLIMIT_TYPE ) ,
CommercialFlood_ByCov_LocControlTotals AS (
SELECT Product_Group_ROE as Portfolio
 ,CASE WHEN BLANKET_LIMIT_TYPE = 'Per Building Per Occurrence' AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'Y' THEN Blanket_Building_Limit
	ELSE CovAlimit_Flood END AS WSCV4LIMIT
 ,CovBlimit_Flood AS WSCV5LIMIT       
 ,CASE WHEN BLANKET_LIMIT_TYPE = 'Per Building Per Occurrence' AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'Y' THEN Blanket_Content_Limit
	ELSE CovClimit_Flood END AS WSCV6LIMIT       
 ,CovDlimit_Flood AS WSCV7LIMIT 
 ,CASE WHEN BLANKET_LIMIT_TYPE = 'Per Building Per Occurrence' AND SEPERATE_BLANKET_BUILDING_AND_CONTENT = 'N' THEN BLANKET_COMBINED_LIMIT
	WHEN SUBLIMIT_TYPE = 'Per Building Per Occurrence' THEN SUBLIMIT ELSE 0 END AS WSCOMBINEDLIM
 ,CASE WHEN BLANKET_DED_TYPE = 'Per Building Per Occurrence' THEN 0	
	WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 AND (BLANKET_DED_TYPE <> 'Per Building Per Occurrence' OR BLANKET_DED_TYPE IS NULL) THEN 0 
	ELSE FloodDed_CovA END AS WSCV4DED
 ,CASE WHEN BLANKET_DED_TYPE = 'Per Building Per Occurrence' THEN 0	
	WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 AND (BLANKET_DED_TYPE <> 'Per Building Per Occurrence' OR BLANKET_DED_TYPE IS NULL) THEN 0 
	ELSE FloodDed_CovB END AS WSCV5DED
 ,CASE WHEN BLANKET_DED_TYPE = 'Per Building Per Occurrence' THEN 0	
	WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 AND (BLANKET_DED_TYPE <> 'Per Building Per Occurrence' OR BLANKET_DED_TYPE IS NULL) THEN 0 
	ELSE FloodDed_CovC END AS WSCV6DED
 ,CASE WHEN BLANKET_DED_TYPE = 'Per Building Per Occurrence' THEN 0	
	WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 AND (BLANKET_DED_TYPE <> 'Per Building Per Occurrence' OR BLANKET_DED_TYPE IS NULL) THEN 0 
	ELSE FloodDed_CovD END AS WSCV7DED
 ,CASE WHEN BLANKET_DED_TYPE = 'Per Building Per Occurrence' THEN FloodDeductible 
	WHEN FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD = 0 AND (BLANKET_DED_TYPE <> 'Per Building Per Occurrence' OR BLANKET_DED_TYPE IS NULL) THEN FloodDeductible 
	ELSE 0 END AS WSSITEDED
FROM dbo.CombinedData_{DATE_VALUE}_Working
WHERE ProductType in ('FB') )

SELECT Count(Distinct AssociationName) PolicyCount
,SUM(PolicyPremium) PolicyPremium
,SUM(Gross_Exposed_Limit) GrossExposedLimit 
,'0' AS AttachmentPoint
,(SELECT SUM(DedAmt1) FROM CommercialFlood_ByCov_PolControlTotals) PolicyDeductible
,(SELECT SUM(Limit1+AggregateLimit) FROM CommercialFlood_ByCov_PolControlTotals) PolicyLimit
,(SELECT SUM(Policy_Sublimit) FROM CommercialFlood_ByCov_PolControlTotals) PolicySublimit
,Count(*) LocationCountDistinct
,SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue
,(SELECT SUM(WSCV4LIMIT+WSCV5LIMIT+WSCV6LIMIT+WSCV7LIMIT+WSCOMBINEDLIM) FROM CommercialFlood_ByCov_LocControlTotals) LocationLimit
,(SELECT SUM(WSCV4DED+WSCV5DED+WSCV6DED+WSCV7DED+WSSITEDED) FROM CommercialFlood_ByCov_LocControlTotals) LocationDeductible
FROM dbo.CombinedData_{DATE_VALUE}_Working
WHERE ProductType in ('FB')

/*===========================================
	US FL Excess Flood
===========================================*/
SELECT Count(Distinct AssociationName) PolicyCount
,SUM(PolicyPremium) PolicyPremium
,SUM(Gross_Exposed_Limit) GrossExposedLimit 
,'0' AS AttachmentPoint
,'0' as PolicyDeductible
,SUM(PolicyLimit) PolicyLimit
,'0' as PolicySublimit
,Count(*) LocationCountDistinct
,SUM(CovAValue+CovBValue+CovCValue+CovDValue) TotalReplacementValue
,SUM(CovAlimit_Flood+CovBlimit_Flood+CovClimit_Flood+CovDlimit_Flood) LocationLimit
,SUM(FloodAttachmentPoint_CovA+FloodAttachmentPoint_CovC) LocationDeductible
FROM dbo.CombinedData_{DATE_VALUE}_Working
WHERE ProductType in ('EF','EG')

/*===========================================
	US FL Other Flood
===========================================*/
SELECT CASE WHEN QEM_Product_Group = 'Lender Placed' AND State IN ('PR','VI','GU') THEN 'Other_CB' ELSE QEM_Product_Group END AS QEM_Product_Group,
Count(*) PolicyCount
,SUM(PolicyPremium) PolicyPremium
,SUM(Gross_Exposed_Limit) GrossExposedLimit 
,SUM(FloodAttachmentPoint_CovA) AS AttachmentPoint
,SUM(CASE WHEN FLOODDEDUCTIBLE IS NULL THEN FLOODDED_COVA ELSE FLOODDEDUCTIBLE END) AS PolicyDeductible
,SUM(MODELED_TIV_FLOOD) PolicyLimit
,0 PolicySublimit
,Count(*) LocationCountDistinct
,SUM(CovAValue+CovBValue+CovCValue+CovDValue+FloodAttachmentPoint_CovA) TotalReplacementValue
,SUM(CovAlimit_Flood+CovBlimit_Flood+CovClimit_Flood+CovDlimit_Flood) LocationLimit
,SUM(FloodDed_CovA+FloodDed_CovB+FloodDed_CovC+FloodDed_CovD) LocationDeductible
FROM dbo.CombinedData_{DATE_VALUE}_Working
WHERE OTHER_FLOOD_IND = 'Y' --All Other Flood
GROUP BY CASE WHEN QEM_Product_Group = 'Lender Placed' AND State IN ('PR','VI','GU') THEN 'Other_CB' ELSE QEM_Product_Group END
ORDER BY 1