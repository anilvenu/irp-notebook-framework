/**********************************************************************************************************************************************
Purpose:	This script obtains the Moodys account import file table control totals that are used to compare to working table control totals.
			The outputs are pasted into Spreadsheet "Moodys Exposure Control Totals 202209.xlsx"
Author: Charlene Chia
Edited By: 
Instructions: 
				1. Update quarter e.g. 202212 to 202306. Use Replace all function
				2. Execute the script

SQL Server: vdbpdw-housing-secondary.database.cead.prd
SQL Database: DW_EXP_MGMT_USER

Input Tables:	All Moodys Account import file tables except for Flood Solutions and Other Flood
Output Tables:  No output tables

Runtime: <1 min
**********************************************************************************************************************************************/

--CB EQ
Select concat('CBEQ_',Product_group) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBEQ_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBEQ_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(EQCV4VAL+EQCV5VAL+EQCV6VAL+EQCV7VAL) TotalReplacementValue
		,Sum(EQCV4LIMIT+EQCV5LIMIT+EQCV6LIMIT+EQCV7LIMIT) LocationLimit
		,SUM(EQCV4DED +EQCV5DED +EQCV6DED +EQCV7DED+EQSITEDED) as LocationDeductible 
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBEQ_Account a
Full Outer Join dbo.modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBEQ_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
Group by Product_group
Order by 1

--CB HU
Select concat('CBHU_',Product_group) AS ExposureGroup, 'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(WSCV4VAL+WSCV5VAL+WSCV6VAL+WSCV7VAL) TotalReplacementValue
		,Sum(WSCV4LIMIT+WSCV5LIMIT+WSCV6LIMIT+WSCV7LIMIT) LocationLimit
		,SUM(WSCV4DED +WSCV5DED +WSCV6DED +WSCV7DED+WSSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Account a
Full Outer Join dbo.modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_CBHU_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
--and a.SublimitArea = 'Wind'
Group by Product_group
Order by 1

--US EQ
Select concat('USEQ_',Product_group) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USEQ_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USEQ_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(EQCV4VAL+EQCV5VAL+EQCV6VAL+EQCV7VAL) TotalReplacementValue
		,Sum(EQCV4LIMIT+EQCV5LIMIT+EQCV6LIMIT+EQCV7LIMIT) LocationLimit
		,SUM(EQCV4DED +EQCV5DED +EQCV6DED +EQCV7DED+EQSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USEQ_Account a
Full Outer Join dbo.modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USEQ_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
Group by Product_group

union all
Select concat('USEQ_',POLICYUSERTXT1) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USEQ_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USEQ_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(EQCV4VAL+EQCV5VAL+EQCV6VAL+EQCV7VAL) TotalReplacementValue
		,Sum(EQCV4LIMIT+EQCV5LIMIT+EQCV6LIMIT+EQCV7LIMIT) LocationLimit
		,SUM(EQCV4DED +EQCV5DED +EQCV6DED +EQCV7DED+EQSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USEQ_Account a
Full Outer Join dbo.modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USEQ_Location b on a.ACCNTNUM = b.ACCNTNUM
where a.USERTXT2 = 'Clay'
Group by POLICYUSERTXT1
Order by 1
	

--US FF
Select concat('USFF_',Product_group) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(EQCV4VAL+EQCV5VAL+EQCV6VAL+EQCV7VAL) TotalReplacementValue
		,Sum(EQCV4LIMIT+EQCV5LIMIT+EQCV6LIMIT+EQCV7LIMIT) LocationLimit
		,SUM(EQCV4DED +EQCV5DED +EQCV6DED +EQCV7DED+EQSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Account a
Full Outer Join dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
and c.Product_Group <> 'Vol. HO (HIP)'
Group by Product_group

union all
Select concat('USFF_',a.ACCNTNAME) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(EQCV4VAL+EQCV5VAL+EQCV6VAL+EQCV7VAL) TotalReplacementValue
		,Sum(EQCV4LIMIT+EQCV5LIMIT+EQCV6LIMIT+EQCV7LIMIT) LocationLimit
		,SUM(EQCV4DED +EQCV5DED +EQCV6DED +EQCV7DED+EQSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Account a
Full Outer Join dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
and  c.Product_Group = 'Vol. HO (HIP)'
Group by a.ACCNTNAME

union all
Select concat('USFF_',POLICYUSERTXT1) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(EQCV4VAL+EQCV5VAL+EQCV6VAL+EQCV7VAL) TotalReplacementValue
		,Sum(EQCV4LIMIT+EQCV5LIMIT+EQCV6LIMIT+EQCV7LIMIT) LocationLimit
		,SUM(EQCV4DED +EQCV5DED +EQCV6DED +EQCV7DED+EQSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Account a
Full Outer Join dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFF_Location b on a.ACCNTNUM = b.ACCNTNUM
where a.USERTXT2 = 'Clay'
Group by POLICYUSERTXT1
Order by 1

--US ST
select concat('USOW_',Product_group) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(TOCV4VAL+TOCV5VAL+TOCV6VAL+TOCV7VAL) TotalReplacementValue
		,Sum(TOCV4LIMIT+TOCV5LIMIT+TOCV6LIMIT+TOCV7LIMIT) LocationLimit
		,SUM(TOCV4DED +TOCV5DED +TOCV6DED +TOCV7DED+TOSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_usow_Account a
Full Outer Join dbo.modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_usow_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
and c.Product_Group <> 'Vol. HO (HIP)'
Group by Product_group

union all
Select concat('USOW_',a.ACCNTNAME) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(TOCV4VAL+TOCV5VAL+TOCV6VAL+TOCV7VAL) TotalReplacementValue
		,Sum(TOCV4LIMIT+TOCV5LIMIT+TOCV6LIMIT+TOCV7LIMIT) LocationLimit
		,SUM(TOCV4DED +TOCV5DED +TOCV6DED +TOCV7DED+TOSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_usow_Account a
Full Outer Join dbo.modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_usow_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
and c.Product_Group = 'Vol. HO (HIP)'
Group by a.ACCNTNAME

union all
Select concat('USOW_',POLICYUSERTXT1) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USOW_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(TOCV4VAL+TOCV5VAL+TOCV6VAL+TOCV7VAL) TotalReplacementValue
		,Sum(TOCV4LIMIT+TOCV5LIMIT+TOCV6LIMIT+TOCV7LIMIT) LocationLimit
		,SUM(TOCV4DED +TOCV5DED +TOCV6DED +TOCV7DED+TOSITEDED) as LocationDeductible 
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_usow_Account a
Full Outer Join dbo.modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_usow_Location b on a.ACCNTNUM = b.ACCNTNUM
where a.USERTXT2 = 'Clay'
Group by POLICYUSERTXT1
Order by 1


--US HU leak
Select concat('USHU_',Product_group,'_Leak') AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(WSCV4VAL+WSCV5VAL+WSCV6VAL+WSCV7VAL) TotalReplacementValue
		,Sum(WSCV4LIMIT+WSCV5LIMIT+WSCV6LIMIT+WSCV7LIMIT) LocationLimit
		,SUM(WSCV4DED +WSCV5DED +WSCV6DED +WSCV7DED+WSSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Account a
Full Outer Join dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
and c.Product_Group <> 'Vol. HO (HIP)'
Group by Product_group

union all
Select concat('USHU_',a.ACCNTNAME,'_Leak') AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(WSCV4VAL+WSCV5VAL+WSCV6VAL+WSCV7VAL) TotalReplacementValue
		,Sum(WSCV4LIMIT+WSCV5LIMIT+WSCV6LIMIT+WSCV7LIMIT) LocationLimit
		,SUM(WSCV4DED +WSCV5DED +WSCV6DED +WSCV7DED+WSSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Account a
Full Outer Join dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
and c.Product_Group = 'Vol. HO (HIP)'
Group by a.ACCNTNAME

union all
Select concat('USHU_',POLICYUSERTXT1,'_Leak') AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(WSCV4VAL+WSCV5VAL+WSCV6VAL+WSCV7VAL) TotalReplacementValue
		,Sum(WSCV4LIMIT+WSCV5LIMIT+WSCV6LIMIT+WSCV7LIMIT) LocationLimit
		,SUM(WSCV4DED +WSCV5DED +WSCV6DED +WSCV7DED+WSSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Account a
Full Outer Join dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Leak_Location b on a.ACCNTNUM = b.ACCNTNUM
where a.USERTXT2 = 'Clay'
Group by POLICYUSERTXT1
Order by 1

--US HU full
Select concat('USHU_',Product_group,'_Full') AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(WSCV4VAL+WSCV5VAL+WSCV6VAL+WSCV7VAL) TotalReplacementValue
		,SUM((WSCV4LIMIT+WSCV5LIMIT+WSCV6LIMIT+WSCV7LIMIT)) LocationLimit
			--(CASE WHEN LimitBldg2 IS NOT NULL THEN LimitBldg2 ELSE 0 END) +
			--(CASE WHEN LimitOther2 IS NOT NULL THEN LimitOther2 ELSE 0 END) +
			--(CASE WHEN LimitContent2 IS NOT NULL THEN LimitContent2 ELSE 0 END) +
			--(CASE WHEN LimitTime2 IS NOT NULL THEN LimitTime2 ELSE 0 END)) LocationLimit
		,SUM(WSCV4DED +WSCV5DED +WSCV6DED +WSCV7DED+WSSITEDED) LocationDeductible --+DeductBldg2+DeductOther2+DeductContent2+DeductTime2) as LocationDeductible 
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Account a
Full Outer Join dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
Group by Product_group

union all
Select concat('USHU_',POLICYUSERTXT1,'_Full') AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(WSCV4VAL+WSCV5VAL+WSCV6VAL+WSCV7VAL) TotalReplacementValue
		,SUM((WSCV4LIMIT+WSCV5LIMIT+WSCV6LIMIT+WSCV7LIMIT)) LocationLimit--+
			--(CASE WHEN LimitBldg2 IS NOT NULL THEN LimitBldg2 ELSE 0 END) +
			--(CASE WHEN LimitOther2 IS NOT NULL THEN LimitOther2 ELSE 0 END) +
			--(CASE WHEN LimitContent2 IS NOT NULL THEN LimitContent2 ELSE 0 END) +
			--(CASE WHEN LimitTime2 IS NOT NULL THEN LimitTime2 ELSE 0 END)) LocationLimit
		,SUM(WSCV4DED +WSCV5DED +WSCV6DED +WSCV7DED+WSSITEDED) as LocationDeductible  --+DeductBldg2+DeductOther2+DeductContent2+DeductTime2) as LocationDeductible  
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Account a
Full Outer Join dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USHU_Full_Location b on a.ACCNTNUM = b.ACCNTNUM
where a.USERTXT2 = 'Clay'
Group by POLICYUSERTXT1
Order by 1

--US WF
Select concat('USWF_',Product_group) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(FRCV4VAL+FRCV5VAL+FRCV6VAL+FRCV7VAL) TotalReplacementValue
		,Sum(FRCV4LIMIT+FRCV5LIMIT+FRCV6LIMIT+FRCV7LIMIT) LocationLimit
		,SUM(FRCV4DED +FRCV5DED +FRCV6DED +FRCV7DED+FRSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Account a
Full Outer Join dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
and c.Product_Group <> 'Vol. HO (HIP)'
Group by Product_group

union all
Select concat('USWF_',a.ACCNTNAME) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(FRCV4VAL+FRCV5VAL+FRCV6VAL+FRCV7VAL) TotalReplacementValue
		,Sum(FRCV4LIMIT+FRCV5LIMIT+FRCV6LIMIT+FRCV7LIMIT) LocationLimit
		,SUM(FRCV4DED +FRCV5DED +FRCV6DED +FRCV7DED+FRSITEDED) as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Account a
Full Outer Join dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Location b on a.ACCNTNUM = b.ACCNTNUM
join [dbo].[Just_Product_Group_Roe_Power_BI] c on a.ACCNTNAME = c.product_group_roe
where a.USERTXT2 <> 'Clay'
and  c.Product_Group = 'Vol. HO (HIP)'
Group by a.ACCNTNAME

union all
Select concat('USWF_',POLICYUSERTXT1) AS ExposureGroup,'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Account' as AccountTable, Count(*) PolicyCount, SUM(BLANPREAMT) PolicyPremium, SUM(BLANLIMAMT) PolicyLimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Location' as LocationTable, Count(distinct b.ACCNTNUM) LocationCountDistinct
		,Sum(FRCV4VAL+FRCV5VAL+FRCV6VAL+FRCV7VAL) TotalReplacementValue
		,Sum(FRCV4LIMIT+FRCV5LIMIT+FRCV6LIMIT+FRCV7LIMIT) LocationLimit
		--,SUM(FRCV4DED +FRCV5DED +FRCV6DED +FRCV7DED+FRSITEDED) as LocationDeductible
		,case when Sum(FRCV4VAL+FRCV5VAL+FRCV6VAL+FRCV7VAL) <> 0 then SUM(FRCV4DED +FRCV5DED +FRCV6DED +FRCV7DED+FRSITEDED) end as LocationDeductible
From dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Account a
Full Outer Join dbo.modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USWF_Location b on a.ACCNTNUM = b.ACCNTNUM
where a.USERTXT2 = 'Clay'
Group by POLICYUSERTXT1
Order by 1

--USFL Commercial
SELECT 'USFL_Commercial' AS ExposureGroup,
'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Account' AS AccountTable,
(SELECT COUNT(DISTINCT ACCNTNUM) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Account) AS PolicyCount,
(SELECT SUM(BLANPREAMT) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Account) AS PolicyPremium,
'0' AS AttachmentPoint,
(SELECT SUM(BLANDEDAMT) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Account) AS PolicyDeductible,
(SELECT SUM(Blanlimamt + COV1LIMIT + COV2LIMIT) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Account) AS PolicyLimit,
(SELECT SUM(COND1LIMIT) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Account) AS PolicySublimit,
'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Location' AS LocationTable,
(SELECT COUNT(DISTINCT LOCNUM) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Location) AS LocationCountDistinct,
(SELECT COUNT(*) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Location) AS LocationCountCampus,
(SELECT SUM(WSCV4VAL + WSCV5VAL + WSCV6VAL + WSCV7VAL) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Location) AS TotalReplacementValue,
(SELECT SUM(WSCV4LIMIT + WSCV5LIMIT + WSCV6LIMIT + WSCV7LIMIT + WSCOMBINEDLIM) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Location) AS LocationLimit,
(SELECT SUM(WSCV4DED + WSCV5DED + WSCV6DED + WSCV7DED + WSSITEDED) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Commercial_Location) AS LocationDeductible

--USFL Excess
SELECT 'USFL_Excess' AS ExposureGroup,
'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Account' AS AccountTable,
(SELECT COUNT(DISTINCT ACCNTNUM) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Account) AS PolicyCount,
(SELECT SUM(BLANPREAMT) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Account) AS PolicyPremium,
'0' AS AttachmentPoint,
(SELECT SUM(BLANDEDAMT) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Account) AS PolicyDeductible,
(SELECT SUM(Blanlimamt) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Account) AS PolicyLimit,
'0' AS PolicySublimit,
'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Location' AS LocationTable,
(SELECT COUNT(DISTINCT LOCNUM) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Location) AS LocationCountDistinct,
(SELECT COUNT(*) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Location) AS LocationCountCampus,
(SELECT SUM(WSCV4VAL + WSCV5VAL + WSCV6VAL + WSCV7VAL) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Location) AS TotalReplacementValue,
(SELECT SUM(WSCV4LIMIT + WSCV5LIMIT + WSCV6LIMIT + WSCV7LIMIT) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Location) AS LocationLimit,
(SELECT SUM(WSCV4DED + WSCV6DED) FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Excess_Location) AS LocationDeductible

--USFL Other
Select CONCAT('USFL_Other_',a.USERTXT1) AS ExposureGroup, 'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account' as AccountTable,
		Count(*) PolicyCount, Sum(BLANPREAMT) PolicyPremium, SUM(UNDCOVAMT) AttachmentPoint, 
		SUM(BLANDEDAMT) PolicyDeductible, SUM(Blanlimamt) PolicyLimit, '0' AS PolicySublimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location' as LocationTable,
		Count(distinct LOCNUM) LocationCountDistinct, Count(*) LocationCountCampus, Sum(WSCV4VAL+WSCV5VAL+WSCV6VAL+WSCV7VAL) TotalReplacementValue,
		Sum(WSCV4LIMIT+WSCV5LIMIT+WSCV6LIMIT+WSCV7LIMIT) LocationLimit, Sum(WSCV4DED+WSCV5DED+WSCV6DED+WSCV7DED) LocationDeductible
FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account a
LEFT JOIN dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location b on a.ACCNTNUM = b.ACCNTNUM
WHERE LOBNAME = 'FLD Other'
GROUP BY a.USERTXT1
UNION ALL
Select CONCAT('USFL_Other_',a.USERTXT1) AS ExposureGroup, 'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account' as AccountTable,
		Count(*) PolicyCount, Sum(BLANPREAMT) PolicyPremium, SUM(UNDCOVAMT) AttachmentPoint,
		SUM(BLANDEDAMT) PolicyDeductible, SUM(Blanlimamt) PolicyLimit, '0' AS PolicySublimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location' as LocationTable,
		Count(distinct LOCNUM) LocationCountDistinct, Count(*) LocationCountCampus, Sum(WSCV4VAL+WSCV5VAL+WSCV6VAL+WSCV7VAL) TotalReplacementValue,
		Sum(WSCV4LIMIT+WSCV5LIMIT+WSCV6LIMIT+WSCV7LIMIT) LocationLimit, Sum(WSCV4DED+WSCV5DED+WSCV6DED+WSCV7DED) LocationDeductible
FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account a
LEFT JOIN dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location b on a.ACCNTNUM = b.ACCNTNUM
WHERE LOBNAME = 'FLD Other Clay'
GROUP BY a.USERTXT1
UNION ALL
Select 'USFL_Other_Other_CB' AS ExposureGroup, 'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account' as AccountTable,
		Count(*) PolicyCount, Sum(BLANPREAMT) PolicyPremium, SUM(UNDCOVAMT) AttachmentPoint, 
		SUM(BLANDEDAMT) PolicyDeductible, SUM(Blanlimamt) PolicyLimit, '0' AS PolicySublimit,
		'Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location' as LocationTable,
		Count(distinct LOCNUM) LocationCountDistinct, Count(*) LocationCountCampus, Sum(WSCV4VAL+WSCV5VAL+WSCV6VAL+WSCV7VAL) TotalReplacementValue,
		Sum(WSCV4LIMIT+WSCV5LIMIT+WSCV6LIMIT+WSCV7LIMIT) LocationLimit, Sum(WSCV4DED+WSCV5DED+WSCV6DED+WSCV7DED) LocationDeductible
FROM dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Account a
LEFT JOIN dbo.Modeling_{{ DATE_VALUE }}_Moodys_{{ CYCLE_TYPE }}_USFL_Other_Location b on a.ACCNTNUM = b.ACCNTNUM
WHERE LOBNAME = 'FLD Other CB'
GROUP BY a.USERTXT1
ORDER BY 1