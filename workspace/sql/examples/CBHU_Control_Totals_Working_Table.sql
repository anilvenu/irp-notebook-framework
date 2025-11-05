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