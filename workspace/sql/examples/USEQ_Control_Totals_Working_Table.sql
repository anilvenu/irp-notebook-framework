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
From dbo.CombinedData_{{ DATE_VALUE }}_Working a
join [dbo].[Just_Product_Group_Roe_Power_BI] b on a.product_group_roe = b.product_group_roe
Where State IN ('PR','VI')
and EQModeled = 'Y'
Group by Product_group
Order by 1