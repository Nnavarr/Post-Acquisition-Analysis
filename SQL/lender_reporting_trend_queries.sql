-- check max date for 'Lender_Financing_Ubox'
SELECT 
	MAX([Date])
FROM [SAP_Data].[dbo].[Lender_Financing_Ubox] 
-- Max date = 2020-03-01


-- Check 'Lender_Financing_Adjustments'
SELECT
	MAX([Date])
FROM [SAP_Data].[dbo].[Lender_Financing_Adjustments]
-- Max Date = 2020-03-01


-- check max date for 'Lender_Financing_Trends'
SELECT
	MAX([Date])
FROM 
	[SAP_Data].[dbo].[Lender_Financing_Trends]
-- Max Date = 2020-03-01

/** From the tables above, we can see that the lender financing trends and adjustments have not been made since last quarter
This means we will need to update the data prior to sendig the report to Jason Berg **/


