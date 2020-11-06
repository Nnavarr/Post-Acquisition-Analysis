DECLARE @end_quarter_date VARCHAR(10)
SET @end_quarter_date = '2020-09-01'

-- Here, we must start the statement with a semicolon
;WITH temp_table AS (
	SELECT
		*,

		-- Set Quarter column 
		CASE 
			WHEN [Month] BETWEEN 1 AND 3 THEN 4
			WHEN [Month] BETWEEN 4 AND 6 THEN 1
			WHEN [Month] BETWEEN 7 AND 9 THEN 2
			WHEN [Month] BETWEEN 10 AND 12 THEN 3
		END as [Quarter],
		([value]/1000) as val_thousands

	FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_IS]
	)

/** Group by the various columns to export directly to Excel **/
SELECT
	[date], 
	line_item, 
	SUM(value) as [Value],
	fiscal_year,
	fiscal_month,
	Quarter,
	month,
	grp_name,
	grp_num,
	SUM(val_thousands) as val_thousands
FROM temp_table
GROUP BY date, line_item, fiscal_year, fiscal_month, Quarter, month, grp_name, grp_num
ORDER BY date ASC
