/**
| Author: Noe Navarro
| Date: 11/4/2020
| Objectvie: Aggregate Occupancy data for the new acquisitions set based off DEVTEST.dbo.Quarterly_Acquisitions_List
**/

-- Most recent complete Month
DECLARE @max_month AS Datetime
SET @max_month = DATEADD(month, DATEDIFF(month, 0, GETDATE())-1, 0)

/**
In the section above, the Datediff returns the number of months since time 0 (1900-01-01). After that, the DATEADD function adds these many months to the original date also set as 0
**/

SELECT 
	Date,
	MEntity,
	unm_product,
	SUM(unm_numunits) as unm_numunits,
	SUM(unm_occunits) as unm_occunits
FROM 
	[Storage].[dbo].[WSS_UnitMixUHI_Monthly_Archive]
WHERE 
	unm_product != 'UBOX' 

	-- inner query for q acq center inclusion
	AND MEntity in 
	(SELECT
		MEntity
		FROM 
			DEVTEST.dbo.Quarterly_Acquisitions_List
		WHERE 
			[Include?] = 1
	)

	AND Date <= @max_month

GROUP BY Date, MEntity, unm_product
ORDER BY Date Desc
