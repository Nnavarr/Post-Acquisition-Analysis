/**
| Author: Noe Navarro
| Date: 11/4/2020
| Objectvie: Aggregate Forecasted Occupancy data for the new acquisitions set based off DEVTEST.dbo.Quarterly_Acquisitions_List
**/

DECLARE @max_month AS Datetime
SET @max_month = DATEADD(month, DATEDIFF(month, 0, GETDATE())-1, 0)

SELECT [ID]
      ,[Entity]
      ,[MEntity]
      ,[Start_Date]
      ,[FC_Date]
      ,[FC_Occ]
      ,[Metadata_ID]
      ,[Upload_Date]
  FROM [FINANALYSIS].[dbo].[Storage_Forecast]
  WHERE

	-- inner query for q acq center inclusion
	MEntity in (
		SELECT 
			DISTINCT(MEntity)
		FROM 
			DEVTEST.dbo.Quarterly_Acquisitions_List
			)
	AND Start_Date > @max_month