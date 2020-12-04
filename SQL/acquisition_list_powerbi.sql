/**
Author: Noe Navarro
Date: 11/9/2020
Objective: Create a dynamic query for quarterly acquisitions list. It will automatically pull with the max date set to last complete month.
	This filter we applied on the "Close of Escrow" field.

Update Log
----------
Version 0.1.0: Inception of life

**/

-- Extract max date for last month
DECLARE @max_date as datetime
SET @max_date = DATEADD(MONTH, DATEDIFF(MONTH, -1, GETDATE())-1, -1)

SELECT 
	* 
FROM [DEVTEST].[dbo].[Quarterly_Acquisitions_List]
WHERE [Close of Escrow] <= @max_date
ORDER BY [Close of Escrow] ASC
