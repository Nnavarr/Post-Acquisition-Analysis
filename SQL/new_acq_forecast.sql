/****** Script for SelectTopNRows command from SSMS  ******/
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