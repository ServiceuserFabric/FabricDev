CREATE PROCEDURE [dbo].[Update_Customer_Status]
AS
BEGIN

DECLARE @Insertdate DATE = GETDATE();
-- =====================================================================
-- Indsæt ny historik-record for rækker, der ikke findes i historikken endnu
-- =====================================================================
TRUNCATE TABLE [dbo].[Customer_Status];
INSERT INTO [dbo].[Customer_Status]
SELECT [Dato]
      ,[accountKey]
      ,[Status]
      ,[Pending_Churned]
  FROM [dbo].[Kunde_Status]

 END