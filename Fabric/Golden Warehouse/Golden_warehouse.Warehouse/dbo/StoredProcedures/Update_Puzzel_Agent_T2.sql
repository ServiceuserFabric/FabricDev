CREATE PROCEDURE [dbo].[Update_Puzzel_Agent_T2]
AS
BEGIN

DECLARE @Insertdate DATE = GETDATE();
-- =====================================================================
-- Indsæt ny historik-record for rækker, der ikke findes i historikken endnu
-- =====================================================================
INSERT INTO [puz].[agents_history] (
       [agent_id]
      ,[deleted]
      ,[FromDate]
      ,[ToDate]
      ,[LastVersion]
    )
  SELECT
    [agent_id]
    ,[deleted]
    ,@Insertdate
    ,'9999-12-31'
    ,1
FROM [Puzzel_Altibox].[dbo].[agents] AS V
WHERE NOT EXISTS (SELECT 1 FROM [puz].[agents_history] H WHERE V.agent_id = H.agent_id);


-- =====================================================================
-- Indsæt ny historik-record hvis et af felterne er ændret ift. aktiv record
-- (SCD Type 2 start: ny række med ny Startdato, gammel forbliver aktiv indtil vi lukker den nedenfor)
-- =====================================================================
INSERT INTO [puz].[agents_history] (
       [agent_id]
      ,[deleted]
      ,[FromDate]
      ,[ToDate]
      ,[LastVersion]
)
SELECT
       V.[agent_id]
      ,V.[deleted]
      ,@Insertdate
      ,'9999-12-31'
      ,1
FROM [Puzzel_Altibox].[dbo].[agents] AS V
JOIN [puz].[agents_history] AS H ON V.agent_id = H.agent_id
   AND H.LastVersion = 1
   AND H.deleted <> V.deleted;

-- =====================================================================
-- Luk gamle records, hvis der er kommet en nyere aktiv version af samme række
-- (den med lavere Startdato bliver inaktiv og får Slutdato sat)
-- =====================================================================
UPDATE [puz].[agents_history]
SET
  LastVersion  = 0,
  ToDate = @Insertdate
FROM [puz].[agents_history] AS T
WHERE LastVersion = 1
  AND EXISTS (
        SELECT 1
        FROM [puz].[agents_history] AS S
        WHERE S.agent_id = T.agent_id
          AND T.LastVersion = 1
          AND T.FromDate < S.FromDate
  )

END