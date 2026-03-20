-- =============================================
-- Author:      Mathias Liedtke
-- Create Date: 10/03/2026
-- Description: Remove duplicates and update log of Nuuday
-- =============================================
CREATE PROCEDURE [dbo].[Update_AltiboxMobil_Nuuday]
(
    @ParameterDate VARCHAR(20) = '01-01-1970'
)
AS
BEGIN
--DECLARE @ParameterDate varchar(20)
 -- set @ParameterDate = '13-02-2026'
  DECLARE @InsertDate DATE = GETDATE();
  SET @InsertDate = IIF(@ParameterDate = '1970-01-01',@InsertDate,CONVERT(date, @ParameterDate, 105));

-- 
WITH AltiboxMobil_Nuuday AS (
    SELECT
       CAST([TCM Kunde ID] AS BIGINT) AS [TCM Kunde ID]
      ,[Bevægelse]
      ,[Status]
      ,[Produkt Type]
      ,CAST([Produkt ID] AS INT) AS [Produkt ID]
      ,CAST([Abonnements ID] AS INT) AS [Abonnements ID]
      ,[Eksternt abonnnementsnavn]
      ,[Internt abonnnementsnavn]
      ,[Familieabonnement til produkt-id]
      ,[Mobil nummer]
      ,[E-Mail]
      ,LTRIM([Sælgerreference]) AS [Sælgerreference]
      ,[Tidligere udbyder]
      ,[Kunde Navn]
      ,[Kunde Adresse]
      ,[Kunde Postnummer]
      ,[Kunde By]
      ,[Permission]
      ,[Bestillingstid]
      ,CONVERT(date, [Aktiveringsdato], 105) AS [Aktiveringsdato]
      ,[Afregningstype]
      ,[Automatisk optankning]
      ,[Registrering af opsigelsesdato]
      ,CONVERT(date, [Opsigelsesdato], 105) AS [Opsigelsesdato]
      ,[Kommentar til opsigelse]
      ,CONVERT(date, [Udporteringsdato], 105) AS [Udporteringsdato]
      ,[Eksport til]
      ,[Tilvalg]
      ,CONVERT(date, [Bindingsperiode slutter], 105) AS [Bindingsperiode slutter]
      ,[Bundles på købstidspunkt],
        rn = ROW_NUMBER() OVER (
            PARTITION BY
                S.[TCM Kunde ID],
                S.[Produkt ID],
                S.[Mobil nummer],
                S.[Bestillingstid]
            ORDER BY S.[TCM Kunde ID],
                S.[Produkt ID],
                S.[Mobil nummer],
                S.[Bestillingstid]
        )
    FROM [NuudayMobil].dbo.AltiboxMobil_Nuuday AS S
    WHERE S.[Mobil nummer] IS NOT NULL
),
Dedup AS (
    SELECT *
    FROM AltiboxMobil_Nuuday
    WHERE rn = 1
)
--


-- =====================================================================
-- Merge ind i tabel
-- =====================================================================
MERGE nuu.[AltiboxMobil_Nuuday] AS t USING Dedup AS s
ON (t.[TCM Kunde ID] = s.[TCM Kunde ID] AND t.[Produkt ID] = s.[Produkt ID] AND t.[Mobil nummer] = s.[Mobil nummer] AND t.[Bestillingstid] = s.[Bestillingstid] AND t.LastVersion = 1)
WHEN MATCHED AND
([t].[Bevægelse] <> [s].[Bevægelse] AND [s].[Bevægelse] IS NOT NULL) OR
([t].[Produkt Type] <> [s].[Produkt Type] AND [s].[Produkt Type] IS NOT NULL) OR
([t].[Abonnements ID] <> s.[Abonnements ID] AND s.[Abonnements ID] IS NOT NULL) OR
([t].[Eksternt abonnnementsnavn] <> [s].[Eksternt abonnnementsnavn] AND [s].[Eksternt abonnnementsnavn] IS NOT NULL) OR
([t].[Internt abonnnementsnavn] <> [s].[Internt abonnnementsnavn] AND [s].[Internt abonnnementsnavn] IS NOT NULL) OR
([t].[Familieabonnement til produkt-id] <> [s].[Familieabonnement til produkt-id] AND [s].[Familieabonnement til produkt-id] IS NOT NULL) OR
([t].[E-Mail] <> [s].[E-Mail] AND [s].[E-Mail] IS NOT NULL) OR
([t].[Sælgerreference] <> [s].[Sælgerreference] AND [s].[Sælgerreference] IS NOT NULL) OR
([t].[Tidligere udbyder] <> [s].[Tidligere udbyder] AND [s].[Tidligere udbyder] IS NOT NULL) OR
([t].[Kunde Navn] <> [s].[Kunde Navn] AND [s].[Kunde Navn] IS NOT NULL) OR
([t].[Kunde Adresse] <> [s].[Kunde Adresse] AND [s].[Kunde Adresse] IS NOT NULL) OR
([t].[Kunde Postnummer] <> [s].[Kunde Postnummer] AND [s].[Kunde Postnummer] IS NOT NULL) OR
([t].[Kunde By] <> [s].[Kunde By] AND [s].[Kunde By] IS NOT NULL) OR
([t].[Permission] <> [s].[Permission] AND [s].[Permission] IS NOT NULL) OR
([t].[Aktiveringsdato] <> s.[Aktiveringsdato] AND s.[Aktiveringsdato] IS NOT NULL) OR
([t].[Afregningstype] <> [s].[Afregningstype] AND [s].[Afregningstype] IS NOT NULL) OR
([t].[Automatisk optankning] <> [s].[Automatisk optankning] AND [s].[Automatisk optankning] IS NOT NULL) OR
([t].[Registrering af opsigelsesdato] <> [s].[Registrering af opsigelsesdato] AND [s].[Registrering af opsigelsesdato] IS NOT NULL) OR
([t].[Opsigelsesdato] <> [s].[Opsigelsesdato] AND [s].[Opsigelsesdato] IS NOT NULL) OR
([t].[Kommentar til opsigelse] <> [s].[Kommentar til opsigelse] AND [s].[Kommentar til opsigelse] IS NOT NULL) OR
([t].[Udporteringsdato] <> [s].[Udporteringsdato] AND [s].[Udporteringsdato] IS NOT NULL) OR
([t].[Eksport til] <> [s].[Eksport til] AND [s].[Eksport til] IS NOT NULL) OR
([t].[Tilvalg] <> [s].[Tilvalg] AND [s].[Tilvalg] IS NOT NULL) OR
([t].[Bindingsperiode slutter] <> [s].[Bindingsperiode slutter] AND [s].[Bindingsperiode slutter] IS NOT NULL) OR
([t].[Bundles på købstidspunkt] <> [s].[Bundles på købstidspunkt] AND [s].[Bundles på købstidspunkt] IS NOT NULL)
THEN UPDATE SET
[Bevægelse] = s.[Bevægelse],
[Produkt Type] = s.[Produkt Type],
[Abonnements ID] = s.[Abonnements ID],
[Eksternt abonnnementsnavn] = s.[Eksternt abonnnementsnavn],
[Internt abonnnementsnavn] = s.[Internt abonnnementsnavn],
[Familieabonnement til produkt-id] = s.[Familieabonnement til produkt-id],
[E-Mail] = s.[E-Mail],
[Sælgerreference] = s.[Sælgerreference],
[Tidligere udbyder] = s.[Tidligere udbyder],
[Kunde Navn] = s.[Kunde Navn],
[Kunde Adresse] = s.[Kunde Adresse],
[Kunde Postnummer] = s.[Kunde Postnummer],
[Kunde By] = s.[Kunde By],
[Permission] = s.[Permission],
[Aktiveringsdato] = s.[Aktiveringsdato],
[Afregningstype] = s.[Afregningstype],
[Automatisk optankning] = s.[Automatisk optankning],
[Registrering af opsigelsesdato] = s.[Registrering af opsigelsesdato],
[Opsigelsesdato] = s.[Opsigelsesdato],
[Kommentar til opsigelse] = s.[Kommentar til opsigelse],
[Udporteringsdato] = s.[Udporteringsdato],
[Eksport til] = s.[Eksport til],
[Tilvalg] = s.[Tilvalg],
[Bindingsperiode slutter] = s.[Bindingsperiode slutter],
[Bundles på købstidspunkt] = s.[Bundles på købstidspunkt]
WHEN NOT MATCHED THEN INSERT (
[TCM Kunde ID],
[Bevægelse],
[Status],
[Produkt Type],
[Produkt ID],
[Abonnements ID],
[Eksternt abonnnementsnavn],
[Internt abonnnementsnavn],
[Familieabonnement til produkt-id],
[Mobil nummer],
[E-Mail],
[Sælgerreference],
[Tidligere udbyder],
[Kunde Navn],
[Kunde Adresse],
[Kunde Postnummer],
[Kunde By],
[Permission],
[Bestillingstid],
[Aktiveringsdato],
[Afregningstype],
[Automatisk optankning],
[Registrering af opsigelsesdato],
[Opsigelsesdato],
[Kommentar til opsigelse],
[Udporteringsdato],
[Eksport til],
[Tilvalg],
[Bindingsperiode slutter],
[Bundles på købstidspunkt],
[Bestillingsdato],
[FromDate],
[ToDate],
[LastVersion])
VALUES (
s.[TCM Kunde ID],
s.[Bevægelse],
s.[Status],
s.[Produkt Type],
s.[Produkt ID],
s.[Abonnements ID],
s.[Eksternt abonnnementsnavn],
s.[Internt abonnnementsnavn],
s.[Familieabonnement til produkt-id],
s.[Mobil nummer],
s.[E-Mail],
s.[Sælgerreference],
s.[Tidligere udbyder],
s.[Kunde Navn],
s.[Kunde Adresse],
s.[Kunde Postnummer],
s.[Kunde By],
s.[Permission],
s.[Bestillingstid],
s.[Aktiveringsdato],
s.[Afregningstype],
s.[Automatisk optankning],
s.[Registrering af opsigelsesdato],
s.[Opsigelsesdato],
s.[Kommentar til opsigelse],
s.[Udporteringsdato],
s.[Eksport til],
s.[Tilvalg],
s.[Bindingsperiode slutter],
s.[Bundles på købstidspunkt],
CONVERT(date, s.[Bestillingstid], 105),
@InsertDate,
'9999-12-31',
1);

--
-- =====================================================================
-- Indsæt ny historik-record hvis et af felterne er ændret ift. aktiv record
-- (SCD Type 2 start: ny række med ny Startdato, gammel forbliver aktiv indtil vi lukker den nedenfor)
-- =====================================================================
WITH AltiboxMobil_Nuuday AS (
    SELECT
       CAST([TCM Kunde ID] AS BIGINT) AS [TCM Kunde ID]
      ,[Bevægelse]
      ,[Status]
      ,[Produkt Type]
      ,CAST([Produkt ID] AS INT) AS [Produkt ID]
      ,CAST([Abonnements ID] AS INT) AS [Abonnements ID]
      ,[Eksternt abonnnementsnavn]
      ,[Internt abonnnementsnavn]
      ,[Familieabonnement til produkt-id]
      ,[Mobil nummer]
      ,[E-Mail]
      ,[Sælgerreference]
      ,[Tidligere udbyder]
      ,[Kunde Navn]
      ,[Kunde Adresse]
      ,[Kunde Postnummer]
      ,[Kunde By]
      ,[Permission]
      ,[Bestillingstid]
      ,CONVERT(date, [Aktiveringsdato], 105) AS [Aktiveringsdato]
      ,[Afregningstype]
      ,[Automatisk optankning]
      ,[Registrering af opsigelsesdato]
      ,CONVERT(date, [Opsigelsesdato], 105) AS [Opsigelsesdato]
      ,[Kommentar til opsigelse]
      ,CONVERT(date, [Udporteringsdato], 105) AS [Udporteringsdato]
      ,[Eksport til]
      ,[Tilvalg]
      ,CONVERT(date, [Bindingsperiode slutter], 105) AS [Bindingsperiode slutter]
      ,[Bundles på købstidspunkt],
        rn = ROW_NUMBER() OVER (
            PARTITION BY
                S.[TCM Kunde ID],
                S.[Produkt ID],
                S.[Mobil nummer],
                S.[Bestillingstid]
            ORDER BY S.[TCM Kunde ID],
                S.[Produkt ID],
                S.[Mobil nummer],
                S.[Bestillingstid]
        )
    FROM [NuudayMobil].dbo.AltiboxMobil_Nuuday AS S
    WHERE S.[Mobil nummer] IS NOT NULL
),
Dedup AS (
    SELECT *
    FROM AltiboxMobil_Nuuday
    WHERE rn = 1
)
INSERT INTO nuu.AltiboxMobil_Nuuday (
       [FromDate] 
      ,[ToDate]
      ,[LastVersion]
      ,[TCM Kunde ID]
      ,[Bevægelse]
      ,[Status]
      ,[Produkt Type]
      ,[Produkt ID]
      ,[Abonnements ID]
      ,[Eksternt abonnnementsnavn]
      ,[Internt abonnnementsnavn]
      ,[Familieabonnement til produkt-id]
      ,[Mobil nummer]
      ,[E-Mail]
      ,[Sælgerreference]
      ,[Tidligere udbyder]
      ,[Kunde Navn]
      ,[Kunde Adresse]
      ,[Kunde Postnummer]
      ,[Kunde By]
      ,[Permission]
      ,[Bestillingstid]
      ,[Aktiveringsdato]
      ,[Afregningstype]
      ,[Automatisk optankning]
      ,[Registrering af opsigelsesdato]
      ,[Opsigelsesdato]
      ,[Kommentar til opsigelse]
      ,[Udporteringsdato]
      ,[Eksport til]
      ,[Tilvalg]
      ,[Bindingsperiode slutter]
      ,[Bundles på købstidspunkt]
)
SELECT
      @Insertdate
      ,'9999-12-31'
      ,1
      ,US.[TCM Kunde ID]
      ,US.[Bevægelse]
      ,US.[Status]
      ,US.[Produkt Type]
      ,US.[Produkt ID]
      ,US.[Abonnements ID]
      ,US.[Eksternt abonnnementsnavn]
      ,US.[Internt abonnnementsnavn]
      ,US.[Familieabonnement til produkt-id]
      ,US.[Mobil nummer]
      ,US.[E-Mail]
      ,US.[Sælgerreference]
      ,US.[Tidligere udbyder]
      ,US.[Kunde Navn]
      ,US.[Kunde Adresse]
      ,US.[Kunde Postnummer]
      ,US.[Kunde By]
      ,US.[Permission]
      ,US.[Bestillingstid]
      ,US.[Aktiveringsdato]
      ,US.[Afregningstype]
      ,US.[Automatisk optankning]
      ,US.[Registrering af opsigelsesdato]
      ,US.[Opsigelsesdato]
      ,US.[Kommentar til opsigelse]
      ,US.[Udporteringsdato]
      ,US.[Eksport til]
      ,US.[Tilvalg]
      ,US.[Bindingsperiode slutter]
      ,US.[Bundles på købstidspunkt]
FROM Dedup AS US
JOIN nuu.AltiboxMobil_Nuuday AS D ON ISNULL(D.[TCM Kunde ID],-1) = ISNULL(US.[TCM Kunde ID],-1) 
        AND ISNULL(D.[Produkt ID],-1) = ISNULL(US.[Produkt ID],-1) AND ISNULL(D.[Mobil nummer],'') = ISNULL(US.[Mobil nummer],'') AND ISNULL(D.[Bestillingstid],-1) = ISNULL(US.[Bestillingstid],-1) AND D.LastVersion = 1
   AND US.[Status] <> D.[Status] 
;

-- =====================================================================
-- Luk gamle records, hvis der er kommet en nyere aktiv version af samme række
-- (den med lavere Startdato bliver inaktiv og får Slutdato sat)
-- =====================================================================
UPDATE nuu.AltiboxMobil_Nuuday
SET
  LastVersion  = 0,
  ToDate = @Insertdate
FROM nuu.AltiboxMobil_Nuuday AS D -- As Destination
WHERE LastVersion = 1
  AND EXISTS (
        SELECT 1
        FROM nuu.AltiboxMobil_Nuuday AS DS -- As DestinationSource
        WHERE DS.[TCM Kunde ID] = D.[TCM Kunde ID] AND DS.[Produkt ID] = D.[Produkt ID] AND DS.[Mobil nummer] = D.[Mobil nummer] AND DS.[Bestillingstid] = D.[Bestillingstid]
          AND D.LastVersion = 1 AND DS.LastVersion = 1
          AND D.FromDate < DS.FromDate
  )

END;