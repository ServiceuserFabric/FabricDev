-- =============================================
-- Author:      Mathias Liedtke
-- Create Date: 10/03/2026
-- Description: Remove duplicates and update log of Nuuday
-- =============================================
CREATE [dbo].[Update_AltiboxMobil_Nuuday]
AS
BEGIN

DECLARE @Insertdate DATE = GETDATE()

-- 
WITH AltiboxMobil_Nuuday AS (
    SELECT
        S.*,
        rn = ROW_NUMBER() OVER (
            PARTITION BY
                S.[TCM Kunde ID],
                S.[Produkt ID],
                S.[Mobil nummer],
                S.[Bestillingstid]
        )
    FROM dbo.AltiboxMobil_Nuuday AS S
),
Dedup AS (
    SELECT *
    FROM AltiboxMobil_Nuuday
    WHERE rn = 1
)
--


-- =====================================================================
-- Indsæt ny historik-record for rækker, der ikke findes i historikken endnu
-- =====================================================================
INSERT INTO dbo.AltiboxMobil_Nuuday (
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
FROM Dedup AS US -- As UniqueSource
WHERE NOT EXISTS (SELECT 1 FROM dbo.AltiboxMobil_Nuuday D WHERE ISNULL(D.[TCM Kunde ID],-1) = ISNULL(US.[TCM Kunde ID],-1) 
        AND ISNULL(D.[Produkt ID],-1) = ISNULL(US.[Produkt ID],-1) AND ISNULL(D.[Mobil nummer],'') = ISNULL(US.[Mobil nummer],'') AND ISNULL(D.[Bestillingstid],-1) = ISNULL(US.[Bestillingstid],-1) AND US.LastVersion = 1);


-- =====================================================================
-- Indsæt ny historik-record hvis et af felterne er ændret ift. aktiv record
-- (SCD Type 2 start: ny række med ny Startdato, gammel forbliver aktiv indtil vi lukker den nedenfor)
-- =====================================================================
INSERT INTO dbo.AltiboxMobil_Nuuday (
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
      US.@Insertdate
      ,US.'9999-12-31'
      ,US.1
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
JOIN dbo.AltiboxMobil_Nuuday AS D ON ISNULL(D.[TCM Kunde ID],-1) = ISNULL(US.[TCM Kunde ID],-1) 
        AND ISNULL(D.[Produkt ID],-1) = ISNULL(US.[Produkt ID],-1) AND ISNULL(D.[Mobil nummer],'') = ISNULL(US.[Mobil nummer],'') AND ISNULL(D.[Bestillingstid],-1) = ISNULL(US.[Bestillingstid],-1) AND US.LastVersion = 1)
   AND (
        US.[Bevægelse] <> D.[Bevægelse] OR
        US.[Status] <> D.[Status] OR
        US.[Sælgerreference] <> D.[Sælgerreference] OR
        US.[Registrering af opsigelsesdato] <> D.[Registrering af opsigelsesdato] OR
        US.[Kommentar til opsigelse] <> D.[Kommentar til opsigelse] OR
        US.[Eksport til] <> D.[Eksport til]
   )
 GROUP BY 
      US.[TCM Kunde ID]
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
      ,US.[Bundles på købstidspunkt];

-- =====================================================================
-- Luk gamle records, hvis der er kommet en nyere aktiv version af samme række
-- (den med lavere Startdato bliver inaktiv og får Slutdato sat)
-- =====================================================================
UPDATE dbo.AltiboxMobil_Nuuday
SET
  LastVersion  = 0,
  ToDate = @Insertdate
FROM dbo.AltiboxMobil_Nuuday AS D -- As Destination
WHERE LastVersion = 1
  AND EXISTS (
        SELECT 1
        FROM dbo.AltiboxMobil_Nuuday AS DS -- As DestinationSource
        WHERE DS.[TCM Kunde ID] = D.[TCM Kunde ID] AND DS.[Produkt ID] = D.[Produkt ID] AND DS.[Mobil nummer] = D.[Mobil nummer] AND DS.[Bestillingstid] = D.[Bestillingstid]
          AND D.LastVersion = 1 AND DS.LastVersion = 1
          AND D.FromDate < DS.FromDate
  )

END;

