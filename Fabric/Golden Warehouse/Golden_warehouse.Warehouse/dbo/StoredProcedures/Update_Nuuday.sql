CREATE PROCEDURE [dbo].[Update_Nuuday]
AS
BEGIN
    TRUNCATE TABLE [nuu].[AltiboxMobil_Nuuday];

    INSERT INTO [nuu].[AltiboxMobil_Nuuday]
    SELECT cast([TCM Kunde ID] as bigint)
      ,[Bevægelse]
      ,[Status]
      ,[Produkt Type]
      ,CAST([Produkt ID] AS INT)
      ,CAST([Abonnements ID] AS INT)
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
      ,CAST([Bestillingstid] AS DATETIME2(6))
      ,CAST([Aktiveringsdato] AS DATE)
      ,[Afregningstype]
      ,[Automatisk optankning]
      ,[Registrering af opsigelsesdato]
      ,CAST([Opsigelsesdato] AS DATE)
      ,[Kommentar til opsigelse]
      ,CAST([Udporteringsdato] AS DATE)
      ,[Eksport til]
      ,[Tilvalg]
      ,CAST([Bindingsperiode slutter] AS DATE)
      ,[Bundles på købstidspunkt]
      ,CAST([Bestillingstid] AS DATE)
  FROM [NuudayMobil].[dbo].[AltiboxMobil_Nuuday]

END