CREATE PROCEDURE [dbo].[Fill_ProductHistory_from_status]
(
    @Insertdate DATE = '2025-01-01'
)
AS
BEGIN
--DECLARE @Insertdate DATE = '2025-01-01'
-- =====================================================================
-- Indsæt ny historik-record for rækker, der ikke findes i historikken endnu
-- =====================================================================
 INSERT INTO dbo.ProductHistory (
    hasBeenBilledOnce
    ,Kunde_Status
    ,startDate
    ,Partner
    ,Produkt_Status
    ,KundeNO
    ,accountKey
    ,id
    ,createdDate
    ,modifiedDate
    ,activeProvisioning
    ,ProduktID
    ,endDate
    ,NySalg
    ,orderedDate
    ,orderedDateTime
    ,FromDate
    ,ToDate
    ,LastVersion
    ,PC_DiscountValue
    ,PC_Sub_Price
    ,BillDate
    ,PC_Pricecategorycode
    ,PC_Sub_discount_rule
    ,PC_ProvisioningModel
    ,PC_BillingPeriodUnit
    )
  SELECT
    hasBeenBilledOnce
    ,Kunde_Status
    ,startDate
    ,ISNULL(Partner,'')
    ,Produkt_Status
    ,KundeNO
    ,isnull(accountKey,-1)
    ,id
    ,createdDate
    ,modifiedDate
    ,activeProvisioning
    ,isnull(ProduktID,-1)
    ,isnull(endDate,'9999-12-31')
    ,CASE 
      WHEN NySalg = 'true' THEN '1'
      WHEN NySalg = 'false' THEN '0'
      ELSE NySalg
    END
    ,orderedDate
    ,orderedDateTime
    ,@Insertdate
    ,'9999-12-31'
    ,1
    ,ISNULL(PC_DiscountValue,0)
    ,ISNULL(PC_Sub_Price,0)
    ,isnull(BillDate,'9999-12-31')
    ,ISNULL(PC_Pricecategorycode,'')
    ,ISNULL(PC_Sub_discount_rule,'')
    ,ISNULL(PC_ProvisioningModel,'')
    ,ISNULL(PC_BillingPeriodUnit,'')
FROM Silver_lakehouse.dbo.[status] AS V
WHERE V.Dato = @Insertdate AND
    NOT EXISTS (SELECT 1 FROM dbo.ProductHistory H WHERE ISNULL(H.id,-1) = ISNULL(V.id,-1) AND ISNULL(H.accountKey,-1) = ISNULL(V.accountKey,-1) AND H.KundeNO = V.KundeNO AND ISNULL(H.ProduktID,-1) = ISNULL(V.ProduktID,-1) AND H.LastVersion = 1)
    AND V.KundeNO IS NOT NULL
    GROUP BY hasBeenBilledOnce
    ,Kunde_Status
    ,startDate
    ,ISNULL(Partner,'')
    ,Produkt_Status
    ,KundeNO
    ,isnull(accountKey,-1)
    ,id
    ,createdDate
    ,modifiedDate
    ,activeProvisioning
    ,isnull(ProduktID,-1)
    ,isnull(endDate,'9999-12-31')
    ,CASE 
      WHEN NySalg = 'true' THEN '1'
      WHEN NySalg = 'false' THEN '0'
      ELSE NySalg
    END
    ,orderedDate
    ,orderedDateTime
    ,ISNULL(PC_DiscountValue,0)
    ,ISNULL(PC_Sub_Price,0)
    ,isnull(BillDate,'9999-12-31')
    ,ISNULL(PC_Pricecategorycode,'')
    ,ISNULL(PC_Sub_discount_rule,'')
    ,ISNULL(PC_ProvisioningModel,'')
    ,ISNULL(PC_BillingPeriodUnit,'');


-- =====================================================================
-- Indsæt ny historik-record hvis et af felterne er ændret ift. aktiv record
-- (SCD Type 2 start: ny række med ny Startdato, gammel forbliver aktiv indtil vi lukker den nedenfor)
-- =====================================================================
INSERT INTO dbo.ProductHistory (
hasBeenBilledOnce
,Kunde_Status
,startDate
,Partner
,Produkt_Status
,KundeNO
,accountKey
,id
,createdDate
,modifiedDate
,activeProvisioning
,ProduktID
,endDate
,NySalg
,orderedDate
,orderedDateTime
,FromDate
,ToDate
,LastVersion
,PC_DiscountValue
,PC_Sub_Price
,BillDate
,PC_Pricecategorycode
,PC_Sub_discount_rule
,PC_ProvisioningModel
,PC_BillingPeriodUnit
)
SELECT
V.hasBeenBilledOnce
,V.Kunde_Status
,V.startDate
,ISNULL(V.Partner,'')
,V.Produkt_Status
,V.KundeNO
,isnull(V.accountKey,-1)
,V.id
,V.createdDate
,V.modifiedDate
,V.activeProvisioning
,isnull(V.ProduktID,-1)
,isnull(V.endDate,'9999-12-31')
,CASE 
      WHEN V.NySalg = 'true' THEN '1'
      WHEN V.NySalg = 'false' THEN '0'
      ELSE V.NySalg
    END
,V.orderedDate
,V.orderedDateTime
,@Insertdate
,'9999-12-31'
,1
    ,ISNULL(V.PC_DiscountValue,0)
    ,ISNULL(V.PC_Sub_Price,0)
    ,isnull(V.BillDate,'9999-12-31')
    ,ISNULL(V.PC_Pricecategorycode,'')
    ,ISNULL(V.PC_Sub_discount_rule,'')
    ,ISNULL(V.PC_ProvisioningModel,'')
    ,ISNULL(V.PC_BillingPeriodUnit,'')
FROM Silver_lakehouse.dbo.[status] AS V
JOIN dbo.ProductHistory AS H ON H.id = ISNULL(V.id,-1) AND H.accountKey = ISNULL(V.accountKey,-1) AND H.KundeNO = V.KundeNO AND H.ProduktID = ISNULL(V.ProduktID,-1) AND V.Dato = @Insertdate AND V.Dato > H.FromDate
   AND H.LastVersion = 1 AND V.KundeNO IS NOT NULL
   AND (
        H.hasBeenBilledOnce <> V.hasBeenBilledOnce OR
        H.Kunde_Status <> V.Kunde_Status OR
        ISNULL(H.Partner,'') <> ISNULL(V.Partner,'') OR
        H.Produkt_Status <> V.Produkt_Status OR
        H.modifiedDate <> V.modifiedDate OR
        H.activeProvisioning <> V.activeProvisioning OR
        ISNULL(H.endDate,'9999-12-31') <> ISNULL(V.endDate,'9999-12-31') OR
        H.NySalg <> (CASE 
      WHEN V.NySalg = 'true' THEN '1'
      WHEN V.NySalg = 'false' THEN '0'
      ELSE V.NySalg
    END) OR
        H.orderedDateTime <> V.orderedDateTime OR        
        ISNULL(H.PC_DiscountValue,0) <> ISNULL(V.PC_DiscountValue,0) OR
        ISNULL(H.PC_Sub_Price,0) <> ISNULL(V.PC_Sub_Price,0) OR
        ISNULL(H.BillDate,'9999-12-31') <> ISNULL(V.BillDate,'9999-12-31') OR
        ISNULL(H.PC_Pricecategorycode,'') <> ISNULL(V.PC_Pricecategorycode,'') OR
        ISNULL(H.PC_Sub_discount_rule,'') <> ISNULL(V.PC_Sub_discount_rule,'') OR
        ISNULL(H.PC_ProvisioningModel,'') <> ISNULL(V.PC_ProvisioningModel,'') OR
        ISNULL(H.PC_BillingPeriodUnit,'') <> ISNULL(V.PC_BillingPeriodUnit,'')
   )
GROUP BY
     V.hasBeenBilledOnce
    ,V.Kunde_Status
    ,V.startDate
    ,isnull(V.Partner,'')
    ,V.Produkt_Status
    ,V.KundeNO
    ,isnull(V.accountKey,-1)
    ,V.id
    ,V.createdDate
    ,V.modifiedDate
    ,V.activeProvisioning
    ,isnull(V.ProduktID,-1)
    ,isnull(V.endDate,'9999-12-31')
    ,V.NySalg
    ,V.orderedDate
    ,V.orderedDateTime
    ,ISNULL(V.PC_DiscountValue,0)
    ,ISNULL(V.PC_Sub_Price,0)
    ,isnull(V.BillDate,'9999-12-31')
    ,ISNULL(V.PC_Pricecategorycode,'')
    ,ISNULL(V.PC_Sub_discount_rule,'')
    ,ISNULL(V.PC_ProvisioningModel,'')
    ,ISNULL(V.PC_BillingPeriodUnit,'');


-- =====================================================================
-- Sæt inaktiv, hvis rækken er fjernet. Går ikke da der kan være huller i datoleverancerne
-- =====================================================================
/*UPDATE dbo.ProductHistory 
SET
  LastVersion = 0,
  ToDate = @Insertdate
FROM dbo.ProductHistory AS H
WHERE LastVersion = 1
  AND NOT EXISTS (
        SELECT 1
        FROM Silver_lakehouse.dbo.[status] AS V
        WHERE H.id = isnull(V.id,-1) AND H.accountKey = isnull(V.accountKey,-1) AND H.KundeNO = isnull(V.KundeNO,'') AND H.ProduktID = isnull(V.ProduktID,-1) AND V.Dato = @Insertdate
  )
;
*/

-- =====================================================================
-- Luk gamle records, hvis der er kommet en nyere aktiv version af samme række
-- (den med lavere Startdato bliver inaktiv og får Slutdato sat)
-- =====================================================================
UPDATE dbo.ProductHistory
SET
  LastVersion  = 0,
  ToDate = @Insertdate
FROM dbo.ProductHistory AS T
WHERE LastVersion = 1
  AND EXISTS (
        SELECT 1
        FROM dbo.ProductHistory AS S
        WHERE S.accountKey = T.accountKey AND S.KundeNO = T.KundeNO AND S.ProduktID = T.ProduktID AND S.id = T.id
          AND T.LastVersion = 1 AND S.LastVersion = 1
          AND T.FromDate < S.FromDate
  )

END