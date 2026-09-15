CREATE PROCEDURE [dbo].[Update_ProductHistory]
AS
BEGIN

DECLARE @Insertdate DATE = GETDATE();
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
    ,MAX(modifiedDate) AS modifiedDate
    ,activeProvisioning
    ,isnull(ProduktID,-1)
    ,isnull(endDate,'9999-12-31')
    ,NySalg
    ,orderedDate
    ,orderedDateTime
    ,@Insertdate
    ,'9999-12-31'
    ,1
    ,ISNULL(PC_DiscountValue,0)
    ,ISNULL(PC_Sub_Price,0)
    ,isnull(MAX(BillDate),'9999-12-31') AS BillDate
    ,ISNULL(PC_Pricecategorycode,'')
    ,ISNULL(PC_Sub_discount_rule,'')
    ,ISNULL(PC_ProvisioningModel,'')
    ,ISNULL(PC_BillingPeriodUnit,'')
FROM Silver_lakehouse.dbo.Status_I_dag_ AS V
WHERE NOT EXISTS (SELECT 1 FROM dbo.ProductHistory H WHERE ISNULL(H.id,-1) = ISNULL(V.id,-1) AND ISNULL(H.accountKey,-1) = ISNULL(V.accountKey,-1) AND H.KundeNO = V.KundeNO AND ISNULL(H.ProduktID,-1) = ISNULL(V.ProduktID,-1) AND H.LastVersion = 1)
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
    ,activeProvisioning
    ,isnull(ProduktID,-1)
    ,isnull(endDate,'9999-12-31')
    ,NySalg
    ,orderedDate
    ,orderedDateTime
    ,ISNULL(PC_DiscountValue,0)
    ,ISNULL(PC_Sub_Price,0)
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
,isnull(V.KundeNO,'')
,isnull(V.accountKey,-1)
,V.id
,V.createdDate
,max(V.modifiedDate) as modifiedDate
,V.activeProvisioning
,isnull(V.ProduktID,-1)
,isnull(V.endDate,'9999-12-31')
,V.NySalg
,V.orderedDate
,V.orderedDateTime
,@Insertdate
,'9999-12-31'
,1
,ISNULL(V.PC_DiscountValue,0)
,ISNULL(V.PC_Sub_Price,0)
,isnull(max(V.BillDate),'9999-12-31')
,ISNULL(V.PC_Pricecategorycode,'')
,ISNULL(V.PC_Sub_discount_rule,'')
,ISNULL(V.PC_ProvisioningModel,'')
,ISNULL(V.PC_BillingPeriodUnit,'')
FROM Silver_lakehouse.dbo.Status_I_dag_ AS V
JOIN dbo.ProductHistory AS H ON H.id = ISNULL(V.id,-1) AND H.accountKey = ISNULL(V.accountKey,-1) AND H.KundeNO = V.KundeNO AND H.ProduktID = ISNULL(V.ProduktID,-1) AND V.startDate = H.startDate
   AND H.LastVersion = 1
   AND (
        H.hasBeenBilledOnce <> V.hasBeenBilledOnce OR
        H.Kunde_Status <> V.Kunde_Status OR
        ISNULL(H.Partner,'') <> ISNULL(V.Partner,'') OR
        H.Produkt_Status <> V.Produkt_Status OR
        H.activeProvisioning <> V.activeProvisioning OR
        ISNULL(H.endDate,'9999-12-31') <> ISNULL(V.endDate,'9999-12-31') OR
        H.orderedDateTime <> V.orderedDateTime OR        
        ISNULL(H.PC_DiscountValue,0) <> ISNULL(V.PC_DiscountValue,0) OR
        ISNULL(H.PC_Sub_Price,0) <> ISNULL(V.PC_Sub_Price,0) OR
        ISNULL(H.PC_Pricecategorycode,'') <> ISNULL(V.PC_Pricecategorycode,'') OR
        ISNULL(H.PC_Sub_discount_rule,'') <> ISNULL(V.PC_Sub_discount_rule,'') OR
        ISNULL(H.PC_ProvisioningModel,'') <> ISNULL(V.PC_ProvisioningModel,'') OR
        ISNULL(H.PC_BillingPeriodUnit,'') <> ISNULL(V.PC_BillingPeriodUnit,'')
   )
WHERE V.KundeNO IS NOT NULL
 GROUP BY V.hasBeenBilledOnce
    ,V.Kunde_Status
    ,V.startDate
    ,isnull(V.Partner,'')
    ,V.Produkt_Status
    ,isnull(V.KundeNO,'')
    ,isnull(V.accountKey,-1)
    ,V.id
    ,V.createdDate
    ,V.activeProvisioning
    ,isnull(V.ProduktID,-1)
    ,isnull(V.endDate,'9999-12-31')
    ,V.NySalg
    ,V.orderedDate
    ,V.orderedDateTime
    ,ISNULL(V.PC_DiscountValue,0)
    ,ISNULL(V.PC_Sub_Price,0)
    ,ISNULL(V.PC_Pricecategorycode,'')
    ,ISNULL(V.PC_Sub_discount_rule,'')
    ,ISNULL(V.PC_ProvisioningModel,'')
    ,ISNULL(V.PC_BillingPeriodUnit,'');

 
-- =====================================================================
-- Luk gamle records, hvis der er kommet en nyere aktiv version af samme række
-- (den med lavere Startdato bliver inaktiv og får Slutdato sat)
-- =====================================================================
UPDATE dbo.ProductHistory
SET
  NySalg  = 0
FROM dbo.ProductHistory AS T
WHERE LastVersion = 1
  AND EXISTS (
        SELECT 1
        FROM Silver_lakehouse.dbo.Status_I_dag_ AS S
        WHERE S.accountKey = T.accountKey AND S.KundeNO = T.KundeNO AND S.ProduktID = T.ProduktID AND S.id = T.id AND (T.NySalg = 1 AND S.NySalg = 0)
          AND T.LastVersion = 1
          AND S.Dato = @Insertdate
  )
  

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

-- Opdater produkt_status hvis den er i brug
  UPDATE dbo.ProductHistory
    SET Produkt_Status = 'I brug'
    WHERE startDate <= GETDATE() AND endDate >= GETDATE() 

END