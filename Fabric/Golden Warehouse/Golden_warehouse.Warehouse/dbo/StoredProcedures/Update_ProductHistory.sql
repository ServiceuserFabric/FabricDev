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
    ,Partner
    ,Produkt_Status
    ,isnull(KundeNO,'')
    ,isnull(accountKey,-1)
    ,id
    ,createdDate
    ,modifiedDate
    ,activeProvisioning
    ,isnull(ProduktID,-1)
    ,endDate
    ,NySalg
    ,orderedDate
    ,orderedDateTime
    ,@Insertdate
    ,'9999-12-31'
    ,1
    ,PC_DiscountValue
    ,PC_Sub_Price
    ,BillDate
    ,PC_Pricecategorycode
    ,PC_Sub_discount_rule
    ,PC_ProvisioningModel
    ,PC_BillingPeriodUnit
FROM Silver_lakehouse.dbo.Status_I_dag_ AS V
WHERE NOT EXISTS (SELECT 1 FROM dbo.ProductHistory H WHERE ISNULL(H.id,-1) = ISNULL(V.id,-1) AND ISNULL(H.accountKey,-1) = ISNULL(V.accountKey,-1) AND ISNULL(H.KundeNO,'') = ISNULL(V.KundeNO,'') AND ISNULL(H.ProduktID,-1) = ISNULL(V.ProduktID,-1) AND H.LastVersion = 1);


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
,V.Partner
,V.Produkt_Status
,isnull(V.KundeNO,'')
,isnull(V.accountKey,-1)
,V.id
,V.createdDate
,V.modifiedDate
,V.activeProvisioning
,isnull(V.ProduktID,-1)
,V.endDate
,V.NySalg
,V.orderedDate
,V.orderedDateTime
,@Insertdate
,'9999-12-31'
,1
,V.PC_DiscountValue
,V.PC_Sub_Price
,V.BillDate
,V.PC_Pricecategorycode
,V.PC_Sub_discount_rule
,V.PC_ProvisioningModel
,V.PC_BillingPeriodUnit
FROM Silver_lakehouse.dbo.Status_I_dag_ AS V
JOIN dbo.ProductHistory AS H ON H.id = ISNULL(V.id,-1) AND H.accountKey = ISNULL(V.accountKey,-1) AND H.KundeNO = ISNULL(V.KundeNO,'') AND H.ProduktID = ISNULL(V.ProduktID,-1) AND V.startDate = H.startDate
   AND H.LastVersion = 1
   AND (
        H.hasBeenBilledOnce <> V.hasBeenBilledOnce OR
        H.Kunde_Status <> V.Kunde_Status OR
        H.Partner <> V.Partner OR
        H.Produkt_Status <> V.Produkt_Status OR
        H.modifiedDate <> V.modifiedDate OR
        H.activeProvisioning <> V.activeProvisioning OR
        H.startDate <> V.startDate OR
        H.endDate <> V.endDate OR
        H.NySalg <> V.NySalg OR
        H.orderedDateTime <> V.orderedDateTime OR        
        H.PC_DiscountValue <> V.PC_DiscountValue OR
        H.PC_Sub_Price <> V.PC_Sub_Price OR
        H.BillDate <> V.BillDate OR
        H.PC_Pricecategorycode <> V.PC_Pricecategorycode OR
        H.PC_Sub_discount_rule <> V.PC_Sub_discount_rule OR
        H.PC_ProvisioningModel <> V.PC_ProvisioningModel OR
        H.PC_BillingPeriodUnit <> V.PC_BillingPeriodUnit
   )
 GROUP BY 
 V.hasBeenBilledOnce
,V.Kunde_Status
,V.startDate
,V.Partner
,V.Produkt_Status
,isnull(V.KundeNO,'')
,isnull(V.accountKey,-1)
,V.id
,V.createdDate
,V.modifiedDate
,V.activeProvisioning
,isnull(V.ProduktID,-1)
,V.endDate
,V.NySalg
,V.orderedDate
,V.orderedDateTime
,V.PC_DiscountValue
,V.PC_Sub_Price
,V.BillDate
,V.PC_Pricecategorycode
,V.PC_Sub_discount_rule
,V.PC_ProvisioningModel
,V.PC_BillingPeriodUnit
 ;

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