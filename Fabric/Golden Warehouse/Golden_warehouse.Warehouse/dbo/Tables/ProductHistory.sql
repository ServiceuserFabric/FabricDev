CREATE TABLE [dbo].[ProductHistory] (

	[hasBeenBilledOnce] bit NULL, 
	[Kunde_Status] varchar(100) NULL, 
	[startDate] date NULL, 
	[Partner] varchar(250) NULL, 
	[Produkt_Status] varchar(100) NULL, 
	[KundeNO] varchar(100) NOT NULL, 
	[accountKey] bigint NOT NULL, 
	[id] bigint NOT NULL, 
	[createdDate] date NULL, 
	[modifiedDate] date NULL, 
	[activeProvisioning] bit NULL, 
	[ProduktID] bigint NOT NULL, 
	[endDate] date NULL, 
	[NySalg] varchar(200) NULL, 
	[orderedDate] datetime2(6) NULL, 
	[orderedDateTime] datetime2(6) NULL, 
	[FromDate] date NULL, 
	[ToDate] date NULL, 
	[LastVersion] int NULL, 
	[PC_DiscountValue] decimal(12,2) NULL, 
	[PC_Sub_Price] decimal(12,2) NULL, 
	[BillDate] date NULL, 
	[PC_Pricecategorycode] varchar(20) NULL, 
	[PC_Sub_discount_rule] varchar(100) NULL, 
	[PC_ProvisioningModel] varchar(100) NULL, 
	[PC_BillingPeriodUnit] varchar(100) NULL
);


GO
ALTER TABLE [dbo].[ProductHistory] ADD CONSTRAINT PK_Productghistory primary key NONCLUSTERED ([id], [accountKey], [ProduktID], [KundeNO]);