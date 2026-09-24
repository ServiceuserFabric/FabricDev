CREATE TABLE [dbo].[ProductHistory] (
    [hasBeenBilledOnce]    BIT             NULL,
    [Kunde_Status]         VARCHAR (100)   NULL,
    [startDate]            DATE            NULL,
    [Partner]              VARCHAR (250)   NULL,
    [Produkt_Status]       VARCHAR (100)   NULL,
    [KundeNO]              VARCHAR (100)   NOT NULL,
    [accountKey]           BIGINT          NOT NULL,
    [id]                   BIGINT          NOT NULL,
    [createdDate]          DATE            NULL,
    [modifiedDate]         DATE            NULL,
    [activeProvisioning]   BIT             NULL,
    [ProduktID]            BIGINT          NOT NULL,
    [endDate]              DATE            NULL,
    [NySalg]               VARCHAR (200)   NULL,
    [orderedDate]          DATETIME2 (6)   NULL,
    [orderedDateTime]      DATETIME2 (6)   NULL,
    [FromDate]             DATE            NULL,
    [ToDate]               DATE            NULL,
    [LastVersion]          INT             NULL,
    [PC_DiscountValue]     DECIMAL (12, 2) NULL,
    [PC_Sub_Price]         DECIMAL (12, 2) NULL,
    [BillDate]             DATE            NULL,
    [PC_Pricecategorycode] VARCHAR (20)    NULL,
    [PC_Sub_discount_rule] VARCHAR (100)   NULL,
    [PC_ProvisioningModel] VARCHAR (100)   NULL,
    [PC_BillingPeriodUnit] VARCHAR (100)   NULL
);


GO

ALTER TABLE [dbo].[ProductHistory]
    ADD CONSTRAINT [PK_Productghistory] PRIMARY KEY NONCLUSTERED ([id] ASC, [accountKey] ASC, [ProduktID] ASC, [KundeNO] ASC) NOT ENFORCED;


GO