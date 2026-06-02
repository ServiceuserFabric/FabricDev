CREATE TABLE [dbo].[CustomerStatusHistory] (

	[accountKey] bigint NOT NULL, 
	[Status] varchar(9) NOT NULL, 
	[Pending_Churned] int NULL, 
	[FromDate] date NULL, 
	[ToDate] date NULL, 
	[LastVersion] int NULL
);