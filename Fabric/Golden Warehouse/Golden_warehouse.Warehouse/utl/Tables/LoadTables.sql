CREATE TABLE [utl].[LoadTables] (

	[TableName] varchar(100) NULL, 
	[SQLQuery] varchar(max) NULL, 
	[IncFull] int NULL, 
	[LoadFrequency] int NULL, 
	[Active] int NULL, 
	[ToSchema] varchar(50) NULL
);