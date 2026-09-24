CREATE TABLE [utl].[LoadTables] (
    [TableName]     VARCHAR (100) NULL,
    [SQLQuery]      VARCHAR (MAX) NULL,
    [IncFull]       INT           NULL,
    [LoadFrequency] INT           NULL,
    [Active]        INT           NULL,
    [ToSchema]      VARCHAR (50)  NULL
);


GO