CREATE TABLE [dbo].[CustomerStatusHistory] (
    [accountKey]      BIGINT      NOT NULL,
    [Status]          VARCHAR (9) NOT NULL,
    [Pending_Churned] INT         NULL,
    [FromDate]        DATE        NULL,
    [ToDate]          DATE        NULL,
    [LastVersion]     INT         NULL
);


GO