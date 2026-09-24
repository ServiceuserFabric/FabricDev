CREATE TABLE [puz].[agents] (
    [agent_id]            INT            NOT NULL,
    [customer_key]        VARCHAR (8000) NULL,
    [user_name]           VARCHAR (8000) NULL,
    [user_num]            VARCHAR (8000) NULL,
    [full_name]           VARCHAR (8000) NULL,
    [usergroup_id]        INT            NULL,
    [usergroup_name]      VARCHAR (8000) NULL,
    [email]               VARCHAR (8000) NULL,
    [mobile]              VARCHAR (8000) NULL,
    [dte_updated]         DATETIME2 (6)  NULL,
    [chat_role]           SMALLINT       NULL,
    [chat_master_user_id] INT            NULL,
    [unblockable_role]    SMALLINT       NULL,
    [unblockable_group]   INT            NULL,
    [deleted]             BIT            NULL,
    [puzzel_id]           VARCHAR (8000) NULL,
    [master_user_name]    VARCHAR (100)  NULL,
    [master_full_name]    VARCHAR (300)  NULL,
    [master_email]        VARCHAR (200)  NULL
);


GO

ALTER TABLE [puz].[agents]
    ADD CONSTRAINT [PK_agents] PRIMARY KEY NONCLUSTERED ([agent_id] ASC) NOT ENFORCED;


GO