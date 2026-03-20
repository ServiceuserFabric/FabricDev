CREATE TABLE [puz].[agents] (

	[agent_id] int NOT NULL, 
	[customer_key] varchar(8000) NULL, 
	[user_name] varchar(8000) NULL, 
	[user_num] varchar(8000) NULL, 
	[full_name] varchar(8000) NULL, 
	[usergroup_id] int NULL, 
	[usergroup_name] varchar(8000) NULL, 
	[email] varchar(8000) NULL, 
	[mobile] varchar(8000) NULL, 
	[dte_updated] datetime2(6) NULL, 
	[chat_role] smallint NULL, 
	[chat_master_user_id] int NULL, 
	[unblockable_role] smallint NULL, 
	[unblockable_group] int NULL, 
	[deleted] bit NULL, 
	[puzzel_id] varchar(8000) NULL, 
	[master_user_name] varchar(100) NULL, 
	[master_full_name] varchar(300) NULL, 
	[master_email] varchar(200) NULL
);


GO
ALTER TABLE [puz].[agents] ADD CONSTRAINT PK_agents primary key NONCLUSTERED ([agent_id]);