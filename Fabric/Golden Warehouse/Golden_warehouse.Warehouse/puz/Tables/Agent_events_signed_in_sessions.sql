CREATE TABLE [puz].[Agent_events_signed_in_sessions] (

	[agent_id] bigint NULL, 
	[rec_id_signed_in] bigint NULL, 
	[dte_start] datetime2(0) NULL, 
	[dte_end] datetime2(0) NULL, 
	[profile] varchar(200) NULL, 
	[event_type_signed_in] varchar(1) NULL, 
	[event_type_signed_out] varchar(1) NULL, 
	[duration_seconds] bigint NULL, 
	[event_date] date NULL, 
	[dte_start_date] date NULL, 
	[dte_start_time] varchar(8) NULL, 
	[dte_start_hour] int NULL, 
	[dte_start_minute] int NULL, 
	[dte_start_index] int NULL
);