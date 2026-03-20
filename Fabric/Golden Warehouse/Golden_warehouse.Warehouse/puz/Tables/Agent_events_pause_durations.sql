CREATE TABLE [puz].[Agent_events_pause_durations] (

	[agent_id] bigint NULL, 
	[rec_id_pause_start] bigint NULL, 
	[dte_start] datetime2(0) NULL, 
	[dte_end] datetime2(0) NULL, 
	[profile] varchar(200) NULL, 
	[event_type_pause_start] char(1) NULL, 
	[event_type_pause_end] char(1) NULL, 
	[duration_seconds] bigint NULL, 
	[event_date] date NULL, 
	[dte_start_date] date NULL, 
	[dte_start_time] time(0) NULL, 
	[dte_start_hour] int NULL, 
	[dte_start_minute] int NULL, 
	[dte_start_index] int NULL
);