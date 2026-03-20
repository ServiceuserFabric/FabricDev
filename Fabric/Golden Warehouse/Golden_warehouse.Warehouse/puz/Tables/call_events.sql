CREATE TABLE [puz].[call_events] (

	[rec_id] bigint NOT NULL, 
	[customer_key] varchar(8000) NULL, 
	[call_id] decimal(19,0) NULL, 
	[call_sequence] int NULL, 
	[media_type_id] int NULL, 
	[dte_start] date NULL, 
	[duration_tot_sec] int NULL, 
	[duration_speak_sec] int NULL, 
	[dte_speak_start] datetime2(6) NULL, 
	[source] varchar(8000) NULL, 
	[destination] varchar(8000) NULL, 
	[additional_source] varchar(8000) NULL, 
	[redirect_source] varchar(8000) NULL, 
	[service_num] varchar(8000) NULL, 
	[queue_key] varchar(8000) NULL, 
	[menue_key] varchar(8000) NULL, 
	[menue_choice] varchar(8000) NULL, 
	[agent_id] int NULL, 
	[event_type] varchar(8000) NULL, 
	[result_code] varchar(8000) NULL, 
	[answered] smallint NULL, 
	[ciq] varchar(8000) NULL, 
	[call_transfer] bit NULL, 
	[wrap_up_sec] int NULL, 
	[alert_ms] int NULL, 
	[setup_ms] int NULL, 
	[block_duration_sec] int NULL, 
	[internal_iq_session_id] varchar(8000) NULL, 
	[internal_odr_id] bigint NULL, 
	[dte_updated] datetime2(6) NULL, 
	[sla] int NULL, 
	[alt_sla] int NULL, 
	[dte_scheduled_callback] datetime2(6) NULL, 
	[result_response] int NULL, 
	[request_id] bigint NULL, 
	[hold] int NULL, 
	[consult] int NULL, 
	[leg_type] varchar(8000) NULL, 
	[originating] varchar(8000) NULL, 
	[add_originating] varchar(8000) NULL, 
	[caller_on_hold_sec] int NULL, 
	[dte_start_time] time(0) NULL, 
	[dte_start_hour] int NULL, 
	[dte_start_minute] int NULL, 
	[dte_start_index] int NULL
);


GO
ALTER TABLE [puz].[call_events] ADD CONSTRAINT PK_call_events primary key NONCLUSTERED ([rec_id]);