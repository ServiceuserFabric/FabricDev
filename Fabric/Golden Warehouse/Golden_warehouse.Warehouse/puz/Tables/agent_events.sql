CREATE TABLE [puz].[agent_events] (
    [rec_id]                  INT            NULL,
    [agent_id]                INT            NULL,
    [dte_start]               DATETIME2 (6)  NULL,
    [profile]                 VARCHAR (8000) NULL,
    [service_num]             VARCHAR (8000) NULL,
    [phone_num]               VARCHAR (8000) NULL,
    [duration_sec]            INT            NULL,
    [event_type]              VARCHAR (8000) NULL,
    [event_source]            VARCHAR (8000) NULL,
    [result_code]             VARCHAR (8000) NULL,
    [queue_key]               VARCHAR (8000) NULL,
    [pause_type_name]         VARCHAR (8000) NULL,
    [pause_type_id]           INT            NULL,
    [call_transfer]           BIT            NULL,
    [wrap_up_sec]             INT            NULL,
    [block_duration_sec]      INT            NULL,
    [internal_adr_id]         BIGINT         NULL,
    [internal_odr_id]         BIGINT         NULL,
    [internal_country_src_db] VARCHAR (8000) NULL,
    [dte_updated]             DATETIME2 (6)  NULL,
    [usergroup_id]            INT            NULL,
    [phone_type]              INT            NULL,
    [dte_start_time]          TIME (0)       NULL,
    [dte_start_hour]          INT            NULL,
    [dte_start_minute]        INT            NULL,
    [dte_start_index]         INT            NULL
);


GO