CREATE TABLE [puz].[call_events] (
    [rec_id]                 BIGINT         NOT NULL,
    [customer_key]           VARCHAR (8000) NULL,
    [call_id]                DECIMAL (19)   NULL,
    [call_sequence]          INT            NULL,
    [media_type_id]          INT            NULL,
    [dte_start]              DATE           NULL,
    [duration_tot_sec]       INT            NULL,
    [duration_speak_sec]     INT            NULL,
    [dte_speak_start]        DATETIME2 (6)  NULL,
    [source]                 VARCHAR (8000) NULL,
    [destination]            VARCHAR (8000) NULL,
    [additional_source]      VARCHAR (8000) NULL,
    [redirect_source]        VARCHAR (8000) NULL,
    [service_num]            VARCHAR (8000) NULL,
    [queue_key]              VARCHAR (8000) NULL,
    [menue_key]              VARCHAR (8000) NULL,
    [menue_choice]           VARCHAR (8000) NULL,
    [agent_id]               INT            NULL,
    [event_type]             VARCHAR (8000) NULL,
    [result_code]            VARCHAR (8000) NULL,
    [answered]               SMALLINT       NULL,
    [ciq]                    VARCHAR (8000) NULL,
    [call_transfer]          BIT            NULL,
    [wrap_up_sec]            INT            NULL,
    [alert_ms]               INT            NULL,
    [setup_ms]               INT            NULL,
    [block_duration_sec]     INT            NULL,
    [internal_iq_session_id] VARCHAR (8000) NULL,
    [internal_odr_id]        BIGINT         NULL,
    [dte_updated]            DATETIME2 (6)  NULL,
    [sla]                    INT            NULL,
    [alt_sla]                INT            NULL,
    [dte_scheduled_callback] DATETIME2 (6)  NULL,
    [result_response]        INT            NULL,
    [request_id]             BIGINT         NULL,
    [hold]                   INT            NULL,
    [consult]                INT            NULL,
    [leg_type]               VARCHAR (8000) NULL,
    [originating]            VARCHAR (8000) NULL,
    [add_originating]        VARCHAR (8000) NULL,
    [caller_on_hold_sec]     INT            NULL,
    [dte_start_time]         TIME (0)       NULL,
    [dte_start_hour]         INT            NULL,
    [dte_start_minute]       INT            NULL,
    [dte_start_index]        INT            NULL
);


GO

ALTER TABLE [puz].[call_events]
    ADD CONSTRAINT [PK_call_events] PRIMARY KEY NONCLUSTERED ([rec_id] ASC) NOT ENFORCED;


GO