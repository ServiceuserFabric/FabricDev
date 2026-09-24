CREATE TABLE [puz].[Agent_events_signed_in_sessions] (
    [agent_id]              BIGINT        NULL,
    [rec_id_signed_in]      BIGINT        NULL,
    [dte_start]             DATETIME2 (0) NULL,
    [dte_end]               DATETIME2 (0) NULL,
    [profile]               VARCHAR (200) NULL,
    [event_type_signed_in]  VARCHAR (1)   NULL,
    [event_type_signed_out] VARCHAR (1)   NULL,
    [duration_seconds]      BIGINT        NULL,
    [event_date]            DATE          NULL,
    [dte_start_date]        DATE          NULL,
    [dte_start_time]        VARCHAR (8)   NULL,
    [dte_start_hour]        INT           NULL,
    [dte_start_minute]      INT           NULL,
    [dte_start_index]       INT           NULL
);


GO