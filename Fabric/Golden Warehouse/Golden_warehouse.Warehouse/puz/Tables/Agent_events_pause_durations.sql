CREATE TABLE [puz].[Agent_events_pause_durations] (
    [agent_id]               BIGINT        NULL,
    [rec_id_pause_start]     BIGINT        NULL,
    [dte_start]              DATETIME2 (0) NULL,
    [dte_end]                DATETIME2 (0) NULL,
    [profile]                VARCHAR (200) NULL,
    [event_type_pause_start] CHAR (1)      NULL,
    [event_type_pause_end]   CHAR (1)      NULL,
    [duration_seconds]       BIGINT        NULL,
    [event_date]             DATE          NULL,
    [dte_start_date]         DATE          NULL,
    [dte_start_time]         TIME (0)      NULL,
    [dte_start_hour]         INT           NULL,
    [dte_start_minute]       INT           NULL,
    [dte_start_index]        INT           NULL
);


GO