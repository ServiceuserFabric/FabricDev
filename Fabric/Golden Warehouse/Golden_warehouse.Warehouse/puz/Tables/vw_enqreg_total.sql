CREATE TABLE [puz].[vw_enqreg_total] (
    [enqreg_header_recid]     INT            NULL,
    [internal_session_id]     VARCHAR (8000) NULL,
    [customer_key]            VARCHAR (8000) NULL,
    [internal_country_src_db] VARCHAR (8000) NULL,
    [dte_time_stamp]          DATETIME2 (6)  NULL,
    [agent_id]                INT            NULL,
    [queue_key]               VARCHAR (8000) NULL,
    [enquiry_media_type]      VARCHAR (8000) NULL,
    [related_iq_session_id]   VARCHAR (8000) NULL,
    [dte_updated]             DATETIME2 (6)  NULL,
    [comment]                 VARCHAR (8000) NULL,
    [reschedule_time]         DATETIME2 (6)  NULL,
    [enqreg_category_recid]   INT            NULL,
    [category_id]             INT            NULL,
    [category_name]           VARCHAR (8000) NULL,
    [enqreg_topic_recid]      INT            NULL,
    [topic_id]                INT            NULL,
    [topic_name]              VARCHAR (8000) NULL,
    [marked_unansw]           VARCHAR (8000) NULL,
    [reserved]                VARCHAR (8000) NULL,
    [parsed_category_id]      VARCHAR (8000) NULL,
    [parsed_category_name]    VARCHAR (8000) NULL,
    [dte_start_time]          TIME (0)       NULL,
    [dte_start_hour]          INT            NULL,
    [dte_start_minute]        INT            NULL,
    [dte_start_index]         INT            NULL,
    [dte_start_date]          DATE           NULL
);


GO