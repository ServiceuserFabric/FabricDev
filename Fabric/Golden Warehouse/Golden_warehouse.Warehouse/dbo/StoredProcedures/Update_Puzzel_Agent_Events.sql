CREATE PROCEDURE [dbo].[Update_Puzzel_Agent_Events]
AS
BEGIN
    MERGE puz.[agent_events] AS t USING [Puzzel_Altibox].dbo.[agent_events] AS s
    ON t.rec_id = s.rec_id
    WHEN MATCHED AND
    ([t].[agent_id] <> [s].[agent_id] AND [s].[agent_id] IS NOT NULL) OR
    ([t].[dte_start] <> CAST([s].[dte_start] AS DATE) AND [s].[dte_start] IS NOT NULL) OR
    ([t].[profile] <> [s].[profile] AND [s].[profile] IS NOT NULL) OR
    ([t].[service_num] <> [s].[service_num] AND [s].[service_num] IS NOT NULL) OR
    ([t].[phone_num] <> [s].[phone_num] AND [s].[phone_num] IS NOT NULL) OR
    ([t].[duration_sec] <> [s].[duration_sec] AND [s].[duration_sec] IS NOT NULL) OR
    ([t].[event_type] <> [s].[event_type] AND [s].[event_type] IS NOT NULL) OR
    ([t].[event_source] <> [s].[event_source] AND [s].[event_source] IS NOT NULL) OR
    ([t].[result_code] <> [s].[result_code] AND [s].[result_code] IS NOT NULL) OR
    ([t].[queue_key] <> [s].[queue_key] AND [s].[queue_key] IS NOT NULL) OR
    ([t].[pause_type_name] <> [s].[pause_type_name] AND [s].[pause_type_name] IS NOT NULL) OR
    ([t].[pause_type_id] <> [s].[pause_type_id] AND [s].[pause_type_id] IS NOT NULL) OR
    ([t].[call_transfer] <> [s].[call_transfer] AND [s].[call_transfer] IS NOT NULL) OR
    ([t].[wrap_up_sec] <> [s].[wrap_up_sec] AND [s].[wrap_up_sec] IS NOT NULL) OR
    ([t].[block_duration_sec] <> [s].[block_duration_sec] AND [s].[block_duration_sec] IS NOT NULL) OR
    ([t].[internal_adr_id] <> [s].[internal_adr_id] AND [s].[internal_adr_id] IS NOT NULL) OR
    ([t].[internal_odr_id] <> [s].[internal_odr_id] AND [s].[internal_odr_id] IS NOT NULL) OR
    ([t].[internal_country_src_db] <> [s].[internal_country_src_db] AND [s].[internal_country_src_db] IS NOT NULL) OR
    ([t].[dte_updated] <> [s].[dte_updated] AND [s].[dte_updated] IS NOT NULL) OR
    ([t].[usergroup_id] <> [s].[usergroup_id] AND [s].[usergroup_id] IS NOT NULL) OR
    ([t].[phone_type] <> [s].[phone_type] AND [s].[phone_type] IS NOT NULL) 
    THEN UPDATE SET
    [agent_id] = s.agent_id,
    [dte_start] = s.dte_start,
    [profile] = s.profile,
    [service_num] = s.service_num,
    [phone_num] = s.phone_num,
    [duration_sec] = s.duration_sec,
    [event_type] = s.event_type,
    [event_source] = s.event_source,
    [result_code] = s.result_code,
    [queue_key] = s.queue_key,
    [pause_type_name] = s.pause_type_name,
    [pause_type_id] = s.pause_type_id,
    [call_transfer] = s.call_transfer,
    [wrap_up_sec] = s.wrap_up_sec,
    [block_duration_sec] = s.block_duration_sec,
    [internal_adr_id] = s.internal_adr_id,
    [internal_odr_id] = s.internal_odr_id,
    [internal_country_src_db] = s.internal_country_src_db,
    [dte_updated] = s.dte_updated,
    [usergroup_id] = s.usergroup_id,
    [phone_type] = s.phone_type,
    [dte_start_time] = CAST(FORMAT(s.[dte_start] ,'yyyy-MM-dd HH:mm') AS TIME(0)),
    [dte_start_hour] = DATEPART(hour,s.[dte_start]),
    [dte_start_minute] = DATEPART(minute,s.[dte_start]),
    [dte_start_index] = DATEPART(hour,s.[dte_start]) * 100 + DATEPART(minute,s.[dte_start])
    WHEN NOT MATCHED THEN INSERT (
    [rec_id],
    [agent_id],
    [dte_start],
    [profile],
    [service_num],
    [phone_num],
    [duration_sec],
    [event_type],
    [event_source],
    [result_code],
    [queue_key],
    [pause_type_name],
    [pause_type_id],
    [call_transfer],
    [wrap_up_sec],
    [block_duration_sec],
    [internal_adr_id],
    [internal_odr_id],
    [internal_country_src_db],
    [dte_updated],
    [usergroup_id],
    [phone_type],
    [dte_start_time],
    [dte_start_hour],
    [dte_start_minute],
    [dte_start_index]
    )
    VALUES (
    s.[rec_id],
    s.[agent_id],
    s.[dte_start],
    s.[profile],
    s.[service_num],
    s.[phone_num],
    s.[duration_sec],
    s.[event_type],
    s.[event_source],
    s.[result_code],
    s.[queue_key],
    s.[pause_type_name],
    s.[pause_type_id],
    s.[call_transfer],
    s.[wrap_up_sec],
    s.[block_duration_sec],
    s.[internal_adr_id],
    s.[internal_odr_id],
    s.[internal_country_src_db],
    s.[dte_updated],
    s.[usergroup_id],
    s.[phone_type],
    CAST(FORMAT(s.[dte_start] ,'yyyy-MM-dd HH:mm') AS TIME(0)),
    DATEPART(hour,s.[dte_start]),
    DATEPART(minute,s.[dte_start]),
    DATEPART(hour,s.[dte_start]) * 100 + DATEPART(minute,s.[dte_start]));

END