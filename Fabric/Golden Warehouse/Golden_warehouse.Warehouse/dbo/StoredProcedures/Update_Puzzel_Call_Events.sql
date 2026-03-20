CREATE PROCEDURE [dbo].[Update_Puzzel_Call_Events]
AS
BEGIN

	MERGE puz.[call_events] AS t USING [Puzzel_Altibox].dbo.[call_events] AS s
	ON t.rec_id = s.rec_id
	WHEN MATCHED AND
	([t].[customer_key] <> [s].[customer_key] AND [s].[customer_key] IS NOT NULL) OR
	([t].[call_id] <> [s].[call_id] AND [s].[call_id] IS NOT NULL) OR
	([t].[call_sequence] <> [s].[call_sequence] AND [s].[call_sequence] IS NOT NULL) OR
	([t].[media_type_id] <> [s].[media_type_id] AND [s].[media_type_id] IS NOT NULL) OR
    ([t].[dte_start] <> CAST([s].[dte_start] AS DATE) AND [s].[dte_start] IS NOT NULL) OR
	([t].[duration_tot_sec] <> [s].[duration_tot_sec] AND [s].[duration_tot_sec] IS NOT NULL) OR
	([t].[duration_speak_sec] <> [s].[duration_speak_sec] AND [s].[duration_speak_sec] IS NOT NULL) OR
	([t].[dte_speak_start] <> [s].[dte_speak_start] AND [s].[dte_speak_start] IS NOT NULL) OR
	([t].[source] <> [s].[source] AND [s].[source] IS NOT NULL) OR
	([t].[destination] <> [s].[destination] AND [s].[destination] IS NOT NULL) OR
	([t].[additional_source] <> [s].[additional_source] AND [s].[additional_source] IS NOT NULL) OR
	([t].[redirect_source] <> [s].[redirect_source] AND [s].[redirect_source] IS NOT NULL) OR
	([t].[service_num] <> [s].[service_num] AND [s].[service_num] IS NOT NULL) OR
	([t].[queue_key] <> [s].[queue_key] AND [s].[queue_key] IS NOT NULL) OR
	([t].[menue_key] <> [s].[menue_key] AND [s].[menue_key] IS NOT NULL) OR
	([t].[menue_choice] <> [s].[menue_choice] AND [s].[menue_choice] IS NOT NULL) OR
	([t].[agent_id] <> [s].[agent_id] AND [s].[agent_id] IS NOT NULL) OR
	([t].[event_type] <> [s].[event_type] AND [s].[event_type] IS NOT NULL) OR
	([t].[result_code] <> [s].[result_code] AND [s].[result_code] IS NOT NULL) OR
	([t].[answered] <> [s].[answered] AND [s].[answered] IS NOT NULL) OR
	([t].[ciq] <> [s].[ciq] AND [s].[ciq] IS NOT NULL) OR
	([t].[call_transfer] <> [s].[call_transfer] AND [s].[call_transfer] IS NOT NULL) OR
	([t].[wrap_up_sec] <> [s].[wrap_up_sec] AND [s].[wrap_up_sec] IS NOT NULL) OR
	([t].[alert_ms] <> [s].[alert_ms] AND [s].[alert_ms] IS NOT NULL) OR
	([t].[setup_ms] <> [s].[setup_ms] AND [s].[setup_ms] IS NOT NULL) OR
	([t].[block_duration_sec] <> [s].[block_duration_sec] AND [s].[block_duration_sec] IS NOT NULL) OR
	([t].[internal_iq_session_id] <> [s].[internal_iq_session_id] AND [s].[internal_iq_session_id] IS NOT NULL) OR
	([t].[internal_odr_id] <> [s].[internal_odr_id] AND [s].[internal_odr_id] IS NOT NULL) OR
	([t].[dte_updated] <> [s].[dte_updated] AND [s].[dte_updated] IS NOT NULL) OR
	([t].[sla] <> [s].[sla] AND [s].[sla] IS NOT NULL) OR
	([t].[alt_sla] <> [s].[alt_sla] AND [s].[alt_sla] IS NOT NULL) OR
	([t].[dte_scheduled_callback] <> [s].[dte_scheduled_callback] AND [s].[dte_scheduled_callback] IS NOT NULL) OR
	([t].[result_response] <> [s].[result_response] AND [s].[result_response] IS NOT NULL) OR
	([t].[request_id] <> [s].[request_id] AND [s].[request_id] IS NOT NULL) OR
	([t].[hold] <> [s].[hold] AND [s].[hold] IS NOT NULL) OR
	([t].[consult] <> [s].[consult] AND [s].[consult] IS NOT NULL) OR
	([t].[leg_type] <> [s].[leg_type] AND [s].[leg_type] IS NOT NULL) OR
	([t].[originating] <> [s].[originating] AND [s].[originating] IS NOT NULL) OR
	([t].[add_originating] <> [s].[add_originating] AND [s].[add_originating] IS NOT NULL) OR
	([t].[caller_on_hold_sec] <> [s].[caller_on_hold_sec] AND [s].[caller_on_hold_sec] IS NOT NULL)
	THEN UPDATE SET
	[customer_key] = s.customer_key,
	[call_id] = s.call_id,
	[call_sequence] = s.call_sequence,
	[media_type_id] = s.media_type_id,
	[dte_start] = s.dte_start,
	[duration_tot_sec] = s.duration_tot_sec,
	[duration_speak_sec] = s.duration_speak_sec,
	[dte_speak_start] = s.dte_speak_start,
	[source] = s.source,
	[destination] = s.destination,
	[additional_source] = s.additional_source,
	[redirect_source] = s.redirect_source,
	[service_num] = s.service_num,
	[queue_key] = s.queue_key,
	[menue_key] = s.menue_key,
	[menue_choice] = s.menue_choice,
	[agent_id] = s.agent_id,
	[event_type] = s.event_type,
	[result_code] = s.result_code,
	[answered] = s.answered,
	[ciq] = s.ciq,
	[call_transfer] = s.call_transfer,
	[wrap_up_sec] = s.wrap_up_sec,
	[alert_ms] = s.alert_ms,
	[setup_ms] = s.setup_ms,
	[block_duration_sec] = s.block_duration_sec,
	[internal_iq_session_id] = s.internal_iq_session_id,
	[internal_odr_id] = s.internal_odr_id,
	[dte_updated] = s.dte_updated,
	[sla] = s.sla,
	[alt_sla] = s.alt_sla,
	[dte_scheduled_callback] = s.dte_scheduled_callback,
	[result_response] = s.result_response,
	[request_id] = s.request_id,
	[hold] = s.hold,
	[consult] = s.consult,
	[leg_type] = s.leg_type,
	[originating] = s.originating,
	[add_originating] = s.add_originating,
	[caller_on_hold_sec] = s.caller_on_hold_sec,
	[dte_start_time] = CAST(FORMAT(s.[dte_start] ,'yyyy-MM-dd HH:mm') AS TIME(0)),
	[dte_start_hour] = DATEPART(hour,s.[dte_start]),
	[dte_start_minute] = DATEPART(minute,s.[dte_start]),
	[dte_start_index] = DATEPART(hour,s.[dte_start]) * 100 + DATEPART(minute,s.[dte_start])
	WHEN NOT MATCHED THEN INSERT (
	[rec_id],
	[customer_key],
	[call_id],
	[call_sequence],
	[media_type_id],
	[dte_start],
	[duration_tot_sec],
	[duration_speak_sec],
	[dte_speak_start],
	[source],
	[destination],
	[additional_source],
	[redirect_source],
	[service_num],
	[queue_key],
	[menue_key],
	[menue_choice],
	[agent_id],
	[event_type],
	[result_code],
	[answered],
	[ciq],
	[call_transfer],
	[wrap_up_sec],
	[alert_ms],
	[setup_ms],
	[block_duration_sec],
	[internal_iq_session_id],
	[internal_odr_id],
	[dte_updated],
	[sla],
	[alt_sla],
	[dte_scheduled_callback],
	[result_response],
	[request_id],
	[hold],
	[consult],
	[leg_type],
	[originating],
	[add_originating],
	[caller_on_hold_sec],
	[dte_start_time],
	[dte_start_hour],
	[dte_start_minute],
	[dte_start_index]
	)
	VALUES (
	s.[rec_id],
	s.[customer_key],
	s.[call_id],
	s.[call_sequence],
	s.[media_type_id],
	s.[dte_start],
	s.[duration_tot_sec],
	s.[duration_speak_sec],
	s.[dte_speak_start],
	s.[source],
	s.[destination],
	s.[additional_source],
	s.[redirect_source],
	s.[service_num],
	s.[queue_key],
	s.[menue_key],
	s.[menue_choice],
	s.[agent_id],
	s.[event_type],
	s.[result_code],
	s.[answered],
	s.[ciq],
	s.[call_transfer],
	s.[wrap_up_sec],
	s.[alert_ms],
	s.[setup_ms],
	s.[block_duration_sec],
	s.[internal_iq_session_id],
	s.[internal_odr_id],
	s.[dte_updated],
	s.[sla],
	s.[alt_sla],
	s.[dte_scheduled_callback],
	s.[result_response],
	s.[request_id],
	s.[hold],
	s.[consult],
	s.[leg_type],
	s.[originating],
	s.[add_originating],
	s.[caller_on_hold_sec],
	CAST(FORMAT(s.[dte_start] ,'yyyy-MM-dd HH:mm') AS TIME(0)),
	DATEPART(hour,s.[dte_start]),
	DATEPART(minute,s.[dte_start]),
    DATEPART(hour,s.[dte_start]) * 100 + DATEPART(minute,s.[dte_start]));
END