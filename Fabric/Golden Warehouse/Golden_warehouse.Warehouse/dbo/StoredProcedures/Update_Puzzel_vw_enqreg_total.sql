-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [dbo].[Update_Puzzel_vw_enqreg_total]
AS
BEGIN
    MERGE puz.[vw_enqreg_total] AS t USING [Puzzel_Altibox].dbo.[vw_enqreg_total] AS s
    ON t.enqreg_header_recid = s.enqreg_header_recid and t.[enqreg_topic_recid] = s.[enqreg_topic_recid]
    WHEN MATCHED AND
    ([t].[internal_session_id] <> [s].[internal_session_id] AND [s].[internal_session_id] IS NOT NULL) OR
    ([t].[customer_key] <> s.[customer_key] AND [s].[customer_key] IS NOT NULL) OR
    ([t].[internal_country_src_db] <> [s].[internal_country_src_db] AND [s].[internal_country_src_db] IS NOT NULL) OR
    ([t].[dte_time_stamp] <> [s].[dte_time_stamp] AND [s].[dte_time_stamp] IS NOT NULL) OR
    ([t].[agent_id] <> [s].[agent_id] AND [s].[agent_id] IS NOT NULL) OR
    ([t].[queue_key] <> [s].[queue_key] AND [s].[queue_key] IS NOT NULL) OR
    ([t].[enquiry_media_type] <> [s].[enquiry_media_type] AND [s].[enquiry_media_type] IS NOT NULL) OR
    ([t].[related_iq_session_id] <> [s].[related_iq_session_id] AND [s].[related_iq_session_id] IS NOT NULL) OR
    ([t].[dte_updated] <> [s].[dte_updated] AND [s].[dte_updated] IS NOT NULL) OR
    ([t].[comment] <> [s].[comment] AND [s].[comment] IS NOT NULL) OR
    ([t].[reschedule_time] <> [s].[reschedule_time] AND [s].[reschedule_time] IS NOT NULL) OR
    ([t].[enqreg_category_recid] <> [s].[enqreg_category_recid] AND [s].[enqreg_category_recid] IS NOT NULL) OR
    ([t].[category_id] <> [s].[category_id] AND [s].[category_id] IS NOT NULL) OR
    ([t].[category_name] <> [s].[category_name] AND [s].[category_name] IS NOT NULL) OR
    ([t].[enqreg_topic_recid] <> [s].[enqreg_topic_recid] AND [s].[enqreg_topic_recid] IS NOT NULL) OR
    ([t].[topic_id] <> [s].[topic_id] AND [s].[topic_id] IS NOT NULL) OR
    ([t].[topic_name] <> [s].[topic_name] AND [s].[topic_name] IS NOT NULL) OR
    ([t].[marked_unansw] <> [s].[marked_unansw] AND [s].[marked_unansw] IS NOT NULL) OR
    ([t].[reserved] <> [s].[reserved] AND [s].[reserved] IS NOT NULL) OR
    ([t].[parsed_category_id] <> [s].[parsed_category_id] AND [s].[parsed_category_id] IS NOT NULL) OR
    ([t].[parsed_category_name] <> [s].[parsed_category_name] AND [s].[parsed_category_name] IS NOT NULL) 
    THEN UPDATE SET
    [internal_session_id] = s.[internal_session_id],
    [customer_key] = s.[customer_key],
    [internal_country_src_db] = s.[internal_country_src_db],
    [dte_time_stamp] = s.[dte_time_stamp],
    [agent_id] = s.[agent_id],
    [queue_key] = s.[queue_key],
    [enquiry_media_type] = s.[enquiry_media_type],
    [related_iq_session_id] = s.[related_iq_session_id],
    [dte_updated] = s.[dte_updated],
    [comment] = s.[comment],
    [reschedule_time] = s.[reschedule_time],
    [enqreg_category_recid] = s.[enqreg_category_recid],
    [category_id] = s.[category_id],
    [category_name] = s.[category_name],
    [enqreg_topic_recid] = s.[enqreg_topic_recid],
    [topic_id] = s.[topic_id],
    [topic_name] = s.[topic_name],
    [marked_unansw] = s.[marked_unansw],
    [reserved] = s.[reserved],
    [parsed_category_id] = s.[parsed_category_id],
    [parsed_category_name] = s.[parsed_category_name],
    [dte_start_time] = CONVERT(time(0), s.dte_time_stamp),
    [dte_start_hour] = DATEPART(hour,s.[dte_time_stamp]),
    [dte_start_minute] = DATEPART(minute,s.[dte_time_stamp]),
    [dte_start_index] = DATEPART(hour,s.[dte_time_stamp]) * 100 + DATEPART(minute,s.[dte_time_stamp]),
    [dte_start_date] = CONVERT(date, s.dte_time_stamp)
    WHEN NOT MATCHED THEN INSERT (
    [enqreg_header_recid]
      ,[internal_session_id]
      ,[customer_key]
      ,[internal_country_src_db]
      ,[dte_time_stamp]
      ,[agent_id]
      ,[queue_key]
      ,[enquiry_media_type]
      ,[related_iq_session_id]
      ,[dte_updated]
      ,[comment]
      ,[reschedule_time]
      ,[enqreg_category_recid]
      ,[category_id]
      ,[category_name]
      ,[enqreg_topic_recid]
      ,[topic_id]
      ,[topic_name]
      ,[marked_unansw]
      ,[reserved]
      ,[parsed_category_id],
      [parsed_category_name],
    [dte_start_time],
    [dte_start_hour],
    [dte_start_minute],
    [dte_start_index],
    [dte_start_date]
    )
    VALUES (
    s.[enqreg_header_recid],
    s.[internal_session_id],
    s.[customer_key],
    s.[internal_country_src_db],
    s.[dte_time_stamp],
    s.[agent_id],
    s.[queue_key],
    s.[enquiry_media_type],
    s.[related_iq_session_id],
    s.[dte_updated],
    s.[comment],
    s.[reschedule_time],
    s.[enqreg_category_recid],
    s.[category_id],
    s.[category_name],
    s.[enqreg_topic_recid],
    s.[topic_id],
    s.[topic_name],
    s.[marked_unansw],
    s.[reserved],
    s.[parsed_category_id],
    s.[parsed_category_name],
    CONVERT(time(0), s.dte_time_stamp),
    DATEPART(hour,s.[dte_time_stamp]),
    DATEPART(minute,s.[dte_time_stamp]),
    DATEPART(hour,s.[dte_time_stamp]) * 100 + DATEPART(minute,s.[dte_time_stamp]),
    CONVERT(date, s.dte_time_stamp));

END