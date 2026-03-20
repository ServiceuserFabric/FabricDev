CREATE   PROCEDURE [puz].[usp_load_agent_events_today]
AS
BEGIN
    DECLARE @d DATE = CAST(GETDATE() AS DATE);

    -- Clear today
    DELETE FROM [Golden_warehouse].puz.Agent_events_signed_in_sessions WHERE event_date = @d;
    DELETE FROM [Golden_warehouse].puz.Agent_events_pause_durations     WHERE event_date = @d;

    -- Insert from Lakehouse SQL endpoint
    INSERT INTO [Golden_warehouse].[puz].[Agent_events_signed_in_sessions]
    SELECT *
    FROM [Puzzel_Altibox].[dbo].[agent_events_signed_in_sessions]
    WHERE event_date = @d;

    INSERT INTO [Golden_warehouse].[puz].[Agent_events_pause_durations]
    SELECT *
    FROM [Puzzel_Altibox].[dbo].[agent_events_pause_durations]
    WHERE event_date = @d;
END;