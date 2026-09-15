-- =============================================
-- Author:      Mathias Liedtke
-- Create Date: 29/04/2026
-- Description: Henter fra internal_iq_session_id og grupperer source, så vi kan mappe sourceværdien på samtlige internal_iq_session_id. Det er nødvendigt for at beregne FCR
-- =============================================
CREATE PROCEDURE [dbo].[Puzzel_FCR]
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [Golden_warehouse].[puz].[Puzzel_FCR] (internal_iq_session_id, [source])
    SELECT DISTINCT
        ce.internal_iq_session_id,
        ce.[source]
    FROM [Puzzel_Altibox].dbo.call_events ce
    WHERE ce.[source] IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM [Golden_warehouse].[puz].[Puzzel_FCR] tgt
          WHERE tgt.internal_iq_session_id = ce.internal_iq_session_id
      );
END