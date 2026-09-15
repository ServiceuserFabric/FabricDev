--ALTER PROCEDURE [dbo].[UpdateOpdateretTidspunkt]
-- (
--	@Entitet VARCHAR(250) = ''
--)
--AS
--BEGIN
--	UPDATE [dbo].[OpdateretTidspunkt]
--	SET [Dato] = GETDATE(),
--		Tidspunkt = CAST(GETDATE() AS TIME)
--	WHERE Entitet = @Entitet;
--END
--GO

-- Har ændret fra ovenstående, da det gav tidspunktet for server i stedet for lokaltid i Danmark. 
CREATE PROCEDURE [dbo].[UpdateOpdateretTidspunkt]
(
    @Entitet VARCHAR(250) = ''
)
AS
BEGIN
    DECLARE @CopenhagenNow DATETIME2;

    SET @CopenhagenNow =
        CAST(
            SYSUTCDATETIME()
                AT TIME ZONE 'UTC'
                AT TIME ZONE 'Central European Standard Time'
            AS DATETIME2
        );

    UPDATE [dbo].[OpdateretTidspunkt]
    SET
        Dato      = CAST(@CopenhagenNow AS DATE),
        Tidspunkt = CAST(@CopenhagenNow AS TIME)
    WHERE Entitet = @Entitet;
END