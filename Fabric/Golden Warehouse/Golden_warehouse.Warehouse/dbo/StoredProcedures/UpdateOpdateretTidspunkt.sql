CREATE PROCEDURE dbo.UpdateOpdateretTidspunkt
(
	@Entitet VARCHAR(250) = ''
)
AS
BEGIN
	UPDATE [dbo].[OpdateretTidspunkt]
	SET [Dato] = GETDATE(),
		Tidspunkt = CAST(GETDATE() AS TIME)
	WHERE Entitet = @Entitet;
END