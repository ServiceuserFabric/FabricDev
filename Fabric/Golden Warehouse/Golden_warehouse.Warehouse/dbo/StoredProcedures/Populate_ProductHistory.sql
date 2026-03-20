-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
CREATE PROCEDURE [dbo].[Populate_ProductHistory]
(
    @FromDate DATE = '2025-01-01',
    @ToDate DATE = '2026-02-23'
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
    DECLARE @RC int
    DECLARE @Insertdate DATE;
    SET @Insertdate = @FromDate;
    
    --PRINT @InsertDate

    WHILE @Insertdate <= @ToDate
    BEGIN
      EXECUTE @RC = [dbo].[Fill_ProductHistory_from_status] @Insertdate;
      SET @Insertdate = DATEADD(DAY,1,@Insertdate)
END
END