CREATE PROCEDURE [utl].[usp_write_watermark]
(
@TableName VARCHAR(255) = '',
@ToSchema VARCHAR(10) = 'puz',
@WatermarkValue VARCHAR(100) = '0'
)
AS
BEGIN
	UPDATE utl.watermarktable
	SET [WatermarkValue] = @WatermarkValue
	WHERE ToSchema = @ToSchema AND TableName = @TableName
END