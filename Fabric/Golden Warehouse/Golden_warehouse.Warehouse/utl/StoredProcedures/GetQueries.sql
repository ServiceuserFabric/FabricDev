CREATE PROC [utl].[GetQueries]
(
@LoadFrequency INT = 1,
@ToSchema VARCHAR(50) = 'puz',
@FullLoad INT = 0
)
AS
BEGIN	
	SELECT L.TableName, W.WatermarkColumn AS IncField, L.[SQLQuery] + ' WHERE ' + W.WatermarkColumn + ' > ' + W.[WatermarkValue] AS SourceQuery, L.IncFull
	  FROM [utl].[LoadTables] AS L
	  JOIN [utl].[watermarktable] AS W ON W.ToSchema = L.ToSchema AND W.TableName = L.TableName
	WHERE L.Active = 1 AND L.IncFull = 1 AND @FullLoad = 0 AND L.ToSchema = @ToSchema AND LoadFrequency = @LoadFrequency
	UNION
	SELECT L.TableName,''  AS IncField, L.[SQLQuery] AS SourceQuery, L.IncFull
	  FROM [utl].[LoadTables] AS L
	WHERE L.Active = 1 AND (L.IncFull = 0 OR @FullLoad = 1) AND L.ToSchema = @ToSchema AND LoadFrequency = @LoadFrequency
END