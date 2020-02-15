SET NOCOUNT ON

DECLARE @RowCount int

-- Data (table) compression (heap or clustered index)
SELECT @RowCount = COUNT([p].[data_compression_desc])
FROM [sys].[partitions] AS [p]
INNER JOIN sys.tables AS [t]
     ON [t].[object_id] = [p].[object_id]
INNER JOIN sys.indexes AS [i]
     ON [i].[object_id] = [p].[object_id]
WHERE [p].[index_id] in (0,1) AND [p].[data_compression_desc] != 'PAGE'
	AND [t].[name] NOT LIKE 'sys%' AND [t].[name] NOT LIKE 'MS%'

SET @RowCount = COALESCE(@RowCount, 0)

-- Index compression (non-clustered index)
SELECT @RowCount = @RowCount + COUNT([p].[data_compression_desc])
FROM [sys].[partitions] AS [p]
INNER JOIN sys.tables AS [t]
     ON [t].[object_id] = [p].[object_id]
INNER JOIN sys.indexes AS [i]
     ON [i].[object_id] = [p].[object_id] AND i.index_id = p.index_id
WHERE [p].[index_id] not in (0,1) AND [p].[data_compression_desc] != 'PAGE'
	AND [t].[name] NOT LIKE 'sys%' AND [t].[name] NOT LIKE 'MS%'

SELECT @RowCount
