SET NOCOUNT ON

DECLARE @TableCount int

SELECT @TableCount = COUNT(t.[name])
FROM sys.tables t
INNER JOIN sys.indexes i ON t.[object_id] = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.[object_id] AND i.index_id = p.index_id
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.uses_ansi_nulls = 0 AND t.name NOT LIKE 'sys%' AND t.name NOT LIKE 'MS%'
GROUP BY t.[name]

SELECT COALESCE(@TableCount, 0) AS TableCount
