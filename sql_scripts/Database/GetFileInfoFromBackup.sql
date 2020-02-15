SET NOCOUNT ON

DECLARE @sql nvarchar(max)

SET @sql = 'RESTORE FILELISTONLY FROM $(bkupfiles)'

EXEC(@sql)
