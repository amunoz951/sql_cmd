SET NOCOUNT ON

DECLARE @sql nvarchar(max)
DECLARE @credential nvarchar(max)

SET @credential = '$(credential)'
SET @sql = 'RESTORE FILELISTONLY FROM $(bkupfiles)' + CASE WHEN @credential NOT LIKE '' THEN ' WITH CREDENTIAL = ''' + @credential + '''' ELSE '' END

EXEC(@sql)
