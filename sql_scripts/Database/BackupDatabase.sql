SET NOCOUNT ON

DECLARE @database_name nvarchar(max)
DECLARE @backup_name nvarchar(max)
DECLARE @credential nvarchar(max)
DECLARE @database_size int
DECLARE @disk_files nvarchar(max)
DECLARE @backup_location nvarchar(max)
DECLARE @backup_type nvarchar(5)
DECLARE @file_counter int
DECLARE @size_increment int
DECLARE @i int
DECLARE @sql nvarchar(max)
DECLARE @log_only bit
DECLARE @split_files bit
DECLARE @compression nvarchar(5)
DECLARE @format bit
DECLARE @copy_only bit
DECLARE @init bit
DECLARE @skip bit
DECLARE @rewind bit
DECLARE @unload bit
DECLARE @stats nvarchar(3)
DECLARE @backup_file_extension nvarchar(4)

SET @database_name = '$(bkupdbname)'
SET @backup_name = '$(bkupname)'
SET @credential = '$(credential)'
SET @log_only = '$(logonly)'
SET @compression = '$(compressbackup)'
SET @backup_location = '$(bkupdest)'
SET @backup_type = '$(bkuptype)'
SET @split_files = '$(splitfiles)'
SET @format = '$(formatbackup)'
SET @copy_only = '$(copyonly)'
SET @init = '$(init)'
SET @skip = '$(skip)'
SET @rewind = '$(rewind)'
SET @unload = '$(unload)'
SET @stats = '$(stats)'
SET @size_increment = '$(bkuppartmaxsize)'
SET @file_counter = 1

IF (RIGHT(@backup_location, 1) != '\' AND @backup_type LIKE 'DISK') SET @backup_location += '\';
SET @backup_file_extension = CASE WHEN @log_only = 1 THEN '.trn' ELSE '.bak' END

IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = @database_name)
BEGIN
	PRINT ('Error: [' + @database_name + '] does not exist on [' + CONVERT(nvarchar,SERVERPROPERTY('ServerName')) + ']!')
	RETURN
END

IF @split_files = 1 AND @backup_type != 'URL'
BEGIN
	SELECT @database_size = CAST(SUM(size) * 8. / 1024 AS DECIMAL(8,2))
	FROM sys.master_files WITH(NOWAIT)
	WHERE database_id = DB_ID(@database_name) -- for current db
	GROUP BY database_id

	SET @i = @size_increment

	WHILE (@i < @database_size)
	BEGIN
		IF @disk_files is null SET @disk_files = @backup_type + ' = N''' + @backup_location + @backup_name + '.part1' + @backup_file_extension + ''''
		SET @file_counter = @file_counter + 1
		SET @disk_files = @disk_files + ', ' + @backup_type + ' = N''' + @backup_location + @backup_name + '.part' + CONVERT(nvarchar(2),@file_counter) + @backup_file_extension + ''''
		SET @i = @i + @size_increment
	END
END
ELSE
BEGIN
	SET @file_counter = 1
END

IF (@file_counter = 1)
BEGIN
	SET @disk_files = @backup_type + ' = N''' + @backup_location + @backup_name + @backup_file_extension + ''''
END

SET @sql = 'BACKUP ' + CASE WHEN @log_only = 1 THEN 'LOG' ELSE 'DATABASE' END + ' [' + @database_name + '] TO  ' + @disk_files + ' WITH ' +
		   CASE @compression WHEN 'true' THEN 'COMPRESSION, ' WHEN 'false' THEN 'NO_COMPRESSION, ' ELSE '' END +
		   CASE WHEN @format = 1 THEN 'FORMAT' ELSE 'NOFORMAT' END + ', ' +
		   CASE WHEN @copy_only = 1 THEN 'COPY_ONLY, ' ELSE '' END +
		   CASE WHEN @init = 1 THEN 'INIT' ELSE 'NOINIT' END +
		   ', NAME = N''' + @database_name + '-Full Database Backup'', ' +
		   CASE WHEN @skip = 1 THEN 'SKIP' ELSE 'NOSKIP' END + ', ' +
		   CASE WHEN @rewind = 1 THEN 'REWIND' ELSE 'NOREWIND' END + ', ' +
		   CASE WHEN @unload = 1 THEN 'UNLOAD' ELSE 'NOUNLOAD' END + ', ' +
		   CASE WHEN @credential NOT LIKE '' THEN 'CREDENTIAL = ''' + @credential + ''', ' ELSE '' END + 'STATS = ' + @stats

PRINT ('')
PRINT ('Starting backup of [' + @database_name + ']...')
PRINT ('')

EXEC (@sql)
