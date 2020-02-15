DECLARE @db_name nvarchar(max)
DECLARE @database_cmd nvarchar(max)
DECLARE @singleuser_cmd nvarchar(max)
DECLARE @replcheck_cmd nvarchar(max)
DECLARE @rowcount int

SET @db_name = '$(databasename)'

IF EXISTS (SELECT * FROM sys.sysdatabases WHERE Name = @db_name)
BEGIN
	SET @singleuser_cmd =
	'ALTER DATABASE [' + @db_name + '] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE'

	SET @database_cmd =
	'DROP DATABASE [' + @db_name + ']
  EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N''' + @db_name + '''
	RAISERROR (''Database [' + @db_name + '] dropped.'', 10, 1) WITH NOWAIT'

	SET @replcheck_cmd =
	N'IF EXISTS (SELECT * FROM [' + @db_name + '].[INFORMATION_SCHEMA].[TABLES]
				WHERE TABLE_NAME = ''syspublications'')
	BEGIN
		SELECT @rowcount=count(*) FROM [' + @db_name + '].[dbo].syspublications
	END
	ELSE
	BEGIN
		SET @rowcount = 0
	END
	'

	RAISERROR ('Checking database for publications...', 10, 1) WITH NOWAIT
	EXEC sp_executesql @replcheck_cmd, N'@rowcount int output', @rowcount output;
	IF (@rowcount > 0)
	BEGIN
		RAISERROR ('Failed to drop database! Database is being used for replication.', 20, 1) WITH LOG
		RETURN
	END
	ELSE
	BEGIN
		RAISERROR ('No replication publications detected. Proceeding...', 10, 1) WITH NOWAIT
	END

	EXEC (@singleuser_cmd)
	EXEC (@database_cmd)

	PRINT ('Command Complete')
END
ELSE
BEGIN
	PRINT ('Database ' + @db_name + ' not found!')
END
