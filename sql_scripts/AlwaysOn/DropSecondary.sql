DECLARE @dbname nvarchar(100)

SET @dbname = '$(databasename)'

IF EXISTS (
	SELECT dbcs.database_name
	FROM master.sys.availability_groups AS AG
	LEFT OUTER JOIN master.sys.dm_hadr_availability_group_states as agstates
		ON AG.group_id = agstates.group_id
	INNER JOIN master.sys.availability_replicas AS AR
		ON AG.group_id = AR.group_id
	INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates
		ON AR.replica_id = arstates.replica_id AND arstates.is_local = 1 AND arstates.role = 1 -- arstates.role: 1=Primary, 2=Secondary
	INNER JOIN master.sys.dm_hadr_database_replica_cluster_states AS dbcs
		ON arstates.replica_id = dbcs.replica_id AND dbcs.database_name LIKE @dbname AND dbcs.is_database_joined = 1
	LEFT OUTER JOIN master.sys.dm_hadr_database_replica_states AS dbrs
		ON dbcs.replica_id = dbrs.replica_id AND dbcs.group_database_id = dbrs.group_database_id AND dbrs.synchronization_state = 2 AND ISNULL(dbrs.is_suspended, 0) = 0)
BEGIN
	PRINT ('FATAL ERROR: The specified database to be dropped is in the primary availability group. Secondary database NOT dropped!')
   SELECT 0 AS Success
	RETURN
END
ELSE
BEGIN
	-- If the database does not exist, return success
	IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE ('[' + name + ']' = @dbname OR name = @dbname))
	BEGIN
		PRINT ('Database does not exist on ' + CONVERT(NVARCHAR,@@SERVERNAME) + '.')
    	SELECT CAST(1 AS bit) AS Success
		RETURN
	END

	-- Try dropping database repeatedly until successful or timeout occurs
	DECLARE @dropcmd nvarchar(max) = 'DROP DATABASE [' + @dbname + ']'
	DECLARE @timeout int = 60 -- wait for up to 1 minute

	WHILE EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE ('[' + name + ']' = @dbname OR name = @dbname)) AND @timeout > 0
	BEGIN
		BEGIN TRY
			EXEC sp_executesql @dropcmd
		END TRY
		BEGIN CATCH
			IF @timeout <= 0
			BEGIN
				PRINT ('Failed to drop secondary database from ' + CONVERT(NVARCHAR,@@SERVERNAME) + '!')
				SELECT CAST(0 AS bit) AS Success
				RETURN
			END

			WAITFOR DELAY '00:00:05'
			SET @timeout = @timeout - 5
			RAISERROR ('WARNING. Database could not be dropped. Trying again in 5 seconds...', 10, 1) WITH NOWAIT
		END CATCH
	END

	PRINT ('Secondary database dropped from ' + CONVERT(NVARCHAR,@@SERVERNAME) + '.')
	SELECT CAST(1 AS bit) AS Success
END
