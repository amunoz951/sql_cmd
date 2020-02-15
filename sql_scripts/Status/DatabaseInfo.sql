SET NOCOUNT ON;

DECLARE @databaseName nvarchar(255)
DECLARE @replicationStatusTable TABLE(
	ReplicationActive bit,
	DatabaseName nvarchar(128)
)
DECLARE @availabilityGroupTable TABLE(
	AvailabilityGroup sysname,
	DatabaseName nvarchar(128),
	AvailabilityGroupRole nvarchar(60),
	ReadableSecondary bit,
	LastRedoneLSN numeric(25,0)
)

SET @databaseName = '$(databasename)'

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name LIKE @databaseName)
BEGIN
	SELECT CAST(1 AS BIT) AS DatabaseNotFound
	RETURN
END;

IF (SELECT state FROM sys.databases WHERE [name] = @databaseName) = 0 -- If the database is not restoring, get this info
BEGIN
	DECLARE @timeout int
	DECLARE @start_time datetime
	DECLARE @message nvarchar(max)
	SET @timeout = 20 -- wait for up to 20 seconds
	SET @start_time = GETDATE()
	WHILE GETDATE() < DATEADD(second, @timeout, @start_time)
	BEGIN TRY
		IF (SELECT secondary_role_allow_connections_desc
				FROM master.sys.availability_groups AS AG
				INNER JOIN master.sys.availability_replicas AS AR
						ON AG.group_id = AR.group_id
				INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates
						ON AR.replica_id = arstates.replica_id AND arstates.role = 2 AND arstates.is_local = 1) = 'NO' OR -- Role 2 = Secondary
				(SELECT state FROM sys.databases WHERE [name] = @databaseName) = 1
		BEGIN
			BREAK
		END
		IF EXISTS (SELECT * FROM [$(databasename)].[INFORMATION_SCHEMA].[TABLES] WHERE TABLE_NAME = 'syspublications')
		BEGIN
			INSERT INTO @replicationStatusTable
			SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END, @databaseName FROM [$(databasename)].[dbo].syspublications
		END
		ELSE
		BEGIN
			INSERT INTO @replicationStatusTable
			SELECT 0, @databaseName
		END
		BREAK
	END TRY
	BEGIN CATCH
		SELECT @message = 'WARNING. Unable to read replication info from database. Database state is ' + CONVERT(nvarchar(5), state) + ' (' + state_desc + '). ' + ERROR_MESSAGE() + ' Retrying in 5 seconds...' FROM sys.databases WHERE [name] = @databaseName
		IF GETDATE() > DATEADD(second, 10, @start_time)
		BEGIN
			RAISERROR (@message, 10, 1) WITH NOWAIT
		END
		WAITFOR DELAY '00:00:05'
	END CATCH
END

IF (SERVERPROPERTY('IsHadrEnabled') IS NOT NULL AND SERVERPROPERTY('IsHadrEnabled') = 1)
BEGIN
	INSERT INTO @availabilityGroupTable
	SELECT AG.name AS [AvailabilityGroup], dbcs.database_name AS [DatabaseName], arstates.role_desc AS [AvailabilityGroupRole],
		CASE WHEN secondary_role_allow_connections_desc = 'NO' THEN 0 ELSE 1 END AS [ReadableSecondary],
		last_redone_lsn
	FROM master.sys.availability_groups AS AG
	LEFT OUTER JOIN master.sys.dm_hadr_availability_group_states as agstates
		ON AG.group_id = agstates.group_id
	INNER JOIN master.sys.availability_replicas AS AR
		ON AG.group_id = AR.group_id
	INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates
		ON AR.replica_id = arstates.replica_id AND arstates.is_local = 1
	INNER JOIN master.sys.dm_hadr_database_replica_cluster_states AS dbcs
		ON arstates.replica_id = dbcs.replica_id AND dbcs.database_name LIKE @databaseName AND dbcs.is_database_joined = 1
	LEFT OUTER JOIN master.sys.dm_hadr_database_replica_states AS dbrs
		ON dbcs.replica_id = dbrs.replica_id AND dbcs.group_database_id = dbrs.group_database_id AND dbrs.synchronization_state = 2 AND ISNULL(dbrs.is_suspended, 0) = 0
END;

WITH fs AS
(
	SELECT database_id, type, size * 8.0 / 1024 size
	FROM sys.master_files
),
DatabaseSizeTable AS
(
	SELECT
		(SELECT sum(size) FROM fs WHERE type = 0 AND fs.database_id = db.database_id) AS DatabaseSize,
		(SELECT sum(size) FROM fs WHERE type = 1 AND fs.database_id = db.database_id) AS LogSize,
		name AS DatabaseName
	FROM sys.databases db
	WHERE name = @databaseName
),
LastBackUp AS
(
	SELECT  bs.database_name,
	        bs.backup_size,
	        bs.backup_start_date,
	        bs.is_copy_only,
	        bmf.physical_device_name,
	        Position = ROW_NUMBER() OVER( PARTITION BY bs.database_name ORDER BY bs.backup_start_date DESC )
	FROM msdb.dbo.backupmediafamily bmf
	JOIN msdb.dbo.backupmediaset bms ON bmf.media_set_id = bms.media_set_id
	JOIN msdb.dbo.backupset bs ON bms.media_set_id = bs.media_set_id
	WHERE bs.[type] = 'D'
),
LastCopyOnlyBackup AS
(
	SELECT TOP 1 database_name, last_lsn AS LastCopyOnlyLSN, backup_set_id AS LastCopyOnlyBackupSetId, backup_start_date AS backup_start_date_copy_only FROM msdb.dbo.backupset
	WHERE [type] = 'D' AND database_name LIKE @databaseName AND is_copy_only = 1
	ORDER BY backup_start_date DESC
),
LastNonCopyOnlyBackup AS
(
	SELECT TOP 1 database_name, last_lsn AS LastNonCopyOnlyLSN, backup_set_id AS LastNonCopyOnlyBackupSetId, backup_start_date AS backup_start_date_non_copy_only FROM msdb.dbo.backupset
	WHERE [type] = 'D' AND database_name LIKE @databaseName AND is_copy_only = 0
	ORDER BY backup_start_date DESC
),
LastLogBackup AS
(
	SELECT database_name, MAX(backup_start_date) AS log_only_backup_start_date
	FROM     msdb..backupset
	WHERE [type] = 'L' AND database_name = @databaseName
	GROUP BY database_name
),
LastRestore AS
(
	SELECT TOP 1 [destination_database_name] AS database_name, restore_date AS LastRestoreDate, r.backup_set_id AS LastRestoreBackupSetID, last_lsn AS LastRestoreLSN, database_backup_lsn AS LastRestoreDatabaseBackupLSN
	FROM msdb.dbo.[restorehistory] r
	INNER JOIN msdb.dbo.backupset b
		ON r.backup_set_id = b.backup_set_id
	WHERE [destination_database_name] = @databaseName AND restore_type = 'D'
	ORDER BY restore_date DESC
),
LastLogRestore AS
(
	SELECT TOP 1 [destination_database_name] AS database_name, restore_date AS LastLogRestoreDate, r.backup_set_id AS LastLogRestoreBackupSetID, last_lsn AS LastLogRestoreLSN, database_backup_lsn AS LastLogRestoreDatabaseBackupLSN
	FROM msdb.dbo.[restorehistory] r
	INNER JOIN msdb.dbo.backupset b
		ON r.backup_set_id = b.backup_set_id
	WHERE [destination_database_name] = @databaseName AND restore_type = 'L'
	ORDER BY restore_date DESC
)
SELECT sd.name,
		CAST(backup_size / 1048576 AS DECIMAL(10, 2) ) AS [BackupSizeMB],
		backup_start_date AS [LastFullBackupDate],
	  backup_start_date_copy_only AS [LastCopyOnlyFullBackupDate],
	  backup_start_date_non_copy_only AS [LastNonCopyOnlyFullBackupDate],
	  log_only_backup_start_date AS [LastLogOnlyBackupDate],
	  physical_device_name AS [BackupFileLocation],
	  LastRestoreDate,
	  LastLogRestoreDate,
		is_copy_only AS [CopyOnly],
	  [sd].[create_date],
	  [sd].[compatibility_level],
	  [sd].[collation_name],
	  [sd].[state_desc],
	  ao.AvailabilityGroup,
	  AvailabilityGroupRole,
		ReadableSecondary,
	  DatabaseSize,
		LogSize,
	  ReplicationActive,
	  LastCopyOnlyBackupSetId,
	  LastCopyOnlyLSN,
	  LastNonCopyOnlyBackupSetId,
	  LastNonCopyOnlyLSN,
	  LastRestoreBackupSetID,
	  LastRestoreLSN,
		LastRestoreDatabaseBackupLSN,
	  LastLogRestoreBackupSetID,
	  LastLogRestoreLSN,
		LastLogRestoreDatabaseBackupLSN,
		LastRedoneLSN
FROM sys.databases AS sd
LEFT JOIN LastBackUp AS lb
    ON sd.name = lb.database_name
    AND Position = 1
LEFT JOIN LastRestore AS lr
    ON sd.name = lr.database_name
LEFT JOIN LastCopyOnlyBackup AS lc
	ON sd.name = lc.database_name
LEFT JOIN LastNonCopyOnlyBackup AS lnc
	ON sd.name = lnc.database_name
LEFT JOIN LastLogBackup AS llb
	ON sd.name = llb.database_name
LEFT JOIN LastLogRestore AS llr
	ON sd.name = llr.database_name
LEFT JOIN @availabilityGroupTable AS ao
	ON sd.name = ao.DatabaseName
LEFT JOIN @replicationStatusTable rs
  ON sd.name = rs.DatabaseName
LEFT JOIN DatabaseSizeTable ds
	ON sd.name = ds.DatabaseName
WHERE sd.name = @databaseName
