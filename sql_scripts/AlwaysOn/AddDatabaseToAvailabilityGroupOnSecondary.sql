USE [master]

SET NOCOUNT ON

DECLARE @timeout int
DECLARE @start_time datetime

RAISERROR ('Waiting for the replica to start communicating...', 10, 1) WITH NOWAIT
-- Wait for the replica to start communicating
BEGIN TRY
	DECLARE @conn bit
	DECLARE @replica_id uniqueidentifier
	DECLARE @group_id uniqueidentifier
	SET @conn = 0
	SET @timeout = 300 -- wait for 5 minutes

	IF (serverproperty('IsHadrEnabled') = 1)
		AND (isnull((SELECT member_state FROM master.sys.dm_hadr_cluster_members WHERE upper(member_name COLLATE Latin1_General_CI_AS) = upper(CAST(serverproperty('ComputerNamePhysicalNetBIOS') AS nvarchar(256)) COLLATE Latin1_General_CI_AS)), 0) <> 0)
		AND (isnull((SELECT state FROM master.sys.database_mirroring_ENDpoints), 1) = 0)
	BEGIN
		SELECT @group_id = ags.group_id FROM master.sys.availability_groups AS ags WHERE name = N'$(availabilitygroupname)'
		SELECT @replica_id = replicas.replica_id FROM master.sys.availability_replicas AS replicas WHERE upper(replicas.replica_server_name COLLATE Latin1_General_CI_AS) = upper(@@SERVERNAME COLLATE Latin1_General_CI_AS) AND group_id = @group_id
		WHILE @conn <> 1 AND @timeout > 0
		BEGIN
			SET @conn = isnull((SELECT connected_state FROM master.sys.dm_hadr_availability_replica_states AS states WHERE states.replica_id = @replica_id), 1)
			IF @conn = 1
			BEGIN
				-- exit loop when the replica is connected, or IF the query cannot find the replica status
				BREAK
			END
			WAITFOR DELAY '00:00:10'
			SET @timeout = @timeout - 10
			RAISERROR ('Retrying communication in 10 seconds...', 10, 1) WITH NOWAIT
		END
		PRINT ('Communication established.')
	END
END TRY
BEGIN CATCH
	PRINT ('WARNING. Communication was NOT established. Check to ensure AlwaysOn Availability was configured correctly!')
	-- IF the wait loop fails, do not stop execution of the alter database statement
END CATCH

SET @timeout = 60 -- wait for up to 60 seconds
SET @start_time = GETDATE()
WHILE GETDATE() < DATEADD(second, @timeout, @start_time)
BEGIN
	BEGIN TRY
		IF EXISTS(SELECT * FROM master.sys.availability_groups AS AG
							INNER JOIN master.sys.dm_hadr_database_replica_states dbrs
								ON AG.group_id = dbrs.group_id
							WHERE DB_NAME(database_id) = '$(databasename)' AND name = '$(availabilitygroupname)' AND dbrs.is_local = 1 AND dbrs.synchronization_state = 2 AND ISNULL(dbrs.is_suspended, 0) = 0)
		BEGIN
			PRINT ('Database is in availability group and synchronized.')
			BREAK
		END
		ALTER DATABASE [$(databasename)] SET HADR AVAILABILITY GROUP = [$(availabilitygroupname)];
		BREAK
	END TRY
	BEGIN CATCH
		RAISERROR ('WARNING. Unable to add database to availability group. Retrying in 5 seconds...', 10, 1) WITH NOWAIT
	END CATCH

	WAITFOR DELAY '00:00:05'
END

IF NOT EXISTS(SELECT * FROM master.sys.availability_groups AS AG
							INNER JOIN master.sys.dm_hadr_database_replica_states dbrs
								ON AG.group_id = dbrs.group_id
							WHERE DB_NAME(database_id) = '$(databasename)' AND name = '$(availabilitygroupname)' AND dbrs.is_local = 1 AND dbrs.synchronization_state = 2 AND ISNULL(dbrs.is_suspended, 0) = 0)
BEGIN
	ALTER DATABASE [$(databasename)] SET HADR AVAILABILITY GROUP = [$(availabilitygroupname)]; -- Run it one more time if it timed out and don't catch any error
END
