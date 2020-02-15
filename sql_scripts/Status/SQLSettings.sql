SET NOCOUNT ON

DECLARE @datafiledir NVARCHAR(255)
DECLARE @logfiledir NVARCHAR(255)
DECLARE @bkupfiledir NVARCHAR(255)
DECLARE @bkupdrive NVARCHAR(3)
DECLARE @sqlversion sql_variant
DECLARE @sqlcompression bit

-- Get SQL server version
SELECT @sqlversion = SERVERPROPERTY('productversion')

-- Data files section
select Top (1) @datafiledir = reverse(substring(reverse(physical_name), charindex('\', reverse(physical_name)),LEN(physical_name) -1)) from sys.master_files
where type_desc = 'ROWS' and (physical_name like '%nbo%' OR physical_name like '%ml[_]%')

select Top (1) @logfiledir = reverse(substring(reverse(physical_name), charindex('\', reverse(physical_name)),LEN(physical_name) -1)) from sys.master_files
where type_desc = 'LOG' and (physical_name like '%nbo%' OR physical_name like '%ml[_]%')

select Top (1) @datafiledir = coalesce(@datafiledir, reverse(substring(reverse(physical_name), charindex('\', reverse(physical_name)),LEN(physical_name) -1))) from sys.master_files
where type_desc = 'ROWS' and (physical_name like '%sqldata%' OR physical_name like '%\data%')

select Top (1) @logfiledir = coalesce(@logfiledir, reverse(substring(reverse(physical_name), charindex('\', reverse(physical_name)),LEN(physical_name) -1))) from sys.master_files
where type_desc = 'LOG' AND (physical_name like '%sqllog%' OR physical_name like '%\log%')

IF @datafiledir IS NULL
BEGIN
	EXEC master.dbo.xp_instance_regread
	N'HKEY_LOCAL_MACHINE'
	, N'Software\Microsoft\MSSQLServer\MSSQLServer'
	, N'DefaultData'
	, @datafiledir output;
END

IF @datafiledir IS NULL
BEGIN
	EXEC master.dbo.xp_instance_regread
	N'HKEY_LOCAL_MACHINE'
	, N'Software\Microsoft\MSSQLServer\Setup'
	, N'SQLDataRoot'
	, @datafiledir output;
END

IF @logfiledir IS NULL
BEGIN
	EXEC master.dbo.xp_instance_regread
	N'HKEY_LOCAL_MACHINE'
	, N'Software\Microsoft\MSSQLServer\MSSQLServer'
	, N'DefaultLog'
	, @logfiledir output;
END

SET @bkupdrive = LEFT(@datafiledir, 3)
SET @bkupfiledir = @bkupdrive + 'sqlbackup\'

IF @bkupdrive IS NOT NULL
BEGIN
	DECLARE @subdirs TABLE (Directory varchar(200))
	INSERT INTO @subdirs
	EXEC master.dbo.xp_subdirs @bkupdrive

	IF NOT EXISTS(SELECT * FROM @subdirs WHERE Directory = 'sqlbackup')
	BEGIN
		EXEC master.dbo.xp_instance_regread
		N'HKEY_LOCAL_MACHINE'
		, N'Software\Microsoft\MSSQLServer\MSSQLServer'
		, N'BackupDirectory'
		, @bkupfiledir output;
	END
END

-- Compression section
DECLARE @CompressionValue sql_variant

SELECT @CompressionValue = value
FROM sys.configurations
WHERE name = 'backup compression default'

-- Distributor section
DECLARE @distributor_name NVARCHAR(255)

SELECT @distributor_name = datasource from master.dbo.sysservers where srvname = 'repl_distributor'

-- Get AlwaysOn listener
DECLARE @AlwaysOnCmd NVARCHAR(MAX)
DECLARE @ParmDefinition NVARCHAR(MAX)
DECLARE @AlwaysOnEnabled BIT
DECLARE @DataSource NVARCHAR(256)
DECLARE @PrimaryReplica NVARCHAR(256)
DECLARE @AvailabilityGroup NVARCHAR(256)
DECLARE @SecondaryReplica NVARCHAR(256)
DECLARE @SecondaryReplicaAllowConnections NVARCHAR(32)
DECLARE @SeedingMode NVARCHAR(32)

SET @AlwaysOnCmd =
'DECLARE @AGListenerName NVARCHAR(100)
DECLARE @AGListenerPort NVARCHAR(5)
DECLARE @PrimaryReplica NVARCHAR(256)
DECLARE @AvailabilityGroup NVARCHAR(256)
DECLARE @SecondaryReplica NVARCHAR(256)
DECLARE @SecondaryReplicaAllowConnections NVARCHAR(32)

select @AvailabilityGroup = AG.name, @AGListenerName = dns_name, @AGListenerPort = CONVERT(NVARCHAR,port), @PrimaryReplica = agstates.primary_replica
from sys.availability_group_listeners AL
LEFT OUTER JOIN master.sys.dm_hadr_availability_group_states as agstates
		ON AL.group_id = agstates.group_id
INNER JOIN master.sys.availability_groups AS AG
		ON AL.group_id = AG.group_id
INNER JOIN master.sys.availability_replicas AS AR
    ON AG.group_id = AR.group_id
INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates
    ON AR.replica_id = arstates.replica_id AND arstates.role = 1 AND arstates.is_local = 1 -- Role 1 = Primary

-- GET Secondary Server Info
SELECT @SecondaryReplica = AR.replica_server_name, @SecondaryReplicaAllowConnections = AR.secondary_role_allow_connections_desc
FROM master.sys.availability_groups AS AG
INNER JOIN master.sys.availability_replicas AS AR
    ON AG.group_id = AR.group_id
INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates
    ON AR.replica_id = arstates.replica_id AND arstates.role = 2 AND arstates.is_local = 0  -- Role 2 = Secondary
ORDER BY [Name] ASC

SELECT @AlwaysOnEnabledOUT = CASE WHEN @AvailabilityGroup IS NULL THEN 0 ELSE 1 END,
	   @DataSourceOUT = CASE WHEN @AGListenerName IS NULL THEN @@SERVERNAME ELSE @AGListenerName + '','' + @AGListenerPort END,
	   @PrimaryReplicaOUT = @PrimaryReplica,
	   @AvailabilityGroupOUT = @AvailabilityGroup,
	   @SecondaryReplicaOUT = @SecondaryReplica,
		 @SecondaryReplicaAllowConnectionsOUT = @SecondaryReplicaAllowConnections
'

SET @ParmDefinition =
'@AlwaysOnEnabledOUT bit OUTPUT,
@DataSourceOUT NVARCHAR(256) OUTPUT,
@PrimaryReplicaOUT NVARCHAR(256) OUTPUT,
@AvailabilityGroupOUT NVARCHAR(256) OUTPUT,
@SecondaryReplicaOUT NVARCHAR(256) OUTPUT,
@SecondaryReplicaAllowConnectionsOUT NVARCHAR(32) OUTPUT
'

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[sys].[availability_group_listeners]'))
BEGIN
  exec sp_executesql
	  @AlwaysOnCmd,
	  @ParmDefinition,
	  @AlwaysOnEnabledOUT = @AlwaysOnEnabled OUTPUT,
	  @DataSourceOUT = @DataSource OUTPUT,
	  @PrimaryReplicaOUT = @PrimaryReplica OUTPUT,
	  @AvailabilityGroupOUT = @AvailabilityGroup OUTPUT,
	  @SecondaryReplicaOUT = @SecondaryReplica OUTPUT,
	  @SecondaryReplicaAllowConnectionsOUT = @SecondaryReplicaAllowConnections OUTPUT
END

SET @AlwaysOnCmd =
'-- GET Secondary Server Name
SELECT @SeedingModeOUT = AR.seeding_mode_desc
FROM master.sys.availability_groups AS AG
INNER JOIN master.sys.availability_replicas AS AR
    ON AG.group_id = AR.group_id
INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates
    ON AR.replica_id = arstates.replica_id AND arstates.role = 2 AND arstates.is_local = 0  -- Role 2 = Secondary
ORDER BY [Name] ASC
'

SET @ParmDefinition = '@SeedingModeOUT NVARCHAR(32) OUTPUT'
IF EXISTS (SELECT * FROM (SELECT LEFT(CAST(SERVERPROPERTY('productversion') AS VARCHAR), 2) ServerMajorVersion) v WHERE ServerMajorVersion >= 13)
BEGIN
  exec sp_executesql
    @AlwaysOnCmd,
		@ParmDefinition,
    @SeedingModeOUT = @SeedingMode OUTPUT
END

SELECT @datafiledir AS [DataDir],
	   @logfiledir AS [LogDir], @bkupfiledir AS [BackupDir],
	   COALESCE(@CompressionValue, 0) AS [CompressBackup],
	   COALESCE(@distributor_name, 'none') AS [Distributor],
 	   COALESCE(@AlwaysOnEnabled, 0) AS [AlwaysOnEnabled],
	   @AvailabilityGroup AS [AvailabilityGroup],
	   COALESCE(@PrimaryReplica, @@SERVERNAME) AS [ServerName],
 	   COALESCE(@DataSource, @@SERVERNAME) AS [DataSource],
	   @SecondaryReplica AS [SecondaryReplica],
		 @SecondaryReplicaAllowConnections AS [SecondaryReplicaAllowConnections],
		 COALESCE(@SeedingMode, 'MANUAL') AS [SeedingMode],
		 @sqlversion AS [SQLVersion]
