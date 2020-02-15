SET NOCOUNT ON

DECLARE @db_name nvarchar(max)
DECLARE @db_state int
DECLARE @bkup_files nvarchar(max)
DECLARE @data_file nvarchar(max)
DECLARE @data_file_logical_name nvarchar(255)
DECLARE @log_file nvarchar(max)
DECLARE @log_file_logical_name nvarchar(255)
DECLARE @database_cmd nvarchar(max)
DECLARE @setproperties_cmd nvarchar(max)
DECLARE @prepareolddb_cmd nvarchar(max)
DECLARE @replcheck_cmd nvarchar(max)
DECLARE @change_logical_names_cmd nvarchar(max)
DECLARE @rowcount int
DECLARE @log_only bit
DECLARE @simple_recovery bit
DECLARE @recovery bit
DECLARE @replace bit
DECLARE @unload bit
DECLARE @stats nvarchar(3)

DECLARE @GetPosition_cmd nvarchar(max)
DECLARE @BackupSetPosition int
DECLARE @Version14Plus bit
DECLARE @SQLMajorVersion nvarchar(128)

SET @db_name = '$(databasename)'
SET @bkup_files = '$(bkupfiles)'
SET @data_file = '$(datafile)'
SET @data_file_logical_name = '$(datafilelogicalname)'
SET @log_file = '$(logfile)'
SET @log_file_logical_name = '$(logfilelogicalname)'
SET @log_only = '$(logonly)'
SET @simple_recovery = '$(simplerecovery)'
SET @recovery = '$(recovery)'
SET @replace = '$(replace)'
SET @unload = '$(unload)'
SET @stats = '$(stats)'

SET @SQLMajorVersion = CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductVersion'))
SET @SQLMajorVersion = LEFT(@SQLMajorVersion, CHARINDEX('.', @SQLMajorVersion) - 1)
SELECT @db_state = [state] FROM sys.databases WHERE [Name] = @db_name

SET @GetPosition_cmd =
'DECLARE @BackupSets TABLE(
	BackupName nvarchar(128),
    BackupDescription nvarchar(255),
    BackupType smallint,
    ExpirationDate datetime,
    Compressed bit,
    Position smallint,
    DeviceType tinyint,
    UserName nvarchar(128),
    ServerName nvarchar(128),
    DatabaseName nvarchar(128),
    DatabaseVersion int,
    DatabaseCreationDate datetime,
    BackupSize numeric(20,0),
    FirstLSN numeric(25,0),
    LastLSN numeric(25,0),
    CheckpointLSN numeric(25,0),
    DatabaseBackupLSN numeric(25,0),
    BackupStartDate datetime,
    BackupFinishDate datetime,
    SortOrder smallint,
    CodePage smallint,
    UnicodeLocaleId int,
    UnicodeComparisonStyle int,
    CompatibilityLevel tinyint,
    SoftwareVendorId int,
    SoftwareVersionMajor int,
    SoftwareVersionMinor int,
    SoftwareVersionBuild int,
    MachineName nvarchar(128),
    Flags int,
    BindingID uniqueidentifier,
    RecoveryForkID uniqueidentifier,
    Collation nvarchar(128),
    FamilyGUID uniqueidentifier,
    HasBulkLoggedData bit,
    IsSnapshot bit,
    IsReadOnly bit,
    IsSingleUser bit,
    HasBackupChecksums bit,
    IsDamaged bit,
    BeginsLogChain bit,
    HasIncompleteMetaData bit,
    IsForceOffline bit,
    IsCopyOnly bit,
    FirstRecoveryForkID uniqueidentifier,
    ForkPointLSN numeric(25,0),
    RecoveryModel nvarchar(60),
    DifferentialBaseLSN numeric(25,0),
    DifferentialBaseGUID uniqueidentifier,
    BackupTypeDescription nvarchar(60),
    BackupSetGUID uniqueidentifier,
    CompressedBackupSize bit,'
    + CASE WHEN (@SQLMajorVersion >= 11) THEN ' Containment tinyint,' ELSE '' END
	+ CASE WHEN (@SQLMajorVersion >= 12) THEN
		'KeyAlgorithm nvarchar(32),
		EncryptorThumbprint varbinary(20),
		EncryptorType nvarchar(32),' ELSE '' END
	+ '--
    -- This field added to retain order by
    --
    Seq int NOT NULL identity(1,1)
)

INSERT INTO @BackupSets
exec (''
RESTORE HEADERONLY
FROM ' + REPLACE(@bkup_files, '''', '''''') + '
WITH NOUNLOAD'')

SELECT TOP(1) @BackupSetPosition = Position FROM @BackupSets
ORDER BY BackupFinishDate DESC
'

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

-- Check if replication is enabled. If so, notify user and exit.
IF EXISTS (SELECT * FROM sys.databases WHERE [Name] = @db_name AND state != 1)
BEGIN
	EXEC sp_executesql @replcheck_cmd, N'@rowcount int output', @rowcount output;
	IF (@rowcount > 0)
	BEGIN
		PRINT ('Replication is enabled on ' + @db_name + '. Remove replication before proceeding...')
		RETURN
	END
END

SET @prepareolddb_cmd =
'
IF EXISTS (SELECT * FROM sys.databases WHERE Name = ''' + @db_name + ''')
BEGIN
	ALTER DATABASE [' + @db_name + '] SET  SINGLE_USER WITH ROLLBACK IMMEDIATE
	ALTER DATABASE [' + @db_name + '] SET  MULTI_USER WITH ROLLBACK IMMEDIATE
END
'

-- Get the position of the most recent backup set.
EXEC sp_executesql @GetPosition_cmd, N'@BackupSetPosition int output', @BackupSetPosition output;
IF (@BackupSetPosition < 1)
BEGIN
	PRINT ('Failed to get backup set position. Using position 1...')
	SET @BackupSetPosition = 1
END

-- Restore database query
SET @database_cmd = 'RESTORE ' + CASE WHEN @log_only = 1 THEN 'LOG' ELSE 'DATABASE' END + ' [' + @db_name + '] FROM ' + @bkup_files + ' WITH FILE = ' + CONVERT(NVARCHAR(5), @BackupSetPosition) + ', ' +
                    CASE WHEN @data_file_logical_name != '' THEN 'MOVE N''' + @data_file_logical_name + ''' TO N''' + @data_file + ''', ' ELSE '' END +
                    CASE WHEN @log_file_logical_name != '' THEN 'MOVE N''' + @log_file_logical_name + ''' TO N''' + @log_file + ''', ' ELSE '' END +
                    CASE WHEN @replace = 1 THEN 'REPLACE, ' ELSE '' END +
                    CASE WHEN @recovery = 1 THEN 'RECOVERY' ELSE 'NORECOVERY' END + ', ' +
                    CASE WHEN @unload = 1 THEN 'UNLOAD' ELSE 'NOUNLOAD' END + ', STATS = ' + @stats

SET @change_logical_names_cmd = 'ALTER DATABASE [' + @db_name + '] MODIFY FILE ( NAME = ' + @data_file_logical_name + ', NEWNAME = ' + @db_name + '_Data );' +
                               'ALTER DATABASE [' + @db_name + '] MODIFY FILE ( NAME = ' + @log_file_logical_name + ', NEWNAME = ' + @db_name + '_Log );'

SET @setproperties_cmd =
'
USE [master]
ALTER DATABASE [' + @db_name + '] SET RECOVERY SIMPLE WITH NO_WAIT
'

IF @db_state != 1 -- If it's not restoring
BEGIN
    EXEC (@prepareolddb_cmd)
END

EXEC (@database_cmd)

IF @db_state != 1
BEGIN
    EXEC (@change_logical_names_cmd)
END

IF @simple_recovery = 1
BEGIN
    EXEC (@setproperties_cmd)
END
