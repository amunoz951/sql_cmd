IF (SELECT recovery_model_desc FROM sys.databases WHERE name = '$(databasename)') NOT LIKE 'FULL'
BEGIN
	PRINT ('Setting recovery to full...')

	ALTER DATABASE [$(databasename)] SET RECOVERY FULL WITH NO_WAIT

	DECLARE @currentrecoverymodel nvarchar(50)
	SELECT @currentrecoverymodel = recovery_model_desc FROM sys.databases WHERE name = '$(databasename)'
	IF (@currentrecoverymodel NOT LIKE 'FULL')
	BEGIN
		RAISERROR('Failed to set recovery to full!', 20, 1) WITH LOG
	END
	PRINT ('Recovery model set to ' + @currentrecoverymodel)
	SELECT CAST(1 AS bit) AS RecoveryModelUpdated
END
ELSE
BEGIN
	SELECT CAST(0 AS bit) AS RecoveryModelUpdated
END
