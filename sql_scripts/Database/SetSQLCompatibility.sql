SET NOCOUNT ON

DECLARE @dbname nvarchar(128)
DECLARE @alter_db_command nvarchar(255)
DECLARE @compatibility_level nvarchar(3)
DECLARE @current_compatibility_level nvarchar(3)
DECLARE @out_msg nvarchar(max)

SET @dbname = '$(databasename)'
SET @compatibility_level = '$(compatibility_level)'
SET @alter_db_command = 'ALTER DATABASE [' + @dbname + '] SET COMPATIBILITY_LEVEL = ' + @compatibility_level

SELECT @current_compatibility_level = compatibility_level FROM sys.databases WHERE name = @dbname

IF @current_compatibility_level < @compatibility_level
BEGIN
	SET @out_msg = 'Updating compatibility level from ' + @current_compatibility_level +  ' to ' + @compatibility_level + '.'
	PRINT @out_msg
	exec sp_executesql @alter_db_command
END
ELSE IF @current_compatibility_level = @compatibility_level
BEGIN
	SET @out_msg = 'Compatibility level already set to ' + @current_compatibility_level + '. No change made.'
	PRINT @out_msg
END
ELSE
BEGIN
  SET @out_msg = 'WARNING! Current compatibility level (' + @current_compatibility_level +  ') is higher than ' + @compatibility_level + '! Downgrading compatibility level!'
	PRINT @out_msg
	exec sp_executesql @alter_db_command
END

SELECT compatibility_level FROM sys.databases WHERE name = @dbname
