SET NOCOUNT ON

DECLARE @dbname nvarchar(128) = DB_NAME()
DECLARE @alter_db_command nvarchar(255)

IF (SELECT compatibility_level FROM sys.databases WHERE name = @dbname) < 110
BEGIN
	SET @alter_db_command = 'ALTER DATABASE [' + @dbname + '] SET COMPATIBILITY_LEVEL = 110'
	exec sp_executesql @alter_db_command
END
SELECT compatibility_level FROM sys.databases WHERE name = @dbname
