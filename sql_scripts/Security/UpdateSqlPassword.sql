SET NOCOUNT ON

DECLARE @login_name nvarchar(max)
DECLARE @password nvarchar(max)
DECLARE @user_cmd nvarchar(max)

SET @login_name = '$(user)'
SET @password = '$(password)'

-- Create login
SET @user_cmd =
'USE [master]
IF NOT EXISTS (SELECT name FROM master.dbo.syslogins WHERE name=''' + @login_name + ''')
BEGIN
		CREATE LOGIN [' + @login_name + '] WITH PASSWORD=N''' + @password + ''', DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
END;

ALTER LOGIN [' + @login_name + '] WITH PASSWORD=N''' + @password + ''', DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;
ALTER LOGIN [' + @login_name + '] ENABLE;
'

EXEC (@user_cmd)

SELECT name, CONVERT(NVARCHAR(max), sid, 1) FROM master.dbo.syslogins WHERE name=@login_name
