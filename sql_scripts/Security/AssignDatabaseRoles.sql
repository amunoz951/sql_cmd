USE [$(databasename)]

DECLARE @login_name nvarchar(max)
DECLARE @db_name nvarchar(max)
DECLARE @user_cmd nvarchar(max)
DECLARE @roles nvarchar(max)
DECLARE @roles_cmd nvarchar(max)
DECLARE @database_roles TABLE (
	DatabaseRole nvarchar(max)
)

SET @login_name = '$(user)'
SET @db_name = '$(databasename)'
SET @roles = '$(databaseroles)' -- comma separated list - do not use commas in the values

SET @roles_cmd = '
DECLARE @database_roles TABLE ( DatabaseRole nvarchar(max) )
INSERT INTO @database_roles ( DatabaseRole )
VALUES ( ' + '''' + REPLACE(@roles, ',', '''), (''') + ''')
SELECT RTRIM(LTRIM(DatabaseRole)) FROM @database_roles'
INSERT INTO @database_roles
exec sp_executesql @roles_cmd

-- Add user and assign security query
SET @user_cmd =
'
USE [' + @db_name + ']
IF  EXISTS (SELECT * FROM sys.schemas WHERE name = N''' + @login_name + ''')
BEGIN
	DROP SCHEMA [' + @login_name + ']
END

IF EXISTS (SELECT * FROM sys.database_principals WHERE name=N''' + @login_name + ''')
BEGIN
	DROP USER [' + @login_name + ']
END

CREATE USER [' + @login_name + '] FOR LOGIN [' + @login_name + '];'

SELECT @user_cmd += 'EXEC sp_addrolemember N''' + DatabaseRole + ''', N''' + @login_name + ''';' FROM @database_roles

EXEC (@user_cmd)

SELECT * FROM sys.database_principals WHERE name=@login_name
