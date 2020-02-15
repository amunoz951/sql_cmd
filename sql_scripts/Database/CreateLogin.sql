DECLARE @sql_login nvarchar(max)
DECLARE @password nvarchar(max)
DECLARE @user_cmd nvarchar(max)

SET @sql_login = '$(templogin)'
SET @password = '$(password)'

-- Add user
SET @user_cmd =
'USE [master]
CREATE LOGIN [' + @sql_login + '] WITH PASSWORD=N''' + @password + ''', DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF'

PRINT ('Creating new SQL Login: ' + @sql_login + '...')
EXEC (@user_cmd)

--PRINT (@user_cmd)
