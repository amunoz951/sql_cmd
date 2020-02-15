SET NOCOUNT ON

USE [master]

DECLARE @newline nvarchar(2)
SET @newline = CHAR(13)+CHAR(10)
DECLARE @sql_hexadecimal nvarchar(max)

SET @sql_hexadecimal = '
DECLARE @binvalue varbinary(256)

SET @binvalue = CONVERT(varbinary(256), ''#(binvalue)'', 1)
DECLARE @charvalue varchar (514)
DECLARE @i int
DECLARE @length int
DECLARE @hexstring char(16)
SELECT @charvalue = ''0x''
SELECT @i = 1
SELECT @length = DATALENGTH (@binvalue)
SELECT @hexstring = ''0123456789ABCDEF''
WHILE (@i <= @length)
BEGIN
  DECLARE @tempint int
  DECLARE @firstint int
  DECLARE @secondint int
  SELECT @tempint = CONVERT(int, SUBSTRING(@binvalue,@i,1))
  SELECT @firstint = FLOOR(@tempint/16)
  SELECT @secondint = @tempint - (@firstint*16)
  SELECT @charvalue = @charvalue +
    SUBSTRING(@hexstring, @firstint+1, 1) +
    SUBSTRING(@hexstring, @secondint+1, 1)
  SELECT @i = @i + 1
END

SELECT @hexvalue = @charvalue
'

DECLARE @sql_password nvarchar(max)
DECLARE @sql_sid nvarchar(max)

DECLARE @name sysname
DECLARE @type varchar (1)
DECLARE @hasaccess int
DECLARE @denylogin int
DECLARE @is_disabled int
DECLARE @PWD_varbinary  varbinary (256)
DECLARE @PWD_string  varchar (514)
DECLARE @SID_varbinary varbinary (85)
DECLARE @SID_string varchar (514)
DECLARE @createlogins_sql varchar (max)
DECLARE @validatelogins_sql varchar (max)
DECLARE @is_policy_checked varchar (3)
DECLARE @is_expiration_checked varchar (3)

DECLARE @defaultdb sysname

DECLARE login_curs CURSOR FOR
  SELECT p.sid, p.name, p.type, p.is_disabled, p.default_database_name, l.hasaccess, l.denylogin
  FROM sys.server_principals p
  LEFT JOIN sys.syslogins l
	ON ( l.name = p.name )
  INNER JOIN [$(databasename)].sys.database_principals d
    ON d.sid = p.sid
  WHERE p.type IN ( 'S', 'G', 'U' ) and d.type_desc = 'SQL_USER' AND d.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')

OPEN login_curs

FETCH NEXT FROM login_curs INTO @SID_varbinary, @name, @type, @is_disabled, @defaultdb, @hasaccess, @denylogin
IF (@@fetch_status = -1)
BEGIN
  SELECT ''
  PRINT 'No logins found.'
  CLOSE login_curs
  DEALLOCATE login_curs
  RETURN
END
SET @validatelogins_sql = ''
SET @createlogins_sql = '/* migrate login script ' + @newline
SET @createlogins_sql += '** Generated ' + CONVERT (varchar, GETDATE()) + ' on ' + @@SERVERNAME + ' */' + @newline
WHILE (@@fetch_status <> -1)
BEGIN
  IF (@@fetch_status <> -2)
  BEGIN
    SET @createlogins_sql += '-- Login: ' + @name + @newline
    SET @createlogins_sql += 'IF NOT EXISTS (SELECT name FROM master.sys.syslogins WHERE name LIKE ''' +  @name  + ''')' + @newline + 'BEGIN' + @newline
    IF (@type IN ( 'G', 'U'))
    BEGIN -- NT authenticated account/group
      SET @createlogins_sql += 'CREATE LOGIN ' + QUOTENAME( @name ) + ' FROM WINDOWS WITH DEFAULT_DATABASE = [' + @defaultdb + ']' + @newline
    END
    ELSE BEGIN -- SQL Server authentication
      -- obtain password and sid
      SET @PWD_varbinary = CAST( LOGINPROPERTY( @name, 'PasswordHash' ) AS varbinary (256) )
	    SET @sql_password = REPLACE(@sql_hexadecimal, '#(binvalue)', CONVERT(varchar(max), @PWD_varbinary, 1))
	    SET @sql_sid = REPLACE(@sql_hexadecimal, '#(binvalue)', CONVERT(varchar(max), @SID_varbinary, 1))
      EXEC sp_executesql @sql_password, N'@hexvalue varchar(514) output', @hexvalue = @PWD_string output;
	    EXEC sp_executesql @sql_sid, N'@hexvalue varchar(514) output', @hexvalue = @SID_string output;

      -- obtain password policy state
      SELECT @is_policy_checked = CASE is_policy_checked WHEN 1 THEN 'ON' WHEN 0 THEN 'OFF' ELSE NULL END FROM sys.sql_logins WHERE name = @name
      SELECT @is_expiration_checked = CASE is_expiration_checked WHEN 1 THEN 'ON' WHEN 0 THEN 'OFF' ELSE NULL END FROM sys.sql_logins WHERE name = @name

      SET @createlogins_sql += 'CREATE LOGIN ' + QUOTENAME( @name ) + ' WITH PASSWORD = ' + @PWD_string + ' HASHED, SID = ' + @SID_string + ', DEFAULT_DATABASE = [' + @defaultdb + ']' + @newline

      IF ( @is_policy_checked IS NOT NULL )
      BEGIN
        SET @createlogins_sql += ', CHECK_POLICY = ' + @is_policy_checked + @newline
      END
      IF ( @is_expiration_checked IS NOT NULL )
      BEGIN
        SET @createlogins_sql += ', CHECK_EXPIRATION = ' + @is_expiration_checked + @newline
      END
    END
    IF (@denylogin = 1)
    BEGIN -- login is denied access
      SET @createlogins_sql += '; DENY CONNECT SQL TO ' + QUOTENAME( @name ) + @newline
    END
    ELSE IF (@hasaccess = 0)
    BEGIN -- login exists but does not have access
      SET @createlogins_sql += '; REVOKE CONNECT SQL TO ' + QUOTENAME( @name ) + @newline
    END
    IF (@is_disabled = 1)
    BEGIN -- login is disabled
      SET @createlogins_sql += '; ALTER LOGIN ' + QUOTENAME( @name ) + ' DISABLE' + @newline
    END
    SET @createlogins_sql += 'END;' + @newline
    SET @validatelogins_sql += 'IF NOT EXISTS (SELECT name FROM master.sys.syslogins WHERE name LIKE ''' +  @name  + ''')' + @newline + 'BEGIN' + @newline
    SET @validatelogins_sql += 'RAISERROR (''Login [' + @name + '] failed to import!'', 20, 1) WITH LOG' + @newline + 'END' + @newline
  END

  FETCH NEXT FROM login_curs INTO @SID_varbinary, @name, @type, @is_disabled, @defaultdb, @hasaccess, @denylogin
   END
CLOSE login_curs
DEALLOCATE login_curs

SELECT @createlogins_sql + @newline + @validatelogins_sql
