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

DECLARE @sql_sid nvarchar(max)
DECLARE @name sysname
DECLARE @type varchar (1)
DECLARE @SID_varbinary varbinary (85)
DECLARE @SID_string varchar (514)
DECLARE @validatelogins_sql varchar (max)

DECLARE login_curs CURSOR FOR
  SELECT p.sid, p.name
  FROM sys.server_principals p
  LEFT JOIN sys.syslogins l
	ON ( l.name = p.name )
  INNER JOIN [$(databasename)].sys.database_principals d
    ON d.sid = p.sid
  WHERE p.type IN ( 'S', 'G', 'U' ) and d.type_desc = 'SQL_USER' AND d.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')

OPEN login_curs

FETCH NEXT FROM login_curs INTO @SID_varbinary, @name
IF (@@fetch_status = -1)
BEGIN
    PRINT 'No logins found.'
    CLOSE login_curs
    DEALLOCATE login_curs
    SELECT 'SELECT CAST(1 AS bit)'
    RETURN
END
SET @validatelogins_sql = ''
WHILE (@@fetch_status <> -1)
BEGIN
    IF (@@fetch_status <> -2)
    BEGIN
        -- obtain sid
        SET @sql_sid = REPLACE(@sql_hexadecimal, '#(binvalue)', CONVERT(varchar(max), @SID_varbinary, 1))
        EXEC sp_executesql @sql_sid, N'@hexvalue varchar(514) output', @hexvalue = @SID_string output;

        SET @validatelogins_sql += 'IF NOT EXISTS (SELECT name FROM master.sys.syslogins WHERE name LIKE ''' +  @name  + ''' AND sid = ' + @SID_string + ')' + @newline + 'BEGIN' + @newline
        SET @validatelogins_sql += 'SELECT CAST(0 AS bit);RETURN' + @newline + 'END' + @newline
    END

    FETCH NEXT FROM login_curs INTO @SID_varbinary, @name
    END
CLOSE login_curs
DEALLOCATE login_curs

SELECT @validatelogins_sql + 'SELECT CAST(1 AS bit)'
