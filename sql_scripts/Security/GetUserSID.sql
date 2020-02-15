SET NOCOUNT ON

SELECT CONVERT(NVARCHAR(max), sid, 1) FROM sys.server_principals WHERE name='$(user)'
