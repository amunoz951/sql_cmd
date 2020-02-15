SET NOCOUNT ON

DECLARE @databaseName nvarchar(128)
DECLARE @logOnly bit
DECLARE @sqlQuery nvarchar(255)

SET @databaseName = '$(databasename)'
SET @logOnly = '$(logonly)'

IF EXISTS (SELECT * FROM sys.databases WHERE name = @databaseName)
BEGIN
	WITH fs
	AS
	(
		SELECT database_id, type, size * 8.0 / 1024 size
		FROM sys.master_files
	)
	SELECT
		(SELECT sum(size) FROM fs WHERE type = @logOnly and fs.database_id = db.database_id) AS DatabaseSize
	FROM sys.databases db
	WHERE name = @databaseName
END
ELSE
BEGIN
	SELECT NULL AS DatabaseSize
END
