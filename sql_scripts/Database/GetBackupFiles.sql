DECLARE @Depth int
DECLARE @FileFlag int
DECLARE @TargetFolder nvarchar(255)
DECLARE @BackupBaseName nvarchar(255)
DECLARE @LogOnly bit
DECLARE @BackupFileExtension nvarchar(3)

SET @Depth = 1
SET @FileFlag = 1
SET @TargetFolder = '$(targetfolder)'
SET @BackupBaseName = '$(bkupname)'
SET @LogOnly = '$(logonly)'

SET @BackupFileExtension = CASE WHEN @LogOnly = 1 THEN 'trn' ELSE 'bak' END

DECLARE @DirTree TABLE (
    FileName nvarchar(255),
    Depth smallint,
    FileFlag smallint
  )

INSERT INTO @DirTree
EXEC xp_dirtree @TargetFolder, @Depth, @FileFlag

SELECT * FROM @DirTree
WHERE FileFlag = @FileFlag AND (
		[FileName] LIKE @BackupBaseName + '[_][0-9][0-9][0-9][0-9][0-9][0-9].part[0-9]%.' + @BackupFileExtension
		OR [FileName] LIKE @BackupBaseName + '[_][0-9][0-9][0-9][0-9][0-9][0-9].' + @BackupFileExtension
		OR [FileName] LIKE @BackupBaseName + '.part[0-9]%.' + @BackupFileExtension
		OR [FileName] LIKE @BackupBaseName + '.' + @BackupFileExtension
	)
