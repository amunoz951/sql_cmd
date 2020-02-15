
/*

Uses:
How To Use:
               Set the compression description @compressionDesc to the type of compression you would like to use.
               Note - the script will change the compression type regardless of previous compression setting

How it Works
               Builds up a table of all tables\index based on the table filter results.
               Uses a cursor to build dynamic ddl sql statements to change the compression based on @compressionDesc
Note:
*/

Declare @compressionDesc as NvarChar(10)
Set @compressionDesc = 'PAGE'  --PAGE, ROW or NONE

BEGIN
       SET NOCOUNT ON
   SET QUOTED_IDENTIFIER ON

       CREATE TABLE #dbObjects(PK INT IDENTITY
                                                                       NOT NULL
                                                                       PRIMARY KEY,
                                                       schema_name VARCHAR(250),
                                                       object_name VARCHAR(250),
                                                       index_id INT,
                                                       ixName VARCHAR(255),
                                                       ixType VARCHAR(50),
                                                       partition_number INT,
                                                       data_compression_desc VARCHAR(50))
       INSERT INTO dbo.#dbObjects(schema_name,
                                                       object_name,
                                                       index_id,
                                                       ixName,
                                                       ixType,
                                                       partition_number,
                                                       data_compression_desc)
       SELECT S.name,
                  O.name,
                  I.index_id,
                  I.name,
                  I.type_desc,
                  P.partition_number,
                  P.data_compression_desc
       FROM sys.schemas AS S
               JOIN sys.objects AS O ON S.schema_id = O.schema_id
               JOIN sys.indexes AS I ON O.object_id = I.object_id
               JOIN sys.partitions AS P ON I.object_id = P.object_id
                                                               AND I.index_id = P.index_id
       WHERE O.TYPE = 'U' and P.data_compression_desc in ('NONE','ROW')

       -- Determine Compression Estimates
       DECLARE @PK INT,
                       @Schema VARCHAR(150),
                       @object VARCHAR(150),
                       @DAD VARCHAR(25),
                       @partNO INT,
                       @indexID INT,
                       @ixName VARCHAR(250),
                       @SQL NVARCHAR(MAX),
                       @ixType VARCHAR(50)

       -- set the compression
       DECLARE cCompress CURSOR FAST_FORWARD
               FOR SELECT #dbObjects.schema_name,
                                  #dbObjects.object_name,
                                  #dbObjects.partition_number,
                                  #dbObjects.ixName,
                                  #dbObjects.ixType
                       FROM dbo.#dbObjects

       OPEN cCompress

       FETCH cCompress INTO @Schema, @object, @partNO, @ixName, @ixType-- prime the cursor

       WHILE @@Fetch_Status = 0
               BEGIN

                       IF @ixType = 'CLUSTERED'
                       OR @ixType = 'HEAP'
                               BEGIN
                                       SET @SQL = 'ALTER TABLE ' + @Schema + '.' + @object + ' Rebuild with (data_compression = '+@compressionDesc+' )'
                               END
                       ELSE
                               BEGIN
                                       SET @SQL = 'ALTER INDEX ' + @ixName + ' on ' + @Schema + '.' + @object + ' Rebuild with (data_compression = '+@compressionDesc+' )'
                               END

                       PRINT @SQL
                       EXEC sp_executesql @SQL

                       FETCH cCompress INTO @Schema, @object, @partNO, @ixName, @ixType
               END

       CLOSE cCompress
       DEALLOCATE cCompress
END

DROP TABLE #dbObjects
