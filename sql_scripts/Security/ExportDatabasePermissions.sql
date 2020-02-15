DECLARE    @DBName sysname = NULL,
    @Principal sysname = NULL,
    @Role sysname = NULL,
    @Type nvarchar(30) = NULL,
    @ObjectName sysname = NULL,
    @Permission sysname = NULL,
    @LoginName sysname = NULL,
    @UseLikeSearch bit = 1,
    @IncludeMSShipped bit = 1,
    @DropTempTables bit = 1,
    @Output varchar(30) = 'Default',
    @Print bit = 0,
	  @IncludeTablePermissions bit = 0,
    @newline nvarchar(2) = CHAR(13)+CHAR(10),
    @sql_script nvarchar(max) = NULL

SET @sql_script = 'USE [$(' + 'databasename)]' + @newline -- Split up variable syntax in order to pass it through as a variable.

SET NOCOUNT ON

SET @DBName = '$(databasename)'
SET @Output = '$(output)' -- 'CreateOnly', 'DropOnly', 'ScriptOnly', 'Report', 'Default'
SET @IncludeTablePermissions = $(includetablepermissions)

DECLARE @Collation nvarchar(75)
SET @Collation = N' COLLATE ' + CAST(SERVERPROPERTY('Collation') AS nvarchar(50))

DECLARE @sql nvarchar(max)
DECLARE @sql2 nvarchar(max)
DECLARE @ObjectList nvarchar(max)
DECLARE @use nvarchar(500)
DECLARE @AllDBNames sysname

IF @DBName IS NULL OR @DBName = N'All'
    BEGIN
        SET @use = ''
        IF @DBName IS NULL
            SET @DBName = DB_NAME()
    END
ELSE
--    IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @DBName)
    IF db_id(@DBName) IS NOT NULL
        SET @use = N'USE ' + QUOTENAME(@DBName) + N';' + NCHAR(13)
    ELSE
        BEGIN
            RAISERROR (N'%s is not a valid database name.',
                            16,
                            1,
                            @DBName)
            RETURN
        END

DECLARE @LikeOperator nvarchar(4)

IF @UseLikeSearch = 1
    SET @LikeOperator = N'LIKE'
ELSE
    SET @LikeOperator = N'='

IF @UseLikeSearch = 1
BEGIN
    IF LEN(ISNULL(@Principal,'')) > 0
        SET @Principal = N'%' + @Principal + N'%'

    IF LEN(ISNULL(@Role,'')) > 0
        SET @Role = N'%' + @Role + N'%'

    IF LEN(ISNULL(@ObjectName,'')) > 0
        SET @ObjectName = N'%' + @ObjectName + N'%'

    IF LEN(ISNULL(@LoginName,'')) > 0
        SET @LoginName = N'%' + @LoginName + N'%'
END

IF @Print = 1 AND @DBName = N'All'
    BEGIN
        PRINT 'DECLARE @AllDBNames sysname'
        PRINT 'SET @AllDBNames = ''master'''
        PRINT ''
    END
--=========================================================================
-- Database Principals
SET @sql =
    N'SELECT ' + CASE WHEN @DBName = 'All' THEN N'@AllDBNames' ELSE N'''' + @DBName + N'''' END + N' AS DBName,' +
    N'   DBPrincipals.principal_id AS DBPrincipalId, DBPrincipals.name AS DBPrincipal, SrvPrincipals.name AS SrvPrincipal, ' + NCHAR(13) +
    N'   DBPrincipals.type, DBPrincipals.type_desc, DBPrincipals.default_schema_name, DBPrincipals.create_date, ' + NCHAR(13) +
    N'   DBPrincipals.modify_date, DBPrincipals.is_fixed_role, ' + NCHAR(13) +
    N'   Authorizations.name AS RoleAuthorization, DBPrincipals.sid, ' + NCHAR(13) +
    N'   CASE WHEN DBPrincipals.is_fixed_role = 0 AND DBPrincipals.name NOT IN (''dbo'',''guest'', ''INFORMATION_SCHEMA'', ''public'', ''sys'') THEN ' + NCHAR(13) +
    CASE WHEN @DBName = 'All' THEN N'   ''USE '' + QUOTENAME(@AllDBNames) + ''; '' + ' + NCHAR(13) ELSE N'' END +
    N'          ''IF DATABASE_PRINCIPAL_ID('''''' + DBPrincipals.name + '''''') IS NOT NULL '' + ' + NCHAR(13) +
    N'           ''DROP '' + CASE DBPrincipals.[type] WHEN ''C'' THEN NULL ' + NCHAR(13) +
    N'               WHEN ''K'' THEN NULL ' + NCHAR(13) +
    N'               WHEN ''R'' THEN ''ROLE'' ' + NCHAR(13) +
    N'               WHEN ''A'' THEN ''APPLICATION ROLE'' ' + NCHAR(13) +
    N'               ELSE ''USER'' END + ' + NCHAR(13) +
    N'           '' ''+QUOTENAME(DBPrincipals.name' + @Collation + N') + '';'' ELSE NULL END AS DropScript, ' + NCHAR(13) +
    N'   CASE WHEN DBPrincipals.is_fixed_role = 0 AND DBPrincipals.name NOT IN (''dbo'',''guest'', ''INFORMATION_SCHEMA'', ''public'', ''sys'') THEN ' + NCHAR(13) +
    CASE WHEN @DBName = 'All' THEN N'   ''USE '' + QUOTENAME(@AllDBNames) + ''; '' + ' +NCHAR(13) ELSE N'' END +
    N'          ''IF DATABASE_PRINCIPAL_ID('''''' + DBPrincipals.name + '''''') IS NULL '' + ' + NCHAR(13) +
    N'           ''CREATE '' + CASE DBPrincipals.[type] WHEN ''C'' THEN NULL ' + NCHAR(13) +
    N'               WHEN ''K'' THEN NULL ' + NCHAR(13) +
    N'               WHEN ''R'' THEN ''ROLE'' ' + NCHAR(13) +
    N'               WHEN ''A'' THEN ''APPLICATION ROLE'' ' + NCHAR(13) +
    N'               ELSE ''USER'' END + ' + NCHAR(13) +
    N'           '' ''+QUOTENAME(DBPrincipals.name' + @Collation + N') END + ' + NCHAR(13) +
    N'           CASE WHEN DBPrincipals.[type] = ''R'' THEN ' + NCHAR(13) +
    N'               ISNULL('' AUTHORIZATION ''+QUOTENAME(Authorizations.name' + @Collation + N'),'''') ' + NCHAR(13) +
    N'               WHEN DBPrincipals.[type] = ''A'' THEN ' + NCHAR(13) +
    N'                   ''''  ' + NCHAR(13) +
    N'               WHEN DBPrincipals.[type] NOT IN (''C'',''K'') THEN ' + NCHAR(13) +
    N'                   ISNULL('' FOR LOGIN '' +
                            QUOTENAME(SrvPrincipals.name' + @Collation + N'),'' WITHOUT LOGIN'') +  ' + NCHAR(13) +
    N'                   ISNULL('' WITH DEFAULT_SCHEMA =  ''+
                            QUOTENAME(DBPrincipals.default_schema_name' + @Collation + N'),'''') ' + NCHAR(13) +
    N'           ELSE '''' ' + NCHAR(13) +
    N'           END + '';'' +  ' + NCHAR(13) +
    N'           CASE WHEN DBPrincipals.[type] NOT IN (''C'',''K'',''R'',''A'') ' + NCHAR(13) +
    N'               AND SrvPrincipals.name IS NULL ' + NCHAR(13) +
    N'               AND DBPrincipals.sid IS NOT NULL ' + NCHAR(13) +
    N'               AND DBPrincipals.sid NOT IN (0x00, 0x01)  ' + NCHAR(13) +
    N'               THEN '' -- Possible missing server principal''  ' + NCHAR(13) +
    N'               ELSE '''' END ' + NCHAR(13) +
    N'       AS CreateScript ' + NCHAR(13) +
    N'FROM sys.database_principals DBPrincipals ' + NCHAR(13) +
    N'LEFT OUTER JOIN sys.database_principals Authorizations ' + NCHAR(13) +
    N'   ON DBPrincipals.owning_principal_id = Authorizations.principal_id ' + NCHAR(13) +
    N'LEFT OUTER JOIN sys.server_principals SrvPrincipals ' + NCHAR(13) +
    N'   ON DBPrincipals.sid = SrvPrincipals.sid ' + NCHAR(13) +
    N'   AND DBPrincipals.sid NOT IN (0x00, 0x01) ' + NCHAR(13) +
    N'WHERE 1=1 '

IF LEN(ISNULL(@Principal,@Role)) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND DBPrincipals.name ' + @LikeOperator + N' ' +
            ISNULL(QUOTENAME(@Principal,N''''),QUOTENAME(@Role,''''))
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND DBPrincipals.name ' + @LikeOperator + N' ISNULL(@Principal,@Role) '

IF LEN(@Type) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND DBPrincipals.type ' + @LikeOperator + N' ' + QUOTENAME(@Type,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND DBPrincipals.type ' + @LikeOperator + N' @Type'

IF LEN(@LoginName) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND SrvPrincipals.name ' + @LikeOperator + N' ' + QUOTENAME(@LoginName,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND SrvPrincipals.name ' + @LikeOperator + N' @LoginName'

IF LEN(@ObjectName) > 0
    BEGIN
        SET @sql = @sql + NCHAR(13) +
        N'   AND EXISTS (SELECT 1 ' + NCHAR(13) +
        N'               FROM sys.all_objects [Objects] ' + NCHAR(13) +
        N'               INNER JOIN sys.database_permissions Permission ' + NCHAR(13) +
        N'                   ON Permission.major_id = [Objects].object_id ' + NCHAR(13) +
        N'               WHERE Permission.major_id = [Objects].object_id ' + NCHAR(13) +
        N'                 AND Permission.grantee_principal_id = DBPrincipals.principal_id ' + NCHAR(13)

        IF @Print = 1
            SET @sql = @sql + N'                 AND [Objects].name ' + @LikeOperator + N' ' + QUOTENAME(@ObjectName,'''')
        ELSE
            SET @sql = @sql + N'                 AND [Objects].name ' + @LikeOperator + N' @ObjectName'

        SET @sql = @sql + N')'
    END

IF LEN(@Permission) > 0
    BEGIN
        SET @sql = @sql + NCHAR(13) +
        N'   AND EXISTS (SELECT 1 ' + NCHAR(13) +
        N'               FROM sys.database_permissions Permission ' + NCHAR(13) +
        N'               WHERE Permission.grantee_principal_id = DBPrincipals.principal_id ' + NCHAR(13)

        IF @Print = 1
            SET @sql = @sql + N'                 AND Permission.permission_name ' + @LikeOperator + N' ' + QUOTENAME(@Permission,'''')
        ELSE
            SET @sql = @sql + N'                 AND Permission.permission_name ' + @LikeOperator + N' @Permission'

        SET @sql = @sql + N')'
    END

IF @IncludeMSShipped = 0
    SET @sql = @sql + NCHAR(13) + N'  AND DBPrincipals.is_fixed_role = 0 ' + NCHAR(13) +
                '  AND DBPrincipals.name NOT IN (''dbo'',''public'',''INFORMATION_SCHEMA'',''guest'',''sys'') '

IF @Print = 1
BEGIN
    PRINT N'-- Database Principals'
    PRINT CAST(@sql AS nvarchar(max))
    PRINT '' -- Spacing before the next print
    PRINT ''
END
ELSE
BEGIN
    IF object_id('tempdb..#DBPrincipals') IS NOT NULL
        DROP TABLE #DBPrincipals

    -- Create temp table to store the data in
    CREATE TABLE #DBPrincipals (
        DBName sysname NULL,
        DBPrincipalId int NULL,
        DBPrincipal sysname NULL,
        SrvPrincipal sysname NULL,
        type char(1) NULL,
        type_desc nchar(60) NULL,
        default_schema_name sysname NULL,
        create_date datetime NULL,
        modify_date datetime NULL,
        is_fixed_role bit NULL,
        RoleAuthorization sysname NULL,
        sid varbinary(85) NULL,
        DropScript nvarchar(max) NULL,
        CreateScript nvarchar(max) NULL
        )

    SET @sql =  @use + N'INSERT INTO #DBPrincipals ' + NCHAR(13) + @sql

    IF @DBName = 'All'
        BEGIN
            -- Declare a READ_ONLY cursor to loop through the databases
            DECLARE cur_DBList CURSOR
            READ_ONLY
            FOR SELECT name FROM sys.databases ORDER BY name

            OPEN cur_DBList

            FETCH NEXT FROM cur_DBList INTO @AllDBNames
            WHILE (@@fetch_status <> -1)
            BEGIN
                IF (@@fetch_status <> -2)
                BEGIN
                    SET @sql2 = N'USE ' + QUOTENAME(@AllDBNames) + N';' + NCHAR(13) + @sql
                    EXEC sp_executesql @sql2,
                        N'@Principal sysname, @Role sysname, @Type nvarchar(30), @ObjectName sysname,
                        @AllDBNames sysname, @Permission sysname, @LoginName sysname',
                        @Principal, @Role, @Type, @ObjectName, @AllDBNames, @Permission, @LoginName
                END
                FETCH NEXT FROM cur_DBList INTO @AllDBNames
            END

            CLOSE cur_DBList
            DEALLOCATE cur_DBList
        END
    ELSE
        EXEC sp_executesql @sql, N'@Principal sysname, @Role sysname, @Type nvarchar(30),
            @ObjectName sysname, @Permission sysname, @LoginName sysname',
            @Principal, @Role, @Type, @ObjectName, @Permission, @LoginName
END
--=========================================================================
-- Database Role Members
SET @sql =
    N'SELECT ' + CASE WHEN @DBName = 'All' THEN N'@AllDBNames' ELSE N'''' + @DBName + N'''' END + N' AS DBName,' +
    N' Users.principal_id AS UserPrincipalId, Users.name AS UserName, Roles.name AS RoleName, ' + NCHAR(13) +
    CASE WHEN @DBName = 'All' THEN N'   ''USE '' + QUOTENAME(@AllDBNames) + ''; '' + ' + NCHAR(13) ELSE N'' END +
    N'   CASE WHEN Users.is_fixed_role = 0 AND Users.name <> ''dbo'' THEN ' + NCHAR(13) +
    N'   ''EXEC sp_droprolemember @rolename = ''+QUOTENAME(Roles.name' + @Collation +
                N','''''''')+'', @membername = ''+QUOTENAME(CASE WHEN Users.name = ''dbo'' THEN NULL
                ELSE Users.name END' + @Collation +
                N','''''''')+'';'' END AS DropScript, ' + NCHAR(13) +
    CASE WHEN @DBName = 'All' THEN N'   ''USE '' + QUOTENAME(@AllDBNames) + ''; '' + ' + NCHAR(13) ELSE N'' END +
    N'   CASE WHEN Users.is_fixed_role = 0 AND Users.name <> ''dbo'' THEN ' + NCHAR(13) +
    N'   ''EXEC sp_addrolemember @rolename = ''+QUOTENAME(Roles.name' + @Collation +
                N','''''''')+'', @membername = ''+QUOTENAME(CASE WHEN Users.name = ''dbo'' THEN NULL
                ELSE Users.name END' + @Collation +
                N','''''''')+'';'' END AS AddScript ' + NCHAR(13) +
    N'FROM sys.database_role_members RoleMembers ' + NCHAR(13) +
    N'JOIN sys.database_principals Users ' + NCHAR(13) +
    N'   ON RoleMembers.member_principal_id = Users.principal_id ' + NCHAR(13) +
    N'JOIN sys.database_principals Roles ' + NCHAR(13) +
    N'   ON RoleMembers.role_principal_id = Roles.principal_id ' + NCHAR(13) +
    N'WHERE 1=1 '

IF LEN(ISNULL(@Principal,'')) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Users.name ' + @LikeOperator + N' '+QUOTENAME(@Principal,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Users.name ' + @LikeOperator + N' @Principal'

IF LEN(ISNULL(@Role,'')) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Roles.name ' + @LikeOperator + N' '+QUOTENAME(@Role,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Roles.name ' + @LikeOperator + N' @Role'

IF LEN(@Type) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Users.type ' + @LikeOperator + N' ' + QUOTENAME(@Type,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Users.type ' + @LikeOperator + N' @Type'

IF LEN(@LoginName) > 0
    BEGIN
        SET @sql = @sql + NCHAR(13) +
        N'   AND EXISTS (SELECT 1 ' + NCHAR(13) +
        N'               FROM sys.server_principals SrvPrincipals ' + NCHAR(13) +
        N'               WHERE Users.sid NOT IN (0x00, 0x01) ' + NCHAR(13) +
        N'                 AND SrvPrincipals.sid = Users.sid ' + NCHAR(13) +
        N'                 AND Users.type NOT IN (''R'') ' + NCHAR(13)
        IF @Print = 1
            SET @sql = @sql + NCHAR(13) + '  AND SrvPrincipals.name ' + @LikeOperator + N' ' + QUOTENAME(@LoginName,'''')
        ELSE
            SET @sql = @sql + NCHAR(13) + '  AND SrvPrincipals.name ' + @LikeOperator + N' @LoginName'

        SET @sql = @sql + N')'
    END

IF LEN(@ObjectName) > 0
    BEGIN
        SET @sql = @sql + NCHAR(13) +
        N'   AND EXISTS (SELECT 1 ' + NCHAR(13) +
        N'               FROM sys.all_objects [Objects] ' + NCHAR(13) +
        N'               INNER JOIN sys.database_permissions Permission ' + NCHAR(13) +
        N'                   ON Permission.major_id = [Objects].object_id ' + NCHAR(13) +
        N'               WHERE Permission.major_id = [Objects].object_id ' + NCHAR(13) +
        N'                 AND Permission.grantee_principal_id = Users.principal_id ' + NCHAR(13)

        IF @Print = 1
            SET @sql = @sql + N'                 AND [Objects].name ' + @LikeOperator + N' ' + QUOTENAME(@ObjectName,'''')
        ELSE
            SET @sql = @sql + N'                 AND [Objects].name ' + @LikeOperator + N' @ObjectName'

        SET @sql = @sql + N')'
    END

IF LEN(@Permission) > 0
    BEGIN
        SET @sql = @sql + NCHAR(13) +
        N'   AND EXISTS (SELECT 1 ' + NCHAR(13) +
        N'               FROM sys.database_permissions Permission ' + NCHAR(13) +
        N'               WHERE Permission.grantee_principal_id = Users.principal_id ' + NCHAR(13)

        IF @Print = 1
            SET @sql = @sql + N'                 AND Permission.permission_name ' + @LikeOperator + N' ' + QUOTENAME(@Permission,'''')
        ELSE
            SET @sql = @sql + N'                 AND Permission.permission_name ' + @LikeOperator + N' @Permission'

        SET @sql = @sql + N')'
    END

IF @IncludeMSShipped = 0
    SET @sql = @sql + NCHAR(13) + N'  AND Users.is_fixed_role = 0 ' + NCHAR(13) +
                '  AND Users.name NOT IN (''dbo'',''public'',''INFORMATION_SCHEMA'',''guest'',''sys'') '

IF @Print = 1
BEGIN
    PRINT N'-- Database Role Members'
    PRINT CAST(@sql AS nvarchar(max))
    PRINT '' -- Spacing before the next print
    PRINT ''
END
ELSE
BEGIN
    IF object_id('tempdb..#DBRoles') IS NOT NULL
        DROP TABLE #DBRoles

    -- Create temp table to store the data in
    CREATE TABLE #DBRoles (
        DBName sysname NULL,
        UserPrincipalId int NULL,
        UserName sysname NULL,
        RoleName sysname NULL,
        DropScript nvarchar(max) NULL,
        AddScript nvarchar(max) NULL
        )

    SET @sql =  @use + NCHAR(13) + 'INSERT INTO #DBRoles ' + NCHAR(13) + @sql

    IF @DBName = 'All'
        BEGIN
            -- Declare a READ_ONLY cursor to loop through the databases
            DECLARE cur_DBList CURSOR
            READ_ONLY
            FOR SELECT name FROM sys.databases ORDER BY name

            OPEN cur_DBList

            FETCH NEXT FROM cur_DBList INTO @AllDBNames
            WHILE (@@fetch_status <> -1)
            BEGIN
                IF (@@fetch_status <> -2)
                BEGIN
                    SET @sql2 = 'USE ' + QUOTENAME(@AllDBNames) + ';' + NCHAR(13) + @sql
                    EXEC sp_executesql @sql2,
                        N'@Principal sysname, @Role sysname, @Type nvarchar(30), @ObjectName sysname,
                        @AllDBNames sysname, @Permission sysname, @LoginName sysname',
                        @Principal, @Role, @Type, @ObjectName, @AllDBNames, @Permission, @LoginName
                END
                FETCH NEXT FROM cur_DBList INTO @AllDBNames
            END

            CLOSE cur_DBList
            DEALLOCATE cur_DBList
        END
    ELSE
        EXEC sp_executesql @sql, N'@Principal sysname, @Role sysname, @Type nvarchar(30),
            @ObjectName sysname, @Permission sysname, @LoginName sysname',
            @Principal, @Role, @Type, @ObjectName, @Permission, @LoginName
END

--=========================================================================
-- Database & object Permissions
SET @ObjectList =
    N'; WITH ObjectList AS (' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName , ' + NCHAR(13) +
    N'       name ' + @Collation + ' AS name, ' + NCHAR(13) +
    N'       database_id AS id, ' + NCHAR(13) +
    N'       ''DATABASE'' AS class_desc,' + NCHAR(13) +
    N'       '''' AS class ' + NCHAR(13) +
    N'   FROM master.sys.databases' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT SCHEMA_NAME(sys.all_objects.schema_id) ' + @Collation + N' AS SchemaName,' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       object_id AS id, ' + NCHAR(13) +
    N'       ''OBJECT_OR_COLUMN'' AS class_desc,' + NCHAR(13) +
    N'       ''OBJECT'' AS class ' + NCHAR(13) +
    N'   FROM sys.all_objects' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT name ' + @Collation + N' AS SchemaName, ' + NCHAR(13) +
    N'       NULL AS name, ' + NCHAR(13) +
    N'       schema_id AS id, ' + NCHAR(13) +
    N'       ''SCHEMA'' AS class_desc,' + NCHAR(13) +
    N'       ''SCHEMA'' AS class ' + NCHAR(13) +
    N'   FROM sys.schemas' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       principal_id AS id, ' + NCHAR(13) +
    N'       ''DATABASE_PRINCIPAL'' AS class_desc,' + NCHAR(13) +
    N'       CASE type_desc ' + NCHAR(13) +
    N'           WHEN ''APPLICATION_ROLE'' THEN ''APPLICATION ROLE'' ' + NCHAR(13) +
    N'           WHEN ''DATABASE_ROLE'' THEN ''ROLE'' ' + NCHAR(13) +
    N'           ELSE ''USER'' END AS class ' + NCHAR(13) +
    N'   FROM sys.database_principals' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       assembly_id AS id, ' + NCHAR(13) +
    N'       ''ASSEMBLY'' AS class_desc,' + NCHAR(13) +
    N'       ''ASSEMBLY'' AS class ' + NCHAR(13) +
    N'   FROM sys.assemblies' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13)

SET @ObjectList = @ObjectList +
    N'   SELECT SCHEMA_NAME(sys.types.schema_id) ' + @Collation + N' AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       user_type_id AS id, ' + NCHAR(13) +
    N'       ''TYPE'' AS class_desc,' + NCHAR(13) +
    N'       ''TYPE'' AS class ' + NCHAR(13) +
    N'   FROM sys.types' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT SCHEMA_NAME(schema_id) ' + @Collation + N' AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       xml_collection_id AS id, ' + NCHAR(13) +
    N'       ''XML_SCHEMA_COLLECTION'' AS class_desc,' + NCHAR(13) +
    N'       ''XML SCHEMA COLLECTION'' AS class ' + NCHAR(13) +
    N'   FROM sys.xml_schema_collections' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       message_type_id AS id, ' + NCHAR(13) +
    N'       ''MESSAGE_TYPE'' AS class_desc,' + NCHAR(13) +
    N'       ''MESSAGE TYPE'' AS class ' + NCHAR(13) +
    N'   FROM sys.service_message_types' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       service_contract_id AS id, ' + NCHAR(13) +
    N'       ''SERVICE_CONTRACT'' AS class_desc,' + NCHAR(13) +
    N'       ''CONTRACT'' AS class ' + NCHAR(13) +
    N'   FROM sys.service_contracts' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       service_id AS id, ' + NCHAR(13) +
    N'       ''SERVICE'' AS class_desc,' + NCHAR(13) +
    N'       ''SERVICE'' AS class ' + NCHAR(13) +
    N'   FROM sys.services' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       remote_service_binding_id AS id, ' + NCHAR(13) +
    N'       ''REMOTE_SERVICE_BINDING'' AS class_desc,' + NCHAR(13) +
    N'       ''REMOTE SERVICE BINDING'' AS class ' + NCHAR(13) +
    N'   FROM sys.remote_service_bindings' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       route_id AS id, ' + NCHAR(13) +
    N'       ''ROUTE'' AS class_desc,' + NCHAR(13) +
    N'       ''ROUTE'' AS class ' + NCHAR(13) +
    N'   FROM sys.routes' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       fulltext_catalog_id AS id, ' + NCHAR(13) +
    N'       ''FULLTEXT_CATALOG'' AS class_desc,' + NCHAR(13) +
    N'       ''FULLTEXT CATALOG'' AS class ' + NCHAR(13) +
    N'   FROM sys.fulltext_catalogs' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       symmetric_key_id AS id, ' + NCHAR(13) +
    N'       ''SYMMETRIC_KEYS'' AS class_desc,' + NCHAR(13) +
    N'       ''SYMMETRIC KEY'' AS class ' + NCHAR(13) +
    N'   FROM sys.symmetric_keys' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       certificate_id AS id, ' + NCHAR(13) +
    N'       ''CERTIFICATE'' AS class_desc,' + NCHAR(13) +
    N'       ''CERTIFICATE'' AS class ' + NCHAR(13) +
    N'   FROM sys.certificates' + NCHAR(13) +
    N'   UNION ALL' + NCHAR(13) +
    N'   SELECT NULL AS SchemaName, ' + NCHAR(13) +
    N'       name ' + @Collation + N' AS name, ' + NCHAR(13) +
    N'       asymmetric_key_id AS id, ' + NCHAR(13) +
    N'       ''ASYMMETRIC_KEY'' AS class_desc,' + NCHAR(13) +
    N'       ''ASYMMETRIC KEY'' AS class ' + NCHAR(13) +
    N'   FROM sys.asymmetric_keys' + NCHAR(13) +
    N'   ) ' + NCHAR(13)

    SET @sql =
    N'SELECT ' + CASE WHEN @DBName = 'All' THEN N'@AllDBNames' ELSE N'''' + @DBName + N'''' END + N' AS DBName,' + NCHAR(13) +
    N'   Grantee.principal_id AS GranteePrincipalId, Grantee.name AS GranteeName, Grantor.name AS GrantorName, ' + NCHAR(13) +
    N'   Permission.class_desc, Permission.permission_name, ' + NCHAR(13) +
    N'   ObjectList.name AS ObjectName, ' + NCHAR(13) +
    N'   ObjectList.SchemaName, ' + NCHAR(13) +
    N'   Permission.state_desc,  ' + NCHAR(13) +
    N'   CASE WHEN Grantee.is_fixed_role = 0 AND Grantee.name <> ''dbo'' THEN ' + NCHAR(13) +
    CASE WHEN @DBName = 'All' THEN N'   ''USE '' + QUOTENAME(@AllDBNames) + ''; '' + ' + NCHAR(13) ELSE N'' END +
    N'   ''REVOKE '' + ' + NCHAR(13) +
    N'   CASE WHEN Permission.[state]  = ''W'' THEN ''GRANT OPTION FOR '' ELSE '''' END + ' + NCHAR(13) +
    N'   '' '' + Permission.permission_name' + @Collation + N' +  ' + NCHAR(13) +
    N'       CASE WHEN Permission.major_id <> 0 THEN '' ON '' + ' + NCHAR(13) +
    N'           ObjectList.class + ''::'' +  ' + NCHAR(13) +
    N'           ISNULL(QUOTENAME(ObjectList.SchemaName),'''') + ' + NCHAR(13) +
    N'           CASE WHEN ObjectList.SchemaName + ObjectList.name IS NULL THEN '''' ELSE ''.'' END + ' + NCHAR(13) +
    N'           ISNULL(QUOTENAME(ObjectList.name),'''') ' + NCHAR(13) +
    N'           ' + @Collation + ' + '' '' ELSE '''' END + ' + NCHAR(13) +
    N'       '' FROM '' + QUOTENAME(Grantee.name' + @Collation + N')  + ''; '' END AS RevokeScript, ' + NCHAR(13) +
    N'   CASE WHEN Grantee.is_fixed_role = 0 AND Grantee.name <> ''dbo'' THEN ' + NCHAR(13) +
    CASE WHEN @DBName = 'All' THEN N'   ''USE '' + QUOTENAME(@AllDBNames) + ''; '' + ' + NCHAR(13) ELSE N'' END +
    N'   CASE WHEN Permission.[state]  = ''W'' THEN ''GRANT'' ELSE Permission.state_desc' + @Collation +
            N' END + ' + NCHAR(13) +
    N'       '' '' + Permission.permission_name' + @Collation + N' + ' + NCHAR(13) +
    N'       CASE WHEN Permission.major_id <> 0 THEN '' ON '' + ' + NCHAR(13) +
    N'           ObjectList.class + ''::'' +  ' + NCHAR(13) +
    N'           ISNULL(QUOTENAME(ObjectList.SchemaName),'''') + ' + NCHAR(13) +
    N'           CASE WHEN ObjectList.SchemaName + ObjectList.name IS NULL THEN '''' ELSE ''.'' END + ' + NCHAR(13) +
    N'           ISNULL(QUOTENAME(ObjectList.name),'''') ' + NCHAR(13) +
    N'           ' + @Collation + N' + '' '' ELSE '''' END + ' + NCHAR(13) +
    N'       '' TO '' + QUOTENAME(Grantee.name' + @Collation + N')  + '' '' +  ' + NCHAR(13) +
    N'       CASE WHEN Permission.[state]  = ''W'' THEN '' WITH GRANT OPTION '' ELSE '''' END +  ' + NCHAR(13) +
    N'       '' AS ''+ QUOTENAME(Grantor.name' + @Collation + N')+'';'' END AS GrantScript ' + NCHAR(13) +
    N'FROM sys.database_permissions Permission ' + NCHAR(13) +
    N'JOIN sys.database_principals Grantee ' + NCHAR(13) +
    N'   ON Permission.grantee_principal_id = Grantee.principal_id ' + NCHAR(13) +
    N'JOIN sys.database_principals Grantor ' + NCHAR(13) +
    N'   ON Permission.grantor_principal_id = Grantor.principal_id ' + NCHAR(13) +
    N'LEFT OUTER JOIN ObjectList ' + NCHAR(13) +
    N'   ON Permission.major_id = ObjectList.id ' + NCHAR(13) +
    N'   AND Permission.class_desc = ObjectList.class_desc ' + NCHAR(13) +
    N'WHERE 1=1 '

IF LEN(ISNULL(@Principal,@Role)) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Grantee.name ' + @LikeOperator + N' ' + ISNULL(QUOTENAME(@Principal,''''),QUOTENAME(@Role,''''))
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Grantee.name ' + @LikeOperator + N' ISNULL(@Principal,@Role) '

IF LEN(@Type) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Grantee.type ' + @LikeOperator + N' ' + QUOTENAME(@Type,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Grantee.type ' + @LikeOperator + N' @Type'

IF LEN(@ObjectName) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND ObjectList.name ' + @LikeOperator + N' ' + QUOTENAME(@ObjectName,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND ObjectList.name ' + @LikeOperator + N' @ObjectName '

IF LEN(@Permission) > 0
    IF @Print = 1
        SET @sql = @sql + NCHAR(13) + N'  AND Permission.permission_name ' + @LikeOperator + N' ' + QUOTENAME(@Permission,'''')
    ELSE
        SET @sql = @sql + NCHAR(13) + N'  AND Permission.permission_name ' + @LikeOperator + N' @Permission'

IF LEN(@LoginName) > 0
    BEGIN
        SET @sql = @sql + NCHAR(13) +
        N'   AND EXISTS (SELECT 1 ' + NCHAR(13) +
        N'               FROM sys.server_principals SrvPrincipals ' + NCHAR(13) +
        N'               WHERE SrvPrincipals.sid = Grantee.sid ' + NCHAR(13) +
        N'                 AND Grantee.sid NOT IN (0x00, 0x01) ' + NCHAR(13) +
        N'                 AND Grantee.type NOT IN (''R'') ' + NCHAR(13)
        IF @Print = 1
            SET @sql = @sql + NCHAR(13) + N'  AND SrvPrincipals.name ' + @LikeOperator + N' ' + QUOTENAME(@LoginName,'''')
        ELSE
            SET @sql = @sql + NCHAR(13) + N'  AND SrvPrincipals.name ' + @LikeOperator + N' @LoginName'

        SET @sql = @sql + ')'
    END

IF @IncludeMSShipped = 0
    SET @sql = @sql + NCHAR(13) + N'  AND Grantee.is_fixed_role = 0 ' + NCHAR(13) +
                '  AND Grantee.name NOT IN (''dbo'',''public'',''INFORMATION_SCHEMA'',''guest'',''sys'') '

IF @Print = 1
    BEGIN
        PRINT '-- Database & object Permissions'
        PRINT CAST(@use AS nvarchar(max))
        PRINT CAST(@ObjectList AS nvarchar(max))
        PRINT CAST(@sql AS nvarchar(max))
    END
ELSE
BEGIN
    IF object_id('tempdb..#DBPermissions') IS NOT NULL
        DROP TABLE #DBPermissions

    -- Create temp table to store the data in
    CREATE TABLE #DBPermissions (
        DBName sysname NULL,
        GranteePrincipalId int NULL,
        GranteeName sysname NULL,
        GrantorName sysname NULL,
        class_desc nvarchar(60) NULL,
        permission_name nvarchar(128) NULL,
        ObjectName sysname NULL,
        SchemaName sysname NULL,
        state_desc nvarchar(60) NULL,
        RevokeScript nvarchar(max) NULL,
        GrantScript nvarchar(max) NULL
        )

    -- Add insert statement to @sql
    SET @sql =  @use + @ObjectList +
                N'INSERT INTO #DBPermissions ' + NCHAR(13) +
                @sql

    IF @DBName = 'All'
        BEGIN
            -- Declare a READ_ONLY cursor to loop through the databases
            DECLARE cur_DBList CURSOR
            READ_ONLY
            FOR SELECT name FROM sys.databases ORDER BY name

            OPEN cur_DBList

            FETCH NEXT FROM cur_DBList INTO @AllDBNames
            WHILE (@@fetch_status <> -1)
            BEGIN
                IF (@@fetch_status <> -2)
                BEGIN
                    SET @sql2 = 'USE ' + QUOTENAME(@AllDBNames) + ';' + NCHAR(13) + @sql
                    EXEC sp_executesql @sql2,
                        N'@Principal sysname, @Role sysname, @Type nvarchar(30), @ObjectName sysname,
                            @AllDBNames sysname, @Permission sysname, @LoginName sysname',
                        @Principal, @Role, @Type, @ObjectName, @AllDBNames, @Permission, @LoginName
                END
                FETCH NEXT FROM cur_DBList INTO @AllDBNames
            END

            CLOSE cur_DBList
            DEALLOCATE cur_DBList
        END
    ELSE
        BEGIN
            EXEC sp_executesql @sql, N'@Principal sysname, @Role sysname, @Type nvarchar(30),
                @ObjectName sysname, @Permission sysname, @LoginName sysname',
                @Principal, @Role, @Type, @ObjectName, @Permission, @LoginName
        END
END

IF @Print <> 1
BEGIN
    IF @Output = 'None'
        PRINT ''
    ELSE IF @Output = 'CreateOnly'
    BEGIN
        SELECT @sql_script += CreateScript + @newline FROM #DBPrincipals WHERE CreateScript IS NOT NULL
        SELECT @sql_script += AddScript + @newline FROM #DBRoles WHERE AddScript IS NOT NULL
        SELECT @sql_script += GrantScript + @newline FROM #DBPermissions WHERE GrantScript IS NOT NULL AND class_desc != CASE WHEN @IncludeTablePermissions = 0 THEN 'OBJECT_OR_COLUMN' ELSE '' END
		SELECT @sql_script AS [RestorePermissionsScript]
    END
    ELSE IF @Output = 'DropOnly'
    BEGIN
        SELECT @sql_script += DropScript + @newline FROM #DBPrincipals WHERE DropScript IS NOT NULL
        SELECT @sql_script += DropScript + @newline FROM #DBRoles WHERE DropScript IS NOT NULL
        SELECT @sql_script += RevokeScript + @newline FROM #DBPermissions WHERE RevokeScript IS NOT NULL AND class_desc != CASE WHEN @IncludeTablePermissions = 0 THEN 'OBJECT_OR_COLUMN' ELSE '' END
		SELECT @sql_script AS [RestorePermissionsScript]
    END
    ELSE IF @Output = 'ScriptOnly'
    BEGIN
        SELECT DropScript, CreateScript FROM #DBPrincipals WHERE DropScript IS NOT NULL OR CreateScript IS NOT NULL
        SELECT DropScript, AddScript FROM #DBRoles WHERE DropScript IS NOT NULL OR AddScript IS NOT NULL
		SELECT RevokeScript, GrantScript FROM #DBPermissions WHERE RevokeScript IS NOT NULL OR GrantScript IS NOT NULL AND class_desc != CASE WHEN @IncludeTablePermissions = 0 THEN 'OBJECT_OR_COLUMN' ELSE '' END
		SELECT @sql_script AS [RestorePermissionsScript]
    END
    ELSE IF @Output = 'Report'
    BEGIN
        SELECT DBName, DBPrincipal, SrvPrincipal, type, type_desc,
                STUFF((SELECT ', ' + #DBRoles.RoleName
                        FROM #DBRoles
                        WHERE #DBPrincipals.DBName = #DBRoles.DBName
                          AND #DBPrincipals.DBPrincipalId = #DBRoles.UserPrincipalId
                        ORDER BY #DBRoles.RoleName
                        FOR XML PATH(''),TYPE).value('.','VARCHAR(MAX)')
                    , 1, 2, '') AS RoleMembership,
                STUFF((SELECT ', ' + #DBPermissions.state_desc + ' ' + #DBPermissions.permission_name + ' on ' +
                            ISNULL('OBJECT:'+#DBPermissions.ObjectName, 'DATABASE:'+#DBPermissions.DBName)
                        FROM #DBPermissions
                        WHERE #DBPrincipals.DBName = #DBPermissions.DBName
                          AND #DBPrincipals.DBPrincipalId = #DBPermissions.GranteePrincipalId
                        ORDER BY #DBPermissions.state_desc, ISNULL(#DBPermissions.ObjectName, #DBPermissions.DBName), #DBPermissions.permission_name
                        FOR XML PATH(''),TYPE).value('.','VARCHAR(MAX)')
                    , 1, 2, '') AS DirectPermissions
        FROM #DBPrincipals
        ORDER BY DBName, type, DBPrincipal
    END
    ELSE -- 'Default' or no match
    BEGIN
        SELECT DBName, DBPrincipal, SrvPrincipal, type, type_desc, default_schema_name,
                create_date, modify_date, is_fixed_role, RoleAuthorization, sid,
                DropScript, CreateScript
        FROM #DBPrincipals ORDER BY DBName, DBPrincipal
        IF LEN(@Role) > 0
            SELECT DBName, UserName, RoleName, DropScript, AddScript
            FROM #DBRoles ORDER BY DBName, RoleName, UserName
        ELSE
            SELECT DBName, UserName, RoleName, DropScript, AddScript
            FROM #DBRoles ORDER BY DBName, UserName, RoleName

		IF LEN(@ObjectName) > 0
			SELECT DBName, GranteeName, GrantorName, class_desc, permission_name, ObjectName,
				SchemaName, state_desc, RevokeScript, GrantScript
			FROM #DBPermissions
			WHERE class_desc != CASE WHEN @IncludeTablePermissions = 0 THEN 'OBJECT_OR_COLUMN' ELSE '' END
			ORDER BY DBName, ObjectName, GranteeName
		ELSE
			SELECT DBName, GranteeName, GrantorName, class_desc, permission_name, ObjectName,
				SchemaName, state_desc, RevokeScript, GrantScript
			FROM #DBPermissions
			WHERE class_desc != CASE WHEN @IncludeTablePermissions = 0 THEN 'OBJECT_OR_COLUMN' ELSE '' END
			ORDER BY DBName, GranteeName, ObjectName
    END

    IF @DropTempTables = 1
    BEGIN
        DROP TABLE #DBPrincipals
        DROP TABLE #DBRoles
        DROP TABLE #DBPermissions
    END
END
