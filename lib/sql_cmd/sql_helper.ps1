function Invoke-SQL {
    param(
        [string] $connectionString = $(throw "Please specify a connection string"),
        [string] $sqlCommand = $(throw "Please specify a query."),
        [string] $timeout = 172800
      )

    $sqlCommands = $sqlCommand -split "\n\s*GO\s*\n" # Split the query on each GO statement.
    $connection = new-object system.data.SqlClient.SQLConnection($connectionString)
    $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] { param($sender, $event) Write-host $event.Message }
    $connection.add_InfoMessage($handler)
    $connection.FireInfoMessageEventOnUserErrors = $true
    $command = new-object system.data.sqlclient.sqlcommand($sqlCommand,$connection)
    $command.CommandTimeout = $timeout
    $adapter = New-Object System.Data.sqlclient.sqlDataAdapter $command
    $tables = [System.Collections.ArrayList]@()
    $tableCount = 0
    $connection.Open()
    $timeZoneCommand = 'BEGIN TRY
        DECLARE @TimeZone VARCHAR(50)
        EXEC MASTER.dbo.xp_regread ''HKEY_LOCAL_MACHINE'',''SYSTEM\CurrentControlSet\Control\TimeZoneInformation'',''TimeZoneKeyName'',@TimeZone OUT
        SELECT * FROM (SELECT @TimeZone AS TimeZone) AS SQLServerTimeZone
      END TRY
      BEGIN CATCH
        SELECT * FROM (SELECT ''FailedToReadTimeZone'' AS TimeZone) AS SQLServerTimeZone
      END CATCH'

    foreach ($sqlcmd in $sqlCommands) {
      if ([string]::IsNullOrEmpty($sqlcmd)) { continue }
      $command.CommandText = $sqlcmd
      $dataset = New-Object System.Data.DataSet
      $adapter.Fill($dataSet) | Out-Null
      foreach ($table in $dataSet.Tables) {
        $tableCount++
        $table.TableName = "Table$tableCount"
        $tables.Add($table) | Out-Null
      }
    }

    # Get the server's time zone
    $command.CommandText = $timeZoneCommand
    $dataset = New-Object System.Data.DataSet
    $adapter.Fill($dataSet) | Out-Null
    foreach ($table in $dataSet.Tables) {
      $tableCount++
      $table.TableName = "Table$tableCount"
      $tables.Add($table) | Out-Null
    }

    $connection.Close()
    $tables
}

function ConvertSqlDatasetTo-Json {
    param(
        [object] $dataset = $(throw "Please specify a dataset")
      )

    $convertedTables = '#return_data#:{ '
    foreach ($table in $dataset.DataSet.Tables) {
      $convertedTable = ($table | select $table.Columns.ColumnName) | ConvertTo-Json -Compress
      if (!$convertedTable) { $convertedTable = '[]' }
      if (!$convertedTable.StartsWith('[')) { $convertedTable = "[ $convertedTable ]" } # Convert to Array if it's not
      $convertedTables += '"' + $table.TableName + '": ' + $convertedTable + ','
    }
    $convertedTables.TrimEnd(',') + ' }'
}

function Build-ConnectionString{
    param(
        [string] $vip = $(throw "Please specify a server vip"),
        [int] $port,
        [string] $database = $(throw "Please specify a database"),
        [string] $username,
        [string] $password
    )
    $target = $vip
    if($port -ne $null -and $port -ne 0){
        $target = "$target,$port"
    }
    if(($username -ne "") -and ($password -ne "")){
        $credentials = "Integrated Security=False;User ID=$username;Password=$password"
    }
    else{
        Write-Warning "no credentials provided.  falling back to integrated security"
        $credentials = "Integrated Security=True;"
    }
    return "Data Source=$target; Initial Catalog=$database; $credentials"
}
