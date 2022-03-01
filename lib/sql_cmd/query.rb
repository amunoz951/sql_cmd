module SqlCmd
  @scripts_cache = "#{SqlCmd.config['paths']['cache']}/sql_cmd/scripts"
  @scripts_cache_windows = @scripts_cache.tr('\\', '/')

  module_function

  # Execute a SQL query and return a dataset, table, row, or scalar, based on the return_type specified
  #   return_type: :all_tables, :first_table, :first_row, :scalar
  #   values: hash of values to replace sqlcmd style variables.
  #           EG: sql_query = SELECT * FROM $(databasename)
  #               values = { 'databasename' => 'my_database_name' }
  #   readonly: if true, sets the connection_string to readonly (Useful with AOAG)
  #   retries: number of times to re-attempt failed queries
  #   retry_delay: how many seconds to wait between retries
  def execute_query(connection_string, sql_query, return_type: :all_tables, values: nil, timeout: 172_800, readonly: false, ignore_missing_values: false, at_timezone: SqlCmd.config['environment']['timezone'], string_to_time_by: '%Y-%m-%d %H:%M:%S %z', retries: 0, retry_delay: 5)
    sql_query = insert_values(sql_query, values) unless values.nil?
    missing_values = sql_query.reverse.scan(/(\)[0-9a-z_]+\(\$)(?!.*--)/i).uniq.join(' ,').reverse # Don't include commented variables
    raise "sql_query has missing variables! Ensure that values are supplied for: #{missing_values}\n" unless missing_values.empty? || ignore_missing_values
    connection_string_updated = connection_string.dup
    connection_string_updated = replace_connection_string_part(connection_string, :applicationintent, 'readonly') if readonly
    raise 'Connection string is nil or incomplete' if connection_string_updated.nil? || connection_string_updated.empty?
    EasyIO.logger.debug "Executing query with connection_string: \n\t#{hide_connection_string_password(connection_string_updated)}"
    EasyIO.logger.debug sql_query if SqlCmd.config['logging']['verbose'] && sql_query.length < 8096
    start_time = Time.now.utc.strftime('%y%m%d_%H%M%S-%L')
    ps_script = <<-EOS.strip
        . "#{@scripts_cache_windows}\\sql_helper_#{start_time}.ps1"

        $connectionString = '#{connection_string_updated}'
        $sqlCommand = '#{sql_query.gsub('\'', '\'\'')}'
        $dataset = Invoke-SQL -timeout #{timeout} -connectionString $connectionString -sqlCommand $sqlCommand
        ConvertSqlDatasetTo-Json -dataset $dataset
      EOS

    ps_script_file = "#{@scripts_cache}/ps_script-thread_id-#{Thread.current.object_id}.ps1"
    FileUtils.mkdir_p ::File.dirname(ps_script_file)
    FileUtils.cp(SqlCmd.config['sql_cmd']['paths']['powershell_helper_script'], "#{@scripts_cache}/sql_helper_#{start_time}.ps1")
    ::File.write(ps_script_file, ps_script)
    retry_count = 0
    begin
      result = ''
      exit_status = ''
      Open3.popen3("powershell -File \"#{ps_script_file}\"") do |_stdin, stdout, stderr, wait_thread|
        buffers = [stdout, stderr]
        queued_buffers = IO.select(buffers) || [[]]
        queued_buffers.first.each do |buffer|
          case buffer
          when stdout
            while (line = buffer.gets)
              stdout_split = line.split('#return_data#:')
              raise "SQL exception: #{stdout_split.first} #{stdout.read} #{stderr.read}\nConnectionString: '#{hide_connection_string_password(connection_string)}'\n#{EasyIO::Terminal.line('=')}\n" if stdout_split.first =~ /error 50000, severity (1[1-9]|2[0-5])/i
              EasyIO.logger.info "SQL message: #{stdout_split.first.strip}" unless stdout_split.first.empty?
              result = stdout_split.last + buffer.read if stdout_split.count > 1
            end
          when stderr
            error_message = stderr.read
            raise "SQL exception: #{error_message}\nConnectionString: '#{hide_connection_string_password(connection_string)}'\n#{'=' * 120}\n" unless error_message.empty?
          end
        end
        exit_status = wait_thread.value
      end
      EasyIO.logger.debug "Script exit status: #{exit_status}"
      EasyIO.logger.debug "JSON result: #{result}"
    rescue
      retry_message = 'Executing SQL query failed! '
      retry_message += if retries == 0
                         'No retries specified. Will not reattempt. '
                       else
                         retry_count < retries ? "Retry #{(retry_count + 1)} of #{retries}" : "All #{retries} retries attempted."
                       end
      EasyIO.logger.info retry_message
      if (retry_count += 1) <= retries
        EasyIO.logger.info "Retrying in #{retry_delay} seconds..."
        sleep(retry_delay)
        retry
      end
      raise
    end

    begin
      convert_powershell_tables_to_hash(result, return_type, at_timezone: at_timezone, string_to_time_by: string_to_time_by)
    rescue # Change it to use terminal size instead of 120 chars in the error here and above
      EasyIO.logger.fatal "Failed to convert SQL data to hash! ConnectionString: '#{hide_connection_string_password(connection_string)}'\n#{EasyIO::Terminal.line('=')}\n"
      raise
    end
  ensure
    ::File.delete "#{@scripts_cache_windows}\\sql_helper_#{start_time}.ps1" if defined?(start_time) && ::File.exist?("#{@scripts_cache_windows}\\sql_helper_#{start_time}.ps1")
  end

  def convert_powershell_tables_to_hash(json_string, return_type = :all_tables, at_timezone: 'UTC', string_to_time_by: '%Y-%m-%d %H:%M:%S %z') # options: :all_tables, :first_table, :first_row
    EasyIO.logger.debug "Output from sql command: #{json_string}" if SqlCmd.config['logging']['verbose']
    parsed_json = JSON.parse(to_utf8(json_string.sub(/[^{\[]*/, ''))) # Ignore any leading characters other than '{' or '['
    timezone_table = parsed_json.delete(parsed_json.keys.last)
    sqlserver_timezone = timezone_table.first.values.first # The last table should be the SQL server's time zone - get the value and remove it from the dataset
    result_hash = if json_string.empty?
                    {}
                  else
                    convert_powershell_time_objects(parsed_json, at_timezone: at_timezone, string_to_time_by: string_to_time_by, timezone_override: sqlserver_timezone)
                  end

    raise 'No tables were returned by specified sql query!' if result_hash.values.first.nil? && return_type != :all_tables
    case return_type
    when :first_table
      result_hash.values.first
    when :first_row
      result_hash.values.first.first
    when :scalar
      return nil if result_hash.values.first.first.nil?
      result_hash.values.first.first.values.first # Return first column of first row of first table
    else
      result_hash.values
    end
  end

  # at_timezone: convert all times to this time zone
  # timezone_override: overwrite the timezone on the time without changing the timestamp value - this occurs before :at_timezone
  # string_to_time_by: Provide the format of the string to be parsed - see https://ruby-doc.org/stdlib-2.4.1/libdoc/time/rdoc/Time.html#method-c-strptime
  #   - Set it to nil or false to not parse any strings
  def convert_powershell_time_objects(value, at_timezone: 'UTC', string_to_time_by: '%Y-%m-%d %H:%M:%S %z', timezone_override: nil, override_strings: false, utc_column: false)
    case value
    when Array
      value.map { |v| convert_powershell_time_objects(v, at_timezone: at_timezone, string_to_time_by: string_to_time_by, timezone_override: timezone_override, utc_column: utc_column) }
    when Hash
      Hash[value.map { |k, v| [k, convert_powershell_time_objects(v, at_timezone: at_timezone, string_to_time_by: string_to_time_by, timezone_override: timezone_override, utc_column: k =~ /utc/i)] }]
    else
      timezone_override = 'UTC' if utc_column
      return value unless value.is_a?(String)
      time_from_js = EasyTime.from_javascript_format(value)
      if time_from_js # A java script time was found
        value = EasyTime.stomp_timezone(time_from_js, timezone_override)
        at_timezone ? EasyTime.at_timezone(value, at_timezone) : value
      else
        return value unless string_to_time_by
        begin
          value = Time.strptime(value, string_to_time_by)
          value = EasyTime.stomp_timezone(value, timezone_override) if override_strings
          at_timezone ? EasyTime.at_timezone(value, at_timezone) : value
        rescue ArgumentError # If an ArgumentError is thrown, the string was not in the :string_to_time_by format expected so it's probably not a time. Return it as is.
          value
        end
      end
    end
  end

  def connection_string_accessible?(connection_string, suppress_failure: true, retries: 3, retry_delay: 5)
    sql_script = 'SELECT @@SERVERNAME AS [ServerName]'
    !execute_query(connection_string, sql_script, return_type: :scalar, retries: retries, retry_delay: retry_delay).nil?
  rescue
    raise unless suppress_failure
    false
  end

  def get_sql_server_settings(connection_string, retries: 3, retry_delay: 15)
    get_sql_settings_script = ::File.read("#{sql_script_dir}/Status/SQLSettings.sql")
    sql_server_settings = execute_query(SqlCmd.remove_connection_string_part(connection_string, :database), get_sql_settings_script, return_type: :first_row, retries: retries, retry_delay: retry_delay)
    EasyIO.logger.debug "sql_server_settings: \n#{JSON.pretty_generate(sql_server_settings)}"
    return nil if sql_server_settings.nil? || sql_server_settings['ServerName'].nil?

    direct_connection_string = connection_string.gsub(sql_server_settings['DataSource'], sql_server_settings['ServerName'])
    application_connection_string = connection_string.gsub(sql_server_settings['ServerName'], sql_server_settings['DataSource'])
    secondary_replica_connection_string = if sql_server_settings['SecondaryReplica'].nil?
                                            nil
                                          else
                                            cnstr = SqlCmd.replace_connection_string_part(connection_string, :server, sql_server_settings['SecondaryReplica'])
                                            SqlCmd.remove_connection_string_part(cnstr, :database)
                                          end
    sql_server_settings['direct_connection_string'] = direct_connection_string # Does not use AlwaysOn listener
    sql_server_settings['connection_string'] = application_connection_string
    sql_server_settings['secondary_replica_connection_string'] = secondary_replica_connection_string
    sql_server_settings['DataDir'] = EasyFormat::Directory.ensure_trailing_slash(sql_server_settings['DataDir'])
    sql_server_settings['LogDir'] = EasyFormat::Directory.ensure_trailing_slash(sql_server_settings['LogDir'])
    sql_server_settings['BackupDir'] = EasyFormat::Directory.ensure_trailing_slash(sql_server_settings['BackupDir'])
    sql_server_settings
  end

  # Substitute sqlcmd style variables with values provided in a hash. Don't include $()
  # Example values: 'varname' => 'Some value', 'varname2' => 'some other value'
  def insert_values(sql_query, values, case_sensitive: false)
    return sql_query if values.nil? || values.all? { |i, _j| i.nil? || i.empty? }

    EasyIO.logger.debug "Inserting variable values into query: #{JSON.pretty_generate(values)}"
    sql_query = to_utf8(sql_query)
    values.each do |key, value|
      regexp_key = case_sensitive ? "$(#{key})" : /\$\(#{Regexp.escape(key)}\)/i
      sql_query.gsub!(regexp_key) { value }
    end

    sql_query
  end

  # Returns the database size or log size in MB
  def get_database_size(connection_string, database_name, log_only: false, retries: 3, retry_delay: 15)
    sql_script = ::File.read("#{sql_script_dir}/Status/DatabaseSize.sql")
    connection_string = remove_connection_string_part(connection_string, :database)
    execute_query(connection_string, sql_script, values: { 'databasename' => database_name, 'logonly' => log_only }, return_type: :scalar, readonly: true, retries: retries, retry_delay: retry_delay).to_f
  end

  # Returns a hash with the following fields: Available_MB, Total_MB, Percent_Free
  def get_sql_disk_space(connection_string, target_folder, retries: 3, retry_delay: 15)
    sql_script = ::File.read("#{sql_script_dir}/Status/DiskSpace.sql")
    execute_query(connection_string, sql_script, return_type: :first_row, values: { 'targetfolder' => target_folder }, retries: retries, retry_delay: retry_delay)
  end

  def create_sql_login(connection_string, user, password, update_existing: false, retries: 3, retry_delay: 5)
    sid_script = ::File.read("#{sql_script_dir}/Security/GetUserSID.sql")
    raise "SQL password for login [#{user}] must not be empty!" if password.nil? || password.empty?
    values = { 'user' => user, 'password' => password }
    EasyIO.logger.info "Checking for existing SqlLogin: #{user}..."
    login_sid = execute_query(connection_string, sid_script, return_type: :scalar, values: values, readonly: true, retries: retries, retry_delay: retry_delay)
    if login_sid.nil?
      sql_script = ::File.read("#{sql_script_dir}/Security/CreateSqlLogin.sql")
      EasyIO.logger.info "Creating SqlLogin: #{user}..."
      result = execute_query(connection_string, sql_script, return_type: :first_row, values: values, retries: retries, retry_delay: retry_delay)
      raise "Failed to create SQL login: [#{user}]!" if result.nil? || result['name'].nil?
      login_sid = result['sid']
    elsif update_existing
      sql_script = ::File.read("#{sql_script_dir}/Security/UpdateSqlPassword.sql")
      EasyIO.logger.info "Login [#{user}] already exists... updating password."
      execute_query(connection_string, sql_script, return_type: :first_row, values: values, retries: retries, retry_delay: retry_delay)
    else
      EasyIO.logger.info "Login [#{user}] already exists..."
    end
    EasyIO.logger.debug "SqlLogin [#{user}] sid: #{login_sid}"
    login_sid
  end

  def sql_login_exists?(connection_string, login, retries: 3, retry_delay: 15)
    sid_script = ::File.read("#{sql_script_dir}/Security/GetUserSID.sql")
    raise "SQL password for login [#{login}] must not be empty!" if password.nil? || password.empty?
    values = { 'user' => login, 'password' => password }
    EasyIO.logger.info "Checking for existing SqlLogin: #{login}..."
    execute_query(connection_string, sid_script, return_type: :scalar, values: values, readonly: true, retries: retries, retry_delay: retry_delay)
  end

  def migrate_logins(start_time, source_connection_string, destination_connection_string, database_name)
    start_time = SqlCmd.unify_start_time(start_time)
    import_script_filename = export_logins(start_time, source_connection_string, database_name)
    if ::File.exist?(import_script_filename)
      EasyIO.logger.info "Importing logins on [#{connection_string_part(destination_connection_string, :server)}]..."
      execute_script_file(destination_connection_string, import_script_filename)
    else
      EasyIO.logger.warn 'Unable to migrate logins. Ensure they exist or manually create them.'
    end
  end

  def export_logins(start_time, connection_string, database_name, remove_existing_logins: true)
    start_time = SqlCmd.unify_start_time(start_time)
    export_folder = "#{SqlCmd.config['paths']['cache']}/sql_cmd/logins"
    server_name = connection_string_part(connection_string, :server)
    import_script_filename = "#{export_folder}/#{EasyFormat::File.windows_friendly_name(server_name)}_#{database_name}_logins.sql"
    if SqlCmd::Database.info(connection_string, database_name)['DatabaseNotFound']
      warning_message = 'Source database was not found'
      unless ::File.exist?(import_script_filename)
        EasyIO.logger.warn "#{warning_message}. Unable to export logins! Ensure logins are migrated or migrate them manually!"
        return import_script_filename
      end

      warning_message += if File.mtime(import_script_filename) >= start_time
                           ' but the import logins script file already exists. Proceeding...'
                         else
                           ' and the import logins script file is out of date. Ensure logins are migrated or migrate them manually!'
                         end
      EasyIO.logger.warn warning_message
      return import_script_filename # TODO: attempt to create logins anyway instead of returning a non-existent script file
    end
    sql_script = ::File.read("#{sql_script_dir}/Security/GenerateCreateLoginsScript.sql")
    values = { 'databasename' => database_name, 'removeexistinglogins' => remove_existing_logins }
    EasyIO.logger.info "Exporting logins associated with database: [#{database_name}] on [#{server_name}]..."
    import_script = execute_query(connection_string, sql_script, return_type: :scalar, values: values, readonly: true, retries: 3)
    return nil if import_script.nil? || import_script.empty?
    FileUtils.mkdir_p(export_folder)
    ::File.write(import_script_filename, import_script)
    import_script_filename
  end

  def validate_logins_script(connection_string, database_name)
    server_name = connection_string_part(connection_string, :server)
    sql_script = ::File.read("#{sql_script_dir}/Security/GenerateValidateLoginsScript.sql")
    values = { 'databasename' => database_name }
    EasyIO.logger.debug "Creating validate logins script for logins associated with database: [#{database_name}] on [#{server_name}]..."
    execute_query(connection_string, sql_script, return_type: :scalar, values: values, readonly: true, retries: 3)
  end

  def execute_script_file(connection_string, import_script_filename, values: nil, readonly: false, retries: 0, retry_delay: 15)
    return nil if import_script_filename.nil? || import_script_filename.empty?
    sql_script = ::File.read(import_script_filename)
    execute_query(connection_string, sql_script, values: values, readonly: readonly, retries: retries, retry_delay: retry_delay)
  end

  def assign_database_roles(connection_string, database_name, user, roles = ['db_owner'], retries: 3, retry_delay: 5) # roles: array of database roles
    values = { 'databasename' => database_name, 'user' => user, 'databaseroles' => roles.join(',') }
    sql_script = ::File.read("#{sql_script_dir}/Security/AssignDatabaseRoles.sql")
    EasyIO.logger.info "Assigning #{roles.join(', ')} access to [#{user}] for database [#{database_name}]..."
    result = execute_query(connection_string, sql_script, return_type: :first_row, values: values, retries: retries, retry_delay: retry_delay)
    raise "Failed to assign SQL database roles for user: [#{user}]!" if result.nil? || result['name'].nil?
    result
  end

  def database_roles_assigned?(connection_string, database_name, user, roles, sid)
    values = { 'databasename' => database_name, 'user' => user, 'sid' => sid }
    validation_script = ::File.read("#{sql_script_dir}/Security/ValidateDatabaseRoles.sql")
    validation_result = execute_query(connection_string, validation_script, return_type: :first_table, values: values, readonly: true, retries: 3)
    roles.all? { |role| validation_result.any? { |row| row['name'].casecmp(role) == 0 } }
  end

  def run_sql_as_job(connection_string, sql_script, sql_job_name, sql_job_owner = 'sa', values: nil, retries: 0, retry_delay: 15)
    sql_script = insert_values(sql_script, values) unless values.nil?
    ensure_sql_agent_is_running(connection_string)
    job_values = { 'sqlquery' => sql_script.gsub('\'', '\'\''),
                   'jobname' => sql_job_name,
                   'jobowner' => sql_job_owner }
    sql_job_script = ::File.read("#{sql_script_dir}/Agent/CreateSQLJob.sql")
    execute_query(connection_string, sql_job_script, values: job_values, retries: retries, retry_delay: retry_delay)
  end

  def ensure_sql_agent_is_running(connection_string)
    sql_script = ::File.read("#{sql_script_dir}/Agent/SQLAgentStatus.sql")
    sql_server_settings = get_sql_server_settings(connection_string)
    raise "SQL Agent is not running on #{sql_server_settings['ServerName']}!" if execute_query(connection_string, sql_script, return_type: :scalar, retries: 3) == 0
  end

  def compress_all_tables(connection_string)
    uncompressed_count_script = ::File.read("#{sql_script_dir}/Status/UncompressedTableCount.sql")
    compress_tables_script = ::File.read("#{sql_script_dir}/Database/CompressAllTables.sql")
    EasyIO.logger.info 'Checking for uncompressed tables...'
    uncompressed_count = execute_query(connection_string, uncompressed_count_script, return_type: :scalar, retries: 3)
    if uncompressed_count > 0
      EasyIO.logger.info "Compressing #{uncompressed_count} tables..."
      execute_query(connection_string, compress_tables_script)
      EasyIO.logger.info 'Compression complete.'
    else
      EasyIO.logger.info 'No uncompressed tables.'
    end
  end

  def update_sql_compatibility(connection_string, database_name, compatibility_level) # compatibility_level options: :sql_2008, :sql_2012, :sql_2016, :sql_2017, :sql_2019
    sql_compatibility_script = ::File.read("#{sql_script_dir}/Database/SetSQLCompatibility.sql")
    compatibility_levels =
      {
        sql_2008: 100,
        sql_2012: 110,
        sql_2014: 120,
        sql_2016: 130,
        sql_2017: 140,
        sql_2019: 150,
      }
    values = { 'databasename' => database_name, 'compatibility_level' => compatibility_levels[compatibility_level] }
    EasyIO.logger.info "Ensuring SQL compatibility is set to #{compatibility_level}..."
    compatibility_result = execute_query(connection_string, sql_compatibility_script, return_type: :scalar, values: values, retries: 3)

    EasyIO.logger.info "Compatibility level is set to #{compatibility_levels.key(compatibility_result)} (#{compatibility_result})"
  end
end
