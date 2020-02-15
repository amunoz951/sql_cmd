module SqlCmd
  module Database
    module_function

    def backup(backup_start_time, connection_string, database_name, backup_folder: nil, backup_basename: nil, asynchronous: false, options: {})
      EasyIO.logger.header "Database #{options['copyonly'] ? 'Full Backup' : 'Backup'}"
      backup_start_time = ::Time.parse(backup_start_time) unless backup_start_time.is_a?(Time)
      raise 'Backup attempted before scheduled time!' if Time.now < backup_start_time

      options = default_backup_options.merge(options)
      free_space_threshold = SqlCmd.config['sql_cmd']['backups']['free_space_threshold']
      sql_server_settings = SqlCmd.get_backup_sql_server_settings(connection_string)
      options['compressbackup'] ||= sql_server_settings['CompressBackup'] ? SqlCmd.config['sql_cmd']['backups']['compress_backups'] : false
      backup_basename = "#{database_name}_#{EasyFormat::DateTime.yyyymmdd(backup_start_time)}" if backup_basename.nil? || backup_basename.empty?
      original_basename = backup_basename

      # Set backup_folder to default_destination if it is not set and the backup_to_host_sql_server flag is not set
      # If the backup_to_host_sql_server flag is set, use the server's default location
      if backup_folder.nil? || backup_folder.empty?
        backup_folder = SqlCmd.config['sql_cmd']['backups']['backup_to_host_sql_server'] ? sql_server_settings['BackupDir'] : SqlCmd.config['sql_cmd']['backups']['default_destination']
      end

      # Check if a backup is currently running
      job_status = SqlCmd::Agent::Job.status(connection_string, "Backup: #{database_name}")['LastRunStatus']
      monitor_backup(backup_start_time, connection_string, database_name, backup_folder: backup_folder, backup_basename: backup_basename, log_only: options['logonly']) if job_status == 'Running'

      # Check if there's a current backup and if so, return without creating another backup
      backup_files, backup_basename = existing_backup_files(sql_server_settings, backup_folder: backup_folder, backup_basename: backup_basename, log_only: options['logonly'])
      unless backup_files.empty?
        sql_backup_header = SqlCmd.get_sql_backup_headers(connection_string, backup_files).first
        return :current if SqlCmd.check_header_date(sql_backup_header, backup_start_time, :prebackup) == :current
        backup_basename = "#{original_basename}_#{EasyFormat::DateTime.hhmmss}" # use a unique name if backup already exists
      end
      backup_basename = original_basename if options['init'] # use original backup name if init (overwrite) is specified

      EasyIO.logger.info "Checking size of [#{database_name}] database..."
      backup_size = SqlCmd.get_database_size(connection_string, database_name, log_only: options['logonly'])
      EasyIO.logger.info "#{options['logonly'] ? 'Log' : 'Database'} size: #{backup_size == 0 ? 'Not found' : backup_size.round(2)} MB"

      # Check disk space
      if backup_folder == sql_server_settings['BackupDir']
        EasyIO.logger.info "Checking disk space on #{sql_server_settings['DataSource']}..."
        sql_server_disk_space = SqlCmd.get_sql_disk_space(sql_server_settings['connection_string'], sql_server_settings['BackupDir'])
        sql_server_free_space = sql_server_disk_space['Available_MB'].to_f
        sql_server_disk_size = sql_server_disk_space['Total_MB'].to_f
        backup_folder = sql_server_settings['default_destination'] unless sufficient_free_space?(sql_server_disk_size, sql_server_free_space, backup_size, free_space_threshold)
      end

      if backup_folder != sql_server_settings['BackupDir'] # If the backup folder is not the host sql box or there was not enough space on the sql box, check for free space where specified
        EasyIO.logger.info "Checking free space for #{backup_folder}..."
        specified_backup_folder_free_space = EasyIO::Disk.free_space(backup_folder)
        specified_backup_folder_disk_size = EasyIO::Disk.size(backup_folder)
        sufficient_space = sufficient_free_space?(specified_backup_folder_disk_size, specified_backup_folder_free_space, backup_size, free_space_threshold)
        raise "Failed to backup database #{database_name} due to insufficient space! Must have greater than #{free_space_threshold}% space remaining after backup." unless sufficient_space
      end

      run_backup_as_job(connection_string, database_name, backup_folder: backup_folder, backup_basename: backup_basename, options: options)
      monitor_backup(backup_start_time, connection_string, database_name, backup_folder: backup_folder, backup_basename: backup_basename, log_only: options['logonly']) unless asynchronous
    end

    def sufficient_free_space?(disk_size, free_space, backup_size, free_space_threshold)
      free_space_percentage = disk_size.nil? ? 'unknown ' : (free_space / disk_size) * 100
      free_space_post_backup = free_space - backup_size
      free_space_percentage_post_backup = disk_size.nil? ? 'unknown ' : (free_space_post_backup / disk_size) * 100
      EasyIO.logger.info "Free space on backup drive: #{free_space.round(2)} MB / #{free_space_percentage.round(2)}%"
      EasyIO.logger.info "Estimated free space on backup drive after backup: #{free_space_post_backup.round(2)} MB / #{free_space_percentage_post_backup.round(2)}%"
      free_space_percentage_post_backup >= free_space_threshold
    end

    def delete_backup_and_restore_history(connection_string, database_name)
      sql_script = "EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'#{database_name}'"
      SqlCmd.execute_query(connection_string, sql_script, retries: 3)
    end

    def existing_backup_files(sql_server_settings, backup_folder: nil, backup_basename: nil, log_only: false)
      if backup_folder.nil? || backup_folder.empty?
        default_destination = SqlCmd.config['sql_cmd']['backups']['default_destination']
        EasyIO.logger.info "Checking for existing backup files in #{default_destination}..."
        primary_backup_files, backup_basename = SqlCmd.get_unc_backup_files(sql_server_settings, default_destination, backup_basename, log_only: log_only)
        return [primary_backup_files, backup_basename] unless primary_backup_files.empty?
        EasyIO.logger.info "Checking for existing backup files on #{sql_server_settings['DataSource']}..."
        SqlCmd.sql_server_backup_files(sql_server_settings, backup_basename, log_only: log_only)
      else
        SqlCmd.get_unc_backup_files(sql_server_settings, backup_folder, backup_basename, log_only: log_only)
      end
    end

    def migrate(start_time, source_connection_string, database_name, destination_connection_string, backup_folder: nil, backup_basename: nil, permissions_only: false, force_restore: false, options: {})
      EasyIO.logger.header 'Database Migration'
      start_time = ::Time.parse(start_time) unless start_time.is_a?(Time)
      source_connection_string = SqlCmd.remove_connection_string_part(source_connection_string, :database)
      database_info = SqlCmd::Database.info(source_connection_string, database_name)
      destination_database_info = SqlCmd::Database.info(destination_connection_string, database_name)
      destination_server_name = SqlCmd.connection_string_part(destination_connection_string, :server)
      source_server_name = SqlCmd.get_sql_server_settings(source_connection_string)[destination_server_name.include?(',') ? 'DataSource' : 'ServerName']
      source_sql_version = SqlCmd.get_sql_server_settings(source_connection_string)['SQLVersion']
      destination_sql_version = SqlCmd.get_sql_server_settings(destination_connection_string)['SQLVersion']
      validate_restorability(source_sql_version, destination_sql_version, source_type: :database) unless permissions_only
      raise "Failed to migrate database! Destination and source servers are the same! (#{source_server_name})" if source_server_name =~ /#{Regexp.escape(destination_server_name)}/i
      SqlCmd.migrate_logins(start_time, source_connection_string, destination_connection_string, database_name)
      if database_info['DatabaseNotFound'] && (destination_database_info['DatabaseNotFound'] || destination_database_info['DatabaseRestoring'] || destination_database_info['LastRestoreDate'] < start_time)
        raise "Failed to migrate database. Database [#{database_name}] does not exist on [#{source_server_name}]!"
      end
      SqlCmd::Database.duplicate(start_time, source_connection_string, database_name, destination_connection_string, database_name, backup_folder: backup_folder, backup_basename: backup_basename, force_restore: force_restore, options: options) unless permissions_only

      SqlCmd::AlwaysOn.remove_from_availability_group(source_connection_string, database_name)
      SqlCmd::Database.drop(source_connection_string, database_name)
    end

    def restore(backup_start_time, connection_string, database_name, backup_folder: nil, backup_basename: nil, full_backup_method: nil, force_restore: false, asynchronous: false, overwrite: true, options: {})
      raise 'backup_basename parameter is required when restoring a database!' if backup_basename.nil?
      EasyIO.logger.header 'Database Restore'
      backup_folder ||= SqlCmd.config['sql_cmd']['backups']['default_destination']
      backup_start_time = ::Time.parse(backup_start_time) unless backup_start_time.is_a?(Time)
      options = default_restore_options.merge(options)
      connection_string = SqlCmd.remove_connection_string_part(connection_string, :database)
      sql_server = SqlCmd.connection_string_part(connection_string, :server)
      database_info = info(connection_string, database_name)
      import_script_filename = nil
      unless database_info['DatabaseNotFound']
        raise "Failed to restore database: [#{database_name}] on [#{sql_server}]! Database already exists!" unless overwrite || force_restore
        import_script_filename = export_security(backup_start_time, connection_string, database_name) unless options['logonly'] || database_info['state_desc'] != 'ONLINE'
      end
      free_space_threshold = SqlCmd.config['sql_cmd']['backups']['free_space_threshold']
      EasyIO.logger.debug "Getting SQL server settings for #{sql_server}..."
      sql_server_settings = SqlCmd.get_backup_sql_server_settings(connection_string)
      EasyIO.logger.info "Reading backup files in #{backup_folder}..."
      non_log_backup_files = SqlCmd.backup_sets_from_unc_path(sql_server_settings, backup_folder, backup_basename).values.first
      raise "Backup files do not exist in #{backup_folder} named #{backup_basename}.bak or .partX.bak" if non_log_backup_files.nil?
      database_backup_header = SqlCmd.get_sql_backup_headers(connection_string, non_log_backup_files).first
      source_sql_version = "#{database_backup_header['SoftwareVersionMajor']}.#{database_backup_header['SoftwareVersionMinor']}"
      destination_sql_version = sql_server_settings['SQLVersion']
      validate_restorability(source_sql_version, destination_sql_version, source_type: :backup)
      backup_sets = SqlCmd.backup_sets_from_unc_path(sql_server_settings, backup_folder, backup_basename, log_only: options['logonly'], database_backup_header: database_backup_header, restored_database_lsn: database_info['LastRestoreLSN'])
      backup_extension = options['logonly'] ? '.trn' : '.bak'
      raise 'Log backups are not complete. Restore missing log backup or delete existing backups and try again' if backup_sets.nil?
      raise "Backup files do not exist in #{backup_folder} named #{backup_basename}.#{backup_extension} or .partX.#{backup_extension}" if backup_sets.empty?
      if options['logonly']
        raise 'No current log backups were found' if backup_sets.nil? || backup_sets.empty?
        first_log_backup_lsn = SqlCmd.get_sql_backup_headers(connection_string, backup_sets.first.last).first['FirstLSN']
        raise "First log backup is too recent to apply. First log backup LSN: #{first_log_backup_lsn} - Restored database LSN: #{database_info['LastRestoreLSN']}" if first_log_backup_lsn > [database_info['LastRestoreLSN'], database_info['LastLogRestoreLSN'] || 0].max
      end
      EasyIO.logger.debug "Backup sets to restore: #{JSON.pretty_generate(backup_sets)}"
      backup_sets.each do |backup_set_basename, backup_files|
        EasyIO.logger.info "Preparing to restore #{backup_set_basename}..."
        EasyIO.logger.debug "Backup files: #{JSON.pretty_generate(backup_files)}"
        raise "Backup #{backup_set_basename} does not exist! Backup file(s) directory: \n  #{backup_folder}" if backup_files.nil? || backup_files.empty?
        sql_backup_header = SqlCmd.get_sql_backup_headers(connection_string, backup_files).first
        raise "Backup #{backup_set_basename} could not be read! It may be from a newer SQL server version or corrupt." if sql_backup_header.nil?
        raise "Backup #{backup_set_basename} is not current! Backup file(s) directory: \n  #{backup_folder}" unless SqlCmd.check_header_date(sql_backup_header, backup_start_time, :prerestore)

        minimum_restore_date = sql_backup_header['BackupFinishDate'] > backup_start_time ? sql_backup_header['BackupFinishDate'] : backup_start_time
        next unless force_restore || !restore_up_to_date?(backup_start_time, connection_string, database_name, backup_files, pre_restore: true)

        raise "Unable to restore database [#{database_name}] to [#{sql_server}]! The database is being used for replication." if database_info['ReplicationActive']
        EasyIO.logger.debug "Restoring backup #{backup_set_basename} with header: #{JSON.pretty_generate(sql_backup_header)}"

        unless force_restore
          # Calculate disk space
          database_size = options['logonly'] ? 0 : database_info['DatabaseSize'] || 0
          EasyIO.logger.info "Existing target database size: #{database_size == 0 ? 'No existing database' : "#{database_size.round(2)} MB"}" unless options['logonly']
          restored_backup_size = SqlCmd.get_backup_size(sql_backup_header)
          EasyIO.logger.info "Size of backup #{backup_set_basename}: #{restored_backup_size.round(2)} MB"
          database_size_difference = restored_backup_size - database_size
          EasyIO.logger.debug "Checking disk space on [#{sql_server_settings['ServerName']}]..."
          sql_server_disk_space = SqlCmd.get_sql_disk_space(sql_server_settings['connection_string'], sql_server_settings['DataDir'])
          sql_server_free_space = sql_server_disk_space['Available_MB'].to_f
          sql_server_disk_size = sql_server_disk_space['Total_MB'].to_f
          sql_server_free_space_after_restore = sql_server_free_space - database_size_difference
          sql_server_free_space_percentage = sql_server_disk_size.nil? ? 'unknown ' : (sql_server_free_space / sql_server_disk_size) * 100
          sql_server_free_space_percentage_post_restore = sql_server_disk_size.nil? ? 'unknown ' : (sql_server_free_space_after_restore / sql_server_disk_size) * 100
          EasyIO.logger.info "Free space on [#{sql_server_settings['ServerName']}] before restore: #{sql_server_free_space.round(2)} MB / #{sql_server_free_space_percentage.round(2)}%"
          EasyIO.logger.info "Estimated free space on [#{sql_server_settings['ServerName']}] after restore: #{sql_server_free_space_after_restore.round(2)} MB / #{sql_server_free_space_percentage_post_restore.round(2)}%"
          insufficient_space = sql_server_free_space_percentage_post_restore < free_space_threshold && database_size_difference > 0
          raise "Insufficient free space on #{sql_server} to restore database! Must have greater than #{free_space_threshold}% space remaining after restore." if insufficient_space
        end

        run_restore_as_job(connection_string, sql_server_settings, backup_files, database_name, options: options)
        monitor_restore(minimum_restore_date, connection_string, database_name, backup_files) unless asynchronous
      end
      import_security(connection_string, import_script_filename, database_name) unless import_script_filename.nil?
      ensure_full_backup_has_occurred(connection_string, database_name, full_backup_method: full_backup_method, database_info: database_info) unless options['secondaryreplica']
      SqlCmd::AlwaysOn.add_to_availability_group(connection_string, database_name) unless options['secondaryreplica'] || options['skip_always_on']
    end

    def validate_restorability(source_sql_version, destination_sql_version, source_type: :backup)
      sql_versions_valid = ::Gem::Version.new(source_sql_version.split('.')[0...1].join('.')) <= ::Gem::Version.new(destination_sql_version.split('.')[0...1].join('.'))
      not_valid_msg = "Unable to restore database. Destination server (#{destination_sql_version}) is on an older version of SQL than the source #{source_type == :backup ? 'backup' : 'server'} (#{source_sql_version})!"
      raise not_valid_msg unless sql_versions_valid
    end

    def restore_up_to_date?(start_time, connection_string, database_name, backup_files, pre_restore: false)
      database_info = info(connection_string, database_name)
      restore_date_key, restore_lsn_key = backup_files.any? { |f| f =~ /\.trn/i } ? %w(LastLogRestoreDate LastLogRestoreLSN) : %w(LastRestoreDate LastRestoreLSN)
      last_restore_database_backup_lsn = database_info['LastRestoreDatabaseBackupLSN'] || 0
      return false if last_restore_database_backup_lsn == 0 || database_info[restore_date_key].nil?
      job_status = SqlCmd::Agent::Job.status(connection_string, "Restore: #{database_name}")['LastRunStatus']
      return false if job_status == 'Running'
      backup_header = SqlCmd.get_sql_backup_headers(connection_string, backup_files).first
      minimum_restore_date = backup_header['BackupFinishDate'] > start_time ? backup_header['BackupFinishDate'] : start_time
      EasyIO.logger.debug "LastLSN from restored database: #{database_info[restore_lsn_key]}"
      EasyIO.logger.debug "LastLSN in backup header : #{backup_header['LastLSN']}"
      EasyIO.logger.debug "DatabaseBackupLSN of restored database: #{last_restore_database_backup_lsn}"
      EasyIO.logger.debug "DatabaseBackupLSN of backup set: #{backup_header['DatabaseBackupLSN']}"
      EasyIO.logger.debug "Restore date key: #{restore_date_key}"
      up_to_date = database_info[restore_date_key] > minimum_restore_date && last_restore_database_backup_lsn == backup_header['DatabaseBackupLSN'] &&
                   database_info[restore_lsn_key] >= backup_header['LastLSN']
      unless pre_restore
        EasyIO.logger.warn "The restore job status is '#{job_status}'" unless job_status == 'NoJob' || job_status == 'Succeeded'
        EasyIO.logger.warn "The restored date in the database (#{database_info[restore_date_key]}) is older than the database backup or the scheduled start time (#{minimum_restore_date})!" unless database_info[restore_date_key] > minimum_restore_date
        EasyIO.logger.warn "The DatabaseBackupLSN for the database (#{last_restore_database_backup_lsn}) doesn't match that of the database backup (#{backup_header['DatabaseBackupLSN']})!" unless last_restore_database_backup_lsn == backup_header['DatabaseBackupLSN']
        EasyIO.logger.warn "The #{restore_lsn_key} for the database (#{database_info[restore_lsn_key]}) is lower than the LastLSN of the database backup (#{backup_header['LastLSN']})!" unless database_info[restore_lsn_key] >= backup_header['LastLSN']
      end
      EasyIO.logger.info "Restored database #{database_name} is up to date." if up_to_date
      up_to_date
    end

    # deletes a Sql database
    def drop(connection_string, database_name)
      EasyIO.logger.header 'Drop Database'
      connection_string = SqlCmd.remove_connection_string_part(connection_string, :database)
      sql_server_settings = SqlCmd.get_sql_server_settings(connection_string)
      database_information = info(connection_string, database_name)
      if database_information['DatabaseNotFound']
        EasyIO.logger.info 'The database was not found. Skipping drop of database...'
        return
      end
      raise "Unable to drop database [#{database_name}] from [#{sql_server_settings['ServerName']}]! The database is being used for replication." if database_information['ReplicationActive']
      EasyIO.logger.info "Dropping database [#{database_name}] from [#{sql_server_settings['ServerName']}]..."

      sql_script = ::File.read("#{SqlCmd.sql_script_dir}/Database/DropDatabase.sql")
      SqlCmd.execute_query(connection_string, sql_script, values: { 'databasename' => database_name }, retries: 3)

      database_information = info(connection_string, database_name)
      raise "Failed to drop database [#{database_name}] from [#{sql_server_settings['ServerName']}]!" unless database_information['DatabaseNotFound']
    end

    # Creates and starts a SQL job to backup a database
    #   * See default_backup_options method for default options
    def run_backup_as_job(connection_string, database_name, backup_folder: nil, backup_basename: nil, options: {})
      backup_status_script = ::File.read("#{SqlCmd.sql_script_dir}/Status/BackupProgress.sql")
      return unless SqlCmd.execute_query(connection_string, backup_status_script, return_type: :first_table, values: { 'databasename' => database_name }, retries: 3).empty?

      values = default_backup_options.merge(options)
      values['bkupdbname'] = database_name
      values['bkupname'] = backup_basename
      values['bkupdestdir'] = backup_folder

      sql_backup_script = ::File.read("#{SqlCmd.sql_script_dir}/Database/BackupDatabase.sql")
      EasyIO.logger.info "Backing up #{options['logonly'] ? 'log for' : 'database'} #{database_name} to: #{backup_folder}..."
      EasyIO.logger.debug "Backup basename: #{backup_basename}"
      EasyIO.logger.debug "Database name: #{database_name}"
      EasyIO.logger.debug "Compress backup: #{values['compressbackup']}"
      EasyIO.logger.debug "Copy only: #{values['copyonly']}"
      SqlCmd.run_sql_as_job(connection_string, sql_backup_script, "Backup: #{backup_basename}", values: values, retries: 1, retry_delay: 30)
    end

    def monitor_backup(backup_start_time, connection_string, database_name, backup_folder: nil, backup_basename: nil, log_only: false)
      backup_status_script = ::File.read("#{SqlCmd.sql_script_dir}/Status/BackupProgress.sql")
      job_name = "Backup: #{database_name}"
      EasyIO.logger.info 'Checking backup status...'
      sleep(15)
      retries ||= 0
      retry_limit = 3
      timeout = 60
      monitoring_start_time = Time.now
      timer_interval = 15
      job_started = false
      last_backup_date_key = log_only ? 'LastLogOnlyBackupDate' : 'LastFullBackupDate'
      EasyIO.logger.debug "Backup start time: #{backup_start_time}"
      loop do
        job_started = true if SqlCmd::Agent::Job.exists?(connection_string, job_name)
        status_row = SqlCmd.execute_query(connection_string, backup_status_script, return_type: :first_row, values: { 'databasename' => database_name }, retries: 3)
        if status_row.nil?
          break if (SqlCmd::Database.info(connection_string, database_name)[last_backup_date_key] || Time.at(0)) >= backup_start_time
          job_status = SqlCmd::Agent::Job.status(connection_string, job_name)['LastRunStatus']
          next if job_status == 'Running'
          # TODO: check job history for errors if not :current
          # job_message = job_history_message(connection_string, job_name) unless result == :current
          if job_started # Check if job has timed out after stopping without completing
            job_completion_time ||= Time.now
            _raise_backup_failure(connection_string, database_name, last_backup_date_key, backup_start_time, job_status: job_status, job_started: job_started) if Time.now > job_completion_time + timeout
          elsif Time.now > monitoring_start_time + timeout # Job never started and timed out
            _raise_backup_failure(connection_string, database_name, last_backup_date_key, backup_start_time, job_status: job_status, job_started: job_started)
          end
          sleep(timer_interval)
          next
        end
        job_started = true
        EasyIO.logger.info "Percent complete: #{status_row['Percent Complete']} / Elapsed min: #{status_row['Elapsed Min']} / Min remaining: #{status_row['ETA Min']} / ETA: #{status_row['ETA Completion Time']}"
        sleep(timer_interval)
        false # if we got here, conditions were not met. Keep looping...
      end
      unless backup_folder.nil? || backup_basename.nil? # Don't validate backup files if backup folder or basename was not provided
        sql_server_settings = SqlCmd.get_sql_server_settings(SqlCmd.to_integrated_security(connection_string))
        backup_files, backup_basename = SqlCmd.get_backup_files(sql_server_settings, backup_folder, backup_basename, log_only: log_only)
        if backup_files.empty?
          EasyIO.logger.warn "Unable to verify backup files. Backup folder '#{backup_folder}' is inaccessible!"
          return :current
        end
        sql_backup_header = SqlCmd.get_sql_backup_headers(connection_string, backup_files).first
        result = SqlCmd.check_header_date(sql_backup_header, backup_start_time)
        raise 'WARNING! Backup files are not current!' if result == :outdated
        raise 'WARNING! Backup files could not be read!' if result == :nobackup
      end
      EasyIO.logger.info 'Backup complete.'
      result
    rescue
      sleep 10
      retry if (retries += 1) <= retry_limit
      raise
    end

    def _raise_backup_failure(connection_string, database_name, last_backup_date_key, backup_start_time, job_status: nil, job_started: false)
      server_name = SqlCmd.connection_string_part(connection_string, :server)
      if job_started
        failure_message = "Backup may have failed as the backup has stopped and the last backup time shows #{SqlCmd::Database.info(connection_string, database_name)[last_backup_date_key]} " \
                          "but the backup should be newer than #{backup_start_time}! "
        failure_message += job_status == 'NoJob' ? 'The job exited with success and so does not exist!' : "Check sql job 'Backup: #{backup_basename}' history on [#{server_name}] for details. \n"
        raise failure_message + "Last backup time retrieved from: #{server_name}\\#{database_name}"
      end
      failure_message = "Backup appears to have failed! The last backup time shows #{SqlCmd::Database.info(connection_string, database_name)[last_backup_date_key]} " \
                        "but the backup should be newer than #{backup_start_time}! "
      failure_message += job_status == 'NoJob' ? 'The backup job could not be found!' : "The job did not start in time. Check sql job 'Backup: #{backup_basename}' history on [#{server_name}] for details."
      raise failure_message + "Last backup time retrieved from: #{server_name}\\#{database_name}"
    end

    # Creates and starts a SQL job to restore a database.
    def run_restore_as_job(connection_string, sql_server_settings, backup_files, database_name, options: {})
      disk_backup_files = SqlCmd.backup_fileset_names(backup_files)
      restore_status_script = ::File.read("#{SqlCmd.sql_script_dir}/Status/RestoreProgress.sql")
      server_connection_string = SqlCmd.remove_connection_string_part(connection_string, :database)
      data_file_logical_name, log_file_logical_name = options['logonly'] ? ['', ''] : SqlCmd.get_backup_logical_names(server_connection_string, backup_files)

      values = default_restore_options.merge(options)
      values['databasename'] = database_name
      values['bkupfiles'] = disk_backup_files
      values['datafile'] ||= "#{sql_server_settings['DataDir']}#{database_name}.mdf"
      values['logfile'] ||= "#{sql_server_settings['LogDir']}#{database_name}.ldf"
      values['datafilelogicalname'] ||= data_file_logical_name
      values['logfilelogicalname'] ||= log_file_logical_name

      return unless SqlCmd.execute_query(server_connection_string, restore_status_script, return_type: :first_table, values: values, retries: 3).empty? # Do nothing if restore is in progress

      # TODO: implement: Replication.script_replication(connection_string, database_name)
      # TODO: implement: Replication.remove_replication(connection_string, database_name)
      SqlCmd::AlwaysOn.remove_from_availability_group(server_connection_string, database_name) unless options['secondaryreplica']
      sql_restore_script = ::File.read("#{SqlCmd.sql_script_dir}/Database/RestoreDatabase.sql")
      EasyIO.logger.info "Restoring #{options['logonly'] ? 'log for' : 'database'} [#{database_name}] on [#{SqlCmd.connection_string_part(connection_string, :server)}]..."
      SqlCmd.run_sql_as_job(server_connection_string, sql_restore_script, "Restore: #{database_name}", values: values, retries: 1, retry_delay: 30)
    end

    def monitor_restore(backup_start_time, connection_string, database_name, backup_files)
      restore_status_script = ::File.read("#{SqlCmd.sql_script_dir}/Status/RestoreProgress.sql")
      job_name = "Restore: #{database_name}"
      server_connection_string = SqlCmd.remove_connection_string_part(connection_string, :database)
      values = { 'databasename' => database_name }
      EasyIO.logger.info 'Checking restore status...'
      sleep(15)
      retries ||= 0
      retry_limit = 4
      timeout = 60
      monitoring_start_time = Time.now
      timer_interval = 15
      job_started = false
      restore_date_key = backup_files.any? { |f| f =~ /\.trn/i } ? 'LastLogRestoreDate' : 'LastRestoreDate'
      loop do
        job_started = true if SqlCmd::Agent::Job.exists?(connection_string, job_name)
        status_row = SqlCmd.execute_query(server_connection_string, restore_status_script, return_type: :first_row, values: values, retries: 3)
        if status_row.nil?
          break if restore_up_to_date?(backup_start_time, connection_string, database_name, backup_files)
          job_status = SqlCmd::Agent::Job.status(connection_string, job_name)['LastRunStatus']
          next if job_status == 'Running'
          # TODO: check job history for errors if not :current
          # job_message = job_history_message(connection_string, job_name) unless result == :current
          if job_started # check if job has timed out after stopping but not completing
            job_completion_time ||= Time.now
            _raise_restore_failure(connection_string, database_name, restore_date_key, backup_start_time, job_status: job_status, job_started: job_started) if Time.now > job_completion_time + timeout
          elsif Time.now > monitoring_start_time + timeout # Job never started and timed out
            _raise_restore_failure(connection_string, database_name, restore_date_key, backup_start_time, job_status: job_status, job_started: job_started)
          end
          sleep(timer_interval)
          next
        end
        job_started = true
        EasyIO.logger.info "Percent complete: #{status_row['Percent Complete']} / Elapsed min: #{status_row['Elapsed Min']} / Min remaining: #{status_row['ETA Min']} / ETA: #{status_row['ETA Completion Time']}"
        sleep(timer_interval)
        false # if we got here, conditions were not met. Keep looping...
      end
      EasyIO.logger.info 'Restore complete.'
      :current
    rescue
      sleep(15 * retries)
      retry if (retries += 1) <= retry_limit
      raise
    end

    def _raise_restore_failure(connection_string, database_name, restore_date_key, backup_start_time, job_status: nil, job_started: false)
      server_name = SqlCmd.connection_string_part(connection_string, :server)
      if job_started
        failure_message = "Restore may have failed as the restore has stopped and the last restore time shows #{SqlCmd::Database.info(connection_string, database_name)[restore_date_key]} " \
                          "but the restore should be newer than #{backup_start_time}! "
        failure_message += job_status == 'NoJob' ? 'The job exited with success and so does not exist!' : "Check sql job 'Restore: #{database_name}' history on [#{server_name}] for details."
        raise failure_message + "Last restore time retrieved from: #{server_name}\\#{database_name}"
      end
      failure_message = 'Restore appears to have failed! '
      failure_message += job_status == 'NoJob' ? 'The job could not be found and the restored database is not up to date!' : "The job did not start in time. Check sql job 'Restore: #{database_name}' history on [#{server_name}] for details."
      raise failure_message + "Restore destination: #{server_name}\\#{database_name}"
    end

    def check_restore_date(backup_start_time, connection_string, database_name, messages = :none, log_only: false)
      backup_start_time = Time.parse(backup_start_time) if backup_start_time.is_a?(String)
      database_information = info(connection_string, database_name)
      last_restore_date_key = log_only ? 'LastLogRestoreDate' : 'LastRestoreDate'
      return :unknown if database_information.nil?
      return :notfound if database_information['DatabaseNotFound']
      return :restoring if database_information['state_desc'] == 'RESTORING'
      return :nodate if database_information[last_restore_date_key].nil?
      return :outdated if database_information[last_restore_date_key] < backup_start_time
      if [:prerestore].include?(messages)
        EasyIO.logger.info "Last restore for [#{database_name}] completed: #{database_information[last_restore_date_key]}"
        EasyIO.logger.info 'Restored database is current.'
      end
      :current
    end

    def ensure_full_backup_has_occurred(connection_string, database_name, force_backup: false, full_backup_method: nil, database_info: nil)
      database_info = info(connection_string, database_name) if database_info.nil?
      EasyIO.logger.info 'Ensuring full backup has taken place...'
      if force_backup || database_info['LastNonCopyOnlyFullBackupDate'].nil? || # Ensure last full backup occurred AFTER the DB was last restored
         (!database_info['LastRestoreDate'].nil? && database_info['LastNonCopyOnlyFullBackupDate'] < database_info['LastRestoreDate'])
        EasyIO.logger.info 'Running full backup...'
        full_backup_method.nil? ? SqlCmd::Database.backup(Time.now, connection_string, database_name, options: { 'copyonly' => false }) : full_backup_method.call
      end
    end

    def duplicate(start_time, source_connection_string, source_database_name, destination_connection_string, destination_database_name, backup_folder: nil, backup_basename: nil, force_restore: false, full_backup_method: nil, options: {})
      backup(start_time, source_connection_string, source_database_name, backup_folder: backup_folder, backup_basename: backup_basename, options: options) unless info(source_connection_string, source_database_name)['DatabaseNotFound']
      backup_folder, backup_basename = SqlCmd.backup_location_and_basename(start_time, source_connection_string, source_database_name)
      if backup_folder.nil? || backup_basename.nil?
        source_server = SqlCmd.connection_string_part(source_connection_string, :server)
        destination_server = SqlCmd.connection_string_part(destination_connection_string, :server)
        database_info = SqlCmd::Database.info(destination_connection_string, destination_database_name)
        raise "Backup files could not be found while duplicating #{source_database_name} from #{source_server} to #{destination_server}. Manually restore the database and run again." if database_info['LastRestoreDate'].nil? || database_info['LastRestoreDate'] < start_time
        EasyIO.logger.warn 'Backup files could not be found but restore appears to be current. Skipping restore...'
        return
      end
      restore(start_time, destination_connection_string, destination_database_name, backup_folder: backup_folder, backup_basename: backup_basename, force_restore: force_restore, full_backup_method: full_backup_method, options: options)
    end

    def info(connection_string, database_name, retries: 3, retry_delay: 5)
      raise 'Failed to get database information! The database_name argument must be specified.' if database_name.nil? || database_name.empty?
      raise 'Failed to get database information! The connection_string argument must be specified.' if connection_string.nil? || connection_string.empty?
      sql_script = ::File.read("#{SqlCmd.sql_script_dir}/Status/DatabaseInfo.sql")
      server_connection_string = SqlCmd.remove_connection_string_part(connection_string, :database)
      result = SqlCmd.execute_query(server_connection_string, sql_script, return_type: :first_row, values: { 'databasename' => database_name }, readonly: true, retries: retries, retry_delay: retry_delay) || {}
      return result if result.empty?
      result['DatabaseName'] ||= database_name
      result
    end

    def export_security(start_time, connection_string, database_name)
      server_name = SqlCmd.connection_string_part(connection_string, :server)
      export_folder = "#{SqlCmd.config['paths']['cache']}/logins"
      import_script_filename = "#{export_folder}/#{EasyFormat::File.windows_friendly_name(server_name)}_#{database_name}_database_permissions_#{EasyFormat::DateTime.yyyymmdd(start_time)}.sql"
      return import_script_filename if ::File.exist?(import_script_filename) && ::File.mtime(import_script_filename) > start_time

      sql_script = ::File.read("#{SqlCmd.sql_script_dir}/Security/ExportDatabasePermissions.sql")
      values = { 'databasename' => database_name, 'output' => 'CreateOnly', 'includetablepermissions' => SqlCmd.config['sql_cmd']['exports']['include_table_permissions'] ? 1 : 0 }
      EasyIO.logger.info "Exporting database permissions for: [#{database_name}] on [#{server_name}]..."
      import_script = SqlCmd.execute_query(connection_string, sql_script, return_type: :scalar, values: values, readonly: true, retries: 3)
      return nil if import_script.nil? || import_script.empty?
      FileUtils.mkdir_p(export_folder)
      ::File.write(import_script_filename, import_script)
      EasyIO.logger.debug "Resulting import script: #{import_script}"
      import_script_filename
    end

    def import_security(connection_string, import_script_filename, database_name)
      EasyIO.logger.info 'Restoring previous security configuration...'
      SqlCmd.execute_script_file(connection_string, import_script_filename, values: { 'databasename' => database_name })
    end

    def default_backup_options
      {
        # 'compressbackup' => false, # Uses server default if not specified
        'splitfiles' => true, # Split files for large databases
        'logonly' => false, # Does a log only backup
        'formatbackup' => false, # Specifies that a new media set be created. Overwrites media header
        'copyonly' => true, # Specifies not to affect normal sequence of backups
        'init' => false, # Specifies that backup sets should be overwritten but preserves the media header
        'skip' => true, # Skips checking backup expiration date and name before overwriting
        'rewind' => false, # Specifies that SQL server releases and rewinds the tape
        'unload' => false, # Specifies that the tape is automatically rewound and unloaded after completion
        'stats' => 5, # Specifies how often sql server reports progress by percentage
      }
    end

    def default_restore_options
      {
        'simplerecovery' => true, # Change recovery mode to simple after performing restore
        'logonly' => false, # Restore a log backup
        'recovery' => true, # Recovers the database after restoring - Should be used unless additional log files are to be restored
        'replace' => false, # Overwrites the existing database
        'unload' => false, # Specifies that the tape is automatically rewound and unloaded after completion
        'stats' => 5, # Specifies how often sql server reports progress by percentage
      }
    end
  end
end
