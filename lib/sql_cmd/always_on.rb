module SqlCmd
  module AlwaysOn
    module_function

    # Optionally provide your own lambda for creating a full non-copyonly backup using the full_backup_method parameter
    # Seeding modes: AUTOMATIC (No backup/restore needed to synchronize), MANUAL (traditional backup and restore process)
    # async option is only observed when using automatic seeding
    def add_to_availability_group(connection_string, database_name, seeding_mode: nil, full_backup_method: nil, async: false, start_time: Time.now)
      EasyIO.logger.header 'AlwaysOn Availability'
      EasyIO.logger.info "Checking if server [#{SqlCmd.connection_string_part(connection_string, :server)}] is configured for AlwaysOn High Availability..."
      server_info = SqlCmd.get_sql_server_settings(connection_string)
      if server_info['AvailabilityGroup'].nil? || server_info['SecondaryReplica'].nil?
        EasyIO.logger.info "Server [#{SqlCmd.connection_string_part(connection_string, :server)}] not configured for AlwaysOn High Availability..."
        return
      end

      seeding_mode ||= server_info['SeedingMode'] || 'MANUAL'
      database_info = SqlCmd::Database.info(connection_string, database_name)
      primary_connection_string = SqlCmd.replace_connection_string_part(connection_string, :database, 'master')
      replica_connection_string = server_info['secondary_replica_connection_string']
      replica_database_info = SqlCmd::Database.info(replica_connection_string, database_name)
      validate_logins_script = SqlCmd.validate_logins_script(connection_string, database_name)
      logins_migrated = SqlCmd.execute_query(replica_connection_string, validate_logins_script, retries: 3)
      unless database_info['AvailabilityGroup'].nil? || replica_database_info['AvailabilityGroup'].nil? || !logins_migrated
        EasyIO.logger.info 'Database already configured for AlwaysOn and synchronized. Skipping...'
        return
      end

      EasyIO.logger.info "Preparing to add [#{database_name}] to AlwaysOn availability group..."
      always_on_backup_time = EasyFormat::DateTime.yyyymmdd_hhmmss(start_time)

      full_recovery_script = ::File.read("#{SqlCmd.sql_script_dir}/Database/SetFullRecovery.sql")

      values = {
        'databasename' => database_name,
        'datetime' => always_on_backup_time,
        'availabilitygroupname' => server_info['AvailabilityGroup'],
        'secondaryreplica' => server_info['SecondaryReplica'],
        'primarysqlserver' => server_info['ServerName'],
        'seedingmode' => seeding_mode.upcase,
        'backupdir' => SqlCmd.config['sql_cmd']['backups']['always_on_backup_temp_dir'],
      }

      raise "Could not determine secondary replica for #{server_info['ServerName']}" if server_info['SecondaryReplica'].nil?

      if database_info['AvailabilityGroup'].nil? || replica_database_info['AvailabilityGroup'].nil?
        EasyIO.logger.header 'AlwaysOn Full Backup'
        EasyIO.logger.info "Ensuring database [#{database_name}] is set to full recovery..."
        recovery_model_updated = SqlCmd.execute_query(connection_string, full_recovery_script, values: values, return_type: :scalar, retries: 3)
        SqlCmd::Database.ensure_full_backup_has_occurred(connection_string, database_name, force_backup: recovery_model_updated, database_info: database_info, full_backup_method: full_backup_method)
        if seeding_mode.upcase == 'AUTOMATIC'
          add_to_availability_group_automatically(primary_connection_string, replica_connection_string, database_name, server_info, values: values, async: async)
        else
          add_database_to_replica_using_backups(primary_connection_string, replica_connection_string, server_info, database_info, replica_database_info, values: values)
        end
      else
        EasyIO.logger.info 'Database already in AlwaysOn Availability group.'
      end

      EasyIO.logger.header 'AlwaysOn Permissions Migration to Replica'
      EasyIO.logger.debug 'Migrating logins to replica...'
      import_script_filename = SqlCmd.export_logins(start_time, primary_connection_string, database_name)
      EasyIO.logger.info "Importing logins on [#{SqlCmd.connection_string_part(replica_connection_string, :server)}]..."
      SqlCmd.execute_script_file(replica_connection_string, import_script_filename)
      EasyIO.logger.debug 'Running database_status script...'
      database_info = SqlCmd::Database.info(connection_string, database_name)
      replica_database_info = SqlCmd::Database.info(replica_connection_string, database_name)
      raise 'Failed to add database to AlwaysOn Availability Group' if database_info['AvailabilityGroup'].nil? || replica_database_info['AvailabilityGroup'].nil?
      EasyIO.logger.info "AlwaysOn availability for [#{database_name}] is configured and active..."
    end

    # TODO: idempotent log backups and restores are not stable and need improvement. Use 'idempotent: false' for now
    def add_database_to_replica_using_backups(primary_connection_string, replica_connection_string, server_info, database_info, replica_database_info, values: {}, idempotent: false)
      database_name = database_info['DatabaseName']
      configure_primary_for_manual_seeding_script = ::File.read("#{SqlCmd.sql_script_dir}/AlwaysOn/ConfigurePrimaryForManualSeeding.sql")
      add_to_primary_availability_group_script = ::File.read("#{SqlCmd.sql_script_dir}/AlwaysOn/AddDatabaseToPrimaryAvailabilityGroup.sql")
      add_to_availability_group_on_secondary_script = ::File.read("#{SqlCmd.sql_script_dir}/AlwaysOn/AddDatabaseToAvailabilityGroupOnSecondary.sql")

      SqlCmd.execute_query(primary_connection_string, configure_primary_for_manual_seeding_script, values: values, retries: 3)

      EasyIO.logger.info "Adding [#{database_name}] to availability group..."
      SqlCmd.execute_query(primary_connection_string, add_to_primary_availability_group_script, values: values, retries: 3)

      # Backup database on primary replica
      EasyIO.logger.header 'AlwaysOn CopyOnly Full Backup'
      copy_only_full_backup_exists = ::File.exist?("#{values['backupdir']}\\#{database_name}.bak")
      copy_only_backup_start_time = Time.now
      if copy_only_full_backup_exists && idempotent
        copy_only_full_backup_header = SqlCmd.get_sql_backup_headers(primary_connection_string, ["#{values['backupdir']}\\#{database_name}.bak"]).first
        copy_only_backup_current = !SqlCmd.gap_in_log_backups?(server_info, copy_only_full_backup_header, replica_database_info['LastRestoreLSN'], values['backupdir'], database_name)
        copy_only_backup_current &&= (database_info['LastRestoreDate'] || Time.at(0)) < copy_only_full_backup_header['BackupFinishDate'] # Make sure the database hasn't been restored again since the last backup
        EasyIO.logger.debug "LSN of CopyOnly backup: #{copy_only_full_backup_header['DatabaseBackupLSN']}"
        EasyIO.logger.debug "Last restore LSN: #{replica_database_info['LastRestoreLSN']}"
        EasyIO.logger.debug "Copy only backup current?: #{copy_only_backup_current}"
      end

      if copy_only_full_backup_exists && copy_only_backup_current && idempotent
        EasyIO.logger.info 'CopyOnly Database backup current. Skipping CopyOnly backup...'
      else
        EasyIO.logger.info 'Backing up database on primary server (CopyOnly)...'
        always_on_backup_options = { 'formatbackup' => true, 'init' => true, 'rewind' => true, 'nounload' => true, 'compressbackup' => true, 'splitfiles' => false }
        SqlCmd::Database.backup(copy_only_backup_start_time, primary_connection_string, database_name, backup_folder: values['backupdir'], backup_basename: database_name, options: always_on_backup_options)
      end

      # Restore database to secondary replica if it was restored before the copy only backup was created or if it doesn't exist
      EasyIO.logger.header 'AlwaysOn Restore of CopyOnly Backup to Replica'
      if !idempotent || !copy_only_full_backup_exists || !copy_only_backup_current || copy_only_full_backup_header.nil? || replica_database_info['LastRestoreDate'].nil? ||
         replica_database_info['LastRestoreDate'] < database_info['LastCopyOnlyFullBackupDate'] || replica_database_info['LastRestoreLSN'] < database_info['LastCopyOnlyLSN']
        EasyIO.logger.info 'Restoring database to secondary replica...'
        restore_options = { 'recovery' => false, 'unload' => false, 'simplerecovery' => false, 'secondaryreplica' => true, 'datafilelogicalname' => '', 'logfilelogicalname' => '' }
        SqlCmd::Database.restore(copy_only_backup_start_time, replica_connection_string, database_name, backup_folder: values['backupdir'], backup_basename: database_name, options: restore_options)
      else
        EasyIO.logger.info 'Restore of CopyOnly database backup current. Skipping restore of CopyOnly database backup...'
      end

      sleep(15) # Give time for database to be in correct state
      # Backup log on primary replica
      EasyIO.logger.header 'AlwaysOn Log Backup'
      log_only_backup_start_time = Time.now
      log_only_options = { 'logonly' => true, 'skip' => false, 'rewind' => true, 'compressbackup' => true, 'splitfiles' => false }
      EasyIO.logger.info 'Backing up log file on primary replica...'
      SqlCmd::Database.backup(log_only_backup_start_time, primary_connection_string, database_name, backup_folder: values['backupdir'], backup_basename: database_name, options: log_only_options)

      # Restore log on secondary replica
      EasyIO.logger.header 'AlwaysOn Log Restore to Replica'
      restore_options = { 'recovery' => false, 'unload' => false, 'simplerecovery' => false, 'logonly' => true, 'secondaryreplica' => true }
      SqlCmd::Database.restore(log_only_backup_start_time, replica_connection_string, database_name, backup_folder: values['backupdir'], backup_basename: database_name, options: restore_options)

      # Add to availability group on secondary replica
      EasyIO.logger.header 'AlwaysOn Database Sync'
      EasyIO.logger.info 'Waiting for communication and adding to availability group on secondary replica...'
      SqlCmd.execute_query(replica_connection_string, add_to_availability_group_on_secondary_script, values: values, retries: 3)
      raise 'Replica does not appear to be in the correct state!' if SqlCmd::Database.info(replica_connection_string, database_name)['AvailabilityGroup'].nil?
      EasyIO.logger.info "[#{database_name}] added to AlwaysOn availability group successfully..."
    end

    def add_to_availability_group_automatically(primary_connection_string, replica_connection_string, database_name, server_info, values: {}, async: false)
      configure_primary_for_automatic_seeding_script = ::File.read("#{SqlCmd.sql_script_dir}/AlwaysOn/ConfigurePrimaryForAutomaticSeeding.sql")
      add_to_primary_availability_group_script = ::File.read("#{SqlCmd.sql_script_dir}/AlwaysOn/AddDatabaseToPrimaryAvailabilityGroup.sql")
      configure_secondary_for_automatic_seeding_script = ::File.read("#{SqlCmd.sql_script_dir}/AlwaysOn/ConfigureSecondaryForAutomaticSeeding.sql")

      SqlCmd.execute_query(primary_connection_string, configure_primary_for_automatic_seeding_script, values: values, retries: 3)
      EasyIO.logger.info "Adding [#{database_name}] to availability group..."
      SqlCmd.execute_query(primary_connection_string, add_to_primary_availability_group_script, values: values, retries: 3)
      SqlCmd.execute_query(replica_connection_string, configure_secondary_for_automatic_seeding_script, values: values, retries: 3)

      if async
        EasyIO.logger.info "#{database_name} added to availability group #{server_info['AvailabilityGroup']}. AlwaysOn will continue synchronizing in the background."
        return
      end

      monitor_automatic_seeding(primary_connection_string, database_name)
      raise 'Failed to add to AlwaysOn availability group!' unless database_synchronized?(primary_connection_string, replica_connection_string, database_name)
      EasyIO.logger.info "[#{database_name}] added to AlwaysOn availability group successfully..."
    end

    def monitor_automatic_seeding(connection_string, database_name)
      progress_script = ::File.read("#{SqlCmd.sql_script_dir}/AlwaysOn/AutomaticSeedingProgress.sql")
      values = { 'databasename' => database_name }

      seeding_status = SqlCmd.execute_query(connection_string, progress_script, values: values, return_type: :first_row, retries: 3)
      if seeding_status['current_state'] == 'COMPLETED'
        EasyIO.logger.info 'AlwaysOn seeding complete.'
        return
      end

      start_time = Time.now
      seeding_start_timeout = 300 # Allow 5 minutes to start the seeding process before failing
      timeout = 86400 # 24 hours
      progress_interval = 4 # seconds
      while Time.now < start_time + timeout
        seeding_status = SqlCmd.execute_query(connection_string, progress_script, values: values, return_type: :first_row, retries: 3)

        if seeding_status['current_state'] == 'COMPLETED'
          EasyIO.logger.info 'AlwaysOn seeding complete.'
          return
        end

        if seeding_status['time_elapsed_percent_complete'].nil? || seeding_status['current_state'] == 'FAILED'
          unless Time.now > start_time + seeding_start_timeout
            EasyIO.logger.info 'Waiting for automatic synchronization process...'
            sleep(progress_interval)
            next
          end
          raise "Failed to seed #{database_name} automatically! Check the synchronization status." if seeding_status.empty?
          failure_message = "Failed to seed #{database_name} automatically in availability group [#{seeding_status['ag_name']}]! Check the synchronization status on #{seeding_status['replica_server_name']}.\n"
          failure_message += seeding_status['failure_state_desc'] unless seeding_status['failure_state_desc'].nil?
          raise failure_message
        end

        percent_complete = (seeding_status['transferred_size_percent_complete'] + seeding_status['time_elapsed_percent_complete'] / 2)
        elapsed_min = (Time.now - start_time) / 60
        eta_min = (elapsed_min / (percent_complete / 100)) - elapsed_min
        eta_time = Time.now + (eta_min * 60).round
        EasyIO.logger.info "Percent complete: #{percent_complete} / Elapsed min: #{elapsed_min} / Min remaining: #{eta_min} / ETA: #{eta_time}"
        sleep(progress_interval)
      end
      raise "Automatic seeding timed out after #{timeout / 60 / 60} hours!" # only gets here if status never reached 'COMPLETED'
    end

    def database_synchronized?(primary_connection_string, replica_connection_string, database_name)
      database_info = SqlCmd::Database.info(primary_connection_string, database_name)
      replica_database_info = SqlCmd::Database.info(replica_connection_string, database_name)
      !database_info['AvailabilityGroup'].nil? && !replica_database_info['AvailabilityGroup'].nil?
    end

    def remove_from_availability_group(connection_string, database_name)
      EasyIO.logger.header 'AlwaysOn Availability Removal'
      EasyIO.logger.info "Checking if server [#{SqlCmd.connection_string_part(connection_string, :server)}] is configured for AlwaysOn High Availability..."
      server_info = SqlCmd.get_sql_server_settings(connection_string)
      if server_info['AvailabilityGroup'].nil? || server_info['SecondaryReplica'].nil?
        EasyIO.logger.info "Server [#{SqlCmd.connection_string_part(connection_string, :server)}] not configured for AlwaysOn High Availability..."
        return
      end

      primary_connection_string = SqlCmd.replace_connection_string_part(server_info['direct_connection_string'], :database, 'master')
      replica_connection_string = server_info['secondary_replica_connection_string']
      primary_server_name = SqlCmd.connection_string_part(primary_connection_string, :server)
      database_info = SqlCmd::Database.info(primary_connection_string, database_name)
      replica_database_info = SqlCmd::Database.info(replica_connection_string, database_name)

      if database_info['DatabaseNotFound']
        EasyIO.logger.info "Skipping removal from availability group: database [#{database_name}] does not exist on [#{primary_server_name}]..."
        return
      end
      if database_info['AvailabilityGroup'].nil? && replica_database_info['DatabaseNotFound']
        EasyIO.logger.info "Skipping removal from availability group: database [#{database_name}] does not belong to an AvailabilityGroup on [#{primary_server_name}]..."
        return
      end

      remove_database_from_group_script = ::File.read("#{SqlCmd.sql_script_dir}/AlwaysOn/RemoveDatabaseFromGroup.sql")
      drop_database_from_secondary_script = ::File.read("#{SqlCmd.sql_script_dir}/AlwaysOn/DropSecondary.sql")

      values = { 'databasename' => database_name, 'availabilitygroupname' => server_info['AvailabilityGroup'] }

      EasyIO.logger.header 'AlwaysOn Remove From Group'
      EasyIO.logger.info "Removing [#{database_name}] from availability group..."
      SqlCmd.execute_query(primary_connection_string, remove_database_from_group_script, values: values, retries: 3) unless database_info['AvailabilityGroup'].nil?
      EasyIO.logger.header 'AlwaysOn Drop Database on Replica'
      drop_result = SqlCmd.execute_query(replica_connection_string, drop_database_from_secondary_script, return_type: :scalar, values: values, retries: 3)
      EasyIO.logger.debug "Drop secondary replica database result: #{drop_result}"
      raise 'Failed to drop database from secondary replica!' unless drop_result
      database_info = SqlCmd::Database.info(primary_connection_string, database_name)
      replica_database_info = SqlCmd::Database.info(replica_connection_string, database_name)
      raise 'Failed to remove database from AlwaysOn Availability Group' unless database_info['AvailabilityGroup'].nil? && replica_database_info['AvailabilityGroup'].nil?
      EasyIO.logger.info "[#{database_name}] removed from AlwaysOn availability group successfully..."
    end
  end
end
