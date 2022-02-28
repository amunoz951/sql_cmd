module SqlCmd
  module_function

  attr_accessor :full_backup_method # set this to a lambda if you would like full non-copy only backups to use a custom backup method such as for using Red-gate or SQLSafe

  def get_backup_sql_server_settings(connection_string)
    sql_server_settings = get_sql_server_settings(connection_string)
    sql_server_settings = get_sql_server_settings(to_integrated_security(connection_string)) if sql_server_settings.nil? || sql_server_settings['BackupDir'].nil? || sql_server_settings['BackupDir'] == 'null'
    raise "FATAL: Current user #{ENV['user'] || ENV['username']} does not have access to backup database!" if sql_server_settings.nil? || sql_server_settings['BackupDir'].nil? || sql_server_settings['BackupDir'] == 'null'
    sql_server_settings
  end

  # sql_server_settings can be for any server that has network access to these files
  def get_unc_backup_files(sql_server_settings, backup_folder, backup_basename, log_only: false, all_time_stamps: false)
    backup_folder = EasyFormat::Directory.ensure_trailing_slash(backup_folder).tr('\\', '/')
    backup_file_extension = backup_basename.slice!(/\.(trn|bak)/i)
    backup_file_extension ||= log_only ? 'trn' : 'bak'
    backup_file_extension = backup_file_extension.reverse.chomp('.').reverse
    backup_files = Dir.glob("#{backup_folder}#{backup_basename}*").grep(/#{Regexp.escape(backup_basename)}(_\d{6})?(\.part\d+)?\.#{backup_file_extension}$/i)
    return [backup_files, backup_basename] if all_time_stamps
    most_recent_backup_files_and_basename(sql_server_settings, backup_files, backup_basename)
  end

  # get a list of backup files from a URL
  #
  # options:
  #   log_only: determines whether to look for log backup files or normal backup files. (.trn vs .bak) default: false
  #   all_time_stamps: returns all backup sets found matching the basename and not just the most recent. default: false
  #   storage_account_name: (required if using Azure blob storage) Azure blob storage account name.
  #   storage_access_key: (required if using Azure blob storage) Azure blob storage access key.
  def get_url_backup_files(sql_server_settings, backup_url, backup_basename, options = {}, log_only: false, all_time_stamps: false)
    if azure_blob_storage_url?(backup_url)
      backup_file_extension = backup_basename.slice!(/\.(trn|bak)/i)
      backup_file_extension ||= log_only ? 'trn' : 'bak'
      backup_file_extension = backup_file_extension.reverse.chomp('.').reverse
      backup_files = azure_blob_storage_list_backup_files(backup_url, backup_basename, options['storage_account_name'], options['storage_access_key'])
      backup_files.select! { |f, _modified| f =~ /#{Regexp.escape(backup_file_extension)}$/i }
      return [backup_files.keys, backup_basename] if options['all_time_stamps']
      most_recent_backup_files_and_basename(sql_server_settings, backup_files.keys, backup_basename, options)
    else
      EasyIO.logger.warn 'Non-azure blob storage URLs are not currently supported.'
    end
  end

  def azure_blob_storage_url?(backup_url)
    return false if backup_url.nil? || backup_url.empty?
    (backup_url =~ /blob\.core\.windows\.net/i) >= 0
  end

  # Get a list of backup files from blob storage
  def azure_blob_storage_list_backup_files(backup_url, backup_basename, storage_account_name, storage_access_key)
    files = SqlCmd::Azure::AttachedStorage.list(storage_account_name, storage_access_key, storage_url: backup_url, filename_prefix: backup_basename)
    base_url = backup_url[%r{[a-z]+://[^/]+/[^/]+}i] # URL without directories
    files.map { |f, properties| ["#{base_url}/#{f}", properties[:last_modified]] }.to_h
  end

  def backup_location_and_basename(start_time, connection_string, database_name, options = {}, backup_url: nil)
    start_time = SqlCmd.unify_start_time(start_time)
    database_info = SqlCmd::Database.info(connection_string, database_name)
    server_settings = get_sql_server_settings(connection_string)
    backup_type = backup_url.nil? || backup_url.empty? ? 'DISK' : 'URL'
    if database_info['DatabaseNotFound']
      backup_name = "#{database_name}_#{EasyTime.yyyymmdd(start_time)}"
      return [backup_url, backup_name] if backup_type == 'URL'
      backup_unc_location = SqlCmd.config['sql_cmd']['backups']['backup_to_host_sql_server'] ? "\\\\#{server_settings['ServerName']}\\#{SqlCmd.config['sql_cmd']['backups']['default_backup_share']}" : SqlCmd.config['sql_cmd']['backups']['default_destination']
      return Dir.glob("#{backup_unc_location}/#{backup_name}*".tr('\\', '/')).empty? ? [nil, nil] : [backup_unc_location, backup_name]
    end

    backup_file_path = database_info['BackupFileLocation']
    backup_file = ::File.basename(backup_file_path)
    backup_name = backup_basename(backup_file)
    return [nil, backup_name] if backup_type == 'URL'
    backup_unc_location = to_unc_path(::File.dirname(backup_file_path), server_settings['ServerName'])
    backup_folder = if ::File.exist?("#{backup_unc_location}/#{backup_file}")
                      backup_unc_location
                    elsif ::File.exist?("\\\\#{server_settings['ServerName']}\\#{SqlCmd.config['sql_cmd']['backups']['default_backup_share']}\\#{backup_file}")
                      "\\\\#{server_settings['ServerName']}\\#{SqlCmd.config['sql_cmd']['backups']['default_backup_share']}"
                    end
    return [nil, nil] unless defined?(backup_folder)
    [backup_folder, backup_name]
  end

  # sql_server_settings can be for any server that has network access to these files
  def backup_sets_from_unc_path(sql_server_settings, backup_folder, backup_basename, log_only: false, database_backup_header: nil, restored_database_lsn: nil)
    backup_files, backup_basename = get_unc_backup_files(sql_server_settings, backup_folder, backup_basename, log_only: log_only, all_time_stamps: log_only)
    backup_sets = log_only ? relevant_log_backup_sets(sql_server_settings, backup_files, database_backup_header, restored_database_lsn) : { backup_basename => backup_files }
    EasyIO.logger.debug "Database backup sets found: #{JSON.pretty_generate(backup_sets)}"
    backup_sets
  end

  def backup_sets_from_url(sql_server_settings, backup_url, backup_basename, options, log_only: false, database_backup_header: nil, restored_database_lsn: nil)
    backup_files, backup_basename = get_url_backup_files(sql_server_settings, backup_url, backup_basename, options, log_only: log_only, all_time_stamps: log_only)
    backup_sets = log_only ? relevant_log_backup_sets(sql_server_settings, backup_files, database_backup_header, restored_database_lsn) : { backup_basename => backup_files }
    EasyIO.logger.debug "Database backup sets found: #{JSON.pretty_generate(backup_sets)}"
    backup_sets
  end

  # Returns a single string to be used for the source for a RESTORE command from an array of backup file paths
  def backup_fileset_names(backup_files)
    result = ''
    backup_files.each do |backup_file|
      backup_type = backup_file =~ /^http/i ? 'URL' : 'DISK'
      current_file = " #{backup_type} = N''#{backup_file}'',"
      current_file = backup_type =~ /url/i ? current_file.tr('\\', '/') : current_file.tr('/', '\\')
      result << current_file
    end
    result.chomp(',')
  end

  # Returns the headers from the backup set provided. Pass an array of path strings to the backup files.
  def get_sql_backup_headers(connection_string, backup_files, options = {})
    EasyFormat.validate_parameters(method(__method__), binding)
    disk_backup_files = backup_fileset_names(backup_files)
    sql_script = ::File.read("#{sql_script_dir}/Database/GetBackupHeaders.sql")
    options['bkupfiles'] = disk_backup_files
    options['credential'] ||= options['storage_account_name'] || ''
    SqlCmd::Security.create_credential(connection_string, options['credential'], options['storage_account_name'], options['storage_access_key'], options) unless options['credential'].nil? || options['credential'].empty?
    execute_query(connection_string, sql_script, return_type: :first_table, values: options, retries: 3, retry_delay: 10)
  end

  def gap_in_log_backups?(sql_server_settings, database_backup_header, restored_database_lsn, backup_folder, backup_basename, options = {})
    last_full_backup_lsn = database_backup_header['LastLSN']
    return true if last_full_backup_lsn.nil? # if the last full backup does not exist, behave as if there is a gap in the log backups
    backup_sets = backup_sets_from_unc_path(sql_server_settings, backup_folder, backup_basename, database_backup_header: database_backup_header, restored_database_lsn: restored_database_lsn, log_only: true)
    return true if backup_sets.nil? # nil is returned if the backup is too new for the restored database LSN, therefore there's a gap
    return false if backup_sets.empty? # if no log backup sets were current, behave as if there is no gap since a log backup hasn't yet been taken since the backup
    first_lsn_from_log_backups = get_sql_backup_headers(sql_server_settings['connection_string'], backup_sets.first.last, options).first['FirstLSN']
    EasyIO.logger.debug "LastLSN from full backup: #{last_full_backup_lsn} | First LSN from log backups: #{first_lsn_from_log_backups}"
    last_full_backup_lsn < first_lsn_from_log_backups && restored_database_lsn < first_lsn_from_log_backups
  end

  # Returns the data and log file information contained in the backup files.
  def get_backup_file_info(connection_string, backup_files, options)
    EasyFormat.validate_parameters(method(__method__), binding)
    disk_backup_files = backup_fileset_names(backup_files)
    sql_script = ::File.read("#{sql_script_dir}/Database/GetFileInfoFromBackup.sql")
    options['bkupfiles'] = disk_backup_files
    options['credential'] ||= options['storage_account_name'] || ''
    execute_query(connection_string, sql_script, return_type: :first_table, values: options, retries: 3)
  end

  def get_backup_logical_names(connection_string, backup_files, options)
    EasyFormat.validate_parameters(method(__method__), binding)
    sql_backup_file_info = SqlCmd.get_backup_file_info(connection_string, backup_files, options)
    data_file_logical_name = sql_backup_file_info.select { |file| file['Type'] == 'D' }.first['LogicalName']
    log_file_logical_name = sql_backup_file_info.select { |file| file['Type'] == 'L' }.first['LogicalName']
    [data_file_logical_name, log_file_logical_name]
  end

  def sql_server_backup_files(sql_server_settings, backup_basename, log_only: false)
    values = { 'targetfolder' => sql_server_settings['BackupDir'],
               'bkupname' => backup_basename,
               'logonly' => log_only }
    sql_script = ::File.read("#{sql_script_dir}/Database/GetBackupFiles.sql")
    backup_files_results = execute_query(sql_server_settings['connection_string'], sql_script, return_type: :first_table, values: values, retries: 3)
    backup_files = []
    backup_files_results.each do |file|
      backup_files.push("#{EasyFormat::Directory.ensure_trailing_slash(sql_server_settings['BackupDir'])}#{file['FileName']}")
    end
    if log_only
      database_backup_files = sql_server_backup_files(sql_server_settings, backup_basename)
      database_backup_header = get_sql_backup_headers(sql_server_settings['connection_string'], database_backup_files).first
      return relevant_log_backup_sets(sql_server_settings, backup_files, database_backup_header, 0)
    end
    most_recent_backup_files_and_basename(sql_server_settings, backup_files, backup_basename)
  end

  def most_recent_backup_files_and_basename(sql_server_settings, backup_files, backup_basename, options = {})
    backup_sets = backup_sets_from_backup_files(backup_files)
    if backup_sets.keys.count > 1 # if there is more than 1 backup set, find the most recent
      backup_headers = {}
      backup_sets.each do |basename, files|
        backup_headers[basename] = get_sql_backup_headers(sql_server_settings['connection_string'], files, options).first
      end
      backup_basename = backup_headers.max_by { |_basename, header| header['BackupFinishDate'] }.first
    elsif backup_sets.keys.count == 0 # if there are no backup sets, use an empty array
      backup_sets[backup_basename] = []
    end
    [backup_sets[backup_basename], backup_basename]
  end

  def relevant_log_backup_sets(sql_server_settings, backup_files, database_backup_header, restored_database_lsn, options = {})
    restored_database_lsn ||= 0
    backup_sets = backup_sets_from_backup_files(backup_files)
    database_backup_lsn = database_backup_header['DatabaseBackupLSN']
    EasyIO.logger.debug "Database backup LSN: #{database_backup_lsn}"
    backup_headers = backup_sets.each_with_object({}) { |(basename, files), headers| headers[basename] = get_sql_backup_headers(sql_server_settings['connection_string'], files, options).first }
    backup_sets = backup_sets.sort_by { |basename, _files| backup_headers[basename]['LastLSN'] }.to_h
    backup_headers = backup_headers.sort_by { |_basename, backup_header| backup_header['LastLSN'] }.to_h
    EasyIO.logger.debug "Backup sets after sorting: #{JSON.pretty_generate(backup_sets)}"
    backup_sets.each { |basename, _files| EasyIO.logger.debug "Backup header for #{basename}: FirstLSN: #{backup_headers[basename]['FirstLSN']} | LastLSN: #{backup_headers[basename]['LastLSN']}" }
    start_lsn = nil
    current_lsn = database_backup_header['LastLSN']
    backup_headers.each do |basename, backup_header|
      start_lsn ||= backup_header['FirstLSN']
      EasyIO.logger.debug "Current LSN: #{current_lsn}"
      EasyIO.logger.debug "Current header (#{basename}) - FirstLSN: #{backup_headers[basename]['FirstLSN']} | LastLSN: #{backup_headers[basename]['LastLSN']} | DatabaseBackupLSN: #{backup_headers[basename]['DatabaseBackupLSN']}"
      unless backup_header['DatabaseBackupLSN'] == database_backup_lsn
        EasyIO.logger.debug "Current backup is from a different database backup as the DatabaseBackupLSN (#{backup_header['DatabaseBackupLSN']}) doesn't match the database backup LSN (#{database_backup_lsn}). Removing backup set..."
        backup_sets.delete(basename)
        next
      end
      if backup_header['LastLSN'] < database_backup_lsn || backup_header['LastLSN'] < restored_database_lsn
        EasyIO.logger.debug "Current backup LastLSN (#{backup_header['FirstLSN']}) older than database backup LSN (#{database_backup_lsn}) or restored database LSN (#{restored_database_lsn}). Removing backup set..."
        backup_sets.delete(basename)
        next
      end
      if backup_header['FirstLSN'] > current_lsn # remove previous backup sets if there's a gap
        EasyIO.logger.debug "Gap found between previous backup LastLSN (#{current_lsn}) and current backup FirstLSN #{backup_header['FirstLSN']}. Updating starting point..." unless current_lsn == 0
        start_lsn = backup_header['FirstLSN']
        if start_lsn > restored_database_lsn # if the starting point is beyond the restored database LastLSN, no backups can be applied
          EasyIO.logger.warn "Gap found in log backups. The previous backup ends at LSN #{current_lsn} and the next log backup starts at LSN #{backup_header['FirstLSN']}!"
          return nil
        end
      end
      current_lsn = backup_header['LastLSN']
    end
    backup_headers.each { |basename, backup_header| backup_sets.delete(basename) unless backup_header['LastLSN'] > start_lsn } # remove any obsolete backup sets
    EasyIO.logger.debug "Backup sets after removing obsolete sets: #{JSON.pretty_generate(backup_sets)}"
    backup_sets
  end

  def backup_sets_from_backup_files(backup_files)
    backup_sets = {}
    backup_files.each do |file|
      current_basename = ::File.basename(file).sub(/(\.part\d+)?\.(bak|trn)$/i, '') # determine basename of current file
      backup_sets[current_basename] = [] if backup_sets[current_basename].nil?
      backup_sets[current_basename].push(file)
    end
    backup_sets
  end

  # get a list of backup files and the backup basename
  #
  # options:
  #   log_only: determines whether to look for log backup files or normal backup files. (.trn vs .bak) default: false
  #   all_time_stamps: returns all backup sets found matching the basename and not just the most recent. default: false
  #   storage_account_name: (required if using Azure blob storage) Azure blob storage account name.
  #   storage_access_key: (required if using Azure blob storage) Azure blob storage access key.
  def get_backup_files(sql_server_settings, options = {}, backup_folder: nil, backup_url: nil, backup_basename: nil)
    if !backup_url.nil? && !backup_url.empty?
      get_url_backup_files(sql_server_settings, backup_url, backup_basename, options)
    elsif backup_folder.start_with?('\\\\')
      get_unc_backup_files(sql_server_settings, backup_folder, backup_basename, log_only: options['log_only'])
    else
      sql_server_backup_files(sql_server_settings, backup_basename, log_only: log_only)
    end
  end

  # Get size of uncompressed database from backup header in MB
  def get_backup_size(sql_backup_header)
    sql_backup_header['BackupSize'].to_f / 1024 / 1024
  end

  # messages:
  #   :none
  #   :prebackup || :prerestore - Output info messages pertaining to a backup or restore
  # returns:
  #   :current
  #   :outdated
  #   :nobackup
  def check_header_date(sql_backup_header, backup_start_time, messages = :none)
    backup_start_time = SqlCmd.unify_start_time(backup_start_time)
    return :nobackup if sql_backup_header.nil? || sql_backup_header.empty?
    EasyIO.logger.info "Last backup for [#{sql_backup_header['DatabaseName']}] completed: #{sql_backup_header['BackupFinishDate']}" if [:prebackup, :prerestore].include?(messages)
    backup_finish_time = sql_backup_header['BackupFinishDate']
    raise "BackupFinishDate missing from backup header: #{sql_backup_header}" if backup_finish_time.nil?
    if backup_finish_time > backup_start_time
      EasyIO.logger.info 'Backup is current. Bypassing database backup.' if messages == :prebackup
      EasyIO.logger.info 'Backup is current. Proceeding with restore...' if messages == :prerestore
      :current
    else
      :outdated
    end
  end
end
