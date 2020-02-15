module SqlCmd
  module_function

  def config
    @config ||= EasyJSON.config(defaults: defaults)
  end

  def defaults
    {
      'environment' => {
        'policy_group' => nil,
        'chef' => false,
      },
      'paths' => {
        'cache' => ::File.expand_path("#{Dir.pwd}/cache"),
      },
      'logging' => {
        'level' => 'info',
        'verbose' => false, # show queries and returned json
      },
      'sql_cmd' => {
        'backups' => {
          'always_on_backup_temp_dir' => nil, # where backups will go when adding to availability groups and seeding_mode is manual or nil
          'default_destination' => nil, # where backups will go by default
          'backup_to_host_sql_server' => false, # if set to true, will backup databases to the SQL host instead of the default destination
          'default_backup_share' => nil, # the name of the windows share relative to SQL hosts where backups go when set to backup to sql hosts
          'free_space_threshold' => 5, # raises an exception if a backup or restore operation would bring free space on the target location below this threshold
          'compress_backups' => false,
        },
        'exports' => {
          'include_table_permissions' => false,
        },
        'paths' => {
          'sql_script_dir' => ::File.expand_path("#{__dir__}/../../sql_scripts"),
          'powershell_helper_script' => ::File.expand_path("#{__dir__}/sql_helper.ps1"),
        },
      },
    }
  end

  def sql_script_dir
    SqlCmd.config['sql_cmd']['paths']['sql_script_dir']
  end

  def apply_log_level
    config_level = config['logging']['level']
    EasyIO.logger.level = EasyIO.levels[config_level]
  end

  apply_log_level
end
