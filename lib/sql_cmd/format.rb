module SqlCmd
  module_function

  def connection_string_part_regex(part)
    case part
    when :server
      /(server|data source)(\s*\=\s*)([^;]*)(;)?/i
    when :database
      /(database|initial catalog)(\s*\=\s*)([^;]*)(;)?/i
    when :user # array of user/password or integrated security
      /(user id|uid)(\s*\=\s*)([^;]*)(;)?/i
    when :password
      /(password|pwd)(\s*\=\s*)([^;]*)(;)?/i
    when :integrated
      /integrated security\s*\=\s*[^;]*(;)?/i
    when :applicationintent
      /applicationintent\s*\=\s*[^;]*(;)?/i
    else
      raise "#{part} is not a supported connection string part!"
    end
  end

  def connection_string_part(connection_string, part, value_only: true) # options: :server, :database, :credentials, :readonly
    raise 'Connection string provided is nil or empty!' if connection_string.nil? || connection_string.empty?
    case part
    when :server, :database, :applicationintent
      connection_string[connection_string_part_regex(part)]
    when :user, :password
      credentials = connection_string_part(connection_string, :credentials)
      return nil if credentials.nil?
      return credentials[part]
    when :credentials # array of user/password or integrated security
      connection_string[connection_string_part_regex(:user)]
      user = Regexp.last_match(3)
      connection_string[connection_string_part_regex(:password)]
      password = Regexp.last_match(3)
      return { user: user, password: password } unless user.nil? || password.nil?
      return connection_string[connection_string_part_regex(:integrated)]
    end
    return Regexp.last_match(3) if value_only
    result = (Regexp.last_match(1) || '') + '=' + (Regexp.last_match(3) || '') + (Regexp.last_match(4) || '')
    result.empty? ? nil : result
  end

  def remove_connection_string_part(connection_string, part) # part options: :server, :database, :credentials, :applicationintent
    connection_string_new = connection_string.dup
    parts = part == :credentials ? [:user, :password, :integrated] : [part]
    parts.each { |p| connection_string_new.gsub!(connection_string_part_regex(p), '') } # unless full_part.nil? }
    connection_string_new
  end

  def hide_connection_string_password(connection_string)
    credentials = connection_string_part(connection_string, :credentials)
    credentials[:password] = credentials[:password].gsub(/(.)([^(.$)]*)(.$)/) { Regexp.last_match(1) + ('*' * Regexp.last_match(2).length) + Regexp.last_match(3) } if credentials.is_a?(Hash) && !credentials[:password].nil? && credentials[:password].length > 2
    raise "Connection string missing authentication information! Connection string: '#{connection_string}'" if credentials.nil? || credentials.empty?
    replace_connection_string_part(connection_string, :credentials, credentials)
  end

  def unify_start_time(start_time, timezone: SqlCmd.config['environment']['timezone'])
    return EasyTime.at_timezone(Time.now, timezone) if start_time.nil? || start_time.to_s.strip.empty?
    return EasyTime.at_timezone(start_time, timezone) if start_time.is_a?(Time)
    EasyTime.stomp_timezone(start_time, timezone) # Stomp the timezone with the config timezone. If no start_time was provided, use the current time
  end

  # Supply entire value (EG: 'user id=value;password=value;' or 'integrated security=SSPI;') as the replacement_value if the part is :credentials
  # or provide a hash containing a username and password { user: 'someuser', password: 'somepassword', }
  def replace_connection_string_part(connection_string, part, replacement_value)
    EasyFormat.validate_parameters(method(__method__), binding)
    new_connection_string = remove_connection_string_part(connection_string, part)
    new_connection_string = case part
                            when :credentials
                              replacement_value = "User Id=#{replacement_value[:user]};Password=#{replacement_value[:password]}" if replacement_value.is_a?(Hash)
                              "#{new_connection_string};#{replacement_value}"
                            else
                              "#{part}=#{replacement_value};#{new_connection_string};"
                            end
    new_connection_string.gsub!(/;+/, ';')
    new_connection_string
  end

  # converts a path to unc_path if it contains a drive letter. Uses the server name provided
  def to_unc_path(path, server_name)
    return nil if path.nil? || path.empty? || server_name.nil? || server_name.empty?
    path.gsub(/(\p{L})+(:\\)/i) { "\\\\#{server_name}\\#{Regexp.last_match(1)}$\\" } # replace local paths with network paths
  end

  # get the basename of the backup based on a full file_path such as the SQL value from [backupmediafamily].[physical_device_name]
  def backup_basename_from_path(backup_path)
    return nil if backup_path.nil? || backup_path.empty?
    ::File.basename(backup_path).gsub(/(\.part\d+)?\.(bak|trn)/i, '')
  end

  def connection_string_credentials_from_hash(**credentials_hash)
    credentials_hash = Hashly.symbolize_all_keys(credentials_hash.dup)
    windows_authentication = credentials_hash[:windows_authentication]
    windows_authentication = credentials_hash[:user].nil? || credentials_hash[:password].nil? if windows_authentication.nil? # If windows authentication wasn't specified, set it if user or pass is nil
    windows_authentication ? 'integrated security=SSPI;' : "user id=#{credentials_hash[:user]};password=#{credentials_hash[:password]}"
  end

  # generates a connection string from the hash provided. Example hash: { 'server' => 'someservername', 'database' => 'somedb', 'user' => 'someuser', 'password' => 'somepass', 'windows_authentication' => false }
  def connection_string_from_hash(**connection_hash)
    connection_hash = Hashly.symbolize_all_keys(connection_hash.dup)
    credentials_segment = connection_string_credentials_from_hash(**connection_hash)
    database_name = connection_hash[:database]
    database_segment = database_name.nil? || database_name.strip.empty? ? '' : "database=#{database_name};"
    "server=#{connection_hash[:server]};#{database_segment}#{credentials_segment}"
  end

  # Ensures a connection string is using integrated security instead of SQL Authentication.
  def to_integrated_security(connection_string, server_only: false)
    raise 'Failed to convert connection string to integrated security. Connection string is nil!' if connection_string.nil?
    parts = connection_string.split(';')
    new_connection_string = ''
    ommitted_parts = ['user id', 'uid', 'password', 'pwd', 'integrated security', 'trusted_connection']
    ommitted_parts += ['database', 'initial catalog'] if server_only
    parts.each { |part| new_connection_string << "#{part};" unless part.downcase.strip.start_with?(*ommitted_parts) } # only keep parts not omitted
    "#{new_connection_string}Integrated Security=SSPI;"
  end

  # Convert encoding of a string to utf8
  def to_utf8(text)
    text.encode('UTF-8', 'binary', invalid: :replace, undef: :replace, replace: '')
  end
end
