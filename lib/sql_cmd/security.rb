module SqlCmd
  module Security
    module_function

    # create a credential in SQL server
    #
    # options:
    #   verbose: Include an output message even if it's only updating an existing login
    def create_credential(connection_string, name, identity, secret, options = {})
      # TODO: don't update password if it hasn't changed
      return if options['skip_credential_creation']
      sql_script = ::File.read("#{SqlCmd.sql_script_dir}/Security/CreateOrUpdateCredential.sql")
      raise 'name for credential must not be empty!' if name.nil? || name.empty?
      raise "secret for credential [#{name}] must not be empty!" if secret.nil? || secret.empty?
      identity ||= name
      values = { 'credential_name' => name, 'identity' => identity, 'secret' => secret }
      message = SqlCmd.execute_query(connection_string, sql_script, return_type: :scalar, values: values, readonly: true, retries: 3, retry_delay: 5)
      EasyIO.logger.info message if message =~ /created/ || options['verbose']
    end
  end
end
