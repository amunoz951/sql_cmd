module SqlCmd
  module Agent
    module Job
      module_function

      def exists?(connection_string, job_name)
        status(connection_string, job_name)['LastRunStatus'] != 'NoJob'
      end

      def running?(connection_string, job_name)
        !run_status(connection_string, job_name).empty?
      end

      def status(connection_string, job_name)
        raise 'Failed to get job status! The connection_string argument must be specified.' if connection_string.nil? || connection_string.empty?
        raise 'Failed to get job status! The job_name argument must be specified.' if job_name.nil? || job_name.empty?
        return { 'LastRunStatus' => 'Running' } if running?(connection_string, job_name)
        sql_script = ::File.read("#{SqlCmd.sql_script_dir}/Agent/JobLastRunInfo.sql")
        server_connection_string = SqlCmd.remove_connection_string_part(connection_string, :database)
        SqlCmd.execute_query(server_connection_string, sql_script, return_type: :first_row, values: { 'jobname' => job_name }, readonly: true, retries: 3) || { 'LastRunStatus' => 'NoJob' }
      end

      def run_status(connection_string, job_name)
        raise 'Failed to get job status! The connection_string argument must be specified.' if connection_string.nil? || connection_string.empty?
        raise 'Failed to get job status! The job_name argument must be specified.' if job_name.nil? || job_name.empty?
        sql_script = ::File.read("#{SqlCmd.sql_script_dir}/Agent/JobRunStatus.sql")
        server_connection_string = SqlCmd.remove_connection_string_part(connection_string, :database)
        SqlCmd.execute_query(server_connection_string, sql_script, return_type: :first_row, values: { 'jobname' => job_name }, readonly: true, retries: 3) || {}
      end
    end
  end
end
