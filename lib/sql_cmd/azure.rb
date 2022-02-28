module SqlCmd
  module Azure
    module AttachedStorage
      module_function

      # Upload a file to blob storage
      # Either container_name or storage_url is required
      def upload(filename, content, storage_account_name, storage_access_key, container_name: nil, storage_url: nil)
        raise 'Either :container_name or :storage_url must be specified for SqlCmd::Azure::AttachedStorage.upload' if container_name.nil? && storage_url.nil?
        OptionalDepedencies.load_azure_blob_storage_dependencies

        # initialize ::Azure::Storage::Blob::BlobService client
        client = ::Azure::Storage::Blob::BlobService.create(storage_account_name: storage_account_name, storage_access_key: storage_access_key)
        container_name ||= container_name_from_url(storage_url, client: client) || ::File.basename(storage_url)
        relative_path = storage_url.nil? ? filename : storage_file_relative_path(storage_url, filename, container_name)

        # Add retry filter to the client
        client.with_filter(::Azure::Storage::Common::Core::Filter::ExponentialRetryPolicyFilter.new)

        client.create_block_blob(container_name, relative_path, content)
      end

      # Download a file from blob storage
      # Either container_name or storage_url is required
      def download(filename, destination_path, storage_account_name, storage_access_key, container_name: nil, storage_url: nil)
        raise 'Either :container_name or :storage_url must be specified for SqlCmd::Azure::AttachedStorage.download' if container_name.nil? && storage_url.nil?
        OptionalDepedencies.load_azure_blob_storage_dependencies

        # initialize ::Azure::Storage::Blob::BlobService client
        client = ::Azure::Storage::Blob::BlobService.create(storage_account_name: storage_account_name, storage_access_key: storage_access_key)
        container_name ||= container_name_from_url(storage_url, client: client) || ::File.basename(storage_url)
        relative_path = storage_url.nil? ? filename : storage_file_relative_path(storage_url, filename, container_name)

        # Add retry filter to the client
        client.with_filter(::Azure::Storage::Common::Core::Filter::ExponentialRetryPolicyFilter.new)

        FileUtils.mkdir_p(::File.dirname(destination_path))
        _blob, content = client.get_blob(container_name, relative_path)
        ::File.open(destination_path, 'wb') { |f| f.write(content) }
      end

      # List files that in blob storage
      # Either container_name or storage_url is required
      # Returns hash of file URLs and their properties
      def list(storage_account_name, storage_access_key, container_name: nil, storage_url: nil, filename_prefix: nil)
        raise 'Either :container_name or :storage_url must be specified for SqlCmd::Azure::AttachedStorage.list' if container_name.nil? && storage_url.nil?
        OptionalDepedencies.load_azure_blob_storage_dependencies

        # initialize ::Azure::Storage::Blob::BlobService client
        client = ::Azure::Storage::Blob::BlobService.create(storage_account_name: storage_account_name, storage_access_key: storage_access_key)
        container_name ||= container_name_from_url(storage_url, client: client) || ::File.basename(storage_url)
        relative_path = storage_url.nil? ? filename_prefix : storage_file_relative_path(storage_url, filename_prefix, container_name)

        # Add retry filter to the client
        client.with_filter(::Azure::Storage::Common::Core::Filter::ExponentialRetryPolicyFilter.new)

        # Get list of files matching filename_prefix
        blob_list_options = filename_prefix.nil? ? {} : { prefix: relative_path }
        files = client.list_blobs(container_name, blob_list_options)
        files.nil? ? [] : files.map { |f| [f.name, f.properties] }.to_h
      end

      def container_name_from_url(storage_url, client: nil)
        if client.nil?
          OptionalDepedencies.load_azure_blob_storage_dependencies

          # initialize ::Azure::Storage::Blob::BlobService client
          client = ::Azure::Storage::Blob::BlobService.create(storage_account_name: storage_account_name, storage_access_key: storage_access_key)
        end
        client.list_containers.map(&:name).select { |c| storage_url =~ %r{/#{Regexp.escape(c)}(/|$)} }.first
      end

      def storage_file_relative_path(storage_url, filename, container_name)
        relative_path = storage_url.sub(/.+?#{Regexp.escape(container_name)}/i, '') + "/#{filename}"
        relative_path.gsub!('//', '/')
        relative_path.reverse.chomp('/').reverse
      end
    end
  end
end
