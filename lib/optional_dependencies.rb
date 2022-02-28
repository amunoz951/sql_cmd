module OptionalDepedencies
  module_function

  def load_azure_blob_storage_dependencies
    azure_blob_storage_dependencies = {
      # 'faraday-excon' => '1.1.0',
      # 'faraday-net_http' => '1.0.1',
      # 'faraday-net_http_persistent' => '1.1.0',
      # 'ruby2_keywords' => '0.0.4',
      # 'faraday' => '1.4.1',
      # 'faraday_middleware' => '1.0.0',
      # 'connection_pool' => '2.2.5'
      # 'net-http-persistent' => '4.0.1',
      # 'racc' => '1.5.2',
      # 'nokogiri' => '1.11.4',
      'azure/storage/common' => '2.0.2',
      'azure/storage/blob' => '2.0.1',
    }
    load_gem_list(azure_blob_storage_dependencies)
  end

  def load_gem_list(gem_list)
    gem_list.each do |current_gem, constraint|
      gem current_gem.tr('/', '-'), constraint
      require current_gem
    end
  rescue Gem::LoadError => e
    raise Gem::LoadError, "You are using functionality requiring the optional gem dependency '#{e.name}', but the gem is not loaded, or is not using a version matching '#{e.requirement}'."
  end
end
