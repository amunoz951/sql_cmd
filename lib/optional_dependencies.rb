module OptionalDepedencies
  module_function

  def load_azure_blob_storage_dependencies
    azure_blob_storage_dependencies = {
      'azure/storage/common' => '>= 2.0.4',
      'azure/storage/blob' => '>= 2.0.3',
    }
    load_gem_list(azure_blob_storage_dependencies)
  end

  def load_gem_list(gem_list)
    gem_list.each do |current_gem, constraint|
      gem current_gem.tr('/', '-'), constraint
      require current_gem
    end
  rescue Gem::LoadError => e
    raise Gem::LoadError, "You are using functionality requiring the optional gem dependency '#{e.name}', but the gem is not installed, or is not using a version matching '#{e.requirement}'.\n\n#{e.message}"
  end
end
