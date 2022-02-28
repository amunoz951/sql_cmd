# coding: utf-8

Gem::Specification.new do |spec|
  spec.name          = 'sql_cmd'
  spec.version       = '0.2.1'
  spec.authors       = ['Alex Munoz']
  spec.email         = ['amunoz951@gmail.com']
  spec.license       = 'Apache-2.0'
  spec.summary       = 'Ruby library for running MSSQL queries using sqlcmd.exe style variables. Also provides canned operations like backups, restores, etc'
  spec.homepage      = 'https://github.com/amunoz951/sql_cmd'

  spec.required_ruby_version = '>= 2.3'

  spec.files         = Dir['LICENSE', 'lib/**/*', 'sql_scripts/**/*']
  spec.require_paths = ['lib']

  spec.add_dependency 'easy_json_config', '~> 0'
  spec.add_dependency 'chef_bridge', '~> 0'
  spec.add_dependency 'easy_format', '~> 0'
  spec.add_dependency 'easy_io', '~> 0'
  spec.add_dependency 'easy_time', '~> 0'
  spec.add_dependency 'hashly', '~> 0'
  spec.add_dependency 'zipr', '~> 0'
  spec.add_dependency 'json', '~> 2'
  spec.add_dependency 'open3', '~> 0'
end
