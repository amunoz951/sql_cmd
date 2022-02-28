#
# Author:: Alex Munoz (<amunoz951@gmail.com>)
# Copyright:: Copyright (c) 2020 Alex Munoz
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'time'
require 'easy_json_config'
require 'chef_bridge'
require 'easy_format'
require 'easy_io'
require 'zipr'
require 'json'
require 'open3'
require 'fileutils'
require 'easy_format'
require 'easy_time'
require 'hashly'

# SqlCmd Modules
require_relative 'sql_cmd/config'
require_relative 'sql_cmd/format'
require_relative 'sql_cmd/always_on'
require_relative 'sql_cmd/backups'
require_relative 'sql_cmd/database'
require_relative 'sql_cmd/agent'
require_relative 'sql_cmd/query'
require_relative 'sql_cmd/security'
require_relative 'sql_cmd/azure'
require_relative 'optional_dependencies'

# Assign globals
EasyTime.timezone = SqlCmd.config['environment']['timezone']
