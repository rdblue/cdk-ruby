# 
# Copyright 2013 Cloudera Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# add bundler's packaging tasks
require "bundler/gem_tasks"

# Add :vendor_jars to the :build task's dependencies
task :build => [ :vendor_jars ]

# add a create-directory task for vendor
require 'pathname'
vendor = Pathname.new(__FILE__).dirname + 'vendor'
directory vendor.to_path

# copies jars from the local maven repository to vendor/
task :vendor_jars => [ 'Jarfile.lock', vendor.to_path ] do
  # JBundler reads the Jarfile.lock and creates JBUNDLER_CLASSPATH
  require 'jbundler'
  JBUNDLER_CLASSPATH.each do |jar|
    FileUtils.copy( jar, vendor )
  end
end

# resolves the Jarfile dependencies
file 'Jarfile.lock' => 'Jarfile' do
  sh 'jbundle install'
  rm_rf vendor # remove the current vendor directory
end

