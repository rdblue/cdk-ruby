#!/usr/bin/env ruby
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


# add all vendored jars to the classpath
require 'pathname'
root_path = Pathname.new(__FILE__).expand_path.parent.parent
vendor = root_path + 'vendor'
Dir[vendor + '*.jar'].each do |jar_file|
  require jar_file
end


# the Jarfile uses slf4j-log4j12, so configure log4j
#import 'org.apache.log4j.BasicConfigurator'
#BasicConfigurator.configure


# needed hadoop classes
module Hadoop
  module Conf
    import 'org.apache.hadoop.conf.Configuration'
  end

  module FS
    import 'org.apache.hadoop.fs.FileSystem'
    import 'org.apache.hadoop.fs.Path'
  end
end


# helpers for working with java
require 'pathname'
class Pathname
  def to_java_file
    java.io.File.new( expand_path.to_s )
  end

  def to_hadoop_path
    Hadoop::FS::Path.new( expand_path.to_s )
  end
end

module CDK
  VERSION = '0.1.0'

  # Ruby integration modules that will be mixed into CDK classes
  module Ruby
    # Methods for using a java Builder as a DSL
    #
    # This should be added to classes containing a Builder by:
    #   ClassToBuild.extend(BuilderMethods)
    #
    # Then, you can call the ClassToBuild::Builder's methods in a +build+
    # block:
    #   instance = ClassToBuild.build do
    #     property "value"
    #   end
    module BuildMethods
      class << self
        def extend_object( what )
          buildable << [ method_name( what.name ), what ]
          super
        end

        def buildable
          @buildable ||= []
        end

        private

        def method_name( class_name )
          name = class_name.rpartition('::').last
          name.gsub!( /([a-z])([A-Z])/, '\1_\2' )
          name.downcase!
          name.to_sym
        end
      end

      def build( *blocks, &block )
        block_or_instance = blocks.first
        if block_or_instance.nil? || block_or_instance.is_a?( Proc )
          builder = builder_class.new
        else
          builder = builder_class.new( blocks.shift )
        end

        # run all of the configuration blocks
        blocks.each do |b|
          builder.instance_exec( &b )
        end
        builder.instance_exec( &block )

        # return the configured instance
        builder.get
      end

      def build_from( base, *blocks, &block )
        # delegate to build
        build( base, *blocks, &block )
      end

      private

      def builder_class
        bclass = const_get(:Builder)

        # because we modify these clases, ensure it is persistent
        bclass.__persistent__ = true unless bclass.__persistent__

        throw RuntimeError.new(
            "Cannot find Builder class for " + self.class.to_s
          ) if bclass.nil?

        bclass
      end
    end

    module BuilderExt
      def add_builder_methods!
        BuildMethods.buildable.each do |method_name, buildable_class|
          next unless instance_methods.include? method_name
          add_builder_method( method_name, buildable_class )
        end
      end

      private

      def add_builder_method( method_name, buildable_class )
        # the associated class has a builder, add a builder method
        java_method_name = :"#{method_name}_java"
        alias_method java_method_name, method_name
        define_method( method_name ) do |*args, &block|
          if block.nil?
            send( java_method_name, *args )
          else
            send( java_method_name, buildable_class.build( *args, &block ) )
          end
        end
      end
    end
  end


  import 'com.cloudera.cdk.data.PartitionStrategy'
  class PartitionStrategy
    __persistent__ = true
    extend Ruby::BuildMethods

    class Builder
      extend Ruby::BuilderExt
      add_builder_methods!
    end
  end

  import 'com.cloudera.cdk.data.DatasetDescriptor'
  class DatasetDescriptor
    __persistent__ = true
    extend Ruby::BuildMethods

    class Builder
      extend Ruby::BuilderExt
      add_builder_methods!
    end
  end

  # import CDK data exceptions
  import 'com.cloudera.cdk.data.DatasetRepositoryException'
  import 'com.cloudera.cdk.data.DatasetExistsException'
  import 'com.cloudera.cdk.data.NoSuchDatasetException'
  import 'com.cloudera.cdk.data.UnknownFormatException'
  import 'com.cloudera.cdk.data.MetadataProviderException'
  import 'com.cloudera.cdk.data.DatasetException'
  import 'com.cloudera.cdk.data.DatasetReaderException'
  import 'com.cloudera.cdk.data.DatasetWriterException'

  # add convenience methods to DatasetRepository
  import 'com.cloudera.cdk.data.DatasetRepositories'
  class DatasetRepositories
    __persistent__ = true

    # keep implementations in here to avoid namespace clutter
    import 'com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository'
    class FileSystemDatasetRepository
      __persistent__ = true
      extend Ruby::BuildMethods

      class Builder
        extend Ruby::BuilderExt
        add_builder_methods!
      end
    end
  end

  class << self
    def load( path )
      file = Pathname.new( path ).expand_path
      throw ArgumentError.new("Path does not exist: " + file.to_s) unless file.exist?
      throw ArgumentError.new("Cannot read: " + file.to_s) unless file.readable?

      eval(file.read)
    end
  end

end

