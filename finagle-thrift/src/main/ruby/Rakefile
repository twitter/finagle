$LOAD_PATH.unshift File.expand_path("../lib", __FILE__)

require 'rubygems'

require 'finagle-thrift/version'
require 'rake/testtask'

desc 'Default: run unit tests.'
task :default => :test

desc 'Test the gem.'
Rake::TestTask.new(:test) do |t|
  t.libs << 'lib'
  t.pattern = 'test/**/*_test.rb'
  t.verbose = true
end

desc 'Build the gem'
task :build do
  system "gem build finagle-thrift.gemspec"
end
