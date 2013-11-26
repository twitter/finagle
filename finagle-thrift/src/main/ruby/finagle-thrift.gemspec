# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'finagle-thrift/version'

Gem::Specification.new do |s|
  s.name        = "finagle-thrift"
  s.version     = FinagleThrift::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Arya Asemanfar"]
  s.email       = ["arya@twitter.com"]
  s.summary     = ""
  s.description = "A Ruby client library for integrating into finagle's thrift tracing protocol"
  s.license     = 'Apache License Version 2.0'

  s.required_rubygems_version = ">= 1.3.5"

  # s.add_runtime_dependency 'thrift', '~> 0.6.0'

  s.files        = Dir.glob("{bin,lib}/**/*")
  s.require_path = 'lib'
end
