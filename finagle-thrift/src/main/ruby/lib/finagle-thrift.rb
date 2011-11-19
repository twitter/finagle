require 'rubygems'

require 'thrift'

require 'finagle-thrift/thrift/tracing_types'
# require 'finagle-thrift/thrift/tracing_constants'

require 'finagle-thrift/client'
require 'finagle-thrift/thrift_client'
require 'finagle-thrift/trace'

module FinagleThrift
  extend self
  def enable_tracing!(client_class)
    client_class.class_eval { include ::FinagleThrift::Client }
  end
end