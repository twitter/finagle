require 'rubygems'

require 'thrift'

require 'finagle-thrift/thrift/tracing_types'
# require 'finagle-thrift/thrift/tracing_constants'

require 'finagle-thrift/client'
require 'finagle-thrift/thrift_client'
require 'finagle-thrift/trace'
require 'finagle-thrift/tracer'

module FinagleThrift
  extend self
  def enable_tracing!(service, client_id = nil, service_name = nil)
    raise ArgumentError, "client_id must be nil or of type FinagleThrift::ClientId" if client_id && !client_id.is_a?(FinagleThrift::ClientId)
    class << service
      include ::FinagleThrift::ThriftClient
    end

    client_class = service.client_class
    client_class.class_eval do
      include ::FinagleThrift::Client
    end
    client_class.send(:define_method, :client_id) { client_id }
    client_class.send(:define_method, :trace_service_name) { service_name }
  end
end