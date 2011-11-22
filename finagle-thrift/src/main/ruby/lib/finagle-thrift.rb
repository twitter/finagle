require 'rubygems'

require 'thrift'

require 'finagle-thrift/thrift/tracing_types'
# require 'finagle-thrift/thrift/tracing_constants'

require 'finagle-thrift/client'
require 'finagle-thrift/thrift_client'
require 'finagle-thrift/trace'

module FinagleThrift
  extend self
  def enable_tracing!(client_class, client_id = nil)
    raise ArgumentError, "client_id must be nil or of type FinagleThrift::ClientId" if client_id && !client_id.is_a?(FinagleThrift::ClientId)
    client_class.class_eval do
      include ::FinagleThrift::Client
    end
    client_class.send(:define_method, :client_id) { client_id }
  end
end