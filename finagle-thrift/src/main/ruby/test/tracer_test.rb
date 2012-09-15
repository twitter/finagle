require 'rubygems'
require 'thrift'
require 'finagle-thrift'
require 'test/unit'
require 'base64'

class ScribeMock
  attr_accessor :message
  def log(message, category)
    @message = message
  end

  def batch
    yield
  end
end

class TracerTest < Test::Unit::TestCase
  def test_serialize_debug_true
    id = Trace::TraceId.new(0, 1, 2, true, Trace::Flags::DEBUG)
    annotation = Trace::Annotation.new(Trace::Annotation::SERVER_SEND, Trace::Endpoint.new(0, 0, "service"))
    annotation.timestamp = 123

    scribe = ScribeMock.new
    tracer = Trace::ZipkinTracer.new(scribe, 10)
    tracer.set_rpc_name(id, "name")
    tracer.record(id, annotation)
    assert_equal "CgABAAAAAAAAAAALAAMAAAAEbmFtZQoABAAAAAAAAAACCgAFAAAAAAAAAAEPAAYMAAAAAQoAAQAAAAAA" +
                 "AAB7CwACAAAAAnNzDAADCAABAAAAAAYAAgAACwADAAAAB3NlcnZpY2UAAA8ACAwAAAAAAgAJAQA=", scribe.message
  end

  def test_serialize_debug_false
    id = Trace::TraceId.new(0, 1, 2, true, Trace::Flags::EMPTY)
    annotation = Trace::Annotation.new(Trace::Annotation::SERVER_SEND, Trace::Endpoint.new(0, 0, "service"))
    annotation.timestamp = 123

    scribe = ScribeMock.new
    tracer = Trace::ZipkinTracer.new(scribe, 10)
    tracer.set_rpc_name(id, "name")
    tracer.record(id, annotation)
    assert_equal "CgABAAAAAAAAAAALAAMAAAAEbmFtZQoABAAAAAAAAAACCgAFAAAAAAAAAAEPAAYMAAAAAQoAAQAAAAAA" +
                 "AAB7CwACAAAAAnNzDAADCAABAAAAAAYAAgAACwADAAAAB3NlcnZpY2UAAA8ACAwAAAAAAgAJAAA=", scribe.message
  end
end