require 'rubygems'
require 'thrift'
require 'finagle-thrift'
require 'test/unit'

class TraceIdTest < Test::Unit::TestCase
  def test_debug_flag
    id = Trace::TraceId.new(0, 1, 2, false, Trace::Flags::DEBUG)
    assert_equal true, id.debug?
    assert_equal true, id.next_id.debug?
    # the passed in sampled is overriden if debug is true
    assert_equal true, id.sampled?
    assert_equal true, id.next_id.sampled?

    id = Trace::TraceId.new(0, 1, 2, false, Trace::Flags::EMPTY)
    assert_equal false, id.debug?
    assert_equal false, id.next_id.debug?
  end

  def test_span_id
    id = Trace::SpanId.new(Trace.generate_id)
    id2 = Trace::SpanId.from_value(id.to_s)
    assert_equal id.to_i, id2.to_i
  end
end