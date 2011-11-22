module Trace
  extend self
  DEFAULT_SAMPLE_RATE = 0.001
  TRACE_ID_UPPER_BOUND = 2 ** 64

  def id
    if stack.empty?
      span_id = generate_id
      trace_id = TraceId.new(span_id, nil, span_id, should_sample?)
      stack.push(trace_id)
    end
    stack.last
  end

  def push(trace_id)
    stack.push(trace_id)
    if block_given?
      begin
        yield
      ensure
        pop
      end
    end
  end

  def pop
    stack.pop
  end

  def unwind
    if block_given?
      begin
        saved_stack = stack.dup
        yield
      ensure
        @stack = saved
      end
    end
  end

  def sample_rate=(sample_rate)
    if sample_rate > 1 || sample_rate < 0
      raise ArgumentError.new("sample rate must be [0,1]")
    end
    @sample_rate = sample_rate
  end

  class TraceId
    attr_reader :trace_id, :parent_id, :span_id, :sampled
    alias :sampled? :sampled
    def initialize(trace_id, parent_id, span_id, sampled)
      @trace_id = SpanId.from_value(trace_id)
      @parent_id = SpanId.from_value(parent_id)
      @span_id = SpanId.from_value(span_id)
      @sampled = !!sampled
    end

    def next_id
      TraceId.new(@trace_id, @span_id, Trace.generate_id, @sampled)
    end

    def to_s
      "TraceId(trace_id = #{@trace_id.to_s}, parent_id = #{@parent_id.to_s}, span_id = #{@span_id.to_s}, sampled = #{@sampled.to_s})"
    end
  end

  class SpanId
    HEX_REGEX = /^[a-f0-9]{16}$/i
    MAX_SIGNED_I64 = 9223372036854775807
    MASK = (2 ** 64) - 1

    def self.from_value(v)
      if v.is_a?(String) && v =~ HEX_REGEX
        new(v.hex)
      elsif v.is_a?(Numeric)
        new(v)
      elsif v.is_a?(SpanId)
        v
      end
    end

    def initialize(value)
      @value = value
      @i64 = if @value > MAX_SIGNED_I64
        -1 * ((@value ^ MASK) + 1)
      else
        @value
      end
    end

    def to_s; "%016x" % @value; end
    def to_i; @i64; end
  end

  def generate_id
    rand(TRACE_ID_UPPER_BOUND)
  end

  def should_sample?
    rand < (@sample_rate || DEFAULT_SAMPLE_RATE)
  end

  private

  def stack
    @stack ||= []
  end

end
