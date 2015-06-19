module Trace
  extend self
  DEFAULT_SAMPLE_RATE = 0.001
  TRACE_ID_UPPER_BOUND = 2 ** 64
  TRACE_STACK = :trace_stack

  def id
    if stack.empty?
      span_id = generate_id
      trace_id = TraceId.new(span_id, nil, span_id, should_sample?, Flags::EMPTY)
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
        stack = saved_stack
      end
    end
  end

  def record(annotation)
    tracer.record(id, annotation) unless stack.empty?
  end

  def set_rpc_name(name)
    tracer.set_rpc_name(id, name) unless stack.empty?
  end

  def sample_rate=(sample_rate)
    if sample_rate > 1 || sample_rate < 0
      raise ArgumentError.new("sample rate must be [0,1]")
    end
    @sample_rate = sample_rate
  end

  def tracer=(tracer)
    @tracer = tracer
  end

  class TraceId
    attr_reader :trace_id, :parent_id, :span_id, :sampled, :flags

    def initialize(trace_id, parent_id, span_id, sampled, flags)
      @trace_id = SpanId.from_value(trace_id)
      @parent_id = parent_id.nil? ? nil : SpanId.from_value(parent_id)
      @span_id = SpanId.from_value(span_id)
      @sampled = !!sampled
      @flags = flags
    end

    def next_id
      TraceId.new(@trace_id, @span_id, Trace.generate_id, @sampled, @flags)
    end

    # the debug flag is used to ensure the trace passes ALL samplers
    def debug?
      @flags & Flags::DEBUG == Flags::DEBUG
    end

    def sampled?
      debug? || @sampled
    end

    def to_s
      "TraceId(trace_id = #{@trace_id.to_s}, parent_id = #{@parent_id.to_s}, span_id = #{@span_id.to_s}, sampled = #{@sampled.to_s}, flags = #{@flags.to_s})"
    end
  end

  # there are a total of 64 flags that can be passed down with the various tracing headers
  # at the time of writing only one is used (debug).
  #
  # Note that using the 64th bit in Ruby requires some sign conversion since Thrift i64s are signed
  # but Ruby won't do the right thing if you try to set 1 << 64
  class Flags
    # no flags set
    EMPTY = 0
    # the debug flag is used to ensure we pass all the sampling layers and that the trace is stored
    DEBUG = 1
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

  def default_endpoint=(endpoint)
    @default_endpoint = endpoint
  end

  def default_endpoint
    @default_endpoint ||= begin
      Endpoint.new(Endpoint.host_to_i32(Socket.gethostname), 0, "finagle-ruby")
    end
  end

  private

  # "stack" acts as a thread local variable and cannot be shared between
  # threads.
  def stack=(stack)
    Thread.current[TRACE_STACK] = stack
  end

  def stack
    Thread.current[TRACE_STACK] ||= []
  end

  def tracer
    @tracer ||= NullTracer.new
  end

end
