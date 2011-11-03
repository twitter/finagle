module Trace
  extend self
  DEFAULT_SAMPLE_RATE = 0.001
  TRACE_ID_UPPER_BOUND = 2 ** 64

  def id
    @id ||= TraceId.new(self.next_id)
  end

  def id=(v)
    @id = TraceId.new(v)
  end

  def next_id
    rand(TRACE_ID_UPPER_BOUND)
  end

  def sampled?
    rand < (@sample_rate || DEFAULT_SAMPLE_RATE)
  end

  def sample_rate=(sample_rate)
    if sample_rate > 1 || sample_rate < 0
      raise ArgumentError.new("sample rate must be [0,1]")
    end
    @sample_rate = sample_rate
  end

  class TraceId
    HEX_REGEX = /^[a-f0-9]{16}$/i
    MAX_SIGNED_I64 = 9223372036854775807
    MASK = (2 ** 64) - 1
    def initialize(v)
      @value = if v.is_a?(String) && v =~ HEX_REGEX
        v.hex
      elsif v.is_a?(Numeric)
        v
      else
        Trace.next_id
      end
      @i64 = if @value > MAX_SIGNED_I64
        -1 * ((@value ^ MASK) + 1)
      else
        @value
      end
    end

    def to_s; @value.to_s(16); end
    def to_i; @i64; end
  end
end
