module Trace
  class Tracer
    def record(id, annotation)
      raise "not implemented"
    end

    def set_rpc_name(id, name)
      raise "not implemented"
    end
  end

  class NullTracer < Tracer
    def record(id, annotation)
    end

    def set_rpc_name(id, name)
    end
  end

  class FanoutTracer < Tracer
    def initialize(tracers)
      @tracers = tracers
    end

    def record(id, annotation)
      @tracers.each { |tracer| tracer.record(id, annotation) }
    end

    def set_rpc_name(id, name)
      @tracers.each { |tracer| tracer.set_rpc_name(id, name) }
    end
  end

  class FileTracer < Tracer
    def initialize
      @last_trace_id = nil
      @file = nil
    end

    def record(id, annotation)
      return unless id.sampled?
      file = get_file_for_id(id)
      file.puts("#{id.to_s}: #{annotation.to_s}")
      file.flush if (annotation.is_a?(Annotation) && annotation.value == Annotation::SERVER_SEND)
    end

    def set_rpc_name(id, name)
      return unless id.sampled?
      get_file_for_id(id).puts("#{id.to_s}: name = #{name}")
    end

    private
    def get_file_for_id(id)
      id = id.trace_id.to_i
      if @last_trace_id != id
        @last_trace_id = id
        @file.close if @file
        @file = File.open("/tmp/traces/#{id}.log", "a")
      end
      @file
    end
  end

  class ZipkinTracer < Tracer
    TRACER_CATEGORY = "zipkin"
    def initialize(scribe, max_buffer)
      @scribe = scribe
      @max_buffer = max_buffer
      reset
    end

    def record(id, annotation)
      return unless id.sampled?
      span = get_span_for_id(id)

      case annotation
      when BinaryAnnotation
        span.binary_annotations << annotation
      when Annotation
        span.annotations << annotation
      end

      @count += 1
      if @count >= @max_buffer || (annotation.is_a?(Annotation) && annotation.value == Annotation::SERVER_SEND)
        flush!
      end
    end

    def set_rpc_name(id, name)
      return unless id.sampled?
      span = get_span_for_id(id)
      span.name = name.to_s
    end

    private
    def get_span_for_id(id)
      key = id.span_id.to_s
      @spans[key] ||= begin
        Span.new("", id)
      end
    end

    def reset
      @count = 0
      @spans = {}
    end

    def flush!
      @scribe.batch do
        messages = @spans.values.map do |span|
          buf = ''
          trans = Thrift::MemoryBufferTransport.new(buf)
          oprot = Thrift::BinaryProtocol.new(trans)
          span.to_thrift.write(oprot)
          binary = Base64.encode64(buf).gsub("\n", "")
          @scribe.log(binary, TRACER_CATEGORY)
        end
      end
      reset
    end
  end

  class Span
    attr_accessor :name, :annotations, :binary_annotations, :debug
    def initialize(name, span_id)
      @name = name
      @span_id = span_id
      @annotations = []
      @binary_annotations = []
      @debug = span_id.debug?
    end

    def to_thrift
      FinagleThrift::Span.new(
        :name => @name,
        :trace_id => @span_id.trace_id.to_i,
        :id => @span_id.span_id.to_i,
        :parent_id => @span_id.parent_id.nil? ? nil : @span_id.parent_id.to_i,
        :annotations => @annotations.map { |a| a.to_thrift },
        :binary_annotations => @binary_annotations.map { |a| a.to_thrift },
        :debug => @debug
      )
    end
  end

  class Annotation
    CLIENT_SEND = "cs"
    CLIENT_RECV = "cr"
    SERVER_SEND = "ss"
    SERVER_RECV = "sr"

    # write access for tests
    attr_accessor :value, :host, :timestamp
    def initialize(value, host)
      @timestamp = (Time.now.to_f * 1000 * 1000).to_i # micros
      @value = value
      @host = host
    end

    def to_thrift
      thrift_host = host ? host.to_thrift : nil
      FinagleThrift::Annotation.new(
        :value => @value,
        :host => thrift_host,
        :timestamp => @timestamp
      )
    end

    def to_s
      "#{@value} at #{@timestamp} for (#{@host.to_s})"
    end
  end

  class BinaryAnnotation
    module Type
      BOOL = "BOOL"
      BYTES = "BYTES"
      I16 = "I16"
      I32 = "I32"
      I64 = "I64"
      DOUBLE = "DOUBLE"
      STRING = "STRING"

      if {}.respond_to?(:key)
        def self.to_thrift(v)
          FinagleThrift::AnnotationType::VALUE_MAP.key(v)
        end
      else
        def self.to_thrift(v)
          FinagleThrift::AnnotationType::VALUE_MAP.index(v)
        end
      end
    end

    attr_reader :key, :value, :host
    def initialize(key, value, annotation_type, host)
      @key = key
      @value = value
      @annotation_type = annotation_type
      @host = host
    end

    def to_thrift
      thrift_host = host ? host.to_thrift : nil
      FinagleThrift::BinaryAnnotation.new(
        :key => @key,
        :value => @value,
        :annotation_type => Type.to_thrift(@annotation_type),
        :host => thrift_host
      )
    end

  end

  class Endpoint < Struct.new(:ipv4, :port, :service_name)
    MAX_I32 = ((2 ** 31) - 1)
    MASK = (2 ** 32) - 1

    def self.host_to_i32(host)
      unsigned_i32 = Socket.getaddrinfo(host, nil)[0][3].split(".").map do |i|
        i.to_i
      end.inject(0) { |a,e| (a << 8) + e }

      signed_i32 = if unsigned_i32 > MAX_I32
        -1 * ((unsigned_i32 ^ MASK) + 1)
      else
        unsigned_i32
      end

      signed_i32
    end

    def with_port(port)
      Endpoint.new(self.ipv4, port, self.service_name)
    end

    def with_service_name(service_name)
      Endpoint.new(self.ipv4, self.port, service_name)
    end

    def to_thrift
      FinagleThrift::Endpoint.new(
        :ipv4 => self.ipv4,
        :port => self.port,
        :service_name => self.service_name
      )
    end

    def to_s
      "#{service_name}@#{ipv4}:#{port}"
    end
  end

end
