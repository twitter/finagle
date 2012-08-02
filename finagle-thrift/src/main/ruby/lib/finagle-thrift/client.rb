module FinagleThrift
  module Client
    CanTraceMethodName = "__can__finagle__trace__v3__"

    include ::Thrift::Client

    alias_method :_orig_send_message, :send_message
    alias_method :_orig_receive_message, :receive_message

    def initialize(iprot, oprot=nil)
      super
      @upgraded = false
      @client_port = 0
      attempt_upgrade!
    end

    def send_message(name, args_class, args = {})
      if @upgraded
        header = ::FinagleThrift::RequestHeader.new
        header.trace_id = Trace.id.trace_id.to_i
        header.parent_span_id = Trace.id.parent_id.to_i
        header.span_id = Trace.id.span_id.to_i
        header.sampled = Trace.id.sampled?
        header.flags = Trace.id.flags.to_i

        header.client_id = client_id if client_id

        header.write(@oprot)
      end
      Trace.record(Trace::Annotation.new(Trace::Annotation::CLIENT_SEND, self.endpoint))

      _orig_send_message(name, args_class, args)
    end

    def receive_message(klass)
      if @upgraded
        response = ::FinagleThrift::ResponseHeader.new
        response.read(@iprot)
      end
      result = _orig_receive_message(klass)
      Trace.record(Trace::Annotation.new(Trace::Annotation::CLIENT_RECV, self.endpoint))
      result
    end

    protected
    def client_id
      nil
    end

    def trace_service_name
      nil
    end

    def endpoint
      @endpoint ||= Trace.default_endpoint.with_port(@client_port).with_service_name(trace_service_name)
    end

    private
    def attempt_upgrade!
      _orig_send_message(CanTraceMethodName, ::FinagleThrift::ConnectionOptions)
      begin
        extract_client_port
        _orig_receive_message(::FinagleThrift::UpgradeReply)
        @upgraded = true
      rescue ::Thrift::ApplicationException
        @upgraded = false
      end
    rescue
      @upgraded = false
      raise
    end

    def extract_client_port
      @client_port = begin
        # im sorry
        trans = @oprot.instance_variable_get("@trans")
        transport = trans.instance_variable_get("@transport")
        handle = transport.instance_variable_get("@handle")
        Socket.unpack_sockaddr_in(handle.getsockname).first
      rescue
        0
      end
    end
  end
end
