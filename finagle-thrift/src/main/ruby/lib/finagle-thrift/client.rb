module FinagleThrift
  module Client
    CanTraceMethodName = "__can__finagle__trace__v3__"

    include ::Thrift::Client

    alias_method :_orig_send_message, :send_message
    alias_method :_orig_receive_message, :receive_message

    def initialize(iprot, oprot=nil)
      super
      @upgraded = false
      attempt_upgrade!
    end

    def send_message(name, args_class, args = {})
      if @upgraded
        header = ::FinagleThrift::TracedRequestHeader.new
        header.trace_id = ::Trace.id.to_i
        header.span_id = ::Trace.next_id.to_i
        header.debug = false
        header.sampled = ::Trace.sampled?
        header.write(@oprot)
      end

      _orig_send_message(name, args_class, args)
    end

    def receive_message(klass)
      if @upgraded
        response = ::FinagleThrift::TracedResponseHeader.new
        response.read(@iprot)
      end

      _orig_receive_message(klass)
    end

    private
    def attempt_upgrade!
      _orig_send_message(CanTraceMethodName, ::FinagleThrift::TraceOptions)
      begin
        _orig_receive_message(::FinagleThrift::UpgradeReply)
        @upgraded = true
      rescue ::Thrift::ApplicationException
        @upgraded = false
      end
    rescue
      @oprot.trans.close
      raise
    end
  end
end
