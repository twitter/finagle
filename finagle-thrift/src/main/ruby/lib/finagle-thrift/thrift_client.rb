module FinagleThrift
  module ThriftClient
    def handled_proxy(method_name, *args)
      Trace.push(Trace.id.next_id) do
        Trace.set_rpc_name(method_name)
        super(method_name, *args)
      end
    end
  end
end