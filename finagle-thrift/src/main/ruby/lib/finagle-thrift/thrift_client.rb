module FinagleThrift
  module ThriftClient
    def initialize(*args)
      super
      client_class.class_eval { include ::FinagleThrift::Client }
    end

    def handled_proxy(method_name, *args)
      Trace.push(Trace.id.next_id) do
        super(method_name, *args)
      end
    end
  end
end