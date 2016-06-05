package com.twitter.finagle

private object ThriftMuxUtil {
  val role = Stack.Role("ProtocolRecorder")

  def classForName(name: String) =
    try Class.forName(name) catch {
      case cause: ClassNotFoundException =>
        throw new IllegalArgumentException("Iface is not a valid thrift iface", cause)
    }

  val protocolRecorder: Stackable[ServiceFactory[mux.Request, mux.Response]] =
    new Stack.Module1[param.Stats, ServiceFactory[mux.Request, mux.Response]] {
      val role = ThriftMuxUtil.role
      val description = "Record ThriftMux protocol usage"
      def make(_stats: param.Stats, next: ServiceFactory[mux.Request, mux.Response]) = {
        val param.Stats(stats) = _stats
        stats.scope("protocol").provideGauge("thriftmux")(1)
        next
      }
    }
}
