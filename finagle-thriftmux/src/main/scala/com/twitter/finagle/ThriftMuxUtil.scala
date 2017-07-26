package com.twitter.finagle

private object ThriftMuxUtil {
  val role: Stack.Role = Stack.Role("ProtocolRecorder")

  def classForName(name: String): Class[_] =
    try Class.forName(name)
    catch {
      case cause: ClassNotFoundException =>
        throw new IllegalArgumentException("Iface is not a valid thrift iface", cause)
    }

  val protocolRecorder: Stackable[ServiceFactory[mux.Request, mux.Response]] =
    new Stack.Module1[param.Stats, ServiceFactory[mux.Request, mux.Response]] {
      val role: Stack.Role = ThriftMuxUtil.role
      val description: String = "Record ThriftMux protocol usage"
      def make(
        _stats: param.Stats,
        next: ServiceFactory[mux.Request, mux.Response]
      ): ServiceFactory[mux.Request, mux.Response] = {
        val param.Stats(stats) = _stats
        stats.scope("protocol").provideGauge("thriftmux")(1)
        next
      }
    }
}
