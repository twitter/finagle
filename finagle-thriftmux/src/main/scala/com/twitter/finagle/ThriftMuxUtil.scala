package com.twitter.finagle

import org.jboss.netty.buffer.{ChannelBuffer => CB}

private object ThriftMuxUtil {
  val role = Stack.Role("ProtocolRecorder")
  def bufferToArray(buf: CB): Array[Byte] =
    if (buf.hasArray && buf.arrayOffset == 0
        && buf.readableBytes == buf.array().length) {
      buf.array()
    } else {
      val arr = new Array[Byte](buf.readableBytes)
      buf.readBytes(arr)
      arr
    }

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
