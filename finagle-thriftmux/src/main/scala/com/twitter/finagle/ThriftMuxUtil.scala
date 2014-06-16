package com.twitter.finagle

import org.jboss.netty.buffer.{ChannelBuffer => CB}

private object ThriftMuxUtil {
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

  object ProtocolRecorder extends Stack.Role
  val protocolRecorder = new Stack.Simple[ServiceFactory[CB, CB]](ProtocolRecorder) {
    val description = "Record ThriftMux protocol usage"
    def make(params: Stack.Params, next: ServiceFactory[CB, CB]) = {
      val param.Stats(stats) = params[param.Stats]
      stats.scope("protocol").counter("thriftmux").incr()
      next
    }
  }
}
