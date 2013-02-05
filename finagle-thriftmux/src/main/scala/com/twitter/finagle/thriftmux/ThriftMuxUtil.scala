package com.twitter.finagle

import org.jboss.netty.buffer.ChannelBuffer

private object ThriftMuxUtil {
  def bufferToArray(buf: ChannelBuffer): Array[Byte] = 
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
}
