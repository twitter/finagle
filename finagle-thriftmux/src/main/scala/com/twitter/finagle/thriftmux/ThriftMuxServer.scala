package com.twitter.finagle

import java.net.SocketAddress
import org.apache.thrift.protocol.{TProtocolFactory, TBinaryProtocol}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

/**
  * $serverExample
  *
  * @define serverExampleObject ThriftMuxServerImpl(..)
  */
case class ThriftMuxServerImpl(
  muxer: Server[ChannelBuffer, ChannelBuffer],
  protocolFactory: TProtocolFactory = new TBinaryProtocol.Factory()
) extends Server[Array[Byte], Array[Byte]] with ThriftRichServer {
  private def bufferToArray(buf: ChannelBuffer): Array[Byte] = 
    if (buf.hasArray && buf.arrayOffset == 0
        && buf.readableBytes == buf.array().length) {
      buf.array()
    } else {
      val arr = new Array[Byte](buf.readableBytes)
      buf.readBytes(arr)
      arr
    }

  def serve(addr: SocketAddress, newService: ServiceFactory[Array[Byte], Array[Byte]]) =
    muxer.serve(addr, newService map { service =>
      new Service[ChannelBuffer, ChannelBuffer] {
        def apply(req: ChannelBuffer) = {
          val arr = ThriftMuxUtil.bufferToArray(req)
          service(arr) map(ChannelBuffers.wrappedBuffer)
        }
        override def isAvailable = service.isAvailable
      }
    })
}

/**
  * A server for thrift served over [[com.twitter.finagle.mux]].
  *
  * $serverExample
  *
  * @define serverExampleObject ThriftMuxServer
  */
object ThriftMuxServer extends ThriftMuxServerImpl(MuxServer)
