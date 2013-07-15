package com.twitter.finagle

import java.net.SocketAddress
import org.apache.thrift.protocol.{TProtocolFactory, TBinaryProtocol}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.server.DefaultServer
import com.twitter.finagle.thrift.HandleUncaughtApplicationExceptions
import com.twitter.util.Future

/**
  * $serverExample
  *
  * @define serverExampleObject ThriftMuxServerImpl(..)
  */
case class ThriftMuxServerImpl(
  muxer: Server[ChannelBuffer, ChannelBuffer],
  protocolFactory: TProtocolFactory = new TBinaryProtocol.Factory()
) extends Server[Array[Byte], Array[Byte]] with ThriftRichServer {
  def serve(addr: SocketAddress, newService: ServiceFactory[Array[Byte], Array[Byte]]) = {
    muxer.serve(addr, newService map { service =>
      val converter = new Filter[ChannelBuffer, ChannelBuffer, Array[Byte], Array[Byte]] {
        def apply(request: ChannelBuffer, service: Service[Array[Byte], Array[Byte]]): Future[ChannelBuffer] = {
          val arr = ThriftMuxUtil.bufferToArray(request)
          service(arr) map(ChannelBuffers.wrappedBuffer)
        }
      }
      // Need a HandleUncaughtApplicationExceptions filter here to maintain
      // the backward compatibility with non-mux thrift clients. Mux thrift
      // clients get the same semantics as a side effect.
      val uncaughtExceptionsFilter = new HandleUncaughtApplicationExceptions(protocolFactory)
      converter andThen uncaughtExceptionsFilter andThen service
    })
  }
}



object ThriftMuxListener
  extends Netty3Listener[ChannelBuffer, ChannelBuffer]("thriftmux", thriftmux.PipelineFactory)

object ThriftMuxer extends DefaultServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
  "mux", ThriftMuxListener, new mux.ServerDispatcher(_, _)
)

/**
 * A server for thrift served over [[com.twitter.finagle.mux]]. It's
 * also backward compatible with thrift clients that use framed
 * transport and binary protocol with strict write. It switches to the
 * backward-compatible mode when the first request is not recognized
 * as a valid mux message but can be successfully handled by the
 * underlying thrift server. Since a thrift message that is encoded
 * by binary protocol with strict write starts with a header
 * 0x800100xx, mux does not confuse it with a valid mux message (
 * 0x80 = -128 is an invalid mux message type) and the server can
 * reliably detect the non-mux thrift client and switch to the
 * backward-compatible mode afterwards. Note the server is also
 * compatible with Finagle thrift clients. It correctly responds to
 * the protocol up-negotiation request and passes the tracing
 * information embedded in the thrift requests to mux which has
 * native tracing support.
 *
 * $serverExample
 *
 * @define serverExampleObject ThriftMuxServer
 */
object ThriftMuxServer extends ThriftMuxServerImpl(ThriftMuxer)
