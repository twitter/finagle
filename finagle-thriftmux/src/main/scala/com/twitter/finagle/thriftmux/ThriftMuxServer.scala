package com.twitter.finagle

import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.server._
import com.twitter.finagle.thrift.{Protocols, HandleUncaughtApplicationExceptions}
import com.twitter.util.{Closable, Future}
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

/**
  * $serverExample
  *
  * @define serverExampleObject ThriftMuxServerImpl(...)
  */
case class ThriftMuxServerImpl(
  muxer: Server[ChannelBuffer, ChannelBuffer],
  protocolFactory: TProtocolFactory = Protocols.binaryFactory()
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
  "mux", ThriftMuxListener, new mux.ServerDispatcher(_, _, true)
)

/**
 * A server for Thrift served over [[com.twitter.finagle.mux]]. It's
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

package exp {
  /**
   * A [[com.twitter.finagle.server.StackServer]] for the ThriftMux protocol.
   */
  private[finagle]
  class ThriftMuxStackServer(
      stack: Stack[ServiceFactory[ChannelBuffer, ChannelBuffer]],
      params: Stack.Params)
    extends StackServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
      stack, params)
  {
    def this() = this(StackServer.newStack[ChannelBuffer, ChannelBuffer], Stack.Params.empty)

    protected val listener: Listener[ChannelBuffer, ChannelBuffer] = ThriftMuxListener
    protected val newDispatcher: StackServer.Dispatcher[
      ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer
    ] = new mux.ServerDispatcher(_, _, true)
  }

  private[finagle]
  object ThriftMuxServerImpl {
    /**
     * Wraps a ChannelBuffer-typed [[com.twitter.finagle.server.StackServer]]
     * to one typed on Array[Byte]. Used to translate Mux servers (which operate
     * in terms of `ChannelBuffer`s) to servers that speak Thrift.
     *
     * TODO: Factor this into StackServer.scala if we ever encounter a need for
     * this sort of wrapping when implementing other protocols atop Mux.
     */
    case class WrappedMuxStackServer(
        underlying: StackServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer])
      extends StackServer[Array[Byte], Array[Byte], Any, Any]
    {
      // This StackServer will proxy to `underlying`'s listener and dispatcher, so
      // we provide null objects here.
      protected val listener = NullListener
      protected val newDispatcher = (_: Any, _: Any) => Closable.nop

      /**
       * @inheritdoc
       */
      override def configured[P: Stack.Param](p: P): StackServer[Array[Byte], Array[Byte], Any, Any] =
        copy(underlying = underlying.configured(p))

      /**
       * @inheritdoc
       */
      override def serve(
        addr: SocketAddress,
        newService: ServiceFactory[Array[Byte], Array[Byte]]
      ) = {
        underlying.serve(addr, newService map { service =>
          val converter = new Filter[ChannelBuffer, ChannelBuffer, Array[Byte], Array[Byte]] {
            def apply(
              request: ChannelBuffer,
              service: Service[Array[Byte], Array[Byte]]
            ): Future[ChannelBuffer] = {
              val arr = ThriftMuxUtil.bufferToArray(request)
              service(arr) map(ChannelBuffers.wrappedBuffer)
            }
          }

          // Need a HandleUncaughtApplicationExceptions filter here to maintain
          // the backward compatibility with non-mux thrift clients. Mux thrift
          // clients get the same semantics as a side effect.
          //
          // TODO: Once we make TProtocolFactory a Param, don't hardcode it here.
          val uncaughtExceptionsFilter =
            new HandleUncaughtApplicationExceptions(Protocols.binaryFactory())

          converter andThen uncaughtExceptionsFilter andThen service
        })
      }
    }
  }

  /**
   * $serverExample
   *
   * @define serverExampleObject ThriftMuxServerImpl(...)
   */
  private[finagle]
  class ThriftMuxServerImpl(
      server: StackServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer])
    extends StackServerLike[
      Array[Byte], Array[Byte], Any, Any, ThriftMuxServerImpl
    ](ThriftMuxServerImpl.WrappedMuxStackServer(server))
    with ThriftRichServer
  {
    protected val protocolFactory: TProtocolFactory = Protocols.binaryFactory()

    protected def newInstance(
      server: StackServer[Array[Byte], Array[Byte], Any, Any]
    ): ThriftMuxServerImpl = server match {
      case ThriftMuxServerImpl.WrappedMuxStackServer(cbServer) =>
        // Unwrap and pass through the ChannelBuffer-typed underlying server.
        // The advantage of extracting in this way is to avoid having to layer
        // additional ChannelBuffer<->Array[Byte] conversions.
        new ThriftMuxServerImpl(cbServer)

      case other => throw new UnsupportedOperationException(
        "Cannot create a ThriftMuxServerImpl from a non-Mux underlying server"
      )
    }
  }

  /**
   * A [[com.twitter.finagle.server.StackServer]] factory function for use
   * with [[com.twitter.finagle.builder.ServerBuilder]].
   */
  object ThriftMuxStackServerFactory
    extends (Stack.Params => StackServer[Array[Byte], Array[Byte], Any, Any])
  {
    def apply(params: Stack.Params): StackServer[Array[Byte], Array[Byte], Any, Any] =
      ThriftMuxServerImpl.WrappedMuxStackServer(
        new ThriftMuxStackServer(
          StackServer.newStack[ChannelBuffer, ChannelBuffer],
          params
        )
      )
  }

  /**
   * A Thrift server served over [[com.twitter.finagle.mux]]. The same
   * backwards-compatibility as in [[com.twitter.finagle.thriftmux.ThriftMuxServer]]
   * is guaranteed.
   *
   * $serverExample
   *
   * @define serverExampleObject ThriftMuxServer
   */
  object ThriftMuxServer extends ThriftMuxServerImpl(new ThriftMuxStackServer)
}
