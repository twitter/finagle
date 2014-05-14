package com.twitter.finagle

import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.server._
import com.twitter.finagle.thrift.{Protocols, HandleUncaughtApplicationExceptions}
import com.twitter.util.{Closable, Future}
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.buffer.{ChannelBuffer => CB, ChannelBuffers}

/**
 * @define serverDescription
 *
 * A server for the Thrift protocol served over [[com.twitter.finagle.mux]].
 * ThriftMuxServer is backwards-compatible with Thrift clients that use the
 * framed transport and binary protocol. It switches to the backward-compatible
 * mode when the first request is not recognized as a valid Mux message but can
 * be successfully handled by the underlying Thrift service. Since a Thrift
 * message that is encoded with the binary protocol starts with a header value of
 * 0x800100xx, Mux does not confuse it with a valid Mux message (0x80 = -128 is
 * an invalid Mux message type) and the server can reliably detect the non-Mux
 * Thrift client and switch to the backwards-compatible mode.
 *
 * Note that the server is also compatible with non-Mux finagle-thrift clients.
 * It correctly responds to the protocol up-negotiation request and passes the
 * tracing information embedded in the thrift requests to Mux (which has native
 * tracing support).
 *
 * $serverDescription
 *
 * $serverExample
 *
 * @define serverExampleObject ThriftMuxServerImpl(...)
 */
class ThriftMuxServerImpl(
  muxer: Server[CB, CB] = ProtocolRecordingMuxServer,
  protected val protocolFactory: TProtocolFactory = Protocols.binaryFactory()
) extends Server[Array[Byte], Array[Byte]] with ThriftRichServer {
  def serve(addr: SocketAddress, newService: ServiceFactory[Array[Byte], Array[Byte]]) = {
    muxer.serve(addr, newService map { service =>
      val converter = new Filter[CB, CB, Array[Byte], Array[Byte]] {
        def apply(request: CB, service: Service[Array[Byte], Array[Byte]]): Future[CB] = {
          val arr = ThriftMuxUtil.bufferToArray(request)
          service(arr) map ChannelBuffers.wrappedBuffer
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
  extends Netty3Listener[CB, CB]("thrift", thriftmux.PipelineFactory)

object ProtocolRecordingMuxServer extends DefaultServer[CB, CB, CB, CB](
  "thrift", ThriftMuxListener, new mux.ServerDispatcher(_, _, true)
) {
  private[this] val protocolCounter = statsReceiver.scope("protocol").counter("thriftmux")

  override def serve(addr: SocketAddress, newService: ServiceFactory[CB, CB]): ListeningServer = {
    protocolCounter.incr()
    super.serve(addr, newService)
  }
}

package thriftmux.exp {
  /**
   * A [[com.twitter.finagle.server.StackServer]] for the ThriftMux protocol.
   */
  private[finagle] object ThriftMuxer extends StackServer[CB, CB, CB, CB] {
    protected val newListener: Stack.Params => Listener[CB, CB] = { params =>
      val Stats(stats) = params[Stats]
      stats.scope("protocol").counter("thriftmux").incr()

      Netty3Listener[CB, CB](thriftmux.PipelineFactory, params)
    }

    protected val newDispatcher: Stack.Params => Dispatcher =
      Function.const(new mux.ServerDispatcher(_, _, true))
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
  private[finagle] class ThriftMuxServer(
      muxer: StackServer[CB, CB, CB, CB])
    extends Server[Array[Byte], Array[Byte]]
    with ThriftRichServer
  {
    // TODO: Make TProtocol definable via a Param.
    protected val protocolFactory: TProtocolFactory = Protocols.binaryFactory()

    private[this] val bufToArrayFilter =
      new Filter[CB, CB, Array[Byte], Array[Byte]] {
        def apply(
          request: CB, service: Service[Array[Byte], Array[Byte]]
        ): Future[CB] = {
          val arr = ThriftMuxUtil.bufferToArray(request)
          service(arr) map ChannelBuffers.wrappedBuffer
        }
      }

    def configured[P: Stack.Param](p: P): ThriftMuxServer =
      new ThriftMuxServer(muxer.configured(p))

    def serve(addr: SocketAddress, factory: ServiceFactory[Array[Byte], Array[Byte]]) = {
      muxer.configured(Label("thrift")).serve(addr, factory map { service =>
        // Need a HandleUncaughtApplicationExceptions filter here to maintain
        // the backward compatibility with non-mux thrift clients. Mux thrift
        // clients get the same semantics as a side effect.
        val uncaughtExceptionsFilter = new HandleUncaughtApplicationExceptions(protocolFactory)
        bufToArrayFilter andThen uncaughtExceptionsFilter andThen service
      })
    }
  }

  /**
   * A [[com.twitter.finagle.server.StackServer]] for use with
   * the legacy [[com.twitter.finagle.builder.ServerBuilder]].
   */
  private[finagle] class ThriftMuxStackServer(underlying: ThriftMuxServer)
    extends StackServer[Array[Byte], Array[Byte], Any, Any]
  {
    // This StackServer's `serve` method proxies to `underlying`, so we do not
    // need to provide an actual listener or dispatcher. In order to avoid
    // failing silently, we throw if these vals are every referenced directly.
    protected val newListener: Stack.Params => Listener[Any, Any] = { _ =>
      throw new UnsupportedOperationException(
        "`ThriftMuxStackServer.newListener` should not be referenced directly")
    }

    protected val newDispatcher: Stack.Params => Dispatcher = { _ =>
      throw new UnsupportedOperationException(
        "`ThriftMuxStackServer.newDispatcher` should not be referenced directly")
    }

    override def configured[P: Stack.Param](p: P) =
      new ThriftMuxStackServer(underlying.configured(p))

    override def serve(addr: SocketAddress, factory: ServiceFactory[Array[Byte], Array[Byte]]) =
      underlying.serve(addr, factory)
  }

  // The API endpoint that works with the new apis and serverbuilder
  object ThriftMuxServer extends ThriftMuxServer(ThriftMuxer)
    with (Stack.Params => StackServer[Array[Byte], Array[Byte], Any, Any])
  {
    def apply(params: Stack.Params) = new ThriftMuxStackServer(ThriftMuxServer)
  }
}
