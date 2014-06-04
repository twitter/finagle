package com.twitter.finagle

import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.server._
import com.twitter.finagle.thrift.{Protocols, HandleUncaughtApplicationExceptions}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Future}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicInteger
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

/**
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
 * This class can't be instantiated. For a default instance of ThriftMuxServerLike,
 * see [[com.twitter.finagle.ThriftMuxServer]]
 */
class ThriftMuxServerLike private[finagle](
  muxer: StackServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer]
) extends Server[Array[Byte], Array[Byte]] with ThriftRichServer
  with (Stack.Params => Server[Array[Byte], Array[Byte]]) {

  /**
   * The [[com.twitter.finagle.ServiceFactory]] stack that requests
   * are dispatched through.
   */
  val stack = muxer.stack

  /**
   * The [[com.twitter.finagle.Stack.Params]] used to configure
   * the stack.
   */
  val params = muxer.params

  // TODO: thread this in via Stack.Params
  protected val protocolFactory: TProtocolFactory = Protocols.binaryFactory()

  private[this] val bufToArrayFilter =
    new Filter[ChannelBuffer, ChannelBuffer, Array[Byte], Array[Byte]] {
      def apply(
        request: ChannelBuffer, service: Service[Array[Byte], Array[Byte]]
      ): Future[ChannelBuffer] = {
        val arr = ThriftMuxUtil.bufferToArray(request)
        service(arr) map ChannelBuffers.wrappedBuffer
      }
    }

  /**
   * Create a new ThriftMuxServerLike with `params` used to configure the
   * muxer. This makes `ThriftMuxServerLike` compatible with the legacy
   * [[com.twitter.finagle.builder.ServerBuilder]].
   */
  def apply(params: Stack.Params): Server[Array[Byte], Array[Byte]] =
    new ThriftMuxServerLike(muxer.copy(params = params))

  /**
   * Create a new ThriftMuxServerLike with `p` added to the
   * parameters used to configure the `muxer`.
   */
  def configured[P: Stack.Param](p: P): ThriftMuxServerLike =
    new ThriftMuxServerLike(muxer.configured(p))

  def serve(addr: SocketAddress, factory: ServiceFactory[Array[Byte], Array[Byte]]) = {
    muxer.serve(addr, factory map { service =>
      // Need a HandleUncaughtApplicationExceptions filter here to maintain
      // the backward compatibility with non-mux thrift clients. Mux thrift
      // clients get the same semantics as a side effect.
      val uncaughtExceptionsFilter = new HandleUncaughtApplicationExceptions(protocolFactory)
      bufToArrayFilter andThen uncaughtExceptionsFilter andThen service
    })
  }
}

private[finagle] object ThriftMuxServerStack {
  def apply(): Stack[ServiceFactory[ChannelBuffer, ChannelBuffer]] =
    ThriftMuxUtil.protocolRecorder +: exp.MuxServer.stack
}

/**
 * A muxer for thrift servers. Note, instead of just deferring to a predefined
 * [[com.twitter.finagle.MuxServer]], we redefine the listener because we have a
 * custom pipeline that supports downgrading to vanilla thrift.
 */
private[finagle] object ThriftServerMuxer
  extends StackServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
    ThriftMuxServerStack(),
    Stack.Params.empty
  )
{
  protected val newListener: Stack.Params => Listener[ChannelBuffer, ChannelBuffer] = { params =>
    val Label(label) = params[Label]
    val Stats(sr) = params[Stats]
    val scoped = sr.scope(label).scope("thriftmux")

    // Create a Listener that maintains gauges of how many ThriftMux and non-Mux
    // downgraded connections are listening to clients.
    new Listener[ChannelBuffer, ChannelBuffer] {
      private[this] val underlying = Netty3Listener[ChannelBuffer, ChannelBuffer](
        new thriftmux.PipelineFactory(scoped),
        params
      )

      def listen(addr: SocketAddress)(
        serveTransport: Transport[ChannelBuffer, ChannelBuffer] => Unit
      ): ListeningServer = underlying.listen(addr)(serveTransport)
    }
  }

  protected val newDispatcher: Stack.Params => Dispatcher =
    Function.const(new mux.ServerDispatcher(_, _, true))
}

/**
 * A Thrift server served over [[com.twitter.finagle.mux]].
 *
 * $serverExample
 *
 * @define serverExampleObject ThriftMuxServer
 */
object ThriftMuxServer extends ThriftMuxServerLike(ThriftServerMuxer)
