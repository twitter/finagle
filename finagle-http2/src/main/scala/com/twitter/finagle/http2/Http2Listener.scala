package com.twitter.finagle.http2

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.http2.transport.server.H2ServerFilter
import com.twitter.finagle.Announcement
import com.twitter.finagle.ListeningServer
import com.twitter.finagle.Stack
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.netty4.http.HttpCodecName
import com.twitter.finagle.netty4.http.initServer
import com.twitter.finagle.netty4.http.newHttpServerCodec
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.server.Listener
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Awaitable.CanAwait
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import io.netty.channel.Channel
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelPipeline
import io.netty.channel.group.DefaultChannelGroup
import io.netty.util.concurrent.GlobalEventExecutor
import java.net.SocketAddress
import scala.jdk.CollectionConverters._

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[finagle] object Http2Listener {

  def apply[In, Out](
    params: Stack.Params
  )(
    implicit mIn: Manifest[In],
    mOut: Manifest[Out]
  ): Listener[In, Out, TransportContext] = {
    val configuration = params[Transport.ServerSsl].sslServerConfiguration

    val initializer =
      if (configuration.isDefined)
        new TlsSnoopingInitializer(_: ChannelInitializer[Channel], params)
      else new Http2CleartextServerInitializer(_: ChannelInitializer[Channel], params)

    new Http2Listener(params, initializer, mIn, mOut)
  }
}

private[http2] class Http2Listener[In, Out](
  params: Stack.Params,
  setupMarshalling: ChannelInitializer[Channel] => ChannelHandler,
  implicit val mIn: Manifest[In],
  implicit val mOut: Manifest[Out])
    extends Listener[In, Out, TransportContext] {

  private[this] val initServerParams = initServer(params)
  private[this] val channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)
  private[this] def pipelineInit(pipeline: ChannelPipeline): Unit = {
    channels.add(pipeline.channel)
    pipeline.addLast(HttpCodecName, newHttpServerCodec(params))
    initServerParams(pipeline)
  }

  private[this] val underlyingListener = Netty4Listener[In, Out, TransportContext](
    pipelineInit = pipelineInit,
    params = params,
    setupMarshalling = setupMarshalling,
    transportFactory = { ch: Channel =>
      new ChannelTransport(ch, new AsyncQueue[Any], omitStackTraceOnInactive = true)
    }
  )

  // we need to find the underlying handler and tell it how long to wait to drain
  // before we actually send the `close` signal.
  private[this] def closeH2Sessions(deadline: Time): Unit = {
    channels.asScala.foreach { channel =>
      // By running in the event loop we can avoid some pipeline manipulation races
      // and we also get better thread safety guarantees.
      channel.eventLoop.execute(new Runnable {
        def run(): Unit = {
          val pipeline = channel.pipeline
          val handler = pipeline.get(classOf[H2ServerFilter])
          if (handler != null) {
            // This is a HTTP/2 connection. Add the deadline to the `H2ServerFilter` and
            // we'll let it take care of the rest. Note that this races with upgrades
            // but we can't win them all.
            handler.gracefulShutdown(deadline)
          } else {
            // We set the close time as a channel attribute so we can avoid races where
            // this channel isn't yet a H2 channel, but becomes so before the standard
            // close mechanism is used.
            channel.attr(H2ServerFilter.CloseRequestAttribute).set(deadline)
          }
        }
      })
    }
  }

  def listen(
    addr: SocketAddress
  )(
    serveTransport: Transport[In, Out] {
      type Context <: TransportContext
    } => Unit
  ): ListeningServer = {
    val underlying = underlyingListener.listen(addr)(serveTransport)
    new Http2ListeningServer(underlying, closeH2Sessions)
  }
}

private[http2] class Http2ListeningServer(
  underlying: ListeningServer,
  propagateDeadline: Time => Unit)
    extends ListeningServer {

  // we override announcement so that we delegate the announcement to the underlying listening
  // server and don't double announce.
  override def announce(addr: String): Future[Announcement] = underlying.announce(addr)

  def closeServer(deadline: Time): Future[Unit] = {
    propagateDeadline(deadline)
    underlying.close(deadline)
  }

  override def isReady(implicit permit: CanAwait): Boolean = underlying.isReady(permit)

  def ready(timeout: Duration)(implicit permit: CanAwait): this.type = {
    underlying.ready(timeout)(permit)
    this
  }

  def result(timeout: Duration)(implicit permit: CanAwait): Unit =
    underlying.result(timeout)(permit)

  def boundAddress: SocketAddress = underlying.boundAddress
}
