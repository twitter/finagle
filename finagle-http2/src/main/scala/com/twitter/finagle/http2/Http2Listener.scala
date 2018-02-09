package com.twitter.finagle.http2

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{http, Stack, ListeningServer, Announcement}
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.netty4.http.{HttpCodecName, initServer}
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.server.Listener
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.Awaitable.CanAwait
import com.twitter.util.{Future, Time, Duration}
import io.netty.channel.{ChannelInitializer, Channel, ChannelPipeline, ChannelHandler}
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.codec.http2.Http2ConnectionHandler
import io.netty.channel.group.DefaultChannelGroup
import io.netty.util.concurrent.GlobalEventExecutor
import java.net.SocketAddress
import scala.collection.JavaConverters._

/**
 * Please note that the listener cannot be used for TLS yet.
 */
private[finagle] object Http2Listener {

  def apply[In, Out](
    params: Stack.Params
  )(implicit mIn: Manifest[In], mOut: Manifest[Out]): Listener[In, Out, TransportContext] = {
    val configuration = params[Transport.ServerSsl].sslServerConfiguration

    val initializer =
      if (configuration.isDefined)
        new Http2TlsServerInitializer(
          _: ChannelInitializer[Channel],
          params
        )
      else new Http2CleartextServerInitializer(_: ChannelInitializer[Channel], params)

    new Http2Listener(params, initializer, mIn, mOut)
  }
}

private[http2] class Http2Listener[In, Out](
  params: Stack.Params,
  setupMarshalling: ChannelInitializer[Channel] => ChannelHandler,
  implicit val mIn: Manifest[In],
  implicit val mOut: Manifest[Out]
) extends Listener[In, Out, TransportContext] {

  private[this] val channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE)

  private[this] def sourceCodec(params: Stack.Params) = {
    val maxInitialLineSize = params[http.param.MaxInitialLineSize].size
    val maxHeaderSize = params[http.param.MaxHeaderSize].size
    val maxRequestSize = params[http.param.MaxRequestSize].size

    new HttpServerCodec(
      maxInitialLineSize.inBytes.toInt,
      maxHeaderSize.inBytes.toInt,
      maxRequestSize.inBytes.toInt
    )
  }

  private[this] val underlyingListener = Netty4Listener[In, Out, TransportContext](
    pipelineInit = { pipeline: ChannelPipeline =>
      channels.add(pipeline.channel)
      pipeline.addLast(HttpCodecName, sourceCodec(params))
      initServer(params)(pipeline)
    },
    params = params,
    setupMarshalling = setupMarshalling,
    transportFactory = { ch: Channel =>
      new ChannelTransport(ch, new AsyncQueue[Any], omitStackTraceOnInactive = true)
    }
  )

  // we need to find the underlying handler and tell it how long to wait to drain
  // before we actually send the `close` signal.
  private[this] def propagateDeadline(deadline: Time): Unit = {
    val duration = (deadline - Time.now).inMillis
    if (duration > 0) {
      channels.asScala.foreach { channel =>
        val pipeline = channel.pipeline
        val handler = Option(pipeline.get(classOf[Http2ConnectionHandler]))
        handler.foreach { handler =>
          handler.gracefulShutdownTimeoutMillis(duration)
        }
      }
    }
  }

  def listen(addr: SocketAddress)(serveTransport: Transport[In, Out] {
    type Context <: TransportContext
  } => Unit): ListeningServer = {
    val underlying = underlyingListener.listen(addr)(serveTransport)
    new Http2ListeningServer(underlying, propagateDeadline)
  }
}

private[http2] class Http2ListeningServer(
  underlying: ListeningServer,
  propagateDeadline: Time => Unit
) extends ListeningServer {

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
