package com.twitter.finagle.http2

import com.twitter.finagle.Http.{param => httpparam}
import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.channel.BufferingChannelOutboundHandler
import com.twitter.finagle.netty4.http.exp.initClient
import com.twitter.finagle.transport.{TransportProxy, Transport}
import com.twitter.util.{Future, Promise, Closable, Time}
import io.netty.channel.{ChannelPipeline, ChannelDuplexHandler, ChannelHandlerContext,
  ChannelPromise}
import io.netty.handler.codec.http.{HttpResponse => Netty4Response, _}
import io.netty.handler.codec.http2.HttpConversionUtil.ExtensionHeaderNames.STREAM_ID
import io.netty.handler.codec.http2._
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private[http2] object Http2Transporter {

  /** Utility for retrieving the HTTP/2 stream id from a HTTP/1 message. */
  def getStreamId(msg: HttpMessage): Option[Int] = {
    val num = msg.headers.getInt(STREAM_ID.text())

    // java / scala primitive compatibility stuff
    if (num == null) None
    else Some(num)
  }

  /** Utility for setting the HTTP/2 stream id on a HTTP/1 message. */
  def setStreamId(msg: HttpMessage, id: Int): Unit =
    msg.headers.setInt(STREAM_ID.text(), id)

  def apply(params: Stack.Params): Transporter[Any, Any] =
    // current http2 client implementation doesn't support
    // netty-style backpressure
    // https://github.com/netty/netty/issues/3667#issue-69640214
    new Http2Transporter(Netty4Transporter[Any, Any](
      init(params),
      params + Netty4Transporter.Backpressure(false)
    ))

  // constructing an http2 cleartext transport
  private[http2] def init(params: Stack.Params): ChannelPipeline => Unit =
    { pipeline: ChannelPipeline =>
      val connection = new DefaultHttp2Connection(false /*server*/)

      val maxResponseSize = params[httpparam.MaxResponseSize].size

      // decompresses data frames according to the content-encoding header
      val adapter = new DelegatingDecompressorFrameListener(
        connection,
        // adapters http2 to http 1.1
        new InboundHttp2ToHttpAdapterBuilder(connection)
          .maxContentLength(maxResponseSize.inBytes.toInt)
          .propagateSettings(true)
          .build()
      )
      val connectionHandler = new HttpToHttp2ConnectionHandlerBuilder()
        .frameListener(adapter)
        .connection(connection)
        .build()

      val maxChunkSize = params[httpparam.MaxChunkSize].size
      val maxHeaderSize = params[httpparam.MaxHeaderSize].size
      val maxInitialLineSize = params[httpparam.MaxInitialLineSize].size

      val sourceCodec = new HttpClientCodec(
        maxInitialLineSize.inBytes.toInt,
        maxHeaderSize.inBytes.toInt,
        maxChunkSize.inBytes.toInt
      )
      val upgradeCodec = new Http2ClientUpgradeCodec(connectionHandler)
      val upgradeHandler = new HttpClientUpgradeHandler(sourceCodec, upgradeCodec, Int.MaxValue)

      pipeline.addLast(sourceCodec,
        upgradeHandler,
        new UpgradeRequestHandler())

      initClient(params)(pipeline)
    }

  // borrows heavily from the netty http2 example
  class UpgradeRequestHandler extends ChannelDuplexHandler with BufferingChannelOutboundHandler {
    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
      // receives an upgrade response
      // removes self from pipeline
      // drops message
      // Done with this handler, remove it from the pipeline.
      msg match {
        case settings: Http2Settings => // drop!
          ctx.pipeline.remove(this)
        case _ =>
          ctx.fireChannelRead(msg)
      }
    }

    private[this] val first = new AtomicBoolean(true)

    /**
     * We write the first message directly and it serves as the upgrade request.
     * The rest of the handlers will mutate it to look like a good upgrade
     * request.  We buffer the rest of the writes until we upgrade successfully.
     */
    override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit =
      if (first.compareAndSet(true, false)) ctx.writeAndFlush(msg, promise)
      else super.write(ctx, msg, promise) // this buffers the write until the handler is removed
  }

  private val unsafeCast: Transport[HttpObject, HttpObject] => Transport[Any, Any] =
    (t: Transport[HttpObject, HttpObject]) => t.map(_.asInstanceOf[HttpObject], _.asInstanceOf[Any])
}

private[http2] class Http2Transporter(underlying: Transporter[Any, Any])
  extends Transporter[Any, Any] {

  import Http2Transporter.unsafeCast

  protected[this] val transporterCache =
    new ConcurrentHashMap[SocketAddress, Future[MultiplexedTransporter]]

  private[this] def newConnection(addr: SocketAddress): Future[Transport[Any, Any]] =
    underlying(addr).map { transport =>
      new TransportProxy[Any, Any](transport) {
        def write(msg: Any): Future[Unit] =
          transport.write(msg)

        def read(): Future[Any] =
          transport.read().flatMap {
            case req: Netty4Response =>
              Future.value(req)
            case settings: Http2Settings =>
              // drop for now
              // TODO: we should handle settings properly
              read()
            case req => Future.exception(new IllegalArgumentException(
              s"expected a Netty4Response, got a ${req.getClass.getName}"))
          }
      }
    }

  @scala.annotation.tailrec
  final def apply(addr: SocketAddress): Future[Transport[Any, Any]] =
    Option(transporterCache.get(addr)) match {
      case Some(f) =>
        println(s"aleady cached $addr")
        f.flatMap { multiplexed =>
          multiplexed(addr).map(unsafeCast)
        }

      case None =>
        val p = Promise[MultiplexedTransporter]()
        if (transporterCache.putIfAbsent(addr, p) == null) {
          println(s"cached $addr")
          val f = newConnection(addr).map { trans =>
            new MultiplexedTransporter(
              Transport.cast[HttpObject, HttpObject](trans),
              Closable.make { time: Time =>
                println(s"cache remove $addr")
                transporterCache.remove(addr, p)
                Future.Done
              }
            )
          }
          p.become(f)
          f.flatMap { multiplexed =>
            multiplexed(addr).map(unsafeCast)
          }
        } else {
          apply(addr) // lost the race, try again
        }
    }
}
