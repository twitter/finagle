package com.twitter.finagle.http2

import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.netty.Bijections._
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.http.Bijections
import com.twitter.finagle.transport.{TransportProxy, Transport}
import com.twitter.util.Future
import io.netty.channel.{ChannelPipeline, ChannelInboundHandlerAdapter, ChannelHandlerContext}
import io.netty.handler.codec.http.{HttpResponse => Netty4Response, _}
import io.netty.handler.codec.http2._
import io.netty.handler.logging.LogLevel
import java.net.SocketAddress
import org.jboss.netty.handler.codec.http.{
  HttpRequest => Netty3Request, HttpResponse => Netty3Response}

private[http2] object Http2Transporter {

  // constructing an http2 cleartext transport
  private[http2] val initialize: ChannelPipeline => Unit = { pipeline: ChannelPipeline =>
    val connection = new DefaultHttp2Connection(false /*server*/)
    val adapter = new DelegatingDecompressorFrameListener(
      connection,
      new InboundHttp2ToHttpAdapterBuilder(connection)
        .maxContentLength(Int.MaxValue)
        .propagateSettings(true)
        .build()
    )
    val connectionHandler = new HttpToHttp2ConnectionHandlerBuilder()
      .frameListener(adapter)
      .frameLogger(new Http2FrameLogger(LogLevel.ERROR))
      .connection(connection)
      .build()

    val sourceCodec = new HttpClientCodec()
    val upgradeCodec = new Http2ClientUpgradeCodec(connectionHandler)
    val upgradeHandler = new HttpClientUpgradeHandler(sourceCodec, upgradeCodec, Int.MaxValue)

    pipeline.addLast(sourceCodec,
      upgradeHandler,
      new UpgradeRequestHandler())
  }

  def apply(params: Stack.Params): Transporter[Any, Any] = new Transporter[Any, Any] {
    // current http2 client implementation doesn't support
    // netty-style backpressure
    // https://github.com/netty/netty/issues/3667#issue-69640214
    val underlying = Netty4Transporter[Any, Any](initialize, params + Netty4Transporter.Backpressure(false))

    def apply(addr: SocketAddress): Future[Transport[Any, Any]] = {
      // TODO: Transports should take finagle Request / Response
      // TODO: this bijection impl breaks streaming
      underlying(addr).map { transport =>
        new TransportProxy[Any, Any](transport) {
          def write(any: Any): Future[Unit] = any match {
            case request: Netty3Request =>
              val finReq = from[Netty3Request, Request](request)
              val netty4Req = Bijections.finagle.requestToNetty(finReq)
              transport.write(netty4Req)
            case _ => Future.exception(
              new IllegalArgumentException(s"expected a Netty3Request, got a ${any.getClass.getName}"))
          }

          def read(): Future[Any] = {
            transport.read().flatMap {
              case req: Netty4Response =>
                val finReq = Bijections.netty.responseToFinagle(req)
                Future.value(from[Response, Netty3Response](finReq))
              case settings: Http2Settings =>
                // drop for now
                // TODO: we should handle settings properly
                read()
              case req => Future.exception(new IllegalArgumentException(s"expected a Netty4Response, got a ${req.getClass.getName}"))
            }
          }
        }
      }
    }
  }

  // borrows heavily from the netty http2 example
  class UpgradeRequestHandler extends ChannelInboundHandlerAdapter {

    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
      // TODO: should change behavior based on response?
      // TODO: should write buffered writes

      // receives an upgrade response
      // removes self from pipeline
      // drops message
      // Done with this handler, remove it from the pipeline.
      ctx.pipeline().remove(this)
    }

    // TODO: should buffer writes on receiving settings from remote
    override def channelActive(ctx: ChannelHandlerContext): Unit = {

      // we send a regular http 1.1 request as an attempt to upgrade in the http2 cleartext protocol
      // it will be modified by HttpClientUpgradeHandler and Http2ClientUpgradeCodec to ensure we
      // upgrade to http2 properly.
      val upgradeRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      ctx.writeAndFlush(upgradeRequest)

      ctx.fireChannelActive()
    }
  }
}
