package com.twitter.finagle.http2

import com.twitter.finagle.http
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.param._
import com.twitter.finagle.http2.transport._
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.DirectToHeapInboundHandlerName
import com.twitter.finagle.netty4.channel.{DirectToHeapInboundHandler, BufferingChannelOutboundHandler}
import com.twitter.finagle.netty4.http.exp.{initClient, Netty4HttpTransporter}
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.finagle.{Stack, Status}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Throw, Return, Promise, Try}
import io.netty.channel.{
  ChannelHandlerContext,
  ChannelInboundHandlerAdapter,
  ChannelPipeline,
  ChannelPromise
}
import io.netty.handler.codec.http._
import io.netty.handler.codec.http2._
import java.net.SocketAddress
import java.util.concurrent.ConcurrentHashMap

private[http2] object Http2Transporter {

  def apply(params: Stack.Params): Transporter[Any, Any] = {
    // current http2 client implementation doesn't support
    // netty-style backpressure
    // https://github.com/netty/netty/issues/3667#issue-69640214
    val underlying = Netty4Transporter[Any, Any](
      init(params),
      params + Netty4Transporter.Backpressure(false)
    )

    val PriorKnowledge(priorKnowledge) = params[PriorKnowledge]

    if (priorKnowledge) {
      new PriorKnowledgeTransporter(underlying, params)
    } else {
      val underlyingHttp11 = Netty4HttpTransporter(params)

      new Http2Transporter(underlying, underlyingHttp11)
    }
  }

  /**
   * Buffers until `channelActive` so we can ensure the connection preface is
   * the first message we send.
   */
  class BufferingHandler
    extends ChannelInboundHandlerAdapter
    with BufferingChannelOutboundHandler { self =>

    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      ctx.fireChannelActive()
      // removing a BufferingChannelOutboundHandler writes and flushes too.
      ctx.pipeline.remove(self)
    }

    def bind(ctx: ChannelHandlerContext, addr: SocketAddress, promise: ChannelPromise): Unit = {
      ctx.bind(addr, promise)
    }
    def close(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
      ctx.close(promise)
    }
    def connect(
      ctx: ChannelHandlerContext,
      local: SocketAddress,
      remote: SocketAddress,
      promise: ChannelPromise
    ): Unit = {
      ctx.connect(local, remote, promise)
    }
    def deregister(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
      ctx.deregister(promise)
    }
    def disconnect(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
      ctx.disconnect(promise)
    }
    def read(ctx: ChannelHandlerContext): Unit = {
      ctx.read()
    }
  }

  // constructing an http2 cleartext transport
  private[http2] def init(params: Stack.Params): ChannelPipeline => Unit =
    { pipeline: ChannelPipeline =>
      pipeline.addLast(DirectToHeapInboundHandlerName, DirectToHeapInboundHandler)
      val connection = new DefaultHttp2Connection(false /*server*/)

      // decompresses data frames according to the content-encoding header
      val adapter = new DelegatingDecompressorFrameListener(
        connection,
        // adapts http2 to http 1.1
        new Http2ClientDowngrader(connection)
      )

      val EncoderIgnoreMaxHeaderListSize(ignoreMaxHeaderListSize) =
        params[EncoderIgnoreMaxHeaderListSize]

      val connectionHandler = new RichHttpToHttp2ConnectionHandlerBuilder()
        .frameListener(adapter)
        .connection(connection)
        .initialSettings(Settings.fromParams(params))
        .encoderIgnoreMaxHeaderListSize(ignoreMaxHeaderListSize)
        .build()

      val PriorKnowledge(priorKnowledge) = params[PriorKnowledge]
      if (priorKnowledge) {
        pipeline.addLast("httpCodec", connectionHandler)
        pipeline.addLast("buffer", new BufferingHandler())
        pipeline.addLast("aggregate", new AdapterProxyChannelHandler({ pipeline =>
          pipeline.addLast("schemifier", new SchemifyingHandler("http"))
          initClient(params)(pipeline)
        }))
      } else {
        val maxChunkSize = params[http.param.MaxChunkSize].size
        val maxHeaderSize = params[http.param.MaxHeaderSize].size
        val maxInitialLineSize = params[http.param.MaxInitialLineSize].size

        val sourceCodec = new HttpClientCodec(
          maxInitialLineSize.inBytes.toInt,
          maxHeaderSize.inBytes.toInt,
          maxChunkSize.inBytes.toInt
        )
        val upgradeCodec = new Http2ClientUpgradeCodec(connectionHandler)
        val upgradeHandler = new HttpClientUpgradeHandler(sourceCodec, upgradeCodec, Int.MaxValue)
        pipeline.addLast("httpCodec", sourceCodec)
        pipeline.addLast("httpUpgradeHandler", upgradeHandler)
        pipeline.addLast(UpgradeRequestHandler.HandlerName, new UpgradeRequestHandler(params))
        initClient(params)(pipeline)
      }
    }

  def unsafeCast(t: Transport[HttpObject, HttpObject]): Transport[Any, Any] =
    t.map(_.asInstanceOf[HttpObject], _.asInstanceOf[Any])
}

/**
 * This `Transporter` makes `Transports` that speak netty http/1.1, but write
 * http/2 to the wire if they can.
 *
 * The `Transports` this hands out should be used serially, and will upgrade to
 * http/2 if possible.  It caches connections associated with a given socket
 * address, so that you can increase concurrency over a single connection by
 * getting another transport, the same way that serial protocols increase
 * concurrency.  Plainly, these transports have the same semantics as serial
 * netty4 http/1.1 transports, but allows them to be multiplexed under the hood.
 *
 * Since the decision on whether to multiplex a connection or not is made after
 * knowing the result of an upgrade, Http2Transporter is also in charge of
 * upgrading, and caches upgrade results.
 *
 * Since the cleartext upgrade is a request, it's possible to write another
 * request while the upgrade is in progress, which it does over http/1.1, and
 * doesn't attempt to upgrade.
 */
private[http2] class Http2Transporter(
    underlying: Transporter[Any, Any],
    underlyingHttp11: Transporter[Any, Any])
  extends Transporter[Any, Any] {

  private[this] val log = Logger.get()

  import Http2Transporter._

  protected[this] val transporterCache =
    new ConcurrentHashMap[SocketAddress, Future[Option[() => Try[Transport[HttpObject, HttpObject]]]]]()

  private[this] def onUpgradeFinished(
    f: Future[Option[() => Try[Transport[HttpObject, HttpObject]]]],
    addr: SocketAddress
  ): Future[Transport[Any, Any]] = f.transform {
    // we rescue here to ensure our future is failed *after* we evict from the cache.
    case Return(Some(fn)) => Future.const(fn().map(unsafeCast)).rescue { case exn: Throwable =>
      log.warning(
        exn,
        s"A previously successful connection to address $addr stopped being successful."
      )

      // kick us out of the cache so we can try to reestablish the connection
      transporterCache.remove(addr, f)
      Future.exception(exn)
    }
    case Return(None) =>
      // we didn't upgrade
      underlyingHttp11(addr)
    case Throw(exn) =>
      log.warning(exn, s"A cached connection to address $addr was failed.")

      // kick us out of the cache so we can try to reestablish the connection
      transporterCache.remove(addr, f)
      Future.exception(exn)
  }

  // uses http11 underneath, but marks itself as dead if the upgrade succeeds or
  // the connection fails
  private[this] def onUpgradeInProgress(
    f: Future[Option[_]]
  ): Transport[Any, Any] => Transport[Any, Any] = { http11: Transport[Any, Any] =>
    val ref = new RefTransport(http11)
    f.respond {
      case Return(None) => // we failed the upgrade, so we can keep the connection
      case _ =>
        // we shut down if we pass the upgrade or fail entirely
        // we're assuming here that a well behaved dispatcher will propagate our status to a pool
        // that will close us
        // TODO: if we fail entirely, we should try to reregister on the new connection attempt
        ref.update { trans =>
          new TransportProxy[Any, Any](trans) {
            def write(any: Any): Future[Unit] = trans.write(any)
            def read(): Future[Any] = trans.read()
            override def status: Status = Status.Closed
          }
        }
    }
    ref
  }

  private[this] def upgrade(addr: SocketAddress): Future[Transport[Any, Any]] = {
    val conn: Future[Transport[Any, Any]] = underlying(addr)
    val p = Promise[Option[() => Try[Transport[HttpObject, HttpObject]]]]()
    if (transporterCache.putIfAbsent(addr, p) == null) {
      conn.transform {
        case Return(trans) =>
          val ref = new RefTransport(trans)
          ref.update { t =>
            t.onClose.ensure {
              transporterCache.remove(addr, p)
            }
            new Http2UpgradingTransport(t, ref, p)
          }
          Future.value(ref)
        case Throw(e) =>
          transporterCache.remove(addr, p)
          p.setException(e)
          Future.exception(e)
      }
    } else apply(addr)
  }

  final def apply(addr: SocketAddress): Future[Transport[Any, Any]] =
    Option(transporterCache.get(addr)) match {
      case Some(f) =>
        if (f.isDefined) {
          onUpgradeFinished(f, addr)
        } else {
          // fall back to http/1.1 while upgrading
          val conn = underlyingHttp11(addr)
          conn.map(onUpgradeInProgress(f))
        }
      case None =>
        upgrade(addr)
    }
}
