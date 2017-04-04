package com.twitter.finagle.http2

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http
import com.twitter.finagle.http2.param._
import com.twitter.finagle.http2.transport._
import com.twitter.finagle.http2.transport.Http2ClientDowngrader.StreamMessage
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.channel.BufferingChannelOutboundHandler
import com.twitter.finagle.netty4.http.exp.{initClient, Netty4HttpTransporter, HttpCodecName}
import com.twitter.finagle.param.{Timer => TimerParam}
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.finagle.{Stack, Status}
import com.twitter.logging.{Logger, HasLogLevel, Level}
import com.twitter.util._
import io.netty.channel.{
  ChannelHandlerContext,
  ChannelInboundHandlerAdapter,
  ChannelOutboundHandlerAdapter,
  ChannelPipeline,
  ChannelPromise
}
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http2._
import io.netty.handler.logging.LogLevel
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicReference

private[finagle] object Http2Transporter {

  def apply(params: Stack.Params)(addr: SocketAddress): Transporter[Any, Any] = {
    // current http2 client implementation doesn't support
    // netty-style backpressure
    // https://github.com/netty/netty/issues/3667#issue-69640214
    val underlying = Netty4Transporter.raw[Any, Any](
      pipelineInit = init(params),
      addr = addr,
      params = params + Netty4Transporter.Backpressure(false)
    )

    val PriorKnowledge(priorKnowledge) = params[PriorKnowledge]
    val Transport.ClientSsl(config) = params[Transport.ClientSsl]
    val tlsEnabled = config.isDefined

    // prior knowledge is only used with h2c
    if (!tlsEnabled && priorKnowledge) {
      new PriorKnowledgeTransporter(underlying, params)
    } else {
      val underlyingHttp11 = Netty4HttpTransporter(params)(addr)
      val TimerParam(timer) = params[TimerParam]
      new Http2Transporter(underlying, underlyingHttp11, tlsEnabled, params, timer)
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
      val connection = new DefaultHttp2Connection(false /*server*/)

      // decompresses data frames according to the content-encoding header
      val adapter = new DelegatingDecompressorFrameListener(
        connection,
        // adapts http2 to http 1.1
        Http2ClientDowngrader
      )

      val EncoderIgnoreMaxHeaderListSize(ignoreMaxHeaderListSize) =
        params[EncoderIgnoreMaxHeaderListSize]

      val connectionHandlerBuilder = new RichHttpToHttp2ConnectionHandlerBuilder()
        .frameListener(adapter)
        .frameLogger(new Http2FrameLogger(LogLevel.TRACE))
        .connection(connection)
        .initialSettings(Settings.fromParams(params))
        .encoderIgnoreMaxHeaderListSize(ignoreMaxHeaderListSize)

      val PriorKnowledge(priorKnowledge) = params[PriorKnowledge]
      val Transport.ClientSsl(config) = params[Transport.ClientSsl]
      val HeaderSensitivity(sensitivityDetector) = params[HeaderSensitivity]
      val tlsEnabled = config.isDefined

      if (tlsEnabled) {
        val p = Promise[Unit]()
        val buffer = new ChannelOutboundHandlerAdapter with BufferingChannelOutboundHandler
        val connectionHandler = connectionHandlerBuilder
          .onActive { () =>
            // need to stop buffering after we've sent the connection preface
            pipeline.remove(buffer)
          }
          .headerSensitivityDetector(new Http2HeadersEncoder.SensitivityDetector {
            def isSensitive(name: CharSequence, value: CharSequence): Boolean = {
              sensitivityDetector(name, value)
            }
          })
          .build()
        pipeline.addLast("alpn", new ClientNpnOrAlpnHandler(connectionHandler, params))
        pipeline.addLast("buffer", buffer)
      } else if (priorKnowledge) {
        val connectionHandler = connectionHandlerBuilder.build()
        pipeline.addLast(HttpCodecName, connectionHandler)
        pipeline.addLast("buffer", new BufferingHandler())
        pipeline.addLast("aggregate", new AdapterProxyChannelHandler({ pipeline =>
          pipeline.addLast("schemifier", new SchemifyingHandler("http"))
          initClient(params)(pipeline)
        }))
      } else {
        val connectionHandler = connectionHandlerBuilder.build()
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
        pipeline.addLast(HttpCodecName, sourceCodec)
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
private[finagle] class Http2Transporter(
    underlying: Transporter[Any, Any],
    underlyingHttp11: Transporter[Any, Any],
    alpnUpgrade: Boolean,
    params: Stack.Params,
    implicit val timer: Timer)
  extends Transporter[Any, Any] with Closable { self =>

  private[this] def deadTransport(exn: Throwable) = new Transport[Any, Any] {
    def read(): Future[Any] = Future.never
    def write(msg: Any): Future[Unit] = Future.never
    val status: Status = Status.Closed
    def onClose: Future[Throwable] = Future.value(exn)
    def remoteAddress: SocketAddress = self.remoteAddress
    def localAddress: SocketAddress = new SocketAddress {}
    def peerCertificate: Option[Certificate] = None
    def close(deadline: Time): Future[Unit] = Future.Done
  }

  def remoteAddress: SocketAddress = underlying.remoteAddress

  private[this] val log = Logger.get()

  import Http2Transporter._

  protected[this] val cachedConnection = new AtomicReference[Future[Option[MultiplexedTransporter]]]()

  private[this] def tryEvict(f: Future[Option[MultiplexedTransporter]]): Unit = {
    // kick us out of the cache so we can try to reestablish the connection
    cachedConnection.compareAndSet(f, null)
  }

  private[this] def useExistingConnection(
    f: Future[Option[MultiplexedTransporter]]
  ): Future[Transport[Any, Any]] = f.transform {
    case Return(Some(fn)) =>
      Future.value(fn() match {
        case Return(transport) => unsafeCast(transport)
        case Throw(exn) =>
          log.warning(
            exn,
            s"A previously successful connection to address $remoteAddress stopped being successful."
          )
          tryEvict(f)

          // we expect finagle to treat this specially and retry if possible
          deadTransport(exn)
      })
    case Return(None) =>
      // we didn't upgrade
      underlyingHttp11()
    case Throw(exn) =>
      log.warning(exn, s"A cached connection to address $remoteAddress was failed.")
      tryEvict(f)
      Future.exception(exn)
  }

  // uses http11 underneath, but marks itself as dead if the upgrade succeeds or
  // the connection fails
  private[this] def fallbackToHttp11(f: Future[Option[_]]): Future[Transport[Any, Any]] = {
    val conn = underlyingHttp11()
    conn.map { http11Trans =>
      val ref = new RefTransport(http11Trans)
      f.respond {
        case Return(None) => // the upgrade was rejected, so we can keep the connection
        case _ =>
          // we allow the pool to close us if we pass the upgrade or fail entirely
          // this will trigger a new connection attempt, which will use the upgraded
          // connection if we passed, or try to upgrade again if there was a non-rejection
          // failure in the upgrade
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
  }

  private[this] def upgrade(): Future[Transport[Any, Any]] = {
    val conn: Future[Transport[Any, Any]] = underlying()
    val p = Promise[Option[MultiplexedTransporter]]()
    if (cachedConnection.compareAndSet(null, p)) {
      p.onFailure { case NonFatal(exn) =>
        val level = exn match {
          case HasLogLevel(level) => level
          case _ => Level.WARNING
        }
        log.log(level, exn, s"An upgrade attempt to $remoteAddress failed.")
        tryEvict(p)
      }
      conn.transform {
        case Return(trans) =>
          trans.onClose.ensure {
            tryEvict(p)
          }

          if (alpnUpgrade) {
            trans.read().onSuccess {
              case UpgradeEvent.UPGRADE_REJECTED =>
                p.setValue(None)
              case UpgradeEvent.UPGRADE_SUCCESSFUL =>
                val casted = Transport.cast[StreamMessage, StreamMessage](trans)
                p.setValue(Some(new MultiplexedTransporter(casted, trans.remoteAddress, params)))
              case msg =>
                log.error(s"Non-upgrade event detected $msg")
            }

            useExistingConnection(p)
          } else {
            val ref = new RefTransport(trans)
            ref.update { t =>
              new Http2UpgradingTransport(t, ref, p, params)
            }
            Future.value(ref)
          }

        case Throw(e) =>
          tryEvict(p)
          p.setException(e)
          Future.exception(e)
      }
    } else apply()
  }

  final def apply(): Future[Transport[Any, Any]] =
    Option(cachedConnection.get) match {
      case Some(f) =>
        if (f.isDefined || alpnUpgrade) useExistingConnection(f)
        else fallbackToHttp11(f) // fall back to http/1.1 while upgrading
      case None =>
        upgrade()
    }

  def close(deadline: Time): Future[Unit] = {
    val maybeClose: Option[Closable] => Future[Unit] = {
      case Some(closable) => closable.close(deadline)
      case None => Future.Done
    }

    val f = cachedConnection.get

    if (f == null) Future.Done
    // TODO: shouldn't rescue on timeout
    else f.flatMap(maybeClose).by(deadline).rescue { case exn: Throwable => Future.Done }
  }

  def status: Status = {
    val f = cachedConnection.get

    // Status.Open is a good default since this is just here to do liveness checks with Ping.
    // We assume all other failure detection is handled up the chain.
    if (f == null) Status.Open else {
      f.poll match {
        case Some(Return(Some(multi))) => multi.status
        case _ => Status.Open
      }
    }
  }
}
