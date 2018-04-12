package com.twitter.finagle.http2

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http
import com.twitter.finagle.http2.param._
import com.twitter.finagle.http2.transport._
import com.twitter.finagle.http2.transport.Http2ClientDowngrader.StreamMessage
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.http.{HttpCodecName, Netty4HttpTransporter, initClient}
import com.twitter.finagle.netty4.transport.HasExecutor
import com.twitter.finagle.param.{Timer => TimerParam}
import com.twitter.finagle.transport.{LegacyContext, Transport, TransportContext, TransportProxy}
import com.twitter.finagle.{Stack, Status}
import com.twitter.logging.{HasLogLevel, Level, Logger}
import com.twitter.util._
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http2._
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicReference

private[finagle] object Http2Transporter {

  // Sentinel that we check early to avoid polling a promise forever
  private val UpgradeRejected: Future[Option[StreamTransportFactory]] = Future.None

  def apply(params: Stack.Params)(addr: SocketAddress): Transporter[Any, Any, TransportContext] = {
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

  // constructing an http2 cleartext transport
  private[http2] def init(params: Stack.Params): ChannelPipeline => Unit = {
    pipeline: ChannelPipeline =>
      val EncoderIgnoreMaxHeaderListSize(ignoreMaxHeaderListSize) =
        params[EncoderIgnoreMaxHeaderListSize]

      val connectionHandlerBuilder = new RichHttpToHttp2ConnectionHandlerBuilder()
        .frameListener(Http2ClientDowngrader)
        .frameLogger(new LoggerPerFrameTypeLogger(params[FrameLoggerNamePrefix].loggerNamePrefix))
        .connection(new DefaultHttp2Connection(false /*server*/ ))
        .initialSettings(Settings.fromParams(params, isServer = false))
        .encoderIgnoreMaxHeaderListSize(ignoreMaxHeaderListSize)

      val PriorKnowledge(priorKnowledge) = params[PriorKnowledge]
      val Transport.ClientSsl(config) = params[Transport.ClientSsl]
      val tlsEnabled = config.isDefined
      val sensitivityDetector = params[HeaderSensitivity].sensitivityDetector

      val maxChunkSize = params[http.param.MaxChunkSize].size
      val maxHeaderSize = params[http.param.MaxHeaderSize].size
      val maxInitialLineSize = params[http.param.MaxInitialLineSize].size
      val sourceCodec = new HttpClientCodec(
        maxInitialLineSize.inBytes.toInt,
        maxHeaderSize.inBytes.toInt,
        maxChunkSize.inBytes.toInt
      )

      if (tlsEnabled) {
        val p = Promise[Unit]()
        val buffer = BufferingHandler.alpn()
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
        pipeline.addLast(BufferingHandler.HandlerName, buffer)
      } else if (priorKnowledge) {
        val connectionHandler = connectionHandlerBuilder.build()
        pipeline.addLast(HttpCodecName, connectionHandler)
        pipeline.addLast(BufferingHandler.HandlerName, BufferingHandler.priorKnowledge())
        pipeline.addLast(
          AdapterProxyChannelHandler.HandlerName,
          new AdapterProxyChannelHandler({ pipeline =>
            pipeline.addLast(SchemifyingHandler.HandlerName, new SchemifyingHandler("http"))
            pipeline.addLast(StripHeadersHandler.HandlerName, StripHeadersHandler)
            initClient(params)(pipeline)
          })
        )
      } else {
        pipeline.addLast(HttpCodecName, sourceCodec)
        pipeline.addLast(UpgradeRequestHandler.HandlerName, new UpgradeRequestHandler(params, sourceCodec, connectionHandlerBuilder))
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
  underlying: Transporter[Any, Any, TransportContext],
  underlyingHttp11: Transporter[Any, Any, TransportContext],
  alpnUpgrade: Boolean,
  params: Stack.Params,
  implicit val timer: Timer
) extends Transporter[Any, Any, TransportContext]
    with Closable { self =>

  private[this] def deadTransport(exn: Throwable) = new Transport[Any, Any] {
    type Context = TransportContext
    def read(): Future[Any] = Future.never
    def write(msg: Any): Future[Unit] = Future.never
    val status: Status = Status.Closed
    def onClose: Future[Throwable] = Future.value(exn)
    def remoteAddress: SocketAddress = self.remoteAddress
    def localAddress: SocketAddress = new SocketAddress {}
    def peerCertificate: Option[Certificate] = None
    def close(deadline: Time): Future[Unit] = Future.Done
    val context: TransportContext = new LegacyContext(this)
  }

  def remoteAddress: SocketAddress = underlying.remoteAddress

  private[this] val log = Logger.get()

  import Http2Transporter._

  protected[this] val cachedConnection =
    new AtomicReference[Future[Option[StreamTransportFactory]]]()

  // We want HTTP/1.x connections to get culled once we have a live HTTP/2 session so
  // we transition their status to `Closed` once we have an H2 session that can be used.
  private[this] val http1Status: () => Status = () => {
    val f = cachedConnection.get
    if (f == null || (f eq UpgradeRejected)) Status.Open
    else f.poll match {
      case Some(Return(Some(fac))) =>
        fac.status match {
          case Status.Open => Status.Closed
          case status => status
        }
      case _ => Status.Open
    }
  }

  private[this] def tryEvict(f: Future[Option[StreamTransportFactory]]): Unit = {
    // kick us out of the cache so we can try to reestablish the connection
    cachedConnection.compareAndSet(f, null)
  }

  private[this] def useExistingConnection(
    f: Future[Option[StreamTransportFactory]]
  ): Future[Transport[Any, Any]] = f.transform {
    case Return(Some(fn)) =>
      fn().transform {
        case Return(transport) => Future.value(unsafeCast(transport))
        case Throw(exn) =>
          log.warning(
            exn,
            s"A previously successful connection to address $remoteAddress stopped being successful."
          )
          tryEvict(f)

          // we expect finagle to treat this specially and retry if possible
          Future.value(deadTransport(exn))
      }
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
      new TransportProxy[Any, Any](http11Trans) {
        def write(req: Any): Future[Unit] = http11Trans.write(req)
        def read(): Future[Any] = http11Trans.read()

        override def status: Status =
          Status.worst(http11Trans.status, http1Status())
      }
    }
  }

  private[this] def upgrade(): Future[Transport[Any, Any]] = {
    val conn: Future[Transport[Any, Any]] = underlying()
    val p = Promise[Option[StreamTransportFactory]]()
    if (cachedConnection.compareAndSet(null, p)) {
      p.respond {
        case Return(None) =>
          // we attempt to set the sentinel value to make polling status cheaper in the future.
          cachedConnection.compareAndSet(p, UpgradeRejected)

        case Return(_) => // nop: successful upgrade. Good news.

        case Throw(exn) =>
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
            if (p.isDefined) {
              // we should only evict if we haven't successfully downgraded
              // before.  if we successfully downgraded, it's unlikely that
              // attempting the upgrade again will be fruitful.
              p.respond {
                case Return(None) => // nop
                case _ => tryEvict(p)
              }
            } else {
              tryEvict(p)
            }
          }

          if (alpnUpgrade) {
            trans.read().transform {
              case Return(UpgradeEvent.UPGRADE_REJECTED) =>
                p.setValue(None)
                // we continue using the same transport, since by now it has
                // been transformed into an http/1.1 connection.
                Future.value(trans)
              case Return(UpgradeEvent.UPGRADE_SUCCESSFUL) =>
                val inOutCasted = Transport.cast[StreamMessage, StreamMessage](trans)
                val contextCasted =
                  inOutCasted.asInstanceOf[
                    Transport[StreamMessage, StreamMessage] {
                      type Context = TransportContext with HasExecutor
                    }
                  ]
                p.setValue(Some(
                  new StreamTransportFactory(contextCasted, trans.remoteAddress, params)))
                useExistingConnection(p)
              case Return(msg) =>
                log.error(s"Non-upgrade event detected $msg")
                trans.close()
                p.setValue(None)
                useExistingConnection(p)
              case Throw(exn) =>
                log.error(exn, "Failed to clearly negotiate either HTTP/2 or HTTP/1.1.  " +
                  "Falling back to HTTP/1.1.")
                trans.close()
                p.setValue(None)
                useExistingConnection(p)
            }
          } else {
            val ref = new RefTransport(trans)
            ref.update { t =>
              new Http2UpgradingTransport(t, ref, p, params, http1Status)
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
    if (f == null) Status.Open
    else {
      f.poll match {
        case Some(Return(Some(fac))) => fac.status
        case _ => Status.Open
      }
    }
  }
}
