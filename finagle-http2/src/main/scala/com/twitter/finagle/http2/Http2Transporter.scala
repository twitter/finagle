package com.twitter.finagle.http2

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.param._
import com.twitter.finagle.http2.transport._
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.http.{
  HttpCodecName,
  Netty4HttpTransporter,
  initClient,
  newHttpClientCodec
}
import com.twitter.finagle.param.{Timer => TimerParam}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.Stack
import com.twitter.finagle.http2.transport.StreamMessage
import com.twitter.finagle.netty4.transport.HasExecutor
import com.twitter.logging.Logger
import com.twitter.util._
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http2._
import java.net.SocketAddress

private[finagle] object Http2Transporter {

  private val log = Logger.get()

  def apply(params: Stack.Params, addr: SocketAddress): Transporter[Any, Any, TransportContext] = {
    // This http2 client implementation doesn't support netty-style back pressure so
    // we disable it. See the MultiplexCodec based client for backpressure support.
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
      val sensitivityDetector = params[HeaderSensitivity].sensitivityDetector

      val connection = new DefaultHttp2Connection(false /*server*/ )

      val connectionHandlerBuilder = new RichHttpToHttp2ConnectionHandlerBuilder()
        .frameListener(new Http2ClientDowngrader(connection))
        .connection(connection)
        .initialSettings(Settings.fromParams(params, isServer = false))
        .encoderIgnoreMaxHeaderListSize(ignoreMaxHeaderListSize)
        .headerSensitivityDetector(new Http2HeadersEncoder.SensitivityDetector {
          def isSensitive(name: CharSequence, value: CharSequence): Boolean = {
            sensitivityDetector(name, value)
          }
        })

      if (params[FrameLogging].enabled) {
        connectionHandlerBuilder.frameLogger(
          new LoggerPerFrameTypeLogger(params[FrameLoggerNamePrefix].loggerNamePrefix))
      }

      val PriorKnowledge(priorKnowledge) = params[PriorKnowledge]
      val Transport.ClientSsl(config) = params[Transport.ClientSsl]
      val tlsEnabled = config.isDefined

      if (tlsEnabled) {
        val buffer = BufferingHandler.alpn()
        val connectionHandler = connectionHandlerBuilder
          .onActive { () =>
            // need to stop buffering after we've sent the connection preface
            pipeline.remove(buffer)
          }
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
        val sourceCodec = newHttpClientCodec(params)
        pipeline.addLast(HttpCodecName, sourceCodec)
        pipeline.addLast(
          UpgradeRequestHandler.HandlerName,
          new UpgradeRequestHandler(params, sourceCodec, connectionHandlerBuilder)
        )
        initClient(params)(pipeline)
      }
  }
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
  underlying: Transporter[Any, Any, TransportContext],
  underlyingHttp11: Transporter[Any, Any, TransportContext],
  alpnUpgrade: Boolean,
  params: Stack.Params,
  implicit val timer: Timer)
    extends Http2NegotiatingTransporter(
      params,
      underlyingHttp11,
      fallbackToHttp11WhileNegotiating = !alpnUpgrade
    ) { self =>

  import Http2Transporter.log

  protected def attemptUpgrade(): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
    val clientSessionF = Promise[Option[ClientSession]]()

    val firstTransport: Future[Transport[Any, Any]] = underlying().transform {
      case Return(trans) =>
        if (alpnUpgrade) {
          trans.read().transform {
            case Return(UpgradeEvent.UPGRADE_REJECTED) =>
              clientSessionF.setValue(None)
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

              val streamTransportFactory =
                new StreamTransportFactory(contextCasted, trans.context.remoteAddress, params)

              clientSessionF.setValue(Some(streamTransportFactory))
              streamTransportFactory.newChildTransport()

            case Return(msg) =>
              log.error(s"Non-upgrade event detected $msg")
              trans.close()
              clientSessionF.setValue(None)
              underlyingHttp11()

            case Throw(exn) =>
              log.error(
                exn,
                "Failed to clearly negotiate either HTTP/2 or HTTP/1.1.  " +
                  "Falling back to HTTP/1.1."
              )
              trans.close()
              clientSessionF.setValue(None)
              underlyingHttp11()
          }
        } else {
          val ref = new RefTransport(trans)
          ref.update { t =>
            new Http2UpgradingTransport(t, ref, clientSessionF, params, () => this.http1Status)
          }
          Future.value(ref)
        }

      case Throw(e) =>
        clientSessionF.setException(e)
        Future.exception(e)
    }

    clientSessionF -> firstTransport
  }
}
