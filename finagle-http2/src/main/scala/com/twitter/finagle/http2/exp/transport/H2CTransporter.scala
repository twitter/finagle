package com.twitter.finagle.http2.exp.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.RefTransport
import com.twitter.finagle.http2.exp.transport.H2Pool.OnH2Session
import com.twitter.finagle.http2.transport.{ClientSession, Http2UpgradingTransport}
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.http.{HttpCodecName, initClient, newHttpClientCodec}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.{Stack, Status, param}
import com.twitter.util._
import io.netty.channel.ChannelPipeline
import java.net.SocketAddress

/**
 * This `Transporter` makes `Transports` that speak the netty http/1.1, object
 * model while attempting to upgrade to HTTP/2 for requests without content.
 *
 * The `Transports` this hands out should be used serially, and will upgrade to
 * HTTP/2 if possible. If an upgrade succeeds the H2 session is passed to the
 * [[H2Pool]] layer via the provided [[H2Pool.OnH2Session]] obtained from the params.
 */
private class H2CTransporter(
  underlying: Transporter[Any, Any, TransportContext],
  modifier: Transport[Any, Any] => Transport[Any, Any],
  params: Stack.Params)
    extends Transporter[Any, Any, TransportContext] { self =>

  private[this] val stats = params[param.Stats].statsReceiver

  // The H2Pool will manage the lifecycles of H1 connections.
  private[this] val onH2Session: OnH2Session =
    params[H2Pool.OnH2SessionParam].onH2Session match {
      case Some(s) => s
      case None =>
        throw new IllegalStateException(
          s"params are missing the ${classOf[H2Pool.OnH2Session].getSimpleName}")
    }

  def remoteAddress: SocketAddress = underlying.remoteAddress

  /** Attempt to upgrade to a multiplex session */
  def apply(): Future[Transport[Any, Any]] = {
    val p = Promise[Option[ClientSession]]()

    underlying().transform {
      case Return(trans) =>
        val ref = new RefTransport(trans)
        ref.update { t =>
          new Http2UpgradingTransport(t, ref, p, params, H2CTransporter.http1StatusFunction)
        }

        p.onSuccess {
          case Some(session) =>
            onH2Session(new ClientServiceImpl(session, stats, modifier))

          case None => // nop: no h2 session.
        }

        Future.value(ref)

      case Throw(e) =>
        p.setException(e)
        Future.exception(e)
    }
  }
}

private[http2] object H2CTransporter {

  private val http1StatusFunction: () => Status = () => Status.Open

  def make(
    addr: SocketAddress,
    modifier: Transport[Any, Any] => Transport[Any, Any],
    params: Stack.Params
  ): Transporter[Any, Any, TransportContext] = {
    val underlying = Netty4Transporter.raw[Any, Any](
      pipelineInit = init(params),
      addr = addr,
      params = params + Netty4Transporter.Backpressure(false)
    )

    new H2CTransporter(underlying, modifier, params)
  }

  private[this] def init(params: Stack.Params)(pipeline: ChannelPipeline): Unit = {
    // h2c pipeline
    val sourceCodec = newHttpClientCodec(params)

    pipeline.addLast(HttpCodecName, sourceCodec)
    pipeline.addLast(
      UpgradeRequestHandler.HandlerName,
      new UpgradeRequestHandler(params, sourceCodec)
    )
    initClient(params)(pipeline)
  }
}
