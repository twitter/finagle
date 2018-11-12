package com.twitter.finagle.http2.exp.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.RefTransport
import com.twitter.finagle.http2.transport.{
  ClientSession,
  Http2NegotiatingTransporter,
  Http2UpgradingTransport
}
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.http.{HttpCodecName, Netty4HttpTransporter, initClient}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.{Stack, Status, http}
import com.twitter.util._
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.http.HttpClientCodec
import java.net.SocketAddress

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
private class H2CTransporter(
  underlying: Transporter[Any, Any, TransportContext],
  underlyingHttp11: Transporter[Any, Any, TransportContext],
  params: Stack.Params
) extends Http2NegotiatingTransporter(
      params,
      underlyingHttp11,
      fallbackToHttp11WhileNegotiating = true
    ) { self =>

  // We want HTTP/1.x connections to get culled once we have a live HTTP/2 session so
  // we transition their status to `Closed` once we have an H2 session that can be used.
  private[this] val http1StatusFunction: () => Status = () => http1Status

  /** Attempt to upgrade to a multiplex session */
  protected def attemptUpgrade(): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
    val p = Promise[Option[ClientSession]]()
    val firstTransport = underlying().transform {
      case Return(trans) =>
        val ref = new RefTransport(trans)
        ref.update { t =>
          new Http2UpgradingTransport(t, ref, p, params, http1StatusFunction)
        }
        Future.value(ref)

      case Throw(e) =>
        p.setException(e)
        Future.exception(e)
    }

    (p, firstTransport)
  }
}

private[http2] object H2CTransporter {

  def make(addr: SocketAddress, params: Stack.Params): Transporter[Any, Any, TransportContext] = {

    // current http2 client implementation doesn't support
    // netty-style backpressure
    // https://github.com/netty/netty/issues/3667#issue-69640214
    val underlying = Netty4Transporter.raw[Any, Any](
      pipelineInit = init(params),
      addr = addr,
      params = params + Netty4Transporter.Backpressure(false)
    )

    val underlyingHttp11 = Netty4HttpTransporter(params)(addr)
    new H2CTransporter(underlying, underlyingHttp11, params)
  }

  private def init(params: Stack.Params)(pipeline: ChannelPipeline): Unit = {
    // h2c pipeline
    val maxHeaderSize = params[http.param.MaxHeaderSize].size
    val maxInitialLineSize = params[http.param.MaxInitialLineSize].size

    val sourceCodec = new HttpClientCodec(
      maxInitialLineSize.inBytes.toInt,
      maxHeaderSize.inBytes.toInt,
      /*maxChunkSize*/ Int.MaxValue
    )

    pipeline.addLast(HttpCodecName, sourceCodec)
    pipeline.addLast(
      UpgradeRequestHandler.HandlerName,
      new UpgradeRequestHandler(params, sourceCodec)
    )
    initClient(params)(pipeline)
  }
}
