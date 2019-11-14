package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.transport.client.H2Pool.OnH2ServiceParam
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.http.{HttpCodecName, initClient, newHttpClientCodec}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.Stack
import com.twitter.util._
import io.netty.channel.ChannelPipeline
import java.net.SocketAddress

/**
 * This `Transporter` makes `Transports` that speak the netty http/1.1, object
 * model while attempting to upgrade to HTTP/2 for requests without content.
 *
 * The `Transports` this hands out should be used serially, and will upgrade to
 * HTTP/2 if possible. If an upgrade succeeds the H2 session is passed to the
 * [[H2Pool]] layer via the provided [[H2Pool.OnH2Service]] obtained from the params.
 */
private class H2CTransporter(
  underlying: Transporter[Any, Any, TransportContext],
  params: Stack.Params)
    extends Transporter[Any, Any, TransportContext] { self =>

  def remoteAddress: SocketAddress = underlying.remoteAddress

  /** Attempt to upgrade to a multiplex session */
  def apply(): Future[Transport[Any, Any]] = {
    underlying().map { trans =>
      val ref = new RefTransport(trans)
      ref.update(new Http2UpgradingTransport(_, ref, params))
      ref
    }
  }
}

private[http2] object H2CTransporter {

  def make(
    addr: SocketAddress,
    modifier: Transport[Any, Any] => Transport[Any, Any],
    params: Stack.Params
  ): Transporter[Any, Any, TransportContext] = {
    val underlying = Netty4Transporter.raw[Any, Any](
      pipelineInit = init(params, modifier),
      addr = addr,
      params = params + Netty4Transporter.Backpressure(false)
    )

    new H2CTransporter(underlying, params)
  }

  private[this] def init(
    params: Stack.Params,
    modifier: Transport[Any, Any] => Transport[Any, Any]
  ): ChannelPipeline => Unit = {
    val onH2Service = params[OnH2ServiceParam].onH2Service.getOrElse {
      throw new IllegalStateException(
        "Couldn't find the `OnH2ServiceParam`. This is a bug in Finagle " +
          "H2C implementation. Please report this to the Finagle maintainers.")
    }

    // setup the vanilla HTTP/1.x stuff and it will interact with the
    // upgrade handler to lift us up to H2 if we get a successful upgrade.
    { pipeline: ChannelPipeline =>
      val sourceCodec = newHttpClientCodec(params)
      pipeline.addLast(HttpCodecName, sourceCodec)
      pipeline.addLast(
        UpgradeRequestHandler.HandlerName,
        new UpgradeRequestHandler(params, onH2Service, sourceCodec, modifier))

      initClient(params)(pipeline)
    }
  }
}
