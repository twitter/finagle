package com.twitter.finagle.http

import com.twitter.finagle.Http.{H2ClientImpl, HttpImpl}
import com.twitter.finagle.{Http, ServiceFactory, Stack, Stackable}
import com.twitter.finagle.client.EndpointerModule
import com.twitter.finagle.http2.Http2Transporter
import com.twitter.finagle.http2.{exp => http2exp}
import com.twitter.finagle.http2.param.PriorKnowledge
import com.twitter.finagle.netty4.http.Netty4HttpTransporter
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.toggle.Toggle
import com.twitter.finagle.transport.Transport
import java.net.SocketAddress

/**
 * `Endpointer` implementations for the HTTP client.
 */
private[finagle] object ClientEndpointer {

  /**
   * Tool for testing. We may want to simulate different transport related events
   * and this gives us a handle into the transport abstractions.
   */
  private[http] case class TransportModifier(modifier: Transport[Any, Any] => Transport[Any, Any])

  private[http] object TransportModifier {
    implicit val transportModifierParam: Stack.Param[TransportModifier] =
      Stack.Param(TransportModifier(identity(_)))
  }

  private[this] object useHttp2MultiplexCodecClient {
    private[this] val underlying: Toggle[Int] = Toggles(
      "com.twitter.finagle.http.UseHttp2MultiplexCodecClient"
    )
    def apply(): Boolean = underlying(ServerInfo().id.hashCode)
  }

  def isMultiplexCodec(params: Stack.Params): Boolean = {
    params[H2ClientImpl].useMultiplexClient match {
      case Some(useMultiplex) => useMultiplex
      case None => useHttp2MultiplexCodecClient()
    }
  }

  def isMultiplexPriorKnowledge(params: Stack.Params): Boolean = {
    params[Http.HttpImpl].clientEndpointer == ClientEndpointer.Http2Endpointer &&
    isMultiplexCodec(params) &&
    params[PriorKnowledge].enabled
  }

  val HttpEndpointer: Stackable[ServiceFactory[Request, Response]] =
    new EndpointerModule[Request, Response](
      Seq(implicitly[Stack.Param[HttpImpl]], implicitly[Stack.Param[Stats]]), {
        (prms: Stack.Params, addr: SocketAddress) =>
          val transporter = Netty4HttpTransporter(prms)(addr)
          new TransporterServiceFactory(transporter, prms)
      }
    )

  val Http2Endpointer: Stackable[ServiceFactory[Request, Response]] =
    new EndpointerModule[Request, Response](
      Seq(implicitly[Stack.Param[HttpImpl]], implicitly[Stack.Param[Stats]]), {
        (prms: Stack.Params, addr: SocketAddress) =>
          // The HTTP/2 multiplex codec with prior knowledge does pooling the same
          // way that Mux does.
          if (isMultiplexPriorKnowledge(prms)) {
            val modifierFn = prms[TransportModifier].modifier
            new http2exp.transport.PriorKnowledgeServiceFactory(addr, modifierFn, prms)
          } else {
            // Based on the provided params, build the correct Transporter,
            // either the classic or the MultiplexCodec based Transporter.
            val transporter =
              if (isMultiplexCodec(prms)) {
                val modifierFn = prms[TransportModifier].modifier
                http2exp.transport.Http2Transporter(addr, modifierFn, prms)
              } else {
                Http2Transporter(prms, addr)
              }

            new TransporterServiceFactory(transporter, prms)
          }
      }
    )
}
