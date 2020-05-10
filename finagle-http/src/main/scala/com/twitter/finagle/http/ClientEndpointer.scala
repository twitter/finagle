package com.twitter.finagle.http

import com.twitter.finagle.Http.HttpImpl
import com.twitter.finagle.{Http, ServiceFactory, Stack, Stackable}
import com.twitter.finagle.client.EndpointerModule
import com.twitter.finagle.http2.param.PriorKnowledge
import com.twitter.finagle.http2.transport.client.{Http2Transporter, PriorKnowledgeServiceFactory}
import com.twitter.finagle.netty4.http.Netty4HttpTransporter
import com.twitter.finagle.param.Stats
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

  private[http] def isPriorKnowledgeEnabled(params: Stack.Params): Boolean =
    params[Http.HttpImpl].clientEndpointer == ClientEndpointer.Http2Endpointer &&
      params[PriorKnowledge].enabled

  private[http] def isHttp2Enabled(params: Stack.Params): Boolean =
    params[Http.HttpImpl].clientEndpointer == ClientEndpointer.Http2Endpointer

  val HttpEndpointer: Stackable[ServiceFactory[Request, Response]] =
    new EndpointerModule[Request, Response](
      Seq(implicitly[Stack.Param[HttpImpl]], implicitly[Stack.Param[Stats]]),
      { (prms: Stack.Params, addr: SocketAddress) =>
        val transporter = Netty4HttpTransporter(prms)(addr)
        new TransporterServiceFactory(transporter, prms)
      }
    )

  val Http2Endpointer: Stackable[ServiceFactory[Request, Response]] =
    new EndpointerModule[Request, Response](
      Seq(implicitly[Stack.Param[HttpImpl]], implicitly[Stack.Param[Stats]]),
      { (prms: Stack.Params, addr: SocketAddress) =>
        val modifierFn = prms[TransportModifier].modifier

        // The HTTP/2 multiplex codec with prior knowledge does pooling the same
        // way that Mux does.
        if (isPriorKnowledgeEnabled(prms)) {
          new PriorKnowledgeServiceFactory(addr, modifierFn, prms)
        } else {
          val transporter = Http2Transporter(addr, modifierFn, prms)
          new TransporterServiceFactory(transporter, prms)
        }
      }
    )
}
