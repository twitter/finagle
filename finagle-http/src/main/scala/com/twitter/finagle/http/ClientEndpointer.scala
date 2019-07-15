package com.twitter.finagle.http

import com.twitter.finagle.{Status => FinagleStatus, _}
import com.twitter.finagle.Http.{H2ClientImpl, HttpImpl}
import com.twitter.finagle.client.{EndpointerModule, Transporter}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http.codec.HttpClientDispatcher
import com.twitter.finagle.http2.transport.MultiplexTransporter
import com.twitter.finagle.http.exp.StreamTransport
import com.twitter.finagle.http2.Http2Transporter
import com.twitter.finagle.http2.exp.transport.{Http2Transport, StreamChannelTransport}
import com.twitter.finagle.netty4.http.{Netty4ClientStreamTransport, Netty4HttpTransporter}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.toggle.Toggle
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.{Future, Time}
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

  val HttpEndpointer: Stackable[ServiceFactory[Request, Response]] =
    transporterEndpointer(
      new Netty4ClientStreamTransport(_),
      Netty4HttpTransporter
    )

  val Http2Endpointer: Stackable[ServiceFactory[Request, Response]] =
    transporterEndpointer(
      new Netty4ClientStreamTransport(_),
      selectH2Transporter
    )

  // Based on the provided params, build the correct Transporter,
  // either the classic or the MultiplexCodec based Transporter.
  private def selectH2Transporter(
    params: Stack.Params
  ): SocketAddress => Transporter[Any, Any, TransportContext] = {
    params[H2ClientImpl].useMultiplexClient match {
      case Some(true) => http2.exp.transport.Http2Transporter(params)
      case Some(false) => Http2Transporter(params)
      case None =>
        // If we haven't been explicitly configured fall back to the toggle
        // to determine the default H2 transporter implementation.
        if (useHttp2MultiplexCodecClient()) http2.exp.transport.Http2Transporter(params)
        else Http2Transporter(params)
    }
  }

  // Construct an `Endpointer` that is based on the `Transporter` model.
  private def transporterEndpointer(
    clientTransport: Transport[Any, Any] => StreamTransport[Request, Response],
    mkTransporter: Stack.Params => SocketAddress => Transporter[Any, Any, TransportContext]
  ): Stackable[ServiceFactory[Request, Response]] = {
    new EndpointerModule[Request, Response](
      Seq(implicitly[Stack.Param[HttpImpl]], implicitly[Stack.Param[Stats]]), {
        (prms: Stack.Params, addr: SocketAddress) =>
          val modifier = prms[TransportModifier].modifier
          val transporter = mkTransporter(prms)(addr)
          val dispatcherStats =
            prms[Stats].statsReceiver.scope(GenSerialClientDispatcher.StatsScope)

          new ServiceFactory[Request, Response] {
            def apply(conn: ClientConnection): Future[Service[Request, Response]] =
              // we do not want to capture and request specific Locals
              // that would live for the life of the session.
              Contexts.letClearAll {
                transporter().map { trans =>
                  val modifiedTransport = modifier(trans)
                  val streamTransport = clientTransport(modifiedTransport)
                  val httpTransport = trans match {
                    case _: StreamChannelTransport => new Http2Transport(streamTransport)
                    case _ => new HttpTransport(streamTransport)
                  }

                  new HttpClientDispatcher(
                    httpTransport,
                    dispatcherStats
                  )
                }
              }

            // `MultiplexTransporter`s are internally caching a H2 session so they
            // must also have their resources released on close.
            def close(deadline: Time): Future[Unit] = transporter match {
              case multiplex: MultiplexTransporter => multiplex.close(deadline)
              case _ => Future.Done
            }

            override def status: FinagleStatus = transporter match {
              case http2: MultiplexTransporter => http2.transporterStatus
              case _ => super.status
            }
          }
      }
    )
  }
}
