package com.twitter.finagle.postgresql

import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.decoder.Framer
import com.twitter.finagle.decoder.LengthFieldFramer
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.postgresql.transport.ClientTransport
import com.twitter.finagle.postgresql.transport.ConnectionHandshaker
import com.twitter.finagle.postgresql.transport.PgTransportContext
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.io.Buf
import com.twitter.util.Future
import java.net.SocketAddress

/**
 * Transport for the Postgres protocol.
 *
 * This is responsible for properly framing the bytes on the wire to form Postgres protocol packets.
 */
class PgSqlTransporter(
  val remoteAddress: SocketAddress,
  params: Stack.Params)
    extends Transporter[FrontendMessage, BackendMessage, PgTransportContext] {

  private[this] def mkFramer(): Framer =
    new LengthFieldFramer(
      lengthFieldBegin = 1,
      lengthFieldLength = 4,
      lengthAdjust = 1,
      maxFrameLength = Int.MaxValue, // TODO: what's an appropriate value here?
      bigEndian = true
    )

  // We have to special-case TLS because Postgres doesn't use the same transport format during TLS negotiation.
  private[this] val transporter: Transporter[Buf, Buf, TransportContext] =
    params[Transport.ClientSsl] match {
      case Transport.ClientSsl(None) =>
        Netty4Transporter.framedBuf(
          Some(mkFramer),
          remoteAddress,
          params
        )
      case Transport.ClientSsl(Some(_)) =>
        new TlsHandshakeTransporter(remoteAddress, params, mkFramer)
    }

  override def apply(): Future[ClientTransport] =
    transporter().flatMap { transport =>
      val rawTransport = transport.map(
        (msg: FrontendMessage) => msg.toBuf,
        (buf: Buf) => BackendMessage.fromBuf(buf)
      )

      ConnectionHandshaker(rawTransport, params)
        .map { cps =>
          rawTransport.mapContext(ctx => new PgTransportContext(cps, ctx))
        }
        .onFailure(_ => rawTransport.close())
    }
}
