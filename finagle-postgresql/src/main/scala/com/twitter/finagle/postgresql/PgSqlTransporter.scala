package com.twitter.finagle.postgresql

import java.net.SocketAddress

import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.decoder.Framer
import com.twitter.finagle.decoder.LengthFieldFramer
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.io.Buf
import com.twitter.util.Future

/**
 * Transport for the Postgres protocol.
 *
 * This is responsible for properly framing the bytes on the wire to form Postgres protocol packets.
 */
class PgSqlTransporter(
  val remoteAddress: SocketAddress,
  params: Stack.Params
) extends Transporter[Buf, Buf, TransportContext] {

  private[this] def framer: Framer =
    new LengthFieldFramer(
      lengthFieldBegin = 1,
      lengthFieldLength = 4,
      lengthAdjust = 1,
      maxFrameLength = Int.MaxValue, // TODO: what's an appropriate value here?
      bigEndian = true
    )

  // We have to special-case TLS because Postgres doesn't use the same transport format during TLS negotiation.
  val transporter: Transporter[Buf, Buf, TransportContext] = params[Transport.ClientSsl] match {
    case Transport.ClientSsl(None) =>
      Netty4Transporter.framedBuf(
        Some(framer _),
        remoteAddress,
        params
      )
    case Transport.ClientSsl(Some(_)) =>
      new TlsHandshakeTransporter(remoteAddress, params, framer)
  }

  override def apply(): Future[Transport[Buf, Buf] {
    type Context <: TransportContext
  }] = {
    transporter()
  }
}
