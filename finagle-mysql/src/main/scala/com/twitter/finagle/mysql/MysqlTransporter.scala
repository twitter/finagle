package com.twitter.finagle.mysql

import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.decoder.LengthFieldFramer
import com.twitter.finagle.mysql.transport.{MysqlTransport, Packet}
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.Future
import java.net.SocketAddress

/**
 * A MySQL specific `framedBuf` `Transporter` which is responsible
 * for connection establishment, framing, and session establishment.
 */
private[finagle] final class MysqlTransporter(
  val remoteAddress: SocketAddress,
  params: Stack.Params)
    extends Transporter[Packet, Packet, TransportContext] {

  private[this] val framerFactory = () => {
    new LengthFieldFramer(
      lengthFieldBegin = 0,
      lengthFieldLength = 3,
      lengthAdjust = Packet.HeaderSize, // Packet size field doesn't include the header size
      maxFrameLength = Packet.HeaderSize + Packet.MaxBodySize,
      bigEndian = false
    )
  }

  private[this] val netty4Transporter =
    Netty4Transporter.framedBuf(
      Some(framerFactory),
      remoteAddress,
      MysqlTransporter.paramsWithoutSsl(params)
    )

  def apply(): Future[Transport[Packet, Packet] { type Context <: TransportContext }] =
    netty4Transporter().flatMap { transport =>
      val mysqlTransport = new MysqlTransport(transport.map(_.toBuf, Packet.fromBuf))
      val handshake = Handshake(params, mysqlTransport)
      handshake.connectionPhase().map(_ => mysqlTransport)
    }

}

private[mysql] object MysqlTransporter {

  // If it is set, we remove the `Transport.ClientSsl` param from the collection
  // of params that we pass to the `Netty4Transporter`. If it is left in, then the
  // `SslHandler` will be added _now_, which we don't want. SSL/TLS for MySQL is
  // negotiated as part of the protocol. So we want the `SslHandler` to be added
  // later as part of the `SecureHandshake` instead.
  def paramsWithoutSsl(params: Stack.Params): Stack.Params =
    params + Transport.ClientSsl(None)

}
