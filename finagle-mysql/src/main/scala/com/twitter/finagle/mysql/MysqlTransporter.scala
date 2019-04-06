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
 * A MySQL specific `framedBuf` `Transporter` which at this time is only
 * responsible for connection establishment and framing. Eventually this
 * `Transporter` should be responsible for session acquisition for a
 * MySQL plain or encrypted session.
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
    Netty4Transporter.framedBuf(Some(framerFactory), remoteAddress, params)

  def apply(): Future[Transport[Packet, Packet] { type Context <: TransportContext }] =
    netty4Transporter().map { transport =>
      new MysqlTransport(transport.map(_.toBuf, Packet.fromBuf))
    }

}
