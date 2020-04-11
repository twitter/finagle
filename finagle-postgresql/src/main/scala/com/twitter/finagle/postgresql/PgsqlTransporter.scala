package com.twitter.finagle.postgresql

import java.net.SocketAddress

import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.decoder.Framer
import com.twitter.finagle.decoder.LengthFieldFramer
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Future

class PgsqlTransporter(
  val remoteAddress: SocketAddress,
  params: Stack.Params) extends Transporter[Packet, Packet, TransportContext] {

  // TODO: this doesn't work during ssl handshaking
  def factory: Framer =
    new LengthFieldFramer(
      lengthFieldBegin = 1,
      lengthFieldLength = 4,
      lengthAdjust = 1,
      maxFrameLength = Int.MaxValue,
      bigEndian = true)

  override def apply(): Future[Transport[Packet, Packet] {
    type Context <: TransportContext
  }] = {
    Netty4Transporter.framedBuf(Some(factory _), remoteAddress, params)
      .apply()
      .map { transport =>
        transport.map[Packet, Packet](_.toBuf, Packet.parse)
      }
      .flatMap { transport =>
        val handshake = Handshake(params, transport)
        handshake.startup().foreach(println).unit before Future.value(transport)
      }
  }
}
