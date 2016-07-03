package com.twitter.finagle.exp.mysql.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.codec.{FrameDecoder, LengthFieldDecoder}
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.Stack

/**
 * Responsible for the transport layer plumbing required to produce
 * a Transporter[Packet, Packet]. The current default is Netty3
 */
object TransportImpl {
  val Netty3: TransportImpl = TransportImpl(params => Netty3Transporter(MysqlClientPipelineFactory, params))

  val Netty4: TransportImpl = TransportImpl(params => Netty4Transporter(
    Some(_.toBuf),
    Some(decoderFactory),
    params
  ))

  implicit val param: Stack.Param[TransportImpl] = Stack.Param(Netty3)

  private def decoderFactory(): FrameDecoder[Packet] = {
    val decoder = new LengthFieldDecoder(
      lengthFieldBegin = 0,
      lengthFieldLength = 3,
      lengthAdjust = Packet.HeaderSize, // Packet size field doesn't include the header size.
      maxFrameLength = Packet.HeaderSize + Packet.MaxBodySize,
      bigEndian = false
    )
    decoder.andThen(_.map(Packet.fromBuf))
  }
}

case class TransportImpl(transporter: Stack.Params => Transporter[Packet, Packet]) {
  def mk(): (TransportImpl, Stack.Param[TransportImpl]) = {
    (this, TransportImpl.param)
  }
}
