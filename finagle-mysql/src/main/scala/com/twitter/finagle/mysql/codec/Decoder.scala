package com.twitter.finagle.mysql.codec

import com.twitter.finagle.mysql.DecoderException
import com.twitter.finagle.mysql.protocol._
import com.twitter.finagle.mysql.util.BufferUtil
import com.twitter.util.Future
import com.twitter.util.StateMachine
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.frame.FrameDecoder

class Decoder extends FrameDecoder with StateMachine {
  val EOF_Byte = 0xFE.toByte
  val OK_Byte = 0x00.toByte
  val ERROR_Byte = 0xFF.toByte
  
  val needMoreData: Packet = null
  state = WaitingForGreetings //initial state

  case object WaitingForGreetings extends State
  case object WaitingForLoginResponse extends State
  case object Idle extends State
  case class QueryInProgress(
    header: Option[Packet] = None,
    fields: Option[List[Packet]] = None,
    data: Option[List[Packet]] = None
  ) extends State

  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Result = {
    val packet = decodePacket(buffer)
    if(packet == needMoreData) 
      null
    else 
      state match {
        case WaitingForGreetings =>
          state = WaitingForLoginResponse
          ServersGreeting.decode(packet)

        case QueryInProgress(_,_,_) =>
          val res = decodeResultSet(packet, buffer)
          if (res != null) 
            state = Idle
          res

        case _ =>
          decodeResult(packet, buffer)
      }
  }

  def decodeResult(packet: Packet, buffer: ChannelBuffer): Result = packet.body(0) match {
    case `OK_Byte` =>
      state = Idle
      OK.decode(packet)
    case `ERROR_Byte`=>
      state = Idle
      Error.decode(packet)
    case byte =>
      state = QueryInProgress()
      decodeResultSet(packet, buffer)
  }

  def decodeResultSet(packet: Packet, buffer: ChannelBuffer): Result = {
    if (packet == null)
      null // Need more data
    else
      (state, packet.body(0)) match {
        case (QueryInProgress(None, _, _), _) =>
          state = QueryInProgress(Some(packet), Some(Nil), None)
          decodeResultSet(decodePacket(buffer), buffer)

        case (QueryInProgress(Some(h), Some(xs), None), `EOF_Byte`) =>
          state = QueryInProgress(Some(h), Some(xs), Some(Nil))
          decodeResultSet(decodePacket(buffer), buffer)

        case (QueryInProgress(Some(h), Some(xs), None), _) =>
          state = QueryInProgress(Some(h), Some(packet :: xs), None)
          decodeResultSet(decodePacket(buffer), buffer)

        case (QueryInProgress(Some(h), Some(xs), Some(ys)), `EOF_Byte`) =>
          state = QueryInProgress()
          ResultSet.decode(h, xs.reverse, ys.reverse)

        case (QueryInProgress(Some(h), Some(xs), Some(ys)), _) =>
          state = QueryInProgress(Some(h), Some(xs), Some(packet :: ys))
          decodeResultSet(decodePacket(buffer), buffer)

        case (QueryInProgress(_, _, _), _) =>
          throw new DecoderException("State inconcistency!")
    }
  }

  /**
   * Decodes a logical MySQL packet from a ChannelBuffer 
   * if there are enough bytes on the buffer.
   *
   * A MySQL packet consists of a header,
   * 3-bytes containing the size of the body and a 
   * 1 byte seq number, followed by the body.
   */
  def decodePacket(buffer: ChannelBuffer): Packet = {
    if (buffer.readableBytes < Packet.headerSize)
      needMoreData
    else {
      var bodySize: Int = buffer.readByte()
      bodySize += buffer.readByte() << 8
      bodySize += buffer.readByte() << 16
      val seq = buffer.readByte()
      if (buffer.readableBytes() < bodySize)
        needMoreData
      else {
        println("<- Decoding MySQL packet (n=%d, size=%d)".format(seq, bodySize))
        val body = new Array[Byte](bodySize)
        buffer.readBytes(body)
        BufferUtil.hex(body)
        Packet(bodySize, seq, body)
      }
    }
  }
}