package com.twitter.finagle.mysql.protocol

import org.jboss.netty.handler.codec.frame.FrameDecoder
import com.twitter.util.StateMachine
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import com.twitter.finagle.mysql.util.ByteArrayUtil
import com.twitter.util.Future
import com.twitter.finagle.mysql.DecoderException

class Decoder extends FrameDecoder with StateMachine {
  val EOF_Byte = 0xFE.toByte
  val OK_Byte = 0x00.toByte
  val ERROR_Byte = 0xFF.toByte
  
  val needMoreData: Packet = null
  val partialResult: Result = null
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
    state match {
      case WaitingForGreetings =>
        val packet = decodePacket(buffer)
        if(packet != needMoreData) {
          state = WaitingForLoginResponse
          ServersGreeting.decode(packet)
        }
        else
          partialResult
        
      case WaitingForLoginResponse =>
        val res = decodeResult(buffer)
        if (res == Error)
          state = WaitingForGreetings
        res

      case Idle =>
        decodeResult(buffer)

      case QueryInProgress(_,_,_) =>
        val res = decodeResultSet(decodePacket(buffer), buffer)
        if (res != null)
          state = Idle
        res
    }
  }

  def decodeResult(buffer: ChannelBuffer): Result = {
    val packet = decodePacket(buffer)
    packet.body(0) match {
      case `OK_Byte` =>
        state = Idle
        OK
      case `ERROR_Byte`=>
        state = Idle
        val code = ByteArrayUtil.read(packet.body, 1, 2).toShort
        val sqlState = new String(packet.body.drop(3).take(6))
        val errorMessage = new String(packet.body.drop(9).take(packet.body.size - 9))
        Error(packet.body(1), sqlState, errorMessage)
      case b =>
        state = QueryInProgress()
        decodeResultSet(packet, buffer)
    }
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

  def decodePacket(buffer: ChannelBuffer): Packet = {
    if (buffer.readableBytes < Packet.headerSize)
      needMoreData
    else {
      //bodySize is a 3-byte numeric value
      var bodySize: Int = buffer.readByte()
      bodySize += buffer.readByte() << 8
      bodySize += buffer.readByte() << 16
      //sequence number is 1 Byte
      val seq = buffer.readByte()
      if (buffer.readableBytes() < bodySize)
        needMoreData
      else {
        println("<- Decoding MySQL packet (n=%d, size=%d)".format(seq,bodySize))
        val body = new Array[Byte](bodySize)
        buffer.readBytes(body)
        ByteArrayUtil.hex(body)
        Packet(bodySize, seq, body)
      }
    }
  }
}