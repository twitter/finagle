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
  val needMoreData: Packet = null
  state = WaitingForGreetings //initial state

  case object WaitingForGreetings extends State
  case object Idle extends State
  case class QueryInProgress(
    header: Option[Packet] = None,
    fields: Option[List[Packet]] = None,
    data: Option[List[Packet]] = None
  ) extends State

  /**
   * Decodes a relevant Result from the ChannelBuffer
   * based on the logical MySQL packet and the current
   * state.
   */
  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Result = {
    val packet = decodePacket(buffer)
    if(packet == needMoreData) 
      null
    else {
      val result = state match {
        case Idle => decodeResult(packet, buffer)
        case WaitingForGreetings => ServersGreeting.decode(packet)
        case QueryInProgress(_,_,_) => decodeResultSet(packet, buffer)
      }
      if(result != null)
        state = Idle
      result
    }
  }

  /**
   * Decodes a logical MySQL packet from a ChannelBuffer 
   * if there are enough bytes on the buffer.
   */
  def decodePacket(buffer: ChannelBuffer): Packet = {
    if (buffer.readableBytes < Packet.headerSize)
      needMoreData
    else {
      val headerBytes = new Array[Byte](Packet.headerSize)
      buffer.readBytes(headerBytes)
      val br = new BufferReader(headerBytes)
      val (bodySize, seq) = (br.readInt24, br.readByte)
      if (buffer.readableBytes() < bodySize)
        needMoreData
      else {
        println("<- Decoding MySQL packet (size=%d, seq=%d)".format(bodySize, seq))
        val body = new Array[Byte](bodySize)
        buffer.readBytes(body)
        BufferUtil.hex(body)
        Packet(bodySize, seq, body)
      }
    }
  }

  /**
   * Decode the packet into a Result object based on the
   * first byte in the packet body.
   */
  def decodeResult(packet: Packet, buffer: ChannelBuffer): Result = packet.body(0) match {
    case Packet.okByte => OK.decode(packet)
    case Packet.errorByte => Error.decode(packet)
    case Packet.eofByte => EOF.decode(packet)
    case byte =>
      state = QueryInProgress()
      decodeResultSet(packet, buffer)
  }

  /**
   * Decode the ChannelBuffer into a Result Set. The ChannelBuffer is expected
   * to contain a Header Packet and 2 sequences of Packets (field and row data 
   * packets, respectively) delimited by EOF bytes.
   */
  def decodeResultSet(packet: Packet, buffer: ChannelBuffer): Result = {
    if (packet == null)
      null // Need more data
    else
      (state, packet.body(0)) match {
        //Store Result Set Header Packet.
        case (QueryInProgress(None, _, _), _) =>
          state = QueryInProgress(Some(packet), Some(Nil), None)
          decodeResultSet(decodePacket(buffer), buffer)

        //The first EOF byte encountered signifies that all
        //the field data has been read.
        case (QueryInProgress(Some(h), Some(xs), None), Packet.eofByte) =>
          state = QueryInProgress(Some(h), Some(xs), Some(Nil))
          decodeResultSet(decodePacket(buffer), buffer)

        //The packet represents field data. Prepend the packet 
        //to field List.
        case (QueryInProgress(Some(h), Some(xs), None), _) =>
          state = QueryInProgress(Some(h), Some(packet :: xs), None)
          decodeResultSet(decodePacket(buffer), buffer)

        //The second EOF byte indicates that all the Result Set data
        //has been read. Decode the complete Result Set which
        //includes the Header, List of Field, and List of RowData Packets.
        case (QueryInProgress(Some(h), Some(xs), Some(ys)), Packet.eofByte) =>
          state = QueryInProgress()
          ResultSet.decode(h, xs.reverse, ys.reverse)

        //The packet represents row data. Prepend the packet 
        //to row data List.
        case (QueryInProgress(Some(h), Some(xs), Some(ys)), _) =>
          state = QueryInProgress(Some(h), Some(xs), Some(packet :: ys))
          decodeResultSet(decodePacket(buffer), buffer)

        case (QueryInProgress(_, _, _), _) =>
          throw new DecoderException("Unexpected results when decoding result set data.")
    }
  }
}