package com.twitter.finagle.mysql.codec

import com.twitter.finagle.mysql.ClientException
import com.twitter.finagle.mysql.protocol._
import com.twitter.finagle.mysql.util.BufferUtil
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

/**
 * Decode a packet into a POJO. 
 * 
 * There are specific packets received from MySQL that can
 * be easily decoded based on their first byte. However, this
 * handler needs to contain state because more complex results
 * can be ambiguous without proper context. The state is stored in the
 * state variable and is synchronized because it is shared between
 * messageReceived and handleUpstream events which can be executed
 * on different threads.
 */
class ResultDecoder extends SimpleChannelHandler {
  private abstract class State
  private case object Idle extends State
  private case object WaitingForGreeting extends State
  private case class PreparedStmtPackets(
    ok: Option[Packet] = None,
    fields: Option[List[Packet]] = None,
    data: Option[List[Packet]] = None
  ) extends State

  private case class ResultSetPackets(
    header: Option[Packet] = None,
    fields: Option[List[Packet]] = None,
    data: Option[List[Packet]] = None
  ) extends State

  private var state: State = WaitingForGreeting

  override def messageReceived(ctx: ChannelHandlerContext, evt: MessageEvent) = {
    evt.getMessage match {
      case packet: Packet =>
        val result: Option[Result] = state match {
          case WaitingForGreeting =>
            transition(Idle)
            Some(ServersGreeting.decode(packet))

          case Idle =>
            decodeSimpleResult(packet)

          case ResultSetPackets(_,_,_) =>
            defragResultSet(packet)
        }

        result map { Channels.fireMessageReceived(ctx, _)   }

      case unknown =>
        Channels.disconnect(ctx.getChannel)
        throw new ClientException("Expected packet and received: " + unknown)
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent): Unit = {
    if(!evt.getMessage.isInstanceOf[ChannelBuffer]) {
      ctx.sendDownstream(evt)
      return
    }

    val buffer = evt.getMessage.asInstanceOf[ChannelBuffer]
    if(buffer.capacity < 5) {
      ctx.sendDownstream(evt)
      return
    }

    //Change the decoder state based on the cmd byte in
    //the channel buffer.
    val cmdByte = buffer.getByte(4)
    println("ResultDecoder.writeRequest with command: " + cmdByte)
    if(cmdByte == Command.COM_QUERY) 
      transition(ResultSetPackets())
    //else if(cmdByte == Command.COM_STMT_PREPARE)
      //transition(PreparedStmtPackets())

    ctx.sendDownstream(evt)
  }

  private def transition(s: State) = synchronized {
    state = s
  }

  /**
   * Decode the packet into a Result object based on the
   * first byte in the packet body.
   */
  private def decodeSimpleResult(packet: Packet): Option[Result] = packet.body(0) match {
    case Packet.okByte => Some(OK.decode(packet))
    case Packet.eofByte => Some(EOF.decode(packet))
    case Packet.errorByte => throw new ClientException(Error.decode(packet).toString)
    case byte => throw new ClientException("Unknown simple result packet.")
  }

  private def defragResultSet(packet: Packet): Option[Result] = {
    (state, packet.body(0)) match {
      //Store Result Set Header Packet.
      case (ResultSetPackets(None, _, _), _) =>
        transition(ResultSetPackets(Some(packet), Some(Nil), None))
        None

      //The first EOF byte encountered signifies that all
      //the field data has been read.
      case (ResultSetPackets(Some(h), Some(xs), None), Packet.eofByte) =>
        transition(ResultSetPackets(Some(h), Some(xs), Some(Nil)))
        None

      //The packet represents field data. Prepend the packet 
      //to field List.
      case (ResultSetPackets(Some(h), Some(xs), None), _) =>
        transition(ResultSetPackets(Some(h), Some(packet :: xs), None))
        None

      //The second EOF byte indicates that all the Result Set data
      //has been read. Decode the complete Result Set which
      //includes the Header, List of Field, and List of RowData Packets.
      case (ResultSetPackets(Some(h), Some(xs), Some(ys)), Packet.eofByte) =>
        transition(Idle)
        Some(ResultSet.decode(h, xs.reverse, ys.reverse))

      //The packet represents row data. Prepend the packet 
      //to row data List.
      case (ResultSetPackets(Some(h), Some(xs), Some(ys)), _) =>
        transition(ResultSetPackets(Some(h), Some(xs), Some(packet :: ys)))
        None

      case (ResultSetPackets(_, _, _), _) =>
        throw new ClientException("Unexpected results when defragmenting ResultSet data.")
    }
  }

  /*private def defragPreparedStatement(packet: Packet): Option[Result] = {
    (state, packet.body(0)) match {
      case PreparedStmtPackets(None, )
    }
  }*/

}