package com.twitter.finagle.mysql.codec

import com.twitter.finagle.mysql.ClientError
import com.twitter.finagle.mysql.protocol._
import com.twitter.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

sealed abstract class State
case object Idle extends State
case object WaitingForGreeting extends State
case class PacketDefragger(
    firstPacket: Option[Packet] = None,
    setOne: Option[List[Packet]] = None,
    setTwo: Option[List[Packet]] = None,
    setOneExpected: Boolean = true,
    setTwoExpected: Boolean = true,
    decoder: (Packet, List[Packet], List[Packet]) => Result
  ) extends State

/**
 * Decode a packet into a POJO. 
 * 
 * There are specific packets received from MySQL that can
 * be easily decoded based on their first byte. However, more complex 
 * results need to be defragged as they arrive in the pipeline.
 * To accomplish this, this handler needs to contain some state.
 * 
 * Some of state is synchronized because it is shared between handleDownstream
 * and handleUpstream events which are usually executed
 * on separate threads.
 */
class ResultDecoder extends SimpleChannelHandler {
  private val log = Logger("finagle-mysql")
  private var state: State = WaitingForGreeting
  @volatile 
  private var expectPrepareOK: Boolean = false
  @volatile 
  private var expectBinaryResults: Boolean = false

  override def messageReceived(ctx: ChannelHandlerContext, evt: MessageEvent) = {
    evt.getMessage match {
      case packet: Packet =>
        val result: Option[Result] = state match {
          case WaitingForGreeting =>
            transition(Idle)
            Some(ServersGreeting.decode(packet))

          case Idle =>
            decodePacket(packet)

          case PacketDefragger(_,_,_,_,_,_) =>
            defrag(packet)
        }

        result map { Channels.fireMessageReceived(ctx, _) }

      case unknown =>
        Channels.disconnect(ctx.getChannel)
        log.error("ResultDecoder: Expected packet and received: " + unknown)
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

    //Do we need to block requests over the same
    //pipeline when we are defragging a result?
    if(state != Idle) {
      log.warning("Cannot process a writeRequest when ResultDecoder is not idle.")
      return
    }

    //set flags that indicate expected results.
    val cmdByte = buffer.getByte(4)
    expectPrepareOK = (cmdByte == Command.COM_STMT_PREPARE)
    expectBinaryResults = (cmdByte == Command.COM_STMT_EXECUTE)

    ctx.sendDownstream(evt)
  }

  private def transition(s: State) = state = s

  /**
   * Decode the packet into a Result object based on the
   * first byte in the packet body. Some packets denote the
   * start of a longer transmission. In those cases, transition
   * into the PacketDefragger state.
   */
  private def decodePacket(packet: Packet): Option[Result] = packet.body(0) match {
    case Packet.okByte if expectPrepareOK =>
      val ok = PrepareOK.decode(packet)
      transition(PacketDefragger(setOneExpected = ok.numParams > 0, 
                                setTwoExpected = ok.numColumns > 0,
                                decoder = PreparedStatement.decode))
      defrag(packet)

    case Packet.okByte    => Some(OK.decode(packet))
    case Packet.eofByte   => Some(EOF.decode(packet))
    case Packet.errorByte => Some(Error.decode(packet))

    case byte => 
      transition(PacketDefragger(decoder = ResultSet.decode(expectBinaryResults)))
      defrag(packet)
  }

  /**
   * Defrags a set of packets expected from the server. This method handles
   * both the ResultSet cases and the more complex PreparedStatement cases.
   */
  private def defrag(packet: Packet): Option[Result] = {
    (state, packet.body(0)) match {
      //first packet, no sets expected to follow
      case (PacketDefragger(None, _, _, false, false, decoder), _) =>
        transition(Idle)
        Some(decoder(packet, Nil, Nil))

      //first packet, setOne expected to follow.
      case (PacketDefragger(None, _, _, true, expect2, decoder), _) =>
        transition(PacketDefragger(Some(packet), Some(Nil), None, true, expect2, decoder))
        None

      //first packet, skip setOne
      case (PacketDefragger(None, _, _, false, expect2, decoder), _) =>
        transition(PacketDefragger(Some(packet), Some(Nil), Some(Nil), false, expect2, decoder))
        None

      //first EOF denotes that setOne is complete. Prepare for setTwo if expected
      case (PacketDefragger(Some(h), Some(xs), None, expect1, true, decoder), Packet.eofByte) =>
        transition(PacketDefragger(Some(h), Some(xs), Some(Nil), expect1, true, decoder))
        None

      //first EOF denotes that setOne is complete, defrag complete if !setTwoExpected
      case (PacketDefragger(Some(h), Some(xs), None, _, false, decoder), Packet.eofByte) =>
        transition(Idle)
        Some(decoder(h, xs.reverse, Nil))

      //Prepend the packet to setOne.
      case (PacketDefragger(Some(h), Some(xs), None, expect1, expect2, decoder), _) =>
        transition(PacketDefragger(Some(h), Some(packet :: xs), None, expect1, expect2, decoder))
        None

      //Second EOF denotes both sets have been received. Call decoder.
      case (PacketDefragger(Some(h), Some(xs), Some(ys), _, _, decoder), Packet.eofByte) =>
        transition(Idle)
        Some(decoder(h, xs.reverse, ys.reverse))

      //Prepend the packet to setTwo.
      case (PacketDefragger(Some(h), Some(xs), Some(ys), expect1, expect2, decoder), _) =>
        transition(PacketDefragger(Some(h), Some(xs), Some(packet :: ys), expect1, expect2, decoder))
        None

      case _ =>
        throw new ClientError("ResultDecoder: Unexpected state when defragmenting packets.")

    }
  }

}