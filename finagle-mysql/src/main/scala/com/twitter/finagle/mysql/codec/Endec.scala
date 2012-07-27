package com.twitter.finagle.mysql.codec

import com.twitter.finagle.mysql.ClientError
import com.twitter.finagle.mysql.protocol._
import com.twitter.logging.Logger
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._

sealed trait State
case object Idle extends State
case object WaitingForGreeting extends State
case class Defragging(
  expected: Int,
  packets: Seq[Seq[Packet]]
) extends State

/**
  * Encoder: Encodes a Request into a ChannelBuffer.
  * Decoder: Decodes a Packet into a POJO.
  * 
  * There are specific packets received from MySQL that can
  * be easily decoded based on their first byte. However, more complex 
  * results need to be defragged as they arrive in the pipeline.
  * To accomplish this, this handler needs to contain some state.
  * 
  * Some of state is volatile because it is shared between handleDownstream
  * and handleUpstream events which are usually executed
  * on separate threads.
 */
class Endec extends SimpleChannelHandler {
  private[this] val log = Logger("finagle-mysql")
  private[this] var state: State = WaitingForGreeting
  private[this] var defragDecoder: (Packet, Seq[Packet], Seq[Packet]) => Result = _ 
  @volatile private[this] var expectPrepareOK: Boolean = false
  @volatile private[this] var expectBinaryResults: Boolean = false

  override def messageReceived(ctx: ChannelHandlerContext, evt: MessageEvent) = {
    evt.getMessage match {
      case packet: Packet =>
        val decodedResult: Option[Result] = state match {
          case WaitingForGreeting =>
            transition(Idle)
            Some(ServersGreeting.decode(packet))

          case Idle =>
            decodePacket(packet)

          case Defragging(_,_) =>
            defrag(packet)
        }

        decodedResult map { Channels.fireMessageReceived(ctx, _) }

      case unknown =>
        Channels.disconnect(ctx.getChannel)
        log.error("ResultDecoder: Expected Packet and received: " + unknown.getClass.getName)
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) = {
    val encodedReq: Option[ChannelBuffer] = evt.getMessage match {
      case req: CommandRequest =>
        expectPrepareOK = (req.cmd == Command.COM_STMT_PREPARE)
        expectBinaryResults = (req.cmd == Command.COM_STMT_EXECUTE)
        Some(req.toChannelBuffer)

      case req: Request => 
        Some(req.toChannelBuffer)

      case unknown => 
        log.error("RequestEncoder: Expected Request and received: " + unknown.getClass.getName)
        None
    }

    encodedReq map { Channels.write(ctx, evt.getFuture(), _, evt.getRemoteAddress()) }
  }

  private[this] def transition(s: State) = state = s

  /**
    * Decode the packet into a Result object based on the
    * first byte in the packet body. Some bytes denote the
    * start of a longer transmission. In those cases, transition
    * into the Defragging state.
    */
  private[this] def decodePacket(packet: Packet): Option[Result] = packet.body(0) match {
    case Packet.OkByte if expectPrepareOK =>
      def expected(n: Int) = if (n > 0) 1 else 0

      val ok = PreparedOK.decode(packet)
      val numSetsExpected = expected(ok.numOfParams) + expected(ok.numOfColumns)

      defragDecoder = PreparedStatement.decode
      transition(Defragging(numSetsExpected, Nil))
      defrag(packet)

    case Packet.OkByte    => Some(OK.decode(packet))
    case Packet.EofByte   => Some(EOF.decode(packet))
    case Packet.ErrorByte => Some(Error.decode(packet))

    case byte => 
      defragDecoder = ResultSet.decode(expectBinaryResults)
      transition(Defragging(2, Nil))
      defrag(packet)
  }

  /**
    * Defrags a set of packets expected from the server. This handles defragging 
    * packets for a ResultSet and a PreparedStatement. 
    * 
    * For a PreparedStatement the packet sequences are not neccessarily 
    * defragged in order and the order needs to be determined based on the 
    * PreparedOK meta data. This happens when the PreparedStatement is decoded
    * in order to simplify this method.
    */
  private[this] def defrag(packet: Packet): Option[Result] = (state, packet.body(0)) match {
    // header packet, no sets expected to follow
    case (Defragging(0, Nil), _) =>
      transition(Idle)
      Some(defragDecoder(packet, Nil, Nil))

    // header packet, some sets expected to follow
    case (Defragging(expected, Nil), _) =>
      transition(Defragging(expected, Seq(Seq(packet), Nil)))
      None

    // first set complete, no sets expected to follow
    case (Defragging(1, Seq(header, xs)), Packet.EofByte) =>
      transition(Idle)
      Some(defragDecoder(header(0), xs.reverse, Nil))

    // first set complete, 1 set expected to follow
    case (Defragging(2, Seq(header, xs)), Packet.EofByte) =>
      transition(Defragging(2, Seq(header, xs, Nil)))
      None

    // prepend onto first set
    case (Defragging(expected, Seq(header, xs)), _) =>
      transition(Defragging(expected, Seq(header, packet +: xs)))
      None

    // second set complete - no sets can follow.
    case (Defragging(2, Seq(header, xs, ys)), Packet.EofByte) =>
      transition(Idle)
      Some(defragDecoder(header(0), xs.reverse, ys.reverse))

    // prepend onto second set
    case (Defragging(2, Seq(header, xs, ys)), _) =>
      transition(Defragging(2, Seq(header, xs, packet +: ys)))
      None

    case _ =>
        throw new ClientError("ResultDecoder: Unexpected state when defragmenting packets.")
  }
}