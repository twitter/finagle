package com.twitter.finagle.exp.mysql.codec

import com.twitter.finagle.exp.mysql.ClientError
import com.twitter.finagle.exp.mysql.protocol._
import com.twitter.finagle.exp.mysql._
import java.util.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._

sealed trait State
case object Idle extends State
case class Handshaking(
  wroteInternalGreet: Boolean,
  serverGreet: Option[ServersGreeting]
) extends State
case object WaitingForPrepareOK extends State
case object WaitingForExecuteResults extends State
case class Defragging(
  expected: Int,
  packets: Seq[Seq[Packet]],
  decoder: (Packet, Seq[Packet], Seq[Packet]) => Result
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
  private[this] val logger = Logger.getLogger("finagle-mysql")
  @volatile private[this] var state: State = Handshaking(false, None)

  private[this] def transition(s: State) = { state = s }

  /**
   * Netty downstream handler. The message event should contain a packet from
   * the MySQL server.
   */
  override def messageReceived(ctx: ChannelHandlerContext, evt: MessageEvent) = evt.getMessage match {
    case packet: Packet =>
      val decodedResult = decode(packet)
      decodedResult map { Channels.fireMessageReceived(ctx, _) }

    case unknown =>
      Channels.disconnect(ctx.getChannel)
      logger.severe("Unexpected message received: %s".format(unknown.getClass.getName))
  }

  /**
   * Netty upstream handler. The message event should contain an
   * object of type Request.
   */
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) = evt.getMessage match {
    case req: Request =>
      val encodedReq = encode(req, Some(ctx))
      encodedReq map { Channels.write(ctx, evt.getFuture, _, evt.getRemoteAddress) }

    case unknown =>
      logger.severe("Unexpected request type: %s".format(unknown.getClass.getName))
  }

  /**
   * Logical entry point for the Decoder.
   * Decodes a packet based on the current state.
   */
  def decode(packet: Packet): Option[Result] = state match {
    // To avoid finagle treating the server greeting as an orphaned response, ensure
    // the server greeting response is paired with a request. If it isn't hang on to
    // it until we get the appropriate ClientInternalGreet request.
    case Handshaking(false, None) =>
      transition(Handshaking(false, Some(ServersGreeting.decode(packet))))
      None

    case Handshaking(true, _) =>
      transition(Idle)
      Some(ServersGreeting.decode(packet))

    case Idle                     => decodePacket(packet, false, false)
    case WaitingForPrepareOK      => decodePacket(packet, true, false)
    case WaitingForExecuteResults => decodePacket(packet, false, true)
    case Defragging(_,_,_)        => defrag(packet)

    case _ =>
      logger.severe("Unexpected state %s while decoding packet.".format(state))
      None
  }

  /**
   * Logical entry point for the Encoder.
   * Encodes a request into ChannelBuffer.
   */
  def encode(req: Request, ctx: Option[ChannelHandlerContext]): Option[ChannelBuffer] = req match {
    // The ClientInternalGreet contains an empty buffer and is used to
    // pair the MySQL server greeting packet with a write request. This satisfies
    // the request-response model assumed in finagle.
    case ClientInternalGreet =>
      state match {
        case Handshaking(false, None) =>
          transition(Handshaking(true, None))

        case Handshaking(_, Some(sg)) =>
          transition(Idle)
          ctx map { Channels.fireMessageReceived(_, sg) }

        case _ =>
          logger.severe("Unexpected state %s during handshaking".format(state))
      }

      Some(ClientInternalGreet.toChannelBuffer)

    // Synthesize a response for a CloseRequest because we won't
    // receive one from the server.
    case req: CommandRequest if req.cmd == Command.COM_STMT_CLOSE =>
      ctx map { Channels.fireMessageReceived(_, CloseStatementOK) }
      Some(req.toChannelBuffer)

    case req: CommandRequest if req.cmd == Command.COM_STMT_PREPARE =>
      transition(WaitingForPrepareOK)
      Some(req.toChannelBuffer)

    case req: CommandRequest if req.cmd == Command.COM_STMT_EXECUTE =>
      transition(WaitingForExecuteResults)
      Some(req.toChannelBuffer)

    case req: CommandRequest =>
      transition(Idle)
      Some(req.toChannelBuffer)

    case _ => Some(req.toChannelBuffer)
  }

  /**
   * Decode the packet into a Result object based on the
   * first byte in the packet body (field_count). Some bytes denote the
   * start of a longer transmission. In those cases, transition
   * into the Defragging state.
   */
  private[this] def decodePacket(
    packet: Packet,
    isPrepareOK: Boolean,
    isExecuteResult: Boolean
  ): Option[Result] = packet.body(0) match {
    case Packet.OkByte if isPrepareOK =>
      def expected(n: Int) = if (n > 0) 1 else 0
      val ok = PreparedOK.decode(packet)
      val numSetsExpected = expected(ok.numOfParams) + expected(ok.numOfColumns)

      transition(Defragging(numSetsExpected, Nil, PreparedStatement.decode))
      defrag(packet)

    case Packet.OkByte    => Some(OK.decode(packet))
    case Packet.EofByte   => Some(EOF.decode(packet))
    case Packet.ErrorByte => Some(Error.decode(packet))

    case byte =>
      transition(Defragging(2, Nil, ResultSet.decode(isExecuteResult)))
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
    case (Defragging(0, Nil, f), _) =>
      transition(Idle)
      Some(f(packet, Nil, Nil))

    // header packet, some sets expected to follow
    case (Defragging(expected, Nil, f), _) =>
      transition(Defragging(expected, Seq(Seq(packet), Nil), f))
      None

    // first set complete, no sets expected to follow
    case (Defragging(1, Seq(header, xs), f), Packet.EofByte) =>
      transition(Idle)
      Some(f(header(0), xs.reverse, Nil))

    // first set complete, 1 set expected to follow
    case (Defragging(2, Seq(header, xs), f), Packet.EofByte) =>
      transition(Defragging(2, Seq(header, xs, Nil), f))
      None

    // prepend onto first set
    case (Defragging(expected, Seq(header, xs), f), _) =>
      transition(Defragging(expected, Seq(header, packet +: xs), f))
      None

    // second set complete - no sets can follow.
    case (Defragging(2, Seq(header, xs, ys), f), Packet.EofByte) =>
      transition(Idle)
      Some(f(header(0), xs.reverse, ys.reverse))

    // prepend onto second set
    case (Defragging(2, Seq(header, xs, ys), f), _) =>
      transition(Defragging(2, Seq(header, xs, packet +: ys), f))
      None

    case _ =>
      logger.severe("Unexpected state %s when defragging packets".format(state))
      None
  }
}