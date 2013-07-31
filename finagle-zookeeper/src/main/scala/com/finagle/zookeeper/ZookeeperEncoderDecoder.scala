package com.finagle.zookeeper

import errors.UnknownCommandException
import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import java.util.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer
import protocol.frame.ReplyHeader
import protocol.{AwaitsResponse, HeaderBodyDeserializer}

/**
 * This is the core artifact of the module, implementing the transition to the wire protocol.
 *
 * It uses a state machine behaviour because of the logic of the Zookeeper protocol.
 */
class ZookeeperEncoderDecoder(val canBeRO: Boolean = false,
                              val desiredTimeout: Int = 1000,
                              var sessionID: Long = 0,
                              var password: Array[Byte] = new Array[Byte](16))
  extends SimpleChannelHandler
  with StateMachine {

  private[this] val logger = Logger.getLogger("finagle-zookeeper")

  logger.info("Constructing main pipeline component.")

  /**
   * Session-related state
   *
   * TODO: Currently incomplete set of variables
   */
  private[this] var negotiatedTimeout: Long = _

  private [this] var pendingRequests: List[HeaderBodyDeserializer] = List()

  /**
   * Possible states of the client
   */
  case object Connecting extends State
  case object Associating extends State
  case object Connected extends State
  case object ReadOnlyConnected extends State
  case object Closed extends State
  case object AuthFailed extends State
  case object NotConnected extends State

  /**
   * Timestamp of the last event prcessed from the server
   */
  @volatile private[this] var lastEvent: Long = _

  private[this] def markEvent(time: Long) {
    this.synchronized {
      lastEvent = time
    }
  }

  //TODO: Not sure if ok, maybe transition is better due to the race conditions that may occur.
  reset()

  /**
   * Begin connection establishment phase.
   */
  def begin() {
    state = Connecting
  }

  /**
   * Constructor code, grouped inside a method for clarity of purpose.
   */
  private[this] def reset() {
    state = NotConnected
    lastEvent = 0
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    super.channelConnected(ctx, e)
    logger.info("Connected to Zookeeper server.")
    begin()
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    super.channelDisconnected(ctx, e)
    logger.info("Connection lost.")
    reset()
  }

  /**
   * When a new request is issued
   * @param ctx
   * @param e
   */
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {

    e.getMessage match {
      case request: ZookeeperRequest => {
        logger.info("Incoming request received:" + request)

        val binaryEncoding = requestToChannelBuffer(request)
        Channels.write(ctx, e.getFuture, binaryEncoding, e.getRemoteAddress)

        /**
         * Mark if a response is expected.
         */
        request match {
          case (_, requestBody: AwaitsResponse) => {
            logger.info("AwaitingResponse request received.")

            pendingRequests = requestBody.responseDeserializer :: pendingRequests
          }

          case _ => logger.info("Request not added to pending.")
        }
      }

      case _ => throw UnknownCommandException()
    }

  }

  /**
   * A ChannelBuffer with a server response should be avaible from the previous element.
   * in the pipeline.
   *
   * This response is made up of a header and, possibly, a response body.
   *
   * Decoding works in the following way:
   *
   * Check what kind of message was received based on the header.
   *
   * If there's also a message body, then this is a response to a pending request
   * and it should be deserialized according to the head of the queue (the oldest
   * request pending) since requests are handled in order by the Zookeeper server.
   *
   * @param ctx
   * @param e
   */
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = e.getMessage match {
    /**
     * By now, the buffer should contain one pair of header and body, and one at most
     */
    case frame: ChannelBuffer => {

//            Channels.fireMessageReceived(ctx, (Some(reply), None))

    }

    case _ => {
      Channels.disconnect(ctx.getChannel)
      logger.severe("Bad message event type received.")
    }
  }
}
