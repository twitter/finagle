package com.finagle.zookeeper

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import java.util.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer
import protocol.frame.ReplyHeader

/**
 * This is the core artifact of the module, implementing the transition to the wire protocol.
 *
 * It uses a state machine behaviour because of the logic of the Zookeeper protocol.
 */
class ZookeeperEncoderDecoder extends SimpleChannelHandler with StateMachine {
  private[this] val logger = Logger.getLogger("finagle-zookeeper")

  /**
   * Session-related state
   *
   * TODO: Currently incomplete set of variables
   */
  private[this] var negotiatedTimeout: Long = _


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

  def reset() {
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
   * A ChannelBuffer with a server response should be avaible from the previous element.
   * In the pipeline.
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
    case frame: ChannelBuffer => {
      /**
       * Extract the header and deconstruct message.
       */
      ReplyHeader.deserialize(frame) match {
        case ReplyHeader(xid, _, err) => xid match {
          case _ => logger.warning("Unknown header type received.")
        }
        case _ => throw new UnsupportedOperationException("Cannot deserialize header response")
      }
    }

    case _ => {
      Channels.disconnect(ctx.getChannel)
      logger.severe("Bad message event type received.")
    }
  }
}
