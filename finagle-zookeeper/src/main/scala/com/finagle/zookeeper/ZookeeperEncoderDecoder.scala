package com.finagle.zookeeper

import org.jboss.netty.channel._
import com.twitter.util.StateMachine
import java.util.logging.Logger
import org.jboss.netty.buffer.ChannelBuffer

/**
 * This is the core artifact of the module, implementing the transition to the wire protocol.
 *
 * It uses a state machine behaviour because of the logic of the Zookeeper protocol.
 */
class ZookeeperHighlevelEncoderDecoder extends SimpleChannelHandler with StateMachine {
  private[this] val logger = Logger.getLogger("finagle-zookeeper")

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

  //TODO: Not sure if ok, maybe transition is better.
  reset()

  /**
   * Begin connection establishment phase.
   */
  def begin() {
    state = Connecting
  }

  def reset() {
    state = NotConnected
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    super.channelConnected(ctx, e)
    begin()
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    super.channelDisconnected(ctx, e)
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
    case frame: ChannelBuffer => (null, null)
    case _ =>
      Channels.disconnect(ctx.getChannel)
      logger.severe("Bad message event type received.")
  }
}
