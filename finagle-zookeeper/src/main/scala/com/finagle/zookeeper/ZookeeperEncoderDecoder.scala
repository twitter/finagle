package com.finagle.zookeeper

import org.jboss.netty.channel.{ChannelStateEvent, ChannelHandlerContext, SimpleChannelHandler}
import com.twitter.util.StateMachine

/**
 * This is the core artifact of the module, implementing the transition to the wire protocol.
 *
 * It uses a state machine behaviour because of the logic of the Zookeeper protocol.
 */
class ZookeeperEncoderDecoder extends SimpleChannelHandler with StateMachine {

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
  state = reset

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
}
