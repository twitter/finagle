package com.finagle.zookeeper

import org.jboss.netty.channel.SimpleChannelHandler
import com.twitter.util.StateMachine


sealed trait State
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
 * This is the core artifact of the module, implementinc the transition to the wire protocol.
 *
 * It encorporates state machine behaviour because of the logic of the Zookeeper protocol.
 */
class ZookeeperEncoderDecoder extends SimpleChannelHandler with StateMachine {

}
