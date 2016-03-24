package com.twitter.finagle.redis.exp

import com.twitter.conversions.time._
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.{Service, ServiceClosedException, ServiceFactory}
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.{BufToString, StringToChannelBuffer}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Charsets
import com.twitter.logging.Logger
import com.twitter.util.{Future, Futures, NonFatal, Throw, Timer}
import java.util.concurrent.ConcurrentHashMap
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.JavaConverters._

object SubscribeCommands {

  object MessageBytes {
    val SUBSCRIBE = StringToChannelBuffer("subscribe")
    val UNSUBSCRIBE = StringToChannelBuffer("unsubscribe")
    val PSUBSCRIBE = StringToChannelBuffer("psubscribe")
    val PUNSUBSCRIBE = StringToChannelBuffer("punsubscribe")
    val MESSAGE = StringToChannelBuffer("message")
    val PMESSAGE = StringToChannelBuffer("pmessage")
  }
}

sealed trait SubscribeHandler {
  def onSuccess(channel: ChannelBuffer, node: Service[SubscribeCommand, Reply]): Unit
  def onException(node: Service[SubscribeCommand, Reply], ex: Throwable): Unit
  def onMessage(message: Reply): Unit
}

sealed trait SubscriptionType[Message] {
  type MessageHandler = Message => Unit
  def subscribeCommand(channel: ChannelBuffer, handler: SubscribeHandler): SubscribeCommand
  def unsubscribeCommand(channel: ChannelBuffer, handler: SubscribeHandler): SubscribeCommand
}

case object Channel extends SubscriptionType[(ChannelBuffer, ChannelBuffer)] {
  def subscribeCommand(channel: ChannelBuffer, handler: SubscribeHandler) = {
    Subscribe(Seq(channel), handler)
  }
  def unsubscribeCommand(channel: ChannelBuffer, handler: SubscribeHandler) = {
    Unsubscribe(Seq(channel), handler)
  }
}

case object Pattern extends SubscriptionType[(ChannelBuffer, ChannelBuffer, ChannelBuffer)] {
  def subscribeCommand(channel: ChannelBuffer, handler: SubscribeHandler) = {
    PSubscribe(Seq(channel), handler)
  }
  def unsubscribeCommand(channel: ChannelBuffer, handler: SubscribeHandler) = {
    PUnsubscribe(Seq(channel), handler)
  }
}

/**
 * SubscribeClient is used to (un)subscribe messages from redis' PUB/SUB subsystem.
 * Once a client enters PUB/SUB state by subscribing to some channel/pattern, it
 * should not issue any other commands, except the (un)subscribe commands, until it
 * exits from the PUB/SUB state, by unsubscribing from all the channels and patterns.
 * For this reason, we put the (un)subscribe commands here, separately from the other
 * ordinary commands.
 */
trait SubscribeCommands {

  self: Client =>

  private[redis] val timer: Timer

  import SubscribeCommands._

  private[this] val log = Logger(getClass)

  private[this] val subManager = new SubscriptionManager(Channel, timer)

  private[this] val pSubManager = new SubscriptionManager(Pattern, timer)

  /**
   * Subscribe to channels. Messages received from the subscribed channels will be processed by
   * the handler.
   *
   * A channel will be subscribed to only once. Subscribing to an already subscribed channel will
   * be ignored. Although a Seq is passed in as argument, the channels are subscribed to one by
   * one, with individual commands, and when the client is connected to multiple server nodes,
   * it is not guaranteed that they are subscribed to from the same node.
   *
   * When the Future returned by this method is completed, it is guaranteed that an attempt is
   * made, to send a subscribe command for each of the channels that is not subscribed to yet.
   * And the failed subscriptions are returned as a map from the failed channel to the exception
   * object. Subscriptions will be managed by the SubscriptionManager, even if it failed at the
   * first attempt. In that case, subsequent attempts will be made regularly until the channel is
   * subscribed to successfully, or the subscription is cancelled by calling the unsubscribed
   * method.
   */
  def subscribe(channels: Seq[ChannelBuffer])(handler: subManager.typ.MessageHandler)
    : Future[Map[ChannelBuffer, Throwable]] = {
    val notSubscribed = subManager.uniquify(channels, handler)
    val subscriptions = notSubscribed.map(subManager.subscribe(_))
    Futures.collectToTry(subscriptions.asJava)
      .map(_.asScala.zip(notSubscribed).collect {
        case (Throw(ex), channel) => (channel, ex)
      }.toMap)
  }

  /**
   * Subscribe to patterns. Messages received from the subscribed patterns will be processed by
   * the handler.
   *
   * A pattern will be subscribed to only once. Subscribing to an already subscribed pattern will
   * be ignored. Although a Seq is passed in as argument, the patterns are subscribed to one by
   * one, with individual commands, and when the client is connected to multiple server nodes,
   * it is not guaranteed that they are subscribed to from the same node.
   *
   * When the Future returned by this method is completed, it is guaranteed that an attempt is
   * made, to send a pSubscribe command for each of the patterns that is not subscribed to yet.
   * And the failed subscriptions are returned as a map from the failed channel to the exception
   * object. Subscriptions will be managed by the SubscriptionManager, even if it failed at the
   * first attempt. In that case, subsequent attempts will be made regularly until the pattern is
   * subscribed to successfully, or the subscription is cancelled by calling the pUnsubscribed
   * method.
   */
  def pSubscribe(patterns: Seq[ChannelBuffer])(handler: pSubManager.typ.MessageHandler)
    : Future[Map[ChannelBuffer, Throwable]] = {
    val notSubscribed = pSubManager.uniquify(patterns, handler)
    val subscriptions = notSubscribed.map(pSubManager.subscribe(_))
    Futures.collectToTry(subscriptions.asJava)
      .map(_.asScala.zip(notSubscribed).collect {
        case (Throw(ex), pattern) => (pattern, ex)
      }.toMap)
  }

  /**
   * Unsubscribe from channels. The subscriptions to the specified channels are removed from the
   * SubscriptionManager. An unsubscribe command is sent for each of the succeeded
   * subscriptions, and the failed ones are returned as a Future of map from the channel to the
   * exception object.
   */
  def unsubscribe(channels: Seq[ChannelBuffer]): Future[Map[ChannelBuffer, Throwable]] = {
    Futures.collectToTry(channels.map(subManager.unsubscribe(_)).asJava)
      .map(_.asScala.zip(channels).collect {
        case (Throw(ex), channel) => (channel, ex)
      }.toMap)
  }

  /**
   * Unsubscribe from patterns. The subscriptions to the specified patterns are removed from the
   * SubscriptionManager. An unsubscribe command is sent for each of the succeeded
   * subscriptions, and the failed ones are returned as a Future of map from the pattern to the
   * exception object.
   */
  def pUnsubscribe(patterns: Seq[ChannelBuffer]): Future[Map[ChannelBuffer, Throwable]] = {
    Futures.collectToTry(patterns.map(pSubManager.unsubscribe(_)).asJava)
      .map(_.asScala.zip(patterns).collect {
        case (Throw(ex), pattern) => (pattern, ex)
      }.toMap)
  }

  private[this] def doRequest(cmd: SubscribeCommand) = {
    RedisPool.forSubscription(factory)(cmd)
  }

  private class SubscriptionManager[Message](val typ: SubscriptionType[Message], timer: Timer)
      extends SubscribeHandler {

    sealed trait State
    case object Pending extends State
    case class Subscribed(node: Service[SubscribeCommand, Reply]) extends State

    private case class Subscription(handler: typ.MessageHandler, state: State)

    private[this] val subscriptions = new ConcurrentHashMap[ChannelBuffer, Subscription]().asScala

    def uniquify(channels: Seq[ChannelBuffer], handler: typ.MessageHandler): Seq[ChannelBuffer] = {
      channels.filter { channel =>
        subscriptions.putIfAbsent(channel, Subscription(handler, Pending)).isEmpty
      }
    }

    def onSuccess(channel: ChannelBuffer, node: Service[SubscribeCommand, Reply]): Unit = {
      subscriptions.get(channel) match {
        case Some(subscription) =>
          subscriptions.put(channel, subscription.copy(state = Subscribed(node)))
        case None =>
          // In case that some retrying attempt is made successfully after the channel is
          // unsubscribed.
          node(typ.unsubscribeCommand(channel, this))
      }
    }

    def onMessage(message: Reply): Unit = {
      message match {
        case MBulkReply(BulkReply(MessageBytes.MESSAGE) :: BulkReply(channel) :: BulkReply(message) :: Nil) =>
          subManager.handleMessage(
            ChannelBufferBuf.Owned.extract(channel),
            (ChannelBufferBuf.Owned.extract(channel), ChannelBufferBuf.Owned.extract(message)))
        case MBulkReply(BulkReply(MessageBytes.PMESSAGE) :: BulkReply(pattern) :: BulkReply(channel) :: BulkReply(message) :: Nil) =>
          pSubManager.handleMessage(
            ChannelBufferBuf.Owned.extract(pattern),
            (ChannelBufferBuf.Owned.extract(pattern),
              ChannelBufferBuf.Owned.extract(channel),
              ChannelBufferBuf.Owned.extract(message)
            )
          )
        case MBulkReply(BulkReply(tpe) :: BulkReply(channel) :: IntegerReply(count) :: Nil) =>
          tpe match {
            case MessageBytes.PSUBSCRIBE
              | MessageBytes.PUNSUBSCRIBE
              | MessageBytes.SUBSCRIBE
              | MessageBytes.UNSUBSCRIBE =>
              // The acknowledgement messages may come after a subscribed channel message.
              // So we register the message handler right after the subscription request
              // is sent. Nothing is going to be done here. We match against them just to
              // detect something unexpected.
            case _ =>
              throw new IllegalArgumentException(s"Unsupported message type: ${BufToString(tpe)}")
          }
        case _ =>
          throw new IllegalArgumentException(s"Unexpected reply type: ${message.getClass.getSimpleName}")
      }
    }

    def handleMessage(channel: ChannelBuffer, message: Message): Unit = {
      try {
        subscriptions.get(channel).map(_.handler(message))
      } catch {
        case NonFatal(ex) =>
          log.error(ex, "Failed to handle a message: %s", message)
      }
    }

    def onException(node: Service[SubscribeCommand, Reply], ex: Throwable): Unit = {
      subManager._onException(node, ex)
      pSubManager._onException(node, ex)
    }

    private def _onException(node: Service[SubscribeCommand, Reply], ex: Throwable): Unit = {
      // Take a snapshot of the managed subscriptions, and change the state.
      subscriptions.toList.collect {
        case (channel, subscription) =>
          subscriptions.put(channel, subscription.copy(state = Pending))
          subscribe(channel)
      }
    }

    private def retry(channel: ChannelBuffer): Future[Reply] =
      doRequest(typ.subscribeCommand(channel, this))

    def subscribe(channel: ChannelBuffer): Future[Reply] = {
      // It is possible that the channel is unsubscribed, so we always check it before making
      // another attempt.
      if (subscriptions.get(channel).isEmpty) Future.value(NoReply)
      else retry(channel).onFailure {
        case sce: ServiceClosedException =>
          subscriptions.remove(channel)
        case _ =>
          timer.doLater(1.second)(subscribe(channel))
      }
    }

    def unsubscribe(channel: ChannelBuffer): Future[Reply] = {
      subscriptions.remove(channel) match {
        case Some(Subscription(_, Subscribed(node))) =>
          node(typ.unsubscribeCommand(channel, this))
        case _ =>
          Future.value(NoReply)
      }
    }
  }
}
