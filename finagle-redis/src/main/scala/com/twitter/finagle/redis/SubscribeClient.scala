package com.twitter.finagle.redis

import com.twitter.finagle.{Service, ServiceClosedException, ServiceFactory}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.protocol.{ Subscribe => SubscribeCmd }
import com.twitter.finagle.redis.util._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.conversions.time._
import com.twitter.io.Charsets
import com.twitter.util._
import java.util.concurrent.ConcurrentHashMap
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.JavaConverters._

object SubscribeClient {

  type MessageHandler = (ChannelBuffer, ChannelBuffer) => Unit
  type PMessageHandler = (ChannelBuffer, ChannelBuffer, ChannelBuffer) => Unit

  /**
   * Construct a SubscribeClient from a single host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): SubscribeClient = {
    new SubscribeClient(com.twitter.finagle.Redis.Subscribe.newService(host))
  }

  private object MessageBytes {
    val SUBSCRIBE = StringToChannelBuffer("subscribe")
    val UNSUBSCRIBE = StringToChannelBuffer("unsubscribe")
    val PSUBSCRIBE = StringToChannelBuffer("psubscribe")
    val PUNSUBSCRIBE = StringToChannelBuffer("punsubscribe")
    val MESSAGE = StringToChannelBuffer("message")
    val PMESSAGE = StringToChannelBuffer("pmessage")
  }
}

trait SubscribeHandler {
  def onException(ex: Throwable): Unit
  def onMessage(msg: Reply): Unit
}

/**
 * SubscribeClient is used to (un)subscribe messages from redis' PUB/SUB subsystem.
 * Once a client enters PUB/SUB state by subscribing to some channel/pattern, it
 * should not issue any other commands, except the (un)subscribe commands, until it
 * exits from the PUB/SUB state, by unsubscribing from all the channels and patterns.
 * For this reason, we put the (un)subscribe commands here, separately from the other
 * ordinary commands.
 */
class SubscribeClient(
  service: Service[SubscribeCommand, Unit],
  timer: Timer = DefaultTimer.twitter)
    extends SubscribeHandler with Closable {

  import SubscribeClient._

  def subscribe(channels: Seq[ChannelBuffer])(handler: MessageHandler): Future[Unit] =
    doRequest(Subscribe(channels, this))
      .onSuccess(_ => channels.foreach(msgHandlers.put(_, handler)))

  def pSubscribe(patterns: Seq[ChannelBuffer])(handler: PMessageHandler): Future[Unit] =
    doRequest(PSubscribe(patterns, this))
      .onSuccess(_ => patterns.foreach(pMsgHandlers.put(_, handler)))

  def unsubscribe(channels: Seq[ChannelBuffer]): Future[Unit] =
    doRequest(Unsubscribe(channels, this))
      .onSuccess(_ => channels.foreach(msgHandlers.remove(_)))

  def pUnsubscribe(patterns: Seq[ChannelBuffer]): Future[Unit] =
    doRequest(PUnsubscribe(patterns, this))
      .onSuccess(_ => patterns.foreach(pMsgHandlers.remove(_)))

  /**
   * Releases underlying service object
   */
  def close(deadline: Time): Future[Unit] = {
    service.close(deadline)
  }

  private[this] def doRequest(cmd: SubscribeCommand) = {
    service(cmd)
  }

  private[this] val msgHandlers = new ConcurrentHashMap[ChannelBuffer, MessageHandler]().asScala
  private[this] val pMsgHandlers = new ConcurrentHashMap[ChannelBuffer, PMessageHandler]().asScala

  def onException(e: Throwable): Unit = {
    val sub = if (msgHandlers.nonEmpty) doRequest(Subscribe(msgHandlers.keys.toSeq, this)) else Future.Done
    val psub = if (pMsgHandlers.nonEmpty) doRequest(PSubscribe(pMsgHandlers.keys.toSeq, this)) else Future.Done
    Futures.join(sub, psub).onFailure {
      case sce: ServiceClosedException =>
        // Service closed, do nothing
      case ex =>
        timer.schedule(Time.now + 1.second)(onException(e))
    }
  }

  def onMessage(message: Reply): Unit = {
    message match {
      case MBulkReply(BulkReply(MessageBytes.MESSAGE) :: BulkReply(channel) :: BulkReply(message) :: Nil) =>
        msgHandlers.get(channel).map(_(channel, message))
      case MBulkReply(BulkReply(MessageBytes.PMESSAGE) :: BulkReply(pattern) :: BulkReply(channel) :: BulkReply(message) :: Nil) =>
        pMsgHandlers.get(pattern).map(_(pattern, channel, message))
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
            throw new IllegalArgumentException(s"Unsupported message type: ${tpe.toString(Charsets.Utf8)}")
        }
      case _ =>
        throw new IllegalArgumentException(s"Unexpected reply type: ${message.getClass.getSimpleName}")
    }
  }
}
