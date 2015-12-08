package com.twitter.finagle.redis

import com.twitter.finagle.Service
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.StringToChannelBuffer
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.Future
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.redis.util.CBToString
import com.twitter.util.Futures
import com.twitter.finagle.util.DefaultTimer
import com.twitter.conversions.time._
import com.twitter.util.Time
import com.twitter.util.TimerTask
import com.twitter.finagle.ServiceClosedException

object SubscribeClient {

  type MessageHandler = (ChannelBuffer, ChannelBuffer) => Unit
  type PMessageHandler = (ChannelBuffer, ChannelBuffer, ChannelBuffer) => Unit

  /**
   * Construct a sentinel client from a single host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): SubscribeClient = {
    new SubscribeClient(com.twitter.finagle.Redis.Subscribe.newService(host))
  }

  object MessageBytes {
    val SUBSCRIBE = StringToChannelBuffer("subscribe")
    val UNSUBSCRIBE = StringToChannelBuffer("unsubscribe")
    val PSUBSCRIBE = StringToChannelBuffer("psubscribe")
    val PUNSUBSCRIBE = StringToChannelBuffer("punsubscribe")
    val MESSAGE = StringToChannelBuffer("message")
    val PMESSAGE = StringToChannelBuffer("pmessage")
  }
}

class SubscribeClient(service: Service[SubscribeCommand, Unit]) extends SubscribeListener {

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
  def release() = {
    service.close()
  }

  def doRequest(cmd: SubscribeCommand) = {
    service(cmd)
  }

  import scala.collection.mutable.HashMap
  import com.twitter.finagle.redis.protocol._
  import com.twitter.finagle.redis.protocol.{ Subscribe => SubscribeCmd }

  private val msgHandlers = HashMap[ChannelBuffer, MessageHandler]()
  private val pMsgHandlers = HashMap[ChannelBuffer, PMessageHandler]()

  def onException(e: Throwable) {
    val sub = if (msgHandlers.nonEmpty) doRequest(Subscribe(msgHandlers.keys.toSeq, this)) else Future.Done
    val psub = if (pMsgHandlers.nonEmpty) doRequest(PSubscribe(pMsgHandlers.keys.toSeq, this)) else Future.Done
    Futures.join(sub, psub).onFailure {
      case sce: ServiceClosedException =>
        // Service closed, do nothing
      case ex =>
        DefaultTimer.twitter.schedule(Time.now + 1.second)(onException(e))
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
          case MessageBytes.PSUBSCRIBE   =>
          case MessageBytes.PUNSUBSCRIBE =>
          case MessageBytes.SUBSCRIBE    =>
          case MessageBytes.UNSUBSCRIBE  =>
          case _ =>
            throw new IllegalArgumentException()
        }
      case _ =>
        throw new IllegalArgumentException()
    }
  }
}
