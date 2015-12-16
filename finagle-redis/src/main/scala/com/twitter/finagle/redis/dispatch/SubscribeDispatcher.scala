package com.twitter.finagle.redis.dispatch

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.redis.SubscribeClient._
import com.twitter.finagle.redis.SubscribeHandler
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.transport.Transport
import com.twitter.io.Charsets
import com.twitter.util.{Future, NonFatal, Promise}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.JavaConverters._

class SubscribeDispatcher(trans: Transport[SubscribeCommand, Reply])
    extends GenSerialClientDispatcher[SubscribeCommand, Unit, SubscribeCommand, Reply](trans) {

  private val handler = new AtomicReference[SubscribeHandler]

  loop()

  private[this] def handleMessage(message: Reply) {
    message match {
      case MBulkReply(BulkReply(MessageBytes.MESSAGE) :: BulkReply(channel) :: BulkReply(message) :: Nil) =>
        handler.get().onMessage(channel, message)
      case MBulkReply(BulkReply(MessageBytes.PMESSAGE) :: BulkReply(pattern) :: BulkReply(channel) :: BulkReply(message) :: Nil) =>
        handler.get().onPMessage(pattern, channel, message)
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

  private[this] def loop(): Unit =
    trans.read().onSuccess { reply =>
      handleMessage(reply)
      loop()
    }.onFailure {
      case NonFatal(ex) =>
        Option(handler.get()).foreach(_.onException(this, ex))
    }

  protected def dispatch(req: SubscribeCommand, p: Promise[Unit]): Future[Unit] = {
    trans.write(req)
      .onSuccess { _ => p.setDone() }
      .onFailure { case NonFatal(ex) => p.setException(ex) }
  }

  override def apply(req: SubscribeCommand): Future[Unit] = {
    handler.compareAndSet(null, req.handler)
    super.apply(req).masked.onSuccess { _ =>
      req match {
        case Subscribe(channels, handler) =>
          channels.foreach(handler.onSuccess(_, this))
        case PSubscribe(patterns, handler) =>
          patterns.foreach(handler.onSuccess(_, this))
        case _ =>
      }
    }
  }
}
