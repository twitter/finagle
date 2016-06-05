package com.twitter.finagle.redis

import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.util.{Future, Time}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

trait PubSubs { self: BaseClient =>

  def pubSubChannels(pattern: Option[ChannelBuffer] = None): Future[Seq[ChannelBuffer]] =
    doRequest(PubSubChannels(pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply() => Future.value(Nil)
    }

  def pubSubNumSub(channels: Seq[ChannelBuffer]): Future[Map[ChannelBuffer, Long]] =
    doRequest(PubSubNumSub(channels)) {
      case MBulkReply(messages) => Future.value(
        messages.grouped(2).toSeq.map {
          case List(BulkReply(channel), IntegerReply(num)) =>
            (ChannelBufferBuf.Owned.extract(channel), num)
        }.toMap)
      case EmptyMBulkReply() => Future.value(Map.empty)
    }

  def pubSubNumPat(): Future[Long] =
    doRequest(PubSubNumPat()) {
      case IntegerReply(num) => Future.value(num)
    }

  /**
   * Publish a message to the specified channel
   */
  def publish(channel: ChannelBuffer, message: ChannelBuffer): Future[Long] =
    doRequest(Publish(channel, message)) {
      case IntegerReply(count) => Future.value(count)
    }
}
