package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.io.Buf
import com.twitter.util.Future

private[redis] trait PubSubCommands { self: BaseClient =>

  /**
   * Lists the currently active channels. An active channel is a Pub/Sub
   * channel with one or more subscribers (not including clients subscribed to patterns).
   */
  def pubSubChannels(pattern: Option[Buf] = None): Future[Seq[Buf]] =
    doRequest(PubSubChannels(pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toBuf(messages))
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Returns the number of subscribers (not counting clients subscribed
   * to patterns) for the specified `channels`.
   */
  def pubSubNumSub(channels: Seq[Buf]): Future[Map[Buf, Long]] =
    doRequest(PubSubNumSub(channels)) {
      case MBulkReply(messages) =>
        Future.value(
          messages
            .grouped(2)
            .map({
              case List(BulkReply(channel), IntegerReply(num)) => channel -> num
            })
            .toMap
        )
      case EmptyMBulkReply => Future.value(Map.empty)
    }

  /**
   * Returns the number of subscriptions to patterns (that are performed
   * using the PSUBSCRIBE command).
   */
  def pubSubNumPat(): Future[Long] =
    doRequest(PubSubNumPat) {
      case IntegerReply(num) => Future.value(num)
    }

  /**
   * Publishes a `message` to the specified `channel`.
   */
  def publish(channel: Buf, message: Buf): Future[Long] =
    doRequest(Publish(channel, message)) {
      case IntegerReply(count) => Future.value(count)
    }
}
