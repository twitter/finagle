package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.io.Buf
import com.twitter.util.Future
import java.lang.{Long => JLong}

private[redis] trait StreamCommands { self: BaseClient =>
  /**
   * Pushes the fields and values in `fv` onto the stream at `key`. If `id` is None
   * an ID will be generated for the message, otherwise the passed `id` will be used
   *
   * @param key
   * @param id
   * @param fv
   * @return The ID for the message
   * @see https://redis.io/commands/xadd
   */
  def xAdd(key: Buf, id: Option[Buf], fv: Map[Buf, Buf]): Future[Buf] =
    doRequest(XAdd(key, id, fv)) {
      case BulkReply(message) => Future.value(message)
    }

  /**
   * Trims the stream at `key` to a particular number of items indicated by `size`. If `exact`
   * is false, Redis will trim to approximately the indicated size (which is more efficient)
   *
   * @param key
   * @param size
   * @param exact
   * @return The number of items removed from the stream
   * @see https://redis.io/commands/xtrim
   */
  def xTrim(key: Buf, size: Long, exact: Boolean): Future[JLong] =
    doRequest(XTrim(key, size, exact)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes specified entries from the stream at `key`
   *
   * @param key
   * @param ids
   * @return The number of entries deleted
   * @see https://redis.io/commands/xdel
   */
  def xDel(key: Buf, ids: Seq[Buf]): Future[JLong] =
    doRequest(XDel(key, ids)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Returns entries from the stream at `key` in the range of `start` to `end`. If `count` is specified no more than `count`
   * items will be returned
   *
   * @param key
   * @param start
   * @param end
   * @param count
   * @return A sequence of entries
   * @see https://redis.io/commands/xrange
   */
  def xRange(key: Buf, start: Buf, end: Buf, count: Option[Long]): Future[Seq[(Buf, Seq[(Buf, Buf)])]] =
    doRequest(XRange(key, start, end, count)) {
      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
      case MBulkReply(replies) => Future.value(handleRangeReply(replies))
    }

  /**
   * Like XRANGE, but returns entries in reverse order
   *
   * @param key
   * @param start
   * @param end
   * @param count
   * @return A sequence of entries
   * @see https://redis.io/commands/xrevrange
   */
  def xRevRange(key: Buf, start: Buf, end: Buf, count: Option[Long]): Future[Seq[(Buf, Seq[(Buf, Buf)])]] =
    doRequest(XRevRange(key, start, end, count)) {
      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
      case MBulkReply(replies) => Future.value(handleRangeReply(replies))
    }

  /**
   *
   * @param key
   * @return
   * @see https://redis.io/commands/xlen
   */
  def xLen(key: Buf): Future[JLong] =
    doRequest(XLen(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Reads entries from one or many streams
   *
   * @param count The maximum amount of messages to return per-stream
   * @param blockMs If no messages are available on the given streams, block for this many millis to awai entries
   * @param keys
   * @param ids
   * @return
   * @see https://redis.io/commands/xread
   */
  def xRead(count: Option[Long], blockMs: Option[Long], keys: Seq[Buf], ids: Seq[Buf]): Future[Seq[(Buf, Seq[(Buf, Seq[(Buf, Buf)])])]] =
    doRequest(XRead(count, blockMs, keys, ids)) {
      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
      case MBulkReply(replies) => Future.value(handleReadReply(replies))
    }

  /**
   * Reads from one or many streams within the context `group` for `consumer`.
   *
   * @param group
   * @param consumer
   * @param count The maximum amount of messages to return per-stream
   * @param blockMs If no messages are available on the given streams, block for this many millis to awai entries
   * @param keys
   * @param ids
   * @return
   * @see https://redis.io/commands/xreadgroup
   */
  def xReadGroup(group: Buf, consumer: Buf, count: Option[Long], blockMs: Option[Long], keys: Seq[Buf], ids: Seq[Buf]): Future[Seq[(Buf, Seq[(Buf, Seq[(Buf, Buf)])])]] =
    doRequest(XReadGroup(group, consumer, count, blockMs, keys, ids)) {
      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
      case MBulkReply(replies) => Future.value(handleReadReply(replies))
    }

  /**
   * Creates a new consumer group named `groupName` for stream `key` starting at entry `id`
   *
   * @param key
   * @param groupName
   * @param id
   * @return
   * @see https://redis.io/commands/xgroup
   */
  def xGroupCreate(key: Buf, groupName: Buf, id: Buf): Future[Unit] =
    doRequest(XGroupCreate(key, groupName, id)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   *
   * @param key
   * @param id
   * @return
   * @see https://redis.io/commands/xgroup
   */
  def xGroupSetId(key: Buf, id: Buf): Future[Unit] =
    doRequest(XGroupSetId(key, id)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   *
   * @param key
   * @param groupName
   * @return
   * @see https://redis.io/commands/xgroup
   */
  def xGroupDestroy(key: Buf, groupName: Buf): Future[Unit] =
    doRequest(XGroupDestroy(key, groupName)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   *
   * @param key
   * @param groupName
   * @param consumerName
   * @return
   * @see https://redis.io/commands/xgroup
   */
  def xGroupDelConsumer(key: Buf, groupName: Buf, consumerName: Buf): Future[Unit] =
    doRequest(XGroupDelConsumer(key, groupName, consumerName)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Removes entries from the pending list with `ids` from the stream at `key` within the context of `group`
   *
   * @param key
   * @param group
   * @param ids
   * @return The number of messages acked
   * @see https://redis.io/commands/xack
   */
  def xAck(key: Buf, group: Buf, ids: Seq[Buf]): Future[JLong] =
    doRequest(XAck(key, group, ids)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Retrieves all pending messages within the stream at `key` for `group`
   * @param key
   * @param group
   * @return
   * @see https://redis.io/commands/xpending
   */
  def xPending(key: Buf, group: Buf): Future[XPendingAllReply] =
    doRequest(XPending(key, group)) {
      case MBulkReply(IntegerReply(count) :: BulkReply(start) :: BulkReply(end) :: MBulkReply(consumerCounts) :: Nil) =>
        val consumerAndCount = consumerCounts.collect {
          case MBulkReply(BulkReply(consumer) :: BulkReply(consumerCount) :: Nil) => consumer -> consumerCount
        }

        Future.value(XPendingAllReply(count, Some(start), Some(end), consumerAndCount))

      case MBulkReply(IntegerReply(count) :: EmptyBulkReply :: EmptyBulkReply :: NilMBulkReply :: Nil) =>
        Future.value(XPendingAllReply(count, None, None, Seq.empty))
    }

  /**
   * Returns all pending messages within a range, optionally for a specific consumer
   *
   * @param key
   * @param group
   * @param start
   * @param end
   * @param count
   * @param consumer
   * @return
   * @see https://redis.io/commands/xpending
   */
  def xPending(key: Buf, group: Buf, start: Buf, end: Buf, count: Long, consumer: Option[Buf]): Future[Seq[XPendingRangeReply]] =
    doRequest(XPendingRange(key, group, start, end, count, consumer)) {
      case MBulkReply(pending) =>
        Future.value {
          pending.collect {
            case MBulkReply(BulkReply(id) :: BulkReply(consumerId) :: IntegerReply(ms) :: IntegerReply(deliveries) :: Nil) =>
              XPendingRangeReply(id, consumerId, ms, deliveries)
          }
        }

      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
    }

  /**
   * Changes the ownership of pending entries to `consumer` with `ids` within the stream at `key` within the context
   * of `group`.
   *
   * @param key
   * @param group
   * @param consumer
   * @param minIdleTime
   * @param ids
   * @param idle
   * @param retryCount
   * @param force
   * @param justId
   * @return If justId is true, a sequence of claimed IDs, otherwise claimed IDs and the contents of the entries
   * @see https://redis.io/commands/xclaim
   */
  def xClaim(
    key: Buf,
    group: Buf,
    consumer: Buf,
    minIdleTime: Long,
    ids: Seq[Buf],
    idle: Option[XClaimMillisOrUnixTs],
    retryCount: Option[Long],
    force: Boolean,
    justId: Boolean
  ): Future[Seq[(Buf, Seq[(Buf, Buf)])]] = {
    doRequest(XClaim(key, group, consumer, minIdleTime, ids, idle, retryCount, force, justId)) {
      case MBulkReply(replies) if justId => Future.value(handleRangeReply(replies))
      case MBulkReply(replies) => Future.value(ReplyFormat.toBuf(replies).map(_ -> Seq.empty))
      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
    }
  }

  private[redis] def handleReadReply(replies: List[Reply]) = {
    replies.collect {
      case MBulkReply(BulkReply(stream) :: MBulkReply(entries) :: Nil) => stream -> handleRangeReply(entries)
    }
  }

  private[redis] def handleRangeReply(replies: List[Reply]) = {
    replies.collect {
      case MBulkReply(BulkReply(id) :: MBulkReply(fields) :: Nil) => id -> returnPairs(ReplyFormat.toBuf(fields))
    }
  }
}

case class XPendingAllReply(
  count: JLong,
  start: Option[Buf],
  end: Option[Buf],
  pendingByConsumer: Seq[(Buf, Buf)]
)

case class XPendingRangeReply(
  id: Buf,
  consumer: Buf,
  millisSinceLastDeliver: JLong,
  numDeliveries: JLong
)
