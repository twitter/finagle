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
   * @param key The key of the stream to add the key-pairs to
   * @param id The ID to use for the new record. Pass "*" to make Redis generate a unique ID for the record
   * @param fv A Map of key-value pairs representing the record
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
   * @param key The key of the stream to trim
   * @param size The size to trim the stream to
   * @param exact If true, trims the stream exactly. Otherwise, Redis internally approximates
   *              the size and optionally trims based on the approximation
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
   * @param key The key of the stream to delete records from
   * @param ids The keys to delete from the stream
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
   * @param key The key of the stream to retrieve records from
   * @param start The inclusive, minimum stream record ID to start retrieving records from
   * @param end The inclusive, maximum stream record ID to start retrieving records from
   * @param count The maximum number of entries to retrieve from the stream
   * @return A sequence of entries
   * @see https://redis.io/commands/xrange
   */
  def xRange(key: Buf, start: Buf, end: Buf, count: Option[Long]): Future[Seq[StreamEntryReply]] =
    doRequest(XRange(key, start, end, count)) {
      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
      case MBulkReply(replies) => Future.value(handleRangeReply(replies))
    }

  /**
   * Like XRANGE, but returns entries in reverse order
   *
   * @param key The key of the stream to retrieve records from
   * @param start The inclusive, maximum stream record ID to start retrieving records from
   * @param end The inclusive, minimum stream record ID to start retrieving records from
   * @param count The maximum number of entries to retrieve from the stream
   * @return A sequence of entries
   * @see https://redis.io/commands/xrevrange
   */
  def xRevRange(
    key: Buf,
    start: Buf,
    end: Buf,
    count: Option[Long]
  ): Future[Seq[StreamEntryReply]] =
    doRequest(XRevRange(key, start, end, count)) {
      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
      case MBulkReply(replies) => Future.value(handleRangeReply(replies))
    }

  /**
   * Retrieve the length of a stream
   *
   * @param key The key of the stream to retrieve records from
   * @return The number of entries in the stream
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
   * @param keys The keys of the streams to retrieve records from
   * @param ids The last ID retrieved for each stream. On the first call to XREAD, "$" should be passed as the first ID for each stream
   * @return A sequence of streams each containing a sequence of read entries
   * @see https://redis.io/commands/xread
   */
  def xRead(
    count: Option[Long],
    blockMs: Option[Long],
    keys: Seq[Buf],
    ids: Seq[Buf]
  ): Future[Seq[XReadStreamReply]] =
    doRequest(XRead(count, blockMs, keys, ids)) {
      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
      case MBulkReply(replies) => Future.value(handleReadReply(replies))
    }

  /**
   * Reads from one or many streams within the context `group` for `consumer`.
   *
   * @param group The group identifier that the consumer belongs to
   * @param consumer The consumer ID to consume on behalf of
   * @param count The maximum amount of messages to return per-stream
   * @param blockMs If no messages are available on the given streams, block for this many millis to awai entries
   * @param keys The keys of the streams to retrieve records from
   * @param ids The last ID retrieved for each stream. On the first call to XREAD, "$" should be passed as the first ID for each stream
   * @return A sequence of streams each containing a sequence of read entries
   * @see https://redis.io/commands/xreadgroup
   */
  def xReadGroup(
    group: Buf,
    consumer: Buf,
    count: Option[Long],
    blockMs: Option[Long],
    keys: Seq[Buf],
    ids: Seq[Buf]
  ): Future[Seq[XReadStreamReply]] =
    doRequest(XReadGroup(group, consumer, count, blockMs, keys, ids)) {
      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
      case MBulkReply(replies) => Future.value(handleReadReply(replies))
    }

  /**
   * Creates a new consumer group named `groupName` for stream `key` starting at entry `id`
   *
   * @param key The stream key that the group will consume
   * @param groupName The name of the new consumer group
   * @param id The ID of the last delivered item from the stream, i.e. where the group begins consuming from
   * @return Successful if the group is successfully created
   * @see https://redis.io/commands/xgroup
   */
  def xGroupCreate(key: Buf, groupName: Buf, id: Buf): Future[Unit] =
    doRequest(XGroupCreate(key, groupName, id)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Sets the ID of the next item to deliver to consumers within a group
   *
   * @param key The stream key the group consumes
   * @param id The new ID that the consumer groups begins consuming from
   * @return Successful if the ID is successfully changed
   * @see https://redis.io/commands/xgroup
   */
  def xGroupSetId(key: Buf, id: Buf): Future[Unit] =
    doRequest(XGroupSetId(key, id)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Deletes a consumer group
   *
   * @param key The stream the group consumes
   * @param groupName The group to delete
   * @return Successful if the group was deleted
   * @see https://redis.io/commands/xgroup
   */
  def xGroupDestroy(key: Buf, groupName: Buf): Future[Unit] =
    doRequest(XGroupDestroy(key, groupName)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Deletes a specific consumer within a consumer group
   *
   * @param key The stream the group consumes
   * @param groupName The group the consumer belongs to
   * @param consumerName The consumer to delete
   * @return Successful if the group was deleted
   * @see https://redis.io/commands/xgroup
   */
  def xGroupDelConsumer(key: Buf, groupName: Buf, consumerName: Buf): Future[Unit] =
    doRequest(XGroupDelConsumer(key, groupName, consumerName)) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Removes entries from the pending list with `ids` from the stream at `key` within the context of `group`
   *
   * @param key The stream the entry belongs to
   * @param group The consumer group to ACK the entries on behalf of
   * @param ids The entry IDs to ACK
   * @return The number of messages acked
   * @see https://redis.io/commands/xack
   */
  def xAck(key: Buf, group: Buf, ids: Seq[Buf]): Future[JLong] =
    doRequest(XAck(key, group, ids)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Retrieves all pending messages within the stream at `key` for `group`
   *
   * @param key The stream the group consumes
   * @param group The consumer group consuming the stream
   * @return A listing of pending entries for the particular consumer group
   * @see https://redis.io/commands/xpending
   */
  def xPending(key: Buf, group: Buf): Future[XPendingAllReply] =
    doRequest(XPending(key, group)) {
      case MBulkReply(
            IntegerReply(count) :: BulkReply(start) :: BulkReply(end) :: MBulkReply(
              consumerCounts) :: Nil
          ) =>
        val consumerAndCount = consumerCounts.collect {
          case MBulkReply(BulkReply(consumer) :: BulkReply(consumerCount) :: Nil) =>
            consumer -> consumerCount
        }

        Future.value(XPendingAllReply(count, Some(start), Some(end), consumerAndCount))

      case MBulkReply(
            IntegerReply(count) :: EmptyBulkReply :: EmptyBulkReply :: NilMBulkReply :: Nil
          ) =>
        Future.value(XPendingAllReply(count, None, None, Seq.empty))
    }

  /**
   * Returns all pending messages within a range, optionally for a specific consumer. Includes additional details about the
   * pending messages relatieve to XPENDING called without an ID-range.
   *
   * @param key The stream the group consumes
   * @param group The consumer group consuming the stream
   * @param start The inclusive, minimum stream record ID to start retrieving records from
   * @param end The inclusive, maximum stream record ID to start retrieving records from
   * @param count The maximum number of pending entries to retrieve
   * @param consumer If specified, retrieves only pending entries for a given consumer
   * @return A sequence of pending entries which include details about the pending messages
   * @see https://redis.io/commands/xpending
   */
  def xPending(
    key: Buf,
    group: Buf,
    start: Buf,
    end: Buf,
    count: Long,
    consumer: Option[Buf]
  ): Future[Seq[XPendingRangeReply]] =
    doRequest(XPendingRange(key, group, start, end, count, consumer)) {
      case MBulkReply(pending) =>
        Future.value {
          pending.collect {
            case MBulkReply(
                  BulkReply(id) :: BulkReply(consumerId) :: IntegerReply(ms) :: IntegerReply(
                    deliveries
                  ) :: Nil
                ) =>
              XPendingRangeReply(id, consumerId, ms, deliveries)
          }
        }

      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
    }

  /**
   * Changes the ownership of pending entries to `consumer` with `ids` within the stream at `key` within the context
   * of `group`.
   *
   * @param key The stream key the consumer group is consuming
   * @param group The consumer group the consumer belongs to
   * @param consumer The consumer that is claiming ownership of the entry
   * @param minIdleTime The minimum idle time an entry must have to be claimed
   * @param ids The entry IDs to "claim"
   * @param idle Sets the idle time of the pending messages. Can be specified in milliseconds of millisecond-unix-time
   * @param retryCount Sets the retry counter for the entries
   * @param force If true, creates entries in the pending entry list for IDs that did not already exist in the list
   * @param justId If true, returns just the pending message IDs in the response
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
  ): Future[Seq[StreamEntryReply]] = {
    doRequest(XClaim(key, group, consumer, minIdleTime, ids, idle, retryCount, force, justId)) {
      case MBulkReply(replies) if justId =>
        Future.value(ReplyFormat.toBuf(replies).map(StreamEntryReply(_, Seq.empty)))
      case MBulkReply(replies) => Future.value(handleRangeReply(replies))
      case NilMBulkReply | EmptyMBulkReply => Future.value(Seq.empty)
    }
  }

  private[redis] def handleReadReply(replies: List[Reply]): Seq[XReadStreamReply] = {
    replies.collect {
      case MBulkReply(BulkReply(stream) :: MBulkReply(entries) :: Nil) =>
        XReadStreamReply(stream, handleRangeReply(entries))
    }
  }

  private[redis] def handleRangeReply(replies: List[Reply]): Seq[StreamEntryReply] = {
    replies.collect {
      case MBulkReply(BulkReply(id) :: MBulkReply(fields) :: Nil) =>
        StreamEntryReply(id, returnPairs(ReplyFormat.toBuf(fields)))
    }
  }
}

case class XPendingAllReply(
  count: JLong,
  start: Option[Buf],
  end: Option[Buf],
  pendingByConsumer: Seq[(Buf, Buf)])

case class XPendingRangeReply(
  id: Buf,
  consumer: Buf,
  millisSinceLastDeliver: JLong,
  numDeliveries: JLong)

case class XReadStreamReply(stream: Buf, entries: Seq[StreamEntryReply])

case class StreamEntryReply(id: Buf, fields: Seq[(Buf, Buf)])
