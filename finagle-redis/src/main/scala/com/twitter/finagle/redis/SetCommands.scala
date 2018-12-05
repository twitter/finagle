package com.twitter.finagle.redis

import com.twitter.io.Buf
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.util.Future
import java.lang.{Long => JLong, Boolean => JBoolean}
import scala.collection.immutable.{Set => ImmutableSet}

private[redis] trait SetCommands { self: BaseClient =>

  /**
   * Adds `members` to the set stored under the `key`. Throws an exception
   * if the `key` does not refer to a set.
   *
   * @return The number of new members added to the set.
   */
  def sAdd(key: Buf, members: List[Buf]): Future[JLong] =
    doRequest(SAdd(key, members)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the members of the set stored under the `key`. Throws an exception
   * if the `key` does not refer to a set.
   */
  def sMembers(key: Buf): Future[ImmutableSet[Buf]] =
    doRequest(SMembers(key)) {
      case MBulkReply(list) => Future.value(ReplyFormat.toBuf(list).toSet)
      case EmptyMBulkReply => Future.value(ImmutableSet.empty)
    }

  /**
   * Checks if the given `member` exists in a set stored under the `key`.
   * Throws an exception if the `key` does not refer to a set.
   *
   * Unassigned keys are considered empty sets.
   */
  def sIsMember(key: Buf, member: Buf): Future[JBoolean] =
    doRequest(SIsMember(key, member)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * Returns the size of the set stored under the `key`. Throws an exception
   * if the key does not refer to a set.
   *
   * Unassigned keys are considered empty sets.
   */
  def sCard(key: Buf): Future[JLong] =
    doRequest(SCard(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes `members` from the set stored under the `key`. Throws an exception
   * if the `key` does not refer to a set.
   *
   * @return The number of elements removed from the set, can be 0 if the key
   *         is unassigned.
   */
  def sRem(key: Buf, members: List[Buf]): Future[JLong] =
    doRequest(SRem(key, members)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes an element randomly from the set stored under the `key`, and returns it.
   * Throws an exception if the key does not refer to a set.
   *
   * @return `Some` if the member exists, or `None` if the set is empty.
   */
  def sPop(key: Buf): Future[Option[Buf]] =
    doRequest(SPop(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply => Future.None
    }

  /**
   * Returns a list of random entries from the set stored under the `key`. If the
   * count is positive, a set is returned, otherwise a list that may contain
   * duplicates is returned.
   */
  def sRandMember(key: Buf, count: Option[Int] = None): Future[Seq[Buf]] =
    doRequest(SRandMember(key, count)) {
      case BulkReply(message) => Future.value(Seq(message))
      case MBulkReply(messages) => Future.value(ReplyFormat.toBuf(messages))
      case EmptyBulkReply | EmptyMBulkReply => Future.Nil
    }

  /**
   * Returns the members of the set resulting from the intersection of all
   * the sets stored under `keys`.
   *
   * Keys that do not exist are considered to be empty sets. With one of
   * the keys being an empty set, the resulting set is also empty
   * (since set intersection with an empty set always results in an empty set).
   *
   * Throws an exception if the `keys` Seq is empty or if any of the keys
   * passed as params are empty.
   */
  def sInter(keys: Seq[Buf]): Future[ImmutableSet[Buf]] =
    doRequest(SInter(keys)) {
      case MBulkReply(messages) =>
        Future.value(ReplyFormat.toBuf(messages).toSet)
      case EmptyMBulkReply => Future.value(ImmutableSet.empty)
    }

  /**
   * Returns keys in given set `key`, starting at `cursor`.
   */
  def sScan(key: Buf, cursor: JLong, count: Option[JLong], pattern: Option[Buf]): Future[Seq[Buf]] =
    doRequest(SScan(key, cursor, count, pattern)) {
      case MBulkReply(messages) => Future.value(ReplyFormat.toBuf(messages))
      case EmptyMBulkReply => Future.Nil
    }

}
