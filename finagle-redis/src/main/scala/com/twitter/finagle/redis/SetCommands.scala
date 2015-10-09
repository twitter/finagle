package com.twitter.finagle.redis

import _root_.java.lang.{Long => JLong,Boolean => JBoolean}
import scala.collection.immutable.{Set => ImmutableSet}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer

trait Sets { self: BaseClient =>
  /**
   * Adds elements to the set, according to the set property.
   * Throws an exception if the key does not refer to a set.
   * @param key
   * @param members
   * @return the number of new members added to the set.
   */
  def sAdd(key: ChannelBuffer, members: List[ChannelBuffer]): Future[JLong] =
    doRequest(SAdd(key, members)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Gets the members of the set.
   * Throws an exception if the key does not refer to a set.
   * @param key
   * @return a list of the members
   */
  def sMembers(key: ChannelBuffer): Future[ImmutableSet[ChannelBuffer]] =
    doRequest(SMembers(key)) {
      case MBulkReply(list) => Future.value(ReplyFormat.toChannelBuffers(list).toSet)
      case EmptyMBulkReply() => Future.value(ImmutableSet())
    }

  /**
   * Is the member in the set?
   * Throws an exception if the key does not refer to a set.
   * @param key
   * @param members
   * @return a boolean, true if it is in the set, false otherwise.  Unassigned
   * keys are considered empty sets.
   */
  def sIsMember(key: ChannelBuffer, member: ChannelBuffer): Future[JBoolean] =
    doRequest(SIsMember(key, member)) {
      case IntegerReply(n) => Future.value(n == 1)
    }

  /**
   * How many elements are in the set?
   * Throws an exception if the key does not refer to a set.
   * @param key
   * @return the number of elements in the set.  Unassigned keys are considered
   * empty sets.
   */
  def sCard(key: ChannelBuffer): Future[JLong] =
    doRequest(SCard(key)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes the element from the set if it is in the set.
   * Throws an exception if the key does not refer to a set.
   * @param key
   * @param member
   * @return an integer, the number of elements removed from the set, can be
   * 0 if the key is unassigned.
   */
  def sRem(key: ChannelBuffer, members: List[ChannelBuffer]): Future[JLong] =
    doRequest(SRem(key, members)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
   * Removes an element randomly from the set, and returns it.
   * Throws an exception if the key does not refer to a set.
   * @param key
   * @return the member, or nothing if the set is empty.
   */
  def sPop(key: ChannelBuffer): Future[Option[ChannelBuffer]] =
    doRequest(SPop(key)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply() => Future.value(None)
    }

  /**
    * Returns a list of random entries from the set. If the count is
    * positive, a set is returned, otherwise a list that may contain
    * duplicates is returned.
    * @param key, count
    * @return a sequence with count random entries from the set
    */
  def sRandMember(key: ChannelBuffer, count: Option[Int] = None): Future[Seq[ChannelBuffer]] =
    doRequest(SRandMember(key, count)) {
      case BulkReply(message) => Future.value(Seq(message))
      case EmptyBulkReply() => Future.Nil
      case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
      case EmptyMBulkReply() => Future.Nil
    }

  /**
   * Returns the members of the set resulting from the intersection of all
   * the given sets.
   *
   * Keys that do not exist are considered to be empty sets. With one of
   * the keys being an empty set, the resulting set is also empty
   * (since set intersection with an empty set always results in an empty set).
   *
   * Throws an exception if the `keys` Seq is empty or if any of the keys
   * passed as params are empty.
   *
   * @param keys list of keys to intersect
   * @return set of members from the resulting intersection
   */
  def sInter(keys: Seq[ChannelBuffer]): Future[ImmutableSet[ChannelBuffer]] =
    doRequest(SInter(keys)) {
      case MBulkReply(messages) =>
        Future.value(ReplyFormat.toChannelBuffers(messages).toSet)
      case EmptyMBulkReply() => Future.value(ImmutableSet())
    }
}
