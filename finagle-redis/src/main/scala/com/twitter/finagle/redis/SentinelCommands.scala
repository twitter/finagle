package com.twitter.finagle.redis

import _root_.java.lang.{ Long => JLong, Boolean => JBoolean }
import scala.collection.immutable.{ Set => ImmutableSet }
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.protocol.commands._
import com.twitter.finagle.redis.util._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffer
import java.net.InetAddress
import java.net.InetSocketAddress

trait SentinelCommands { self: BaseClient =>

  import SentinelClient._

  /**
   * Show a list of monitored masters and their state.
   */
  def masters(): Future[List[MasterNode]] =
    doRequest(SentinelMasters()) {
      case MBulkReply(masters) => Future.value(
        masters map {
          case MBulkReply(messages) => new MasterNode(
            returnMap(ReplyFormat.toChannelBuffers(messages)))
        })
      case EmptyMBulkReply() => Future.value(Nil)
    }

  /**
   * Show the state and info of the specified master.
   */
  def master(name: ChannelBuffer): Future[MasterNode] =
    doRequest(SentinelMaster(name)) {
      case MBulkReply(messages) => Future.value(
        new MasterNode(returnMap(ReplyFormat.toChannelBuffers(messages))))
    }

  /**
   * Show a list of slaves for this master, and their state.
   */
  def slaves(name: ChannelBuffer): Future[List[SlaveNode]] =
    doRequest(SentinelSlaves(name)) {
      case MBulkReply(names) => Future.value(
        names map {
          case MBulkReply(messages) =>
            new SlaveNode(returnMap(ReplyFormat.toChannelBuffers(messages)))
        })
      case EmptyMBulkReply() => Future.value(Nil)
    }

  /**
   * Show a list of sentinel instances for this master, and their state.
   */
  def sentinels(name: ChannelBuffer): Future[List[SentinelNode]] =
    doRequest(SentinelSentinels(name)) {
      case MBulkReply(names) => Future.value(
        names map {
          case MBulkReply(messages) =>
            new SentinelNode(returnMap(ReplyFormat.toChannelBuffers(messages)))
        })
      case EmptyMBulkReply() => Future.value(Nil)
    }

  /**
   * Return the ip and port number of the master with that name. If a failover
   * is in progress or terminated successfully for this master it returns the
   * address and port of the promoted slave.
   */
  def getMasterAddrByName(name: ChannelBuffer): Future[Option[InetSocketAddress]] =
    doRequest(SentinelGetMasterAddrByName(name)) {
      case NilMBulkReply() =>
        Future.value(None)
      case MBulkReply(messages) => Future.value(
        ReplyFormat.toChannelBuffers(messages) match {
          case host :: port :: Nil =>
            Some(InetSocketAddress.createUnresolved(
              CBToString(host), NumberFormat.toInt(CBToString(port))))
        })
    }

  /**
   * Reset all the masters with matching name.
   */
  def reset(pattern: ChannelBuffer): Future[Unit] =
    doRequest(SentinelReset(pattern)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Force a failover.
   */
  def failover(name: ChannelBuffer): Future[Unit] =
    doRequest(SentinelFailover(name)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Check if the current Sentinel configuration is able to reach the quorum
   * needed to failover a master, and the majority needed to authorize the
   * failover.
   */
  def ckquorum(name: ChannelBuffer): Future[String] =
    doRequest(SentinelCkQuorum(name)) {
      case StatusReply(message) => Future.value(message)
    }

  /**
   * Force Sentinel to rewrite its configuration on disk, including the
   * current Sentinel state.
   */
  def flushConfig(): Future[Unit] =
    doRequest(SentinelFlushConfig()) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Tells the Sentinel to start monitoring a new master with the
   * specified name, ip, port, and quorum.
   */
  def monitor(name: ChannelBuffer, addr: InetSocketAddress, quorum: Int): Future[Unit] =
    doRequest(SentinelMonitor(name, addr, quorum)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Remove the specified master.
   */
  def remove(name: ChannelBuffer): Future[Unit] =
    doRequest(SentinelRemove(name)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Change configuration parameters of a specific master.
   */
  def set(name: ChannelBuffer, option: ChannelBuffer, value: ChannelBuffer): Future[Unit] =
    doRequest(SentinelSet(name, option, value)) {
      case StatusReply(msg) => Future.Unit
    }
}
