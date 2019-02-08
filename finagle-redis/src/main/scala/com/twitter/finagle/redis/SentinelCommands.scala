package com.twitter.finagle.redis

import java.net.InetSocketAddress
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util._
import com.twitter.util.Future

private[redis] trait SentinelCommands { self: BaseClient =>

  import SentinelClient._

  private[redis] def returnMap(messages: Seq[String]): Map[String, String] = {
    assert(messages.length % 2 == 0, "Odd number of items in response")
    messages
      .grouped(2)
      .collect {
        case Seq(a, b) => (a, b)
      }
      .toMap
  }

  /**
   * Show a list of monitored masters and their state.
   */
  def masters(): Future[Seq[MasterNode]] =
    doRequest(SentinelMasters) {
      case MBulkReply(masters) =>
        Future.value(masters.map {
          case MBulkReply(messages) => new MasterNode(returnMap(ReplyFormat.toString(messages)))
        })
      case EmptyMBulkReply => Future.Nil
    }

  /**
   * Show the state and info of the specified master.
   */
  def master(name: String): Future[MasterNode] =
    doRequest(SentinelMaster(name)) {
      case MBulkReply(messages) =>
        Future.value(new MasterNode(returnMap(ReplyFormat.toString(messages))))
    }

  /**
   * Show a list of replicas for this master, and their state.
   */
  def slaves(name: String): Future[List[SlaveNode]] =
    doRequest(SentinelSlaves(name)) {
      case MBulkReply(names) =>
        Future.value(names.map {
          case MBulkReply(messages) =>
            new SlaveNode(returnMap(ReplyFormat.toString(messages)))
        })
      case EmptyMBulkReply => Future.value(Nil)
    }

  /**
   * Show a list of sentinel instances for this master, and their state.
   */
  def sentinels(name: String): Future[List[SentinelNode]] =
    doRequest(SentinelSentinels(name)) {
      case MBulkReply(names) =>
        Future.value(names.map {
          case MBulkReply(messages) =>
            new SentinelNode(returnMap(ReplyFormat.toString(messages)))
        })
      case EmptyMBulkReply => Future.value(Nil)
    }

  /**
   * Return the ip and port number of the master with that name. If a failover
   * is in progress or terminated successfully for this master it returns the
   * address and port of the promoted replica.
   */
  def getMasterAddrByName(name: String): Future[Option[InetSocketAddress]] =
    doRequest(SentinelGetMasterAddrByName(name)) {
      case NilMBulkReply => Future.None
      case MBulkReply(messages) =>
        Future.value(ReplyFormat.toBuf(messages) match {
          case host :: port :: Nil =>
            Some(InetSocketAddress.createUnresolved(BufToString(host), BufToString(port).toInt))
        })
    }

  /**
   * Reset all the masters with matching name.
   */
  def reset(pattern: String): Future[Unit] =
    doRequest(SentinelReset(pattern)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Force a failover.
   */
  def failover(name: String): Future[Unit] =
    doRequest(SentinelFailover(name)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Check if the current Sentinel configuration is able to reach the quorum
   * needed to failover a master, and the majority needed to authorize the
   * failover.
   */
  def ckQuorum(name: String): Future[String] =
    doRequest(SentinelCkQuorum(name)) {
      case StatusReply(message) => Future.value(message)
    }

  /**
   * Force Sentinel to rewrite its configuration on disk, including the
   * current Sentinel state.
   */
  def flushConfig(): Future[Unit] =
    doRequest(SentinelFlushConfig) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Tells the Sentinel to start monitoring a new master with the
   * specified name, ip, port, and quorum.
   */
  def monitor(name: String, ip: String, port: Int, quorum: Int): Future[Unit] =
    doRequest(SentinelMonitor(name, ip, port, quorum)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Remove the specified master.
   */
  def remove(name: String): Future[Unit] =
    doRequest(SentinelRemove(name)) {
      case StatusReply(msg) => Future.Unit
    }

  /**
   * Change configuration parameters of a specific master.
   */
  def set(name: String, option: String, value: String): Future[Unit] =
    doRequest(SentinelSet(name, option, value)) {
      case StatusReply(msg) => Future.Unit
    }
}
