package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.ServiceFactory

object SentinelClient {

  /**
   * Construct a sentinel client from a single host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): SentinelClient = {
    SentinelClient(com.twitter.finagle.Redis.newClient(host))
  }

  /**
   * Construct a sentinel client from a single Service.
   */
  def apply(raw: ServiceFactory[Command, Reply]): SentinelClient =
    new SentinelClient(raw)

  sealed trait Node {
    val props: Map[String, String]
    val name: String = props("name")
    val ip: String = props("ip")
    val port: Int = props("port").toInt
    val runid: String = props("runid")
    val flags: Seq[String] = props("flags").split(",")
    // In Redis 3.2 and newer, the property was renamed to link-pending-commands
    // preserve compatibility with older releases.
    val linkPendingCommands: Int = props
      .get("link-pending-commands")
      .getOrElse(props("pending-commands"))
      .toInt
    val lastPingSent: Int = props("last-ping-sent").toInt
    val lastPingReply: Int = props("last-ping-reply").toInt
    val downAfterMilliseconds: Int = props("down-after-milliseconds").toInt
  }

  sealed trait DataNode extends Node {
    val infoRefresh: Long = props("info-refresh").toLong
    val roleReported: String = props("role-reported")
    val roleReportedTime: Long = props("role-reported-time").toLong
  }

  class MasterNode private[redis] (val props: Map[String, String]) extends DataNode {
    val quorum: Int = props("quorum").toInt
    val configEpoch: Option[Long] = props.get("configEpoch").map(_.toLong)
    val numSlaves: Int = props("num-slaves").toInt
    val numOtherSentinels: Int = props("num-other-sentinels").toInt
    val failoverTimeout: Int = props("failover-timeout").toInt
    val parallelSyncs: Int = props("parallel-syncs").toInt
  }

  class SlaveNode private[redis] (val props: Map[String, String]) extends DataNode {
    val masterLinkDownTime: Int = props("master-link-down-time").toInt
    val masterLinkStatus: String = props("master-link-status")
    val masterHost: String = props("master-host")
    val masterPort: Int = props("master-port").toInt
    val slavePriority: Int = props("slave-priority").toInt
    val slaveReplOffset: Long = props("slave-repl-offset").toLong
  }

  class SentinelNode private[redis] (val props: Map[String, String]) extends Node {
    val lastHelloMessage: Int = props("last-hello-message").toInt
    val votedLeader: String = props("voted-leader")
    val votedLeaderEpoch: Int = props("voted-leader-epoch").toInt
  }
}

class SentinelClient(factory: ServiceFactory[Command, Reply])
    extends BaseClient(factory)
    with SentinelCommands
    with ServerCommands
