package com.twitter.finagle.redis

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.{ Service, ServiceFactory }
import com.twitter.util.Future
import org.jboss.netty.buffer.{ ChannelBuffers, ChannelBuffer }

object SentinelClient {

  /**
   * Construct a sentinel client from a single host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): SentinelClient = SentinelClient(
    ClientBuilder()
      .hosts(host)
      .hostConnectionLimit(1)
      .codec(Redis())
      .daemon(true)
      .build())

  /**
   * Construct a sentinel client from a single Service.
   */
  def apply(raw: Service[Command, Reply]): SentinelClient =
    new SentinelClient(raw)
  
  object Node {
    def apply(props: Map[String, String]) = {
      props("role-reported").toLowerCase() match {
        case "master" => new MasterNode(props)
        case "slave" => new SlaveNode(props)
        case "sentinel" => new SentinelNode(props)
      }
    }
  }
  
  sealed trait Node {
    val props: Map[String, String]
    lazy val name: String = props("name")
    lazy val ip: String = props("ip")
    lazy val port: Int = props("port").toInt
    lazy val runid: String = props("runid")
    lazy val flags: Seq[String] = props("flags").split(",")
    lazy val pendingCommands: Int = props("pending-commands").toInt
    lazy val lastPingSent: Int = props("last-ping-sent").toInt
    lazy val lastPingReply: Int = props("last-ping-reply").toInt
    lazy val downAfterMilliseconds: Int = props("down-after-milliseconds").toInt
  }
  
  trait DataNode extends Node {
    lazy val infoRefresh: Int = props("info-refresh").toInt
    lazy val roleReported: String = props("role-reported")
    lazy val roleReportedTime: Long = props("role-reported-time").toLong    
  }
  
  class MasterNode private[redis] (val props: Map[String, String]) extends DataNode {
    lazy val quorum: Int = props("quorum").toInt
    lazy val configEpoch: Long = props("configEpoch").toLong
    lazy val numSlaves: Int = props("num-slaves").toInt
    lazy val numOtherSentinels: Int = props("num-other-sentinels").toInt
    lazy val failoverTimeout: Int = props("failover-timeout").toInt
    lazy val parallelSyncs: Int = props("parallel-syncs").toInt
  }
  
  class SlaveNode private[redis] (val props: Map[String, String]) extends DataNode {
    lazy val masterLinkDownTime: Int = props("master-link-down-time").toInt
    lazy val masterLinkStatus: String = props("master-link-status")
    lazy val masterHost: String = props("master-host")
    lazy val masterPort: Int = props("master-port").toInt
    lazy val slavePriority: Int = props("slave-priority").toInt
    lazy val slaveReplOffset: Long = props("slave-repl-offset").toLong
  }
  
  class SentinelNode private[redis] (val props: Map[String, String]) extends Node {
    lazy val lastHelloMessage: Int = props("last-hello-message").toInt
    lazy val votedLeader: String = props("voted-leader")
    lazy val votedLeaderEpoch: Int = props("voted-leader-epoch").toInt
  }
}

class SentinelClient(service: Service[Command, Reply])
    extends BaseClient(service)
    with SentinelCommands
