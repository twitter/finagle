package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.util._

case class SentinelMaster(name: String) extends Sentinel("MASTER", Seq(name))

case object SentinelMasters extends Sentinel("MASTERS", Nil)

case class SentinelSlaves(name: String) extends Sentinel("SLAVES", Seq(name))

case class SentinelSentinels(name: String) extends Sentinel("SENTINELS", Seq(name))

case class SentinelGetMasterAddrByName(name: String)
  extends Sentinel("GET-MASTER-ADDR-BY-NAME", Seq(name))

case class SentinelReset(pattern: String)
  extends Sentinel("RESET", Seq(pattern))

case class SentinelFailover(name: String)
  extends Sentinel("FAILOVER", Seq(name))

case class SentinelCkQuorum(name: String)
  extends Sentinel("CKQUORUM", Seq(name))

case object SentinelFlushConfig extends Sentinel("FLUSHCONFIG", Nil)

case class SentinelMonitor(name: String, ip: String, port: Int, quorum: Int)
  extends Sentinel("MONITOR", Seq(name, ip, port.toString, quorum.toString))

case class SentinelRemove(name: String)
  extends Sentinel("REMOVE", Seq(name))

case class SentinelSet(name: String, option: String, value: String)
  extends Sentinel("SET", Seq(name, option, value))

abstract class Sentinel(sub: String, args: Seq[String]) extends Command {
  def command = Commands.SENTINEL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(
      Seq(CommandBytes.SENTINEL, StringToChannelBuffer(sub)) ++
        args.map(StringToChannelBuffer(_)))
}
