package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case class SentinelMaster(arg: String) extends Sentinel("MASTER", Seq(arg))

case object SentinelMasters extends Sentinel("MASTERS", Nil)

case class SentinelSlaves(arg: String) extends Sentinel("SLAVES", Seq(arg))

case class SentinelSentinels(arg: String) extends Sentinel("SENTINELS", Seq(arg))

case class SentinelGetMasterAddrByName(arg: String)
    extends Sentinel("GET-MASTER-ADDR-BY-NAME", Seq(arg))

case class SentinelReset(pattern: String) extends Sentinel("RESET", Seq(pattern))

case class SentinelFailover(arg: String) extends Sentinel("FAILOVER", Seq(arg))

case class SentinelCkQuorum(arg: String) extends Sentinel("CKQUORUM", Seq(arg))

case object SentinelFlushConfig extends Sentinel("FLUSHCONFIG", Nil)

case class SentinelMonitor(arg: String, ip: String, port: Int, quorum: Int)
    extends Sentinel("MONITOR", Seq(arg, ip, port.toString, quorum.toString))

case class SentinelRemove(arg: String) extends Sentinel("REMOVE", Seq(arg))

case class SentinelSet(arg: String, option: String, value: String)
    extends Sentinel("SET", Seq(arg, option, value))

abstract class Sentinel(sub: String, args: Seq[String]) extends Command {
  def name: Buf = Command.SENTINEL
  override def body: Seq[Buf] = Buf.Utf8(sub) +: args.map(Buf.Utf8.apply)
}
