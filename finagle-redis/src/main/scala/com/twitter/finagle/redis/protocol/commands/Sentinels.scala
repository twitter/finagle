package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.protocol.Commands._
import com.twitter.finagle.redis.util._
import com.twitter.io.Charsets

case class SentinelMaster(name: String)
  extends Sentinel(SentinelMaster.command, Seq(name))

object SentinelMaster extends SentinelHelper {
  val command = "MASTER"
  def apply(args: Seq[String]): SentinelMaster = {
    val name = trimList(args, 2, "SENTINEL MASTER")(0)
    new SentinelMaster(name)
  }
}

case class SentinelMasters() extends Sentinel(SentinelMasters.command, Nil)

object SentinelMasters extends SentinelHelper {
  val command = "MASTERS"
  def apply(args: Seq[String]): SentinelMasters = {
    new SentinelMasters()
  }
}

case class SentinelSlaves(name: String)
  extends Sentinel(SentinelSlaves.command, Seq(name))

object SentinelSlaves extends SentinelHelper {
  val command = "SLAVES"
  def apply(args: Seq[String]): SentinelSlaves = {
    val name = trimList(args, 2, "SENTINEL SLAVES")(0)
    new SentinelSlaves(name)
  }
}

case class SentinelSentinels(name: String)
  extends Sentinel(SentinelSentinels.command, Seq(name))

object SentinelSentinels extends SentinelHelper {
  val command = "SENTINELS"
  def apply(args: Seq[String]): SentinelSentinels = {
    val name = trimList(args, 2, "SENTINEL SENTINELS")(0)
    new SentinelSentinels(name)
  }
}

case class SentinelGetMasterAddrByName(name: String)
  extends Sentinel(SentinelGetMasterAddrByName.command, Seq(name))

object SentinelGetMasterAddrByName extends SentinelHelper {
  val command = "GET-MASTER-ADDR-BY-NAME"
  def apply(args: Seq[String]): SentinelGetMasterAddrByName = {
    val name = trimList(args, 2, "SENTINEL GET-MASTER-ADDR-BY-NAME")(0)
    new SentinelGetMasterAddrByName(name)
  }
}

case class SentinelReset(pattern: String)
  extends Sentinel(SentinelReset.command, Seq(pattern))

object SentinelReset extends SentinelHelper {
  val command = "RESET"
  def apply(args: Seq[String]): SentinelReset = {
    val name = trimList(args, 2, "SENTINEL RESET")(0)
    new SentinelReset(name)
  }
}

case class SentinelFailover(name: String)
  extends Sentinel(SentinelFailover.command, Seq(name))

object SentinelFailover extends SentinelHelper {
  val command = "FAILOVER"
  def apply(args: Seq[String]): SentinelFailover = {
    val name = trimList(args, 2, "SENTINEL FAILOVER")(0)
    new SentinelFailover(name)
  }
}

case class SentinelCkQuorum(name: String)
  extends Sentinel(SentinelCkQuorum.command, Seq(name))

object SentinelCkQuorum extends SentinelHelper {
  val command = "CKQUORUM"
  def apply(args: Seq[String]): SentinelCkQuorum = {
    val name = trimList(args, 2, "SENTINEL CKQUORUM")(0)
    new SentinelCkQuorum(name)
  }
}

case class SentinelFlushConfig()
  extends Sentinel(SentinelFlushConfig.command, Nil)

object SentinelFlushConfig extends SentinelHelper {
  val command = "FLUSHCONFIG"
  def apply(args: Seq[String]): SentinelFlushConfig = {
    new SentinelFlushConfig()
  }
}

case class SentinelMonitor(name: String, ip: String, port: Int, quorum: Int)
  extends Sentinel(SentinelMonitor.command, Seq(name, ip, port.toString, quorum.toString))

object SentinelMonitor extends SentinelHelper {
  val command = "MONITOR"
  def apply(args: Seq[String]): SentinelMonitor = {
    trimList(args, 2, "SENTINEL MONITOR") match {
      case name :: ip :: port :: quorum :: Nil =>
        new SentinelMonitor(name, ip, port.toInt, quorum.toInt)
    }
  }
}

case class SentinelRemove(name: String)
  extends Sentinel(SentinelRemove.command, Seq(name))

object SentinelRemove extends SentinelHelper {
  val command = "REMOVE"
  def apply(args: Seq[String]): SentinelRemove = {
    val list = trimList(args, 2, "SENTINEL REMOVE")
    new SentinelRemove(list(0))
  }
}

case class SentinelSet(name: String, option: String, value: String)
  extends Sentinel(SentinelSet.command, Seq(name, option, value))

object SentinelSet extends SentinelHelper {
  val command = "SET"
  def apply(args: Seq[String]): SentinelSet = {
    trimList(args, 2, "SENTINEL SET") match {
      case name :: option :: value :: Nil =>
        new SentinelSet(name, option, value)
    }
  }
}

sealed trait SentinelHelper {
  def command: String
  def apply(args: Seq[String]): Sentinel
}

abstract class Sentinel(sub: String, args: Seq[String]) extends Command {
  def command = Commands.SENTINEL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(
      Seq(CommandBytes.SENTINEL, StringToChannelBuffer(sub)) ++
        args.map(StringToChannelBuffer(_)))
}

object Sentinel {

  val subCommands: Seq[SentinelHelper] = Seq(
    SentinelMaster, SentinelMasters, SentinelSlaves, SentinelSentinels, SentinelGetMasterAddrByName,
    SentinelReset, SentinelFailover, SentinelCkQuorum, SentinelFlushConfig, SentinelMonitor,
    SentinelRemove, SentinelSet)

  def fromBytes(args: Seq[Array[Byte]]): Sentinel = {
    apply(args.map(new String(_, Charsets.Utf8)))
  }
    
  def apply(args: Seq[String]): Sentinel = {
    val subCommandString = trimList(args.headOption.toList, 1, "SENTINEL")(0).toUpperCase
    val subCommand = subCommands.find { _.command == subCommandString }
      .getOrElse(throw ClientError(s"Invalid Sentinel command ${subCommandString}"))
    subCommand(args.tail)
  }
}
