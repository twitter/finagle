package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.protocol.Commands._
import com.twitter.finagle.redis.util._
import com.twitter.finagle.redis.ClientError
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

case class SentinelMaster(name: ChannelBuffer)
  extends Sentinel(SentinelMaster.channelBuffer, Seq(name))

object SentinelMaster extends SentinelHelper {
  val command = "MASTER"
  def apply(args: Seq[Array[Byte]]): SentinelMaster = {
    val name = trimList(args, 2, "SENTINEL MASTER")(0)
    new SentinelMaster(ChannelBuffers.wrappedBuffer(name))
  }
}

case class SentinelMasters() extends Sentinel(SentinelMasters.channelBuffer, Nil)

object SentinelMasters extends SentinelHelper {
  val command = "MASTERS"
  def apply(args: Seq[Array[Byte]]): SentinelMasters = {
    new SentinelMasters()
  }
}

case class SentinelSlaves(name: ChannelBuffer)
  extends Sentinel(SentinelSlaves.channelBuffer, Seq(name))

object SentinelSlaves extends SentinelHelper {
  val command = "SLAVES"
  def apply(args: Seq[Array[Byte]]): SentinelSlaves = {
    val name = trimList(args, 2, "SENTINEL SLAVES")(0)
    new SentinelSlaves(ChannelBuffers.wrappedBuffer(name))
  }
}

case class SentinelSentinels(name: ChannelBuffer)
  extends Sentinel(SentinelSentinels.channelBuffer, Seq(name))

object SentinelSentinels extends SentinelHelper {
  val command = "SENTINELS"
  def apply(args: Seq[Array[Byte]]): SentinelSentinels = {
    val name = trimList(args, 2, "SENTINEL SENTINELS")(0)
    new SentinelSentinels(ChannelBuffers.wrappedBuffer(name))
  }
}

case class SentinelGetMasterAddrByName(name: ChannelBuffer)
  extends Sentinel(SentinelGetMasterAddrByName.channelBuffer, Seq(name))

object SentinelGetMasterAddrByName extends SentinelHelper {
  val command = "GET-MASTER-ADDR-BY-NAME"
  def apply(args: Seq[Array[Byte]]): SentinelGetMasterAddrByName = {
    val name = trimList(args, 2, "SENTINEL GET-MASTER-ADDR-BY-NAME")(0)
    new SentinelGetMasterAddrByName(ChannelBuffers.wrappedBuffer(name))
  }
}

case class SentinelReset(pattern: ChannelBuffer)
  extends Sentinel(SentinelReset.channelBuffer, Seq(pattern))

object SentinelReset extends SentinelHelper {
  val command = "RESET"
  def apply(args: Seq[Array[Byte]]): SentinelReset = {
    val name = trimList(args, 2, "SENTINEL RESET")(0)
    new SentinelReset(ChannelBuffers.wrappedBuffer(name))
  }
}

case class SentinelFailover(name: ChannelBuffer)
  extends Sentinel(SentinelFailover.channelBuffer, Seq(name))

object SentinelFailover extends SentinelHelper {
  val command = "FAILOVER"
  def apply(args: Seq[Array[Byte]]): SentinelFailover = {
    val name = trimList(args, 2, "SENTINEL FAILOVER")(0)
    new SentinelFailover(ChannelBuffers.wrappedBuffer(name))
  }
}

case class SentinelCkQuorum(name: ChannelBuffer)
  extends Sentinel(SentinelCkQuorum.channelBuffer, Seq(name))

object SentinelCkQuorum extends SentinelHelper {
  val command = "CKQUORUM"
  def apply(args: Seq[Array[Byte]]): SentinelCkQuorum = {
    val name = trimList(args, 2, "SENTINEL CKQUORUM")(0)
    new SentinelCkQuorum(ChannelBuffers.wrappedBuffer(name))
  }
}

case class SentinelFlushConfig()
  extends Sentinel(SentinelFlushConfig.channelBuffer, Nil)

object SentinelFlushConfig extends SentinelHelper {
  val command = "FLUSHCONFIG"
  def apply(args: Seq[Array[Byte]]): SentinelFlushConfig = {
    new SentinelFlushConfig()
  }
}

case class SentinelMonitor(name: ChannelBuffer, addr: InetSocketAddress, quorum: Int)
  extends Sentinel(SentinelMonitor.channelBuffer, Seq(
    name,
    StringToChannelBuffer(addr.getHostString),
    StringToChannelBuffer(addr.getPort.toString),
    StringToChannelBuffer(quorum.toString)))

object SentinelMonitor extends SentinelHelper {
  val command = "MONITOR"
  def apply(args: Seq[Array[Byte]]): SentinelMonitor = {
    trimList(args, 2, "SENTINEL MONITOR") match {
      case name :: ip :: port :: quorum :: Nil =>
        new SentinelMonitor(
          ChannelBuffers.wrappedBuffer(name),
          InetSocketAddress.createUnresolved(
            BytesToString(ip),
            NumberFormat.toInt(BytesToString(port))),
          NumberFormat.toInt(BytesToString(quorum)))
    }
  }
}

case class SentinelRemove(name: ChannelBuffer)
  extends Sentinel(SentinelRemove.channelBuffer, Seq(name))

object SentinelRemove extends SentinelHelper {
  val command = "REMOVE"
  def apply(args: Seq[Array[Byte]]): SentinelRemove = {
    val list = trimList(args, 2, "SENTINEL REMOVE")
    new SentinelRemove(ChannelBuffers.wrappedBuffer(list(0)))
  }
}

case class SentinelSet(name: ChannelBuffer, option: ChannelBuffer, value: ChannelBuffer)
  extends Sentinel(SentinelSet.channelBuffer, Seq(name, option, value))

object SentinelSet extends SentinelHelper {
  val command = "SET"
  def apply(args: Seq[Array[Byte]]): SentinelSet = {
    trimList(args, 2, "SENTINEL SET") match {
      case name :: option :: value :: Nil =>
        new SentinelSet(
          ChannelBuffers.wrappedBuffer(name),
          ChannelBuffers.wrappedBuffer(option),
          ChannelBuffers.wrappedBuffer(value))
    }
  }
}

trait SentinelHelper {
  def command: String
  def apply(args: Seq[Array[Byte]]): Sentinel
  def channelBuffer: ChannelBuffer = StringToChannelBuffer(command)
  def bytes: Array[Byte] = StringToBytes(command)
}

abstract class Sentinel(sub: ChannelBuffer, args: Seq[ChannelBuffer]) extends Command {
  def command = Commands.SENTINEL
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.SENTINEL, sub) ++ args)
}

object Sentinel {

  val subCommands: Seq[SentinelHelper] = Seq(
    SentinelMaster, SentinelMasters, SentinelMonitor, SentinelRemove, SentinelSet)

  def apply(args: Seq[Array[Byte]]): Sentinel = {
    val subCommandString = new String(trimList(args.headOption.toList, 1, "SENTINEL")(0)).toUpperCase
    val subCommand = subCommands.find { _.command == subCommandString }
      .getOrElse(throw ClientError(s"Invalid Sentinel command ${subCommandString}"))
    subCommand(args.tail)
  }
}
