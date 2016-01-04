package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.{ClientError}
import com.twitter.finagle.redis.exp.SubscribeHandler
import com.twitter.finagle.redis.protocol.Commands._
import com.twitter.finagle.redis.util._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

abstract class SubscribeCommand extends Command {
  def handler: SubscribeHandler
}

case class PSubscribe(
  patterns: Seq[ChannelBuffer],
  handler: SubscribeHandler)
    extends SubscribeCommand {
  def command = Commands.PSUBSCRIBE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.PSUBSCRIBE +: patterns)
}

case class PUnsubscribe(patterns: Seq[ChannelBuffer], handler: SubscribeHandler) extends SubscribeCommand {
  def command = Commands.PUNSUBSCRIBE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.PUNSUBSCRIBE +: patterns)
}

case class Subscribe(
  channels: Seq[ChannelBuffer],
  handler: SubscribeHandler)
    extends SubscribeCommand {
  def command = Commands.SUBSCRIBE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.SUBSCRIBE +: channels)
}

case class Unsubscribe(channels: Seq[ChannelBuffer], handler: SubscribeHandler) extends SubscribeCommand {
  def command = Commands.UNSUBSCRIBE
  def toChannelBuffer = RedisCodec.toUnifiedFormat(CommandBytes.UNSUBSCRIBE +: channels)
}

object Publish {
  def apply(args: List[Array[Byte]]): Publish = {
    val list = trimList(args, 2, "PUBLISH")
    val key = ChannelBuffers.wrappedBuffer(list(0))
    val message = ChannelBuffers.wrappedBuffer(list(1))
    new Publish(key, message)
  }
}
case class Publish(key: ChannelBuffer, message: ChannelBuffer) extends StrictKeyCommand {
  def command = Commands.PUBLISH
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.PUBLISH, key, message))
}

case class PubSubChannels(pattern: Option[ChannelBuffer]) extends PubSub(sub = PubSubChannels.channelBuffer, args = pattern.toSeq)
object PubSubChannels extends PubSubHelper {
  val command = "CHANNELS"
  def fromBytes(args: Seq[Array[Byte]]): PubSubChannels = {
    val list = trimList(args, 1, "PUBSUB CHANNELS")
    new PubSubChannels(Some(ChannelBuffers.wrappedBuffer(list(0))))
  }
}

case class PubSubNumSub(channels: Seq[ChannelBuffer]) extends PubSub(sub = PubSubNumSub.channelBuffer, args = channels)
object PubSubNumSub extends PubSubHelper {
  val command = "NUMSUB"
  def fromBytes(args: Seq[Array[Byte]]): PubSubNumSub = {
    val list = trimList(args, 1, "PUBSUB NUMSUB")
    new PubSubNumSub(list.map(ChannelBuffers.wrappedBuffer(_)))
  }
}

case class PubSubNumPat() extends PubSub(sub = PubSubNumPat.channelBuffer, args = Nil)
object PubSubNumPat extends PubSubHelper {
  val command = "NUMPAT"
  def fromBytes(args: Seq[Array[Byte]]): PubSubNumPat = new PubSubNumPat()
}

abstract class PubSub(sub: ChannelBuffer, args: Seq[ChannelBuffer]) extends Command {
  def command = Commands.PUBSUB
  def toChannelBuffer = RedisCodec.toUnifiedFormat(Seq(CommandBytes.PUBSUB, sub) ++ args)
}

sealed trait PubSubHelper {
  def command: String
  def fromBytes(args: Seq[Array[Byte]]): PubSub
  def channelBuffer: ChannelBuffer = StringToChannelBuffer(command)
  def bytes: Array[Byte] = StringToBytes(command)
}

object PubSub {
  val subCommands: Seq[PubSubHelper] = Seq(PubSubChannels, PubSubNumSub, PubSubNumPat)

  def apply(args: Seq[Array[Byte]]): PubSub = {
    val subCommandString = new String(trimList(args.headOption.toList, 1, "PUBSUB")(0)).toUpperCase
    val subCommand = subCommands.find{_.command == subCommandString}.getOrElse(throw ClientError("Invalid PubSub command " + subCommandString))
    subCommand.fromBytes(args.tail)
  }
}
