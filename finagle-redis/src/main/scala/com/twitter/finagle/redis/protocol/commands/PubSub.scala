package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.exp.SubscribeHandler
import com.twitter.finagle.redis.protocol.Commands._
import com.twitter.finagle.redis.util._
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer

abstract class SubscribeCommand extends Command {
  def handler: SubscribeHandler
}

case class PSubscribe(
  patterns: Seq[Buf],
  handler: SubscribeHandler)
    extends SubscribeCommand {
  def command = Commands.PSUBSCRIBE
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.PSUBSCRIBE +: patterns)
}

case class PUnsubscribe(patterns: Seq[Buf], handler: SubscribeHandler) extends SubscribeCommand {
  def command = Commands.PUNSUBSCRIBE
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.PUNSUBSCRIBE +: patterns)
}

case class Subscribe(
  channels: Seq[Buf],
  handler: SubscribeHandler)
    extends SubscribeCommand {
  def command = Commands.SUBSCRIBE
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.SUBSCRIBE +: channels)
}

case class Unsubscribe(channels: Seq[Buf], handler: SubscribeHandler) extends SubscribeCommand {
  def command = Commands.UNSUBSCRIBE
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(CommandBytes.UNSUBSCRIBE +: channels)
}

object Publish {
  def apply(args: List[Array[Byte]]): Publish = {
    val list = trimList(args, 2, "PUBLISH")
    val key = Buf.ByteArray.Owned(list(0))
    val message = Buf.ByteArray.Owned(list(1))
    new Publish(key, message)
  }
}
case class Publish(keyBuf: Buf, message: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.PUBLISH
  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PUBLISH, keyBuf, message))
}

case class PubSubChannels(pattern: Option[Buf]) extends PubSub(sub = PubSubChannels.buf, args = pattern.toSeq)
object PubSubChannels extends PubSubHelper {
  val command = "CHANNELS"
  def fromBytes(args: Seq[Array[Byte]]): PubSubChannels = {
    val list = trimList(args, 1, "PUBSUB CHANNELS")
    new PubSubChannels(Some(Buf.ByteArray.Owned(list(0))))
  }
}

case class PubSubNumSub(channels: Seq[Buf]) extends PubSub(sub = PubSubNumSub.buf, args = channels)
object PubSubNumSub extends PubSubHelper {
  val command = "NUMSUB"
  def fromBytes(args: Seq[Array[Byte]]): PubSubNumSub = {
    val list = trimList(args, 1, "PUBSUB NUMSUB")
    new PubSubNumSub(list.map(Buf.ByteArray.Owned.apply))
  }
}

case class PubSubNumPat() extends PubSub(sub = PubSubNumPat.buf, args = Nil)

object PubSubNumPat extends PubSubHelper {
  val command = "NUMPAT"
  def fromBytes(args: Seq[Array[Byte]]): PubSubNumPat = new PubSubNumPat()
}

abstract class PubSub(sub: Buf, args: Seq[Buf]) extends Command {
  def command = Commands.PUBSUB
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PUBSUB, sub) ++ args)
}

sealed trait PubSubHelper {
  def command: String
  def fromBytes(args: Seq[Array[Byte]]): PubSub
  def buf: Buf = Buf.Utf8(command)
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
