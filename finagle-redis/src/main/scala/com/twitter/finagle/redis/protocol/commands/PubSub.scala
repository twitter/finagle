package com.twitter.finagle.redis.protocol

import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.redis.exp.SubscribeHandler
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

case class Publish(keyBuf: Buf, message: Buf) extends StrictKeyCommand {
  override def key: ChannelBuffer = BufChannelBuffer(keyBuf)

  def command = Commands.PUBLISH
  def toChannelBuffer =
    RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PUBLISH, keyBuf, message))
}

case class PubSubChannels(pattern: Option[Buf]) extends PubSub(Buf.Utf8("CHANNELS"), pattern.toSeq)

case class PubSubNumSub(channels: Seq[Buf]) extends PubSub(Buf.Utf8("NUMSUB"), channels)

case object PubSubNumPat extends PubSub(Buf.Utf8("NUMPAT"), Seq.empty)

abstract class PubSub(sub: Buf, args: Seq[Buf]) extends Command {
  def command = Commands.PUBSUB
  def toChannelBuffer = RedisCodec.bufToUnifiedChannelBuffer(Seq(CommandBytes.PUBSUB, sub) ++ args)
}
