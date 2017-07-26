package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.exp.SubscribeHandler
import com.twitter.io.Buf

abstract class SubscribeCommand extends Command {
  def handler: SubscribeHandler
}

case class PSubscribe(patterns: Seq[Buf], handler: SubscribeHandler) extends SubscribeCommand {

  def name: Buf = Command.PSUBSCRIBE
  override def body: Seq[Buf] = patterns
}

case class PUnsubscribe(patterns: Seq[Buf], handler: SubscribeHandler) extends SubscribeCommand {

  def name: Buf = Command.PUNSUBSCRIBE
  override def body: Seq[Buf] = patterns
}

case class Subscribe(channels: Seq[Buf], handler: SubscribeHandler) extends SubscribeCommand {

  def name: Buf = Command.SUBSCRIBE
  override def body: Seq[Buf] = channels
}

case class Unsubscribe(channels: Seq[Buf], handler: SubscribeHandler) extends SubscribeCommand {

  def name: Buf = Command.UNSUBSCRIBE
  override def body: Seq[Buf] = channels
}

case class Publish(key: Buf, message: Buf) extends StrictKeyCommand {
  def name: Buf = Command.PUBLISH
  override def body: Seq[Buf] = Seq(key, message)
}

case class PubSubChannels(pattern: Option[Buf]) extends PubSub(Buf.Utf8("CHANNELS"), pattern.toSeq)

case class PubSubNumSub(channels: Seq[Buf]) extends PubSub(Buf.Utf8("NUMSUB"), channels)

case object PubSubNumPat extends PubSub(Buf.Utf8("NUMPAT"), Seq.empty)

abstract class PubSub(sub: Buf, args: Seq[Buf]) extends Command {
  def name: Buf = Command.PUBSUB
  override def body: Seq[Buf] = sub +: args
}
