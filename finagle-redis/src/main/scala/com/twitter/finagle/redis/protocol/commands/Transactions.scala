package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case object Discard extends Command {
  def name: Buf = Command.DISCARD
}

case object Exec extends Command {
  def name: Buf = Command.EXEC
}

case object Multi extends Command {
  def name: Buf = Command.MULTI
}

case object UnWatch extends Command {
  def name: Buf = Command.UNWATCH
}

case class Watch(keys: Seq[Buf]) extends KeysCommand {
  def name: Buf = Command.WATCH
  override def body: Seq[Buf] = keys
}
object Watch {
  def apply(args: => Seq[Array[Byte]]): Watch = new Watch(args.map(Buf.ByteArray.Owned(_)))
}
