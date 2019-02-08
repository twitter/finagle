package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

case object FlushAll extends Command {
  def name: Buf = Command.FLUSHALL
}

case object FlushDB extends Command {
  def name: Buf = Command.FLUSHDB
}

case class Select(index: Int) extends Command {
  def name: Buf = Command.SELECT
  override def body: Seq[Buf] = Seq(Buf.Utf8(index.toString))
}

case object DBSize extends Command {
  def name: Buf = Command.DBSIZE
}

case class Auth(code: Buf) extends Command {
  def name: Buf = Command.AUTH
  override def body: Seq[Buf] = Seq(code)
}

case class Info(section: Buf) extends Command {
  def name: Buf = Command.INFO
  override def body: Seq[Buf] = if (section.isEmpty) Nil else Seq(section)
}

case object Ping extends Command {
  def name: Buf = Command.PING
}

case object Quit extends Command {
  def name: Buf = Command.QUIT
}

case class ConfigSet(param: Buf, value: Buf) extends Config(Buf.Utf8("SET"), Seq(param, value))

case class ConfigGet(param: Buf) extends Config(Buf.Utf8("GET"), Seq(param))

case class ConfigResetStat() extends Config(Buf.Utf8("RESETSTAT"), Seq.empty)

abstract class Config(sub: Buf, args: Seq[Buf]) extends Command {
  def name: Buf = Command.CONFIG
  override def body: Seq[Buf] = sub +: args
}

case class SlaveOf(host: Buf, port: Buf) extends Command {
  def name: Buf = Command.SLAVEOF
  override def body: Seq[Buf] = Seq(host, port)
}

case class ReplicaOf(host: Buf, port: Buf) extends Command {
  def name: Buf = Command.REPLICAOF
  override def body: Seq[Buf] = Seq(host, port)
}
