package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf
import java.lang.{Long => JLong}

case class HDel(key: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  def name: Buf = Command.HDEL
  override def body: Seq[Buf] = key +: fields
}

case class HExists(key: Buf, field: Buf) extends StrictKeyCommand {
  def name: Buf = Command.HEXISTS
  override def body: Seq[Buf] = Seq(key, field)
}

case class HGet(key: Buf, field: Buf) extends StrictKeyCommand {
  def name: Buf = Command.HGET
  override def body: Seq[Buf] = Seq(key, field)
}

case class HGetAll(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.HGETALL
}

case class HIncrBy(key: Buf, field: Buf, amount: Long) extends StrictKeyCommand {
  def name: Buf = Command.HINCRBY
  override def body: Seq[Buf] = Seq(key, field, Buf.Utf8(amount.toString))
}

case class HKeys(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.HKEYS
}

case class HLen(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.HLEN
}

case class HMGet(key: Buf, fields: Seq[Buf]) extends StrictKeyCommand {
  def name: Buf = Command.HMGET
  override def body: Seq[Buf] = key +: fields
}

case class HMSet(key: Buf, fv: Map[Buf, Buf]) extends StrictKeyCommand {
  def name: Buf = Command.HMSET
  override def body: Seq[Buf] = {
    val fvList: Seq[Buf] = fv.iterator.flatMap {
      case (f, v) =>
        f :: v :: Nil
    }.toSeq

    key +: fvList
  }
}

case class HMSetEx(key: Buf, fv: Map[Buf, Buf], milliseconds: Long) extends StrictKeyCommand {
  def name: Buf = Command.HMSETEX
  override def body: Seq[Buf] = {
    val fvList: Seq[Buf] = fv.iterator.flatMap {
      case (f, v) =>
        f :: v :: Nil
    }.toSeq

    key +: (Buf.Utf8(milliseconds.toString) +: fvList)
  }
}

case class HMergeEx(key: Buf, fv: Map[Buf, Buf], milliseconds: Long) extends StrictKeyCommand {
  def name: Buf = Command.HMADDEX
  override def body: Seq[Buf] = {
    val fvList: Seq[Buf] = fv.iterator.flatMap {
      case (f, v) =>
        f :: v :: Nil
    }.toSeq

    key +: (Buf.Utf8(milliseconds.toString) +: fvList)
  }
}

case class HScan(key: Buf, cursor: Long, count: Option[JLong] = None, pattern: Option[Buf] = None)
    extends Command {
  def name: Buf = Command.HSCAN
  override def body: Seq[Buf] = {
    val bufs = Seq(key, Buf.Utf8(cursor.toString))

    val withCount = count match {
      case Some(count) => bufs ++ Seq(Command.COUNT, Buf.Utf8(count.toString))
      case None => bufs
    }

    pattern match {
      case Some(pattern) => withCount ++ Seq(Command.MATCH, pattern)
      case None => withCount
    }
  }
}

case class HSet(key: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  def name: Buf = Command.HSET
  override def body: Seq[Buf] = Seq(key, field, value)
}

case class HSetNx(key: Buf, field: Buf, value: Buf) extends StrictKeyCommand {
  def name: Buf = Command.HSETNX
  override def body: Seq[Buf] = Seq(key, field, value)
}

case class HVals(key: Buf) extends StrictKeyCommand {
  def name: Buf = Command.HVALS
  override def body: Seq[Buf] = Seq(key)
}

case class HStrlen(key: Buf, field: Buf) extends StrictKeyCommand {
  def name: Buf = Command.HSTRLEN
  override def body: Seq[Buf] = Seq(key, field)
}
