package com.twitter.finagle.memcached.protocol

import com.twitter.finagle.memcached.KeyValidation
import com.twitter.io.Buf
import com.twitter.util.Time
import scala.collection.immutable

sealed abstract class Command(val name: String)

private[memcached] object StorageCommand {
  val StorageCommands = immutable.Set[Buf](
    Buf.Utf8("set"),
    Buf.Utf8("add"),
    Buf.Utf8("replace"),
    Buf.Utf8("append"),
    Buf.Utf8("prepend"),
    Buf.Utf8("cas")
  )
}

sealed abstract class StorageCommand(
  val key: Buf,
  flags: Int,
  expiry: Time,
  value: Buf,
  name: String)
    extends Command(name) {
  KeyValidation.checkKey(key)
  ExpiryValidation.checkExpiry(name, expiry)
}

sealed abstract class NonStorageCommand(name: String) extends Command(name)

sealed abstract class ArithmeticCommand(val key: Buf, delta: Long, name: String)
    extends NonStorageCommand(name) {
  KeyValidation.checkKey(key)
}

sealed abstract class RetrievalCommand(name: String) extends NonStorageCommand(name) {
  def keys: Seq[Buf]
  KeyValidation.checkKeys(keys)
}

// storage commands
case class Set(override val key: Buf, flags: Int, expiry: Time, value: Buf)
    extends StorageCommand(key, flags, expiry, value, "Set")
case class Add(override val key: Buf, flags: Int, expiry: Time, value: Buf)
    extends StorageCommand(key, flags, expiry, value, "Add")
case class Replace(override val key: Buf, flags: Int, expiry: Time, value: Buf)
    extends StorageCommand(key, flags, expiry, value, "Replace")
case class Append(override val key: Buf, flags: Int, expiry: Time, value: Buf)
    extends StorageCommand(key, flags, expiry, value, "Append")
case class Prepend(override val key: Buf, flags: Int, expiry: Time, value: Buf)
    extends StorageCommand(key, flags, expiry, value, "Prepend")
case class Cas(override val key: Buf, flags: Int, expiry: Time, value: Buf, casUnique: Buf)
    extends StorageCommand(key, flags, expiry, value, "Cas")

// retrieval commands
case class Get(keys: Seq[Buf]) extends RetrievalCommand("Get")
case class Gets(keys: Seq[Buf]) extends RetrievalCommand("Gets")

// arithmetic commands
case class Incr(override val key: Buf, value: Long) extends ArithmeticCommand(key, value, "Incr")
case class Decr(override val key: Buf, value: Long) extends ArithmeticCommand(key, -value, "Decr")

// other commands
case class Delete(key: Buf) extends Command("Delete") {
  KeyValidation.checkKey(key)
}
case class Stats(args: Seq[Buf]) extends NonStorageCommand("Stats")
case class Quit() extends Command("Quit")

// twemcache specific commands
case class Upsert(override val key: Buf, flags: Int, expiry: Time, value: Buf, version: Buf)
    extends StorageCommand(key, flags, expiry, value, "Upsert")
case class Getv(keys: Seq[Buf]) extends RetrievalCommand("Getv")
