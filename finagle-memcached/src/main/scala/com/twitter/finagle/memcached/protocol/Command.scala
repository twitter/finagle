package com.twitter.finagle.memcached.protocol

import com.twitter.finagle.memcached.util.Bufs
import com.twitter.io.Buf
import com.twitter.util.Time

private object KeyValidation {
  private val MaxKeyLength = 250

  private def tooLong(key: Buf): Boolean = key.length > MaxKeyLength

  /** Return -1 if no invalid bytes */
  private def invalidByteIndex(key: Buf): Int = {
    val bs = Buf.ByteArray.Owned.extract(key)
    var i = 0
    while (i < bs.length) {
      if (Bufs.INVALID_KEY_CHARACTERS.contains(bs(i)))
        return i
      i += 1
    }
    -1
  }

  private val KeyCheck: Buf => Unit =
    key => {
      if (key == null)
        throw new IllegalArgumentException("Invalid keys: key cannot be null")

      if (tooLong(key))
        throw new IllegalArgumentException(
          "Invalid keys: key cannot be longer than %d bytes (%d)".format(MaxKeyLength, key.length))

      val index = invalidByteIndex(key)
      if (index != -1) {
        val ch = Buf.ByteArray.Owned.extract(key)(index)
        throw new IllegalArgumentException(
          "Invalid keys: key cannot have whitespace or control characters: '0x%d'".format(ch))
      }
    }

}

/**
 * This trait contains cache command key validation logic.
 * The validation is done by searching each buffer for invalid character defined in
 * Bufs.INVALID_KEY_CHARACTER, during construction of this trait.
 *
 * All cache commands accepting key/keys should mixin this trait.
 */
trait KeyValidation {
  import KeyValidation._

  def keys: Seq[Buf]

  {
    // Validating keys
    val ks = keys
    if (ks == null)
      throw new IllegalArgumentException("Invalid keys: cannot have null for keys")

    ks.foreach(KeyCheck)
  }

  def badKey(key: Buf): Boolean = {
    if (key == null) true else {
      tooLong(key) || invalidByteIndex(key) != -1
    }
  }
}

sealed abstract class Command(val name: String)

abstract class StorageCommand(
    key: Buf,
    flags: Int,
    expiry: Time,
    value: Buf,
    name: String)
  extends Command(name)
  with KeyValidation {
  def keys: Seq[Buf] = Seq(key)
}

abstract class NonStorageCommand(name: String) extends Command(name)

abstract class ArithmeticCommand(
    key: Buf,
    delta: Long,
    name: String)
  extends NonStorageCommand(name)
  with KeyValidation {
  def keys: Seq[Buf] = Seq(key)
}

abstract class RetrievalCommand(name: String) extends NonStorageCommand(name) with KeyValidation {
  def keys: Seq[Buf]
}

// storage commands
case class Set(key: Buf, flags: Int, expiry: Time, value: Buf)
  extends StorageCommand(key, flags, expiry, value, "Set")
case class Add(key: Buf, flags: Int, expiry: Time, value: Buf)
  extends StorageCommand(key, flags, expiry, value, "Add")
case class Replace(key: Buf, flags: Int, expiry: Time, value: Buf)
  extends StorageCommand(key, flags, expiry, value, "Replace")
case class Append(key: Buf, flags: Int, expiry: Time, value: Buf)
  extends StorageCommand(key, flags, expiry, value, "Append")
case class Prepend(key: Buf, flags: Int, expiry: Time, value: Buf)
  extends StorageCommand(key, flags, expiry, value, "Prepend")
case class Cas(key: Buf, flags: Int, expiry: Time, value: Buf, casUnique: Buf)
  extends StorageCommand(key, flags, expiry, value, "Cas")

// retrieval commands
case class Get(keys: Seq[Buf]) extends RetrievalCommand("Get")
case class Gets(keys: Seq[Buf]) extends RetrievalCommand("Gets")

// arithmetic commands
case class Incr(key: Buf, value: Long) extends ArithmeticCommand(key, value, "Incr")
case class Decr(key: Buf, value: Long) extends ArithmeticCommand(key, -value, "Decr")

// other commands
case class Delete(key: Buf) extends Command("Delete") with KeyValidation {
  def keys: Seq[Buf] = Seq(key)
}
case class Stats(args: Seq[Buf]) extends NonStorageCommand("Stats")
case class Quit() extends Command("Quit")

// twemcache specific commands
case class Upsert(key: Buf, flags: Int, expiry: Time, value: Buf, version: Buf)
    extends StorageCommand(key, flags, expiry, value, "Upsert")
case class Getv(keys: Seq[Buf]) extends RetrievalCommand("Getv")
