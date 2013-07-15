package com.twitter.finagle.memcached.protocol

import com.twitter.finagle.memcached.util.ChannelBufferUtils
import com.twitter.util.Time
import org.jboss.netty.buffer.ChannelBuffer

/**
 * This trait contains cache command key validation logic.
 * The validation is done by searching each key channel buffer for invalid character defined in
 * ChannelBufferUtils.FIND_INVALID_KEY_CHARACTER index finder, during construction of this trait.
 *
 * All cache commands accepting key/keys should mixin this trait.
 */
trait KeyValidation {
  private val MAXKEYLENGTH = 250
  def keys: Seq[ChannelBuffer]

  {
    // Validating keys
    if (keys==null || keys.exists(badKey(_)))
      throw new IllegalArgumentException(
        "Invalid keys: key cannot be empty, having whitespace or control characters" +
            ", or longer than " + MAXKEYLENGTH + " bytes.")
  }

  def badKey(key: ChannelBuffer): Boolean = {
    if(key == null) true else {
      key.readableBytes > MAXKEYLENGTH ||
          key.indexOf(key.readerIndex(), key.writerIndex(), ChannelBufferUtils.FIND_INVALID_KEY_CHARACTER) != -1
    }
  }
}

sealed abstract class Command(val name: String)

abstract class StorageCommand(
    key: ChannelBuffer,
    flags: Int,
    expiry: Time,
    value: ChannelBuffer,
    name: String)
  extends Command(name)
  with KeyValidation {
  def keys: Seq[ChannelBuffer] = Seq(key)
}

abstract class NonStorageCommand(name: String) extends Command(name)

abstract class ArithmeticCommand(
    key: ChannelBuffer,
    delta: Long,
    name: String)
  extends NonStorageCommand(name)
  with KeyValidation {
  def keys: Seq[ChannelBuffer] = Seq(key)
}

abstract class RetrievalCommand(name: String) extends NonStorageCommand(name) with KeyValidation {
  def keys: Seq[ChannelBuffer]
}

// storage commands
case class Set(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)
  extends StorageCommand(key, flags, expiry, value, "Set")
case class Add(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)
  extends StorageCommand(key, flags, expiry, value, "Add")
case class Replace(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)
  extends StorageCommand(key, flags, expiry, value, "Replace")
case class Append(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)
  extends StorageCommand(key, flags, expiry, value, "Append")
case class Prepend(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)
  extends StorageCommand(key, flags, expiry, value, "Prepend")
case class Cas(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer, casUnique: ChannelBuffer)
  extends StorageCommand(key, flags, expiry, value, "Cas")

// retrieval commands
case class Get(keys: Seq[ChannelBuffer]) extends RetrievalCommand("Get")
case class Gets(keys: Seq[ChannelBuffer]) extends RetrievalCommand("Gets")

// arithmetic commands
case class Incr(key: ChannelBuffer, value: Long) extends ArithmeticCommand(key, value, "Incr")
case class Decr(key: ChannelBuffer, value: Long) extends ArithmeticCommand(key, -value, "Decr")

// other commands
case class Delete(key: ChannelBuffer) extends Command("Delete") with KeyValidation {
  def keys: Seq[ChannelBuffer] = Seq(key)
}
case class Stats(args: Seq[ChannelBuffer]) extends NonStorageCommand("Stats")
case class Quit() extends Command("Quit")

// twemcache specific commands
case class Upsert(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer, version: ChannelBuffer)
    extends StorageCommand(key, flags, expiry, value, "Upsert")
case class Getv(keys: Seq[ChannelBuffer]) extends RetrievalCommand("Getv")
