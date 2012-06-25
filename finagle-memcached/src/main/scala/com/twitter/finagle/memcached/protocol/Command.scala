package com.twitter.finagle.memcached.protocol

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.Time

sealed abstract class Command(val name: String)

abstract class StorageCommand(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer, name: String) extends Command(name)
abstract class NonStorageCommand(name: String)                                                      extends Command(name)
abstract class ArithmeticCommand(key: ChannelBuffer, delta: Long, name: String)                     extends NonStorageCommand(name)
abstract class RetrievalCommand(name: String) extends NonStorageCommand(name) {
  def keys: Seq[ChannelBuffer]
}

case class Set(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)     extends StorageCommand(key, flags, expiry, value, "Set")
case class Add(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)     extends StorageCommand(key, flags, expiry, value, "Add")
case class Replace(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer) extends StorageCommand(key, flags, expiry, value, "Replace")
case class Append(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)  extends StorageCommand(key, flags, expiry, value, "Append")
case class Prepend(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer) extends StorageCommand(key, flags, expiry, value, "Prepend")
case class Cas(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer, casUnique: ChannelBuffer)
  extends StorageCommand(key, flags, expiry, value, "Cas")

case class Get(keys: Seq[ChannelBuffer])                                               extends RetrievalCommand("Get")
case class Gets(keys: Seq[ChannelBuffer])                                              extends RetrievalCommand("Gets")

case class Delete(key: ChannelBuffer)                                                  extends Command("Delete")
case class Incr(key: ChannelBuffer, value: Long)                                       extends ArithmeticCommand(key, value, "Incr")
case class Decr(key: ChannelBuffer, value: Long)                                       extends ArithmeticCommand(key, -value, "Decr")

case class Stats(args: Seq[ChannelBuffer])                                             extends NonStorageCommand("Stats")
case class Quit()                                                                      extends Command("Quit")
