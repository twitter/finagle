package com.twitter.finagle.memcached.protocol

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.Time

sealed abstract class Command

abstract class StorageCommand(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer) extends Command
abstract class NonStorageCommand                                                      extends Command
abstract class ArithmeticCommand(key: ChannelBuffer, delta: Long)                     extends NonStorageCommand
abstract class RetrievalCommand extends NonStorageCommand {
  def keys: Seq[ChannelBuffer]
}

case class Set(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)     extends StorageCommand(key, flags, expiry, value)
case class Add(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)     extends StorageCommand(key, flags, expiry, value)
case class Replace(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer) extends StorageCommand(key, flags, expiry, value)
case class Append(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer)  extends StorageCommand(key, flags, expiry, value)
case class Prepend(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer) extends StorageCommand(key, flags, expiry, value)
case class Cas(key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer, casUnique: ChannelBuffer)
  extends StorageCommand(key, flags, expiry, value)

case class Get(keys: Seq[ChannelBuffer])                                               extends RetrievalCommand
case class Gets(keys: Seq[ChannelBuffer])                                              extends RetrievalCommand

case class Delete(key: ChannelBuffer)                                                  extends Command
case class Incr(key: ChannelBuffer, value: Long)                                       extends ArithmeticCommand(key, value)
case class Decr(key: ChannelBuffer, value: Long)                                       extends ArithmeticCommand(key, -value)

case class Stats(args: Seq[ChannelBuffer])                                             extends NonStorageCommand
case class Quit()                                                                      extends Command
