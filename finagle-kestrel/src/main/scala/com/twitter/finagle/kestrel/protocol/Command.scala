package com.twitter.finagle.kestrel.protocol

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.{Time, Duration}

sealed abstract class Command

sealed abstract class GetCommand extends Command {
  val queueName: ChannelBuffer
  val timeout: Option[Duration]
}

case class Get(val queueName: ChannelBuffer, val timeout: Option[Duration] = None)             extends GetCommand
case class Open(val queueName: ChannelBuffer, val timeout: Option[Duration] = None)            extends GetCommand
case class Close(val queueName: ChannelBuffer, val timeout: Option[Duration] = None)           extends GetCommand
case class CloseAndOpen(val queueName: ChannelBuffer, val timeout: Option[Duration] = None)    extends GetCommand
case class Abort(val queueName: ChannelBuffer, val timeout: Option[Duration] = None)           extends GetCommand
case class Peek(val queueName: ChannelBuffer, val timeout: Option[Duration] = None)            extends GetCommand
case class Set(queueName: ChannelBuffer, expiry: Time, value: ChannelBuffer)                   extends Command

case class Delete(queueName: ChannelBuffer)                                                    extends Command
case class Flush(queueName: ChannelBuffer)                                                     extends Command
case class FlushAll()                                                                          extends Command

case class Version()                                                                           extends Command
case class ShutDown()                                                                          extends Command
case class Reload()                                                                            extends Command
case class DumpConfig()                                                                        extends Command
case class Stats()                                                                             extends Command
case class DumpStats()                                                                         extends Command

