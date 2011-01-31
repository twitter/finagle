package com.twitter.finagle.kestrel.protocol

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util.{Time, Duration}

sealed abstract class Command

case class Get(queueName: ChannelBuffer, options: collection.Set[GetOption])                   extends Command
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

sealed abstract class GetOption

case class Timeout(duration: Duration) extends GetOption
case class Open()                      extends GetOption
case class Close()                     extends GetOption
case class Abort()                     extends GetOption
case class Peek()                      extends GetOption