package com.twitter.finagle.kestrel.protocol

import com.twitter.io.Buf
import com.twitter.util.{Time, Duration}

sealed abstract class Command(val name: String)

sealed abstract class GetCommand(name: String) extends Command(name) {
  val queueName: Buf
  val timeout: Option[Duration]
}

case class Get(val queueName: Buf, val timeout: Option[Duration] = None)             extends GetCommand("Get")
case class Open(val queueName: Buf, val timeout: Option[Duration] = None)            extends GetCommand("Open")
case class Close(val queueName: Buf, val timeout: Option[Duration] = None)           extends GetCommand("Close")
case class CloseAndOpen(val queueName: Buf, val timeout: Option[Duration] = None)    extends GetCommand("CloseAndOpen")
case class Abort(val queueName: Buf, val timeout: Option[Duration] = None)           extends GetCommand("Abort")
case class Peek(val queueName: Buf, val timeout: Option[Duration] = None)            extends GetCommand("Peek")
case class Set(queueName: Buf, expiry: Time, value: Buf)                             extends Command("Set")

case class Delete(queueName: Buf)                                                    extends Command("Delete")
case class Flush(queueName: Buf)                                                     extends Command("Flush")
case class FlushAll()                                                                extends Command("FlushAll")

case class Version()                                                                 extends Command("Version")
case class ShutDown()                                                                extends Command("ShutDown")
case class Reload()                                                                  extends Command("Reload")
case class DumpConfig()                                                              extends Command("DumpConfig")
case class Stats()                                                                   extends Command("Stats")
case class DumpStats()                                                               extends Command("DumpStats")

