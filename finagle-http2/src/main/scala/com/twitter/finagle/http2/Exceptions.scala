package com.twitter.finagle.http2

import com.twitter.finagle.FailureFlags
import com.twitter.logging.{HasLogLevel, Level}
import java.net.SocketAddress

private[http2] class DeadConnectionException(addr: SocketAddress, val flags: Long)
    extends Exception(s"assigned an already dead connection to address $addr")
    with FailureFlags[DeadConnectionException]
    with HasLogLevel {

  protected def copyWithFlags(newFlags: Long): DeadConnectionException =
    new DeadConnectionException(addr, newFlags)

  def logLevel: Level = Level.DEBUG
}
