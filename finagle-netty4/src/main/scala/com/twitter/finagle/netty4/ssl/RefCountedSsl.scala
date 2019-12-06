package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.netty4.Toggles
import com.twitter.finagle.server.ServerInfo
import com.twitter.logging.Logger
import java.util.concurrent.atomic.AtomicBoolean

private[finagle] object RefCountedSsl {

  val Id: String = "com.twitter.finagle.netty4.ssl.UseRefCountedSsl"

  private[this] val toggle = Toggles(Id)
  private[this] val loggingOnce = new AtomicBoolean()

  @volatile
  private[this] var programmaticValue: Option[Boolean] = None

  def Enabled: Boolean = {
    val isEnabled = programmaticValue match {
      case Some(value) => value
      case None => toggle(ServerInfo().id.hashCode)
    }

    if (isEnabled && loggingOnce.compareAndSet(false, true)) {
      Logger().info("Enabling Ref-Counted SSL Implementation")
    }

    isEnabled
  }

  private[netty4] def setRefCounting(enabled: Option[Boolean]): Unit = {
    programmaticValue = enabled
  }
}
