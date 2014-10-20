package com.twitter.finagle.serverset2.client

import java.util.concurrent.LinkedBlockingDeque
import com.twitter.util.{Monitor, Updatable}

private[client] object EventDeliveryThread
    extends Thread("com.twitter.zookeeper.client.internal event delivery") {
  private val q = new LinkedBlockingDeque[(Updatable[WatchState], WatchState)]

  def offer(u: Updatable[WatchState], s: WatchState) {
    q.offer((u, s))
  }

  override def run() {
    while (true) {
      val (u, s) = q.take()
      try {
        u() = s
      } catch  {
        case exc: Throwable => Monitor.handle(exc)
      }
    }
  }

  setDaemon(true)
  start()
}
