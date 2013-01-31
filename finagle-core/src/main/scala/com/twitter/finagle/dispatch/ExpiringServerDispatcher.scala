package com.twitter.finagle.dispatch

import com.twitter.finagle.Service
import com.twitter.finagle.service.ExpiringService
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Duration, Timer, Time, Closable}

object ExpiringServerDispatcher {
  def apply[Req, Rep](
    maxIdleTime: Option[Duration],
    maxLifeTime: Option[Duration],
    timer: Timer,
    statsReceiver: StatsReceiver,
    newDispatcher: (Transport[Rep, Req], Service[Req, Rep]) => Closable
  ): (Transport[Rep, Req], Service[Req, Rep]) => Closable =
    (transport: Transport[Rep, Req], service: Service[Req, Rep]) =>
        new ExpiringService(service, maxIdleTime, maxLifeTime, timer, statsReceiver) {
          private[this] val dispatcher = newDispatcher(transport, this)
          protected def onExpire() { dispatcher.close(Time.now) }
        }
}
