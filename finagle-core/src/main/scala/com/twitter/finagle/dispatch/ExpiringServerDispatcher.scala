package com.twitter.finagle.dispatch

import com.twitter.finagle.Service
import com.twitter.finagle.service.ExpiringService
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Duration, Timer, Time, Closable}

object ExpiringServerDispatcher {
  def apply[Req, Rep, In, Out](
    maxIdleTime: Option[Duration],
    maxLifeTime: Option[Duration],
    timer: Timer,
    statsReceiver: StatsReceiver,
    newDispatcher: (Transport[In, Out], Service[Req, Rep]) => Closable
  ): (Transport[In, Out], Service[Req, Rep]) => Closable =
    (transport: Transport[In, Out], service: Service[Req, Rep]) =>
        new ExpiringService(service, maxIdleTime, maxLifeTime, timer, statsReceiver) {
          private[this] val dispatcher = newDispatcher(transport, this)
          protected def onExpire() { dispatcher.close(Time.now) }
        }
}
