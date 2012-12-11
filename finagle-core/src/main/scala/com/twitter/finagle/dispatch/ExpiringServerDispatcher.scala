package com.twitter.finagle.dispatch

import com.twitter.finagle.Service
import com.twitter.finagle.service.ExpiringService
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.{TransportFactory, Transport}
import com.twitter.util.{Duration, Promise, Timer}

object ExpiringServerDispatcher {
  def apply[Req, Rep](
    maxIdleTime: Option[Duration],
    maxLifeTime: Option[Duration],
    timer: Timer,
    statsReceiver: StatsReceiver,
    newDispatcher: (TransportFactory, Service[Req, Rep]) => ServerDispatcher
  ): (TransportFactory, Service[Req, Rep]) => ServerDispatcher =
    (newTransport: TransportFactory, service: Service[Req, Rep]) =>
        new ExpiringService(service, maxIdleTime, maxLifeTime, timer, statsReceiver) with ServerDispatcher {
          private[this] val dispatcher = newDispatcher(newTransport, this)
          def onExpire() { drain() }
          def drain() { dispatcher.drain() }
        }
}
