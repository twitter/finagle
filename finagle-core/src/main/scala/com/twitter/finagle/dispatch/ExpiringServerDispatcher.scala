package com.twitter.finagle.dispatch

import com.twitter.util.{Duration, Promise, Timer}

import com.twitter.finagle.service.ExpiringService
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.TransportFactory

class ExpiringServerDispatcher[Req, Rep](
  maxIdleTime: Option[Duration],
  maxLifeTime: Option[Duration],
  timer: Timer,
  stats: StatsReceiver)
  extends ServerDispatcherFactoryTransformer[Req, Rep]
{
  def apply(mkDisp: ServerDispatcherFactory[Req, Rep]) = {
    (mkTrans, service) => {
      val pDisp = new Promise[ServerDispatcher]
      val expiringService = new ExpiringService(
        service, maxIdleTime, maxLifeTime, timer, stats,
        { () => pDisp onSuccess { _.drain() } }
      )
      val disp = mkDisp(mkTrans, expiringService)
      pDisp.setValue(disp)
      disp
    }
  }
}
