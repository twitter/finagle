package com.twitter.finagle.loadbalancer.aperture

import com.twitter.util.{Future, Promise, Return}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite

class ApertureEagerConnectionsTest extends AnyFunSuite with Eventually {

  class SampleConnection extends Function0[Future[Unit]] {
    val p = Promise[Unit]()
    def apply(): Future[Unit] = p

    def satisfy() = {
      p.updateIfEmpty(Return(()))
    }
  }

  test("limits the maximum number of concurrent connections") {
    val endpoints = 0
      .until(ApertureEagerConnections.MaxConcurrentConnections + 2)
      .map(i => new SampleConnection)

    endpoints.foreach { endpoint =>
      ApertureEagerConnections.submit(endpoint())
    }
    assert(ApertureEagerConnections.semaphore.numWaiters == 2)

    endpoints(0).satisfy()
    eventually {
      assert(ApertureEagerConnections.semaphore.numWaiters == 1)
    }

    // satisfy all the pending promises to cleanup
    endpoints.map(_.satisfy())
    eventually {
      assert(ApertureEagerConnections.semaphore.numWaiters == 0)
      assert(
        ApertureEagerConnections.semaphore.numPermitsAvailable == ApertureEagerConnections.MaxConcurrentConnections)
    }
  }
}
