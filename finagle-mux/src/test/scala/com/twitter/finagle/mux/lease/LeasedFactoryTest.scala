package com.twitter.finagle.mux.lease

import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LeasedFactoryTest extends FunSuite {
  test("LeasedFactory should handle leases properly") {
    var _leased = true
    def mk(): Future[Service[Unit, Unit] with Acting] = Future.value(
      new Service[Unit, Unit] with Acting {
        def apply(a: Unit) = Future.Done
        def isActive = _leased
      }
    )
    val factory = new LeasedFactory(mk)
    assert(factory.isAvailable) // trivial case
    val svc = Await.result(factory())
    assert(factory.isAvailable) // one, lease
    val svc2 = Await.result(factory())
    assert(factory.isAvailable) // two, lease

    _leased = false
    assert(!factory.isAvailable) // two, no lease
    Await.result(svc2.close())
    assert(!factory.isAvailable) // one, no lease
    Await.result(svc.close())
    assert(factory.isAvailable) // trivial case
  }
}
