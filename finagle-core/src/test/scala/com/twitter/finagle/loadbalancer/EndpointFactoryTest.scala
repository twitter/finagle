package com.twitter.finagle.loadbalancer

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.util.{Await, Future, Time}
import java.net.InetSocketAddress
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class EndpointFactoryTest extends AnyFunSuite with OneInstancePerTest {
  private[this] val address = Address(InetSocketAddress.createUnresolved("nop", 0))

  private[this] var makeCount = 0
  private[this] val make = () =>
    new ServiceFactory[Int, Int] {
      makeCount += 1
      var closed = false

      def apply(conn: ClientConnection) =
        Future.value(new Service[Int, Int] {
          def apply(req: Int) = Future.value(req)
          override def status = if (closed) Status.Closed else Status.Open
        })

      def close(when: Time) = {
        closed = true
        Future.Done
      }

      override def status = if (closed) Status.Closed else Status.Open
    }

  private[this] val ef = new LazyEndpointFactory(make, address)

  test("caches the result of mk") {
    assert(makeCount == 0)
    for (_ <- 0 to 100) { Await.result(ef(), 1.second) }
    assert(makeCount == 1)
  }

  test("remake breaks the cache") {
    assert(makeCount == 0)
    val svc = Await.result(ef(), 1.second)
    assert(makeCount == 1)
    assert(svc.status == Status.Open)

    ef.remake()
    assert(svc.status == Status.Closed)

    for (_ <- 0 to 100) { Await.result(ef(), 1.second) }
    assert(makeCount == 2)
  }

  test("close is terminal") {
    Await.result(ef.close(), 1.second)
    intercept[ServiceClosedException] { Await.result(ef(), 1.second) }
  }

  test("handles when mk throws") {
    val exc = new Exception("boom")
    val failingMk: () => ServiceFactory[Int, Int] = () => throw exc
    val failingEf = new LazyEndpointFactory(failingMk, address)
    assert(exc eq intercept[Exception] { Await.result(failingEf(), 1.second) })
  }

  test("handles when mk throws fatal exceptions") {
    var exc: Throwable = new InterruptedException
    val failingMk: () => ServiceFactory[Int, Int] = () => throw exc
    val failingEf = new LazyEndpointFactory(failingMk, address)
    assert(exc eq intercept[InterruptedException] { failingEf() })

    // Now we try again with a non-fatal exception and we should get a result
    exc = new Exception("boom")
    assert(exc eq intercept[Exception] { Await.result(failingEf(), 1.second) })
  }
}
